//! Keeps Postgres on the primary in line with the replication mode, and runs
//! the `sync` fallback. The leader calls this on every health-check tick, so a
//! mode change, a membership change and a new primary all take the same path.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tracing::{error, info, warn};

use super::health_check::HealthCheckResult;
use super::NodeMonitor;
use crate::raft::commands::TopologyCommand;
use crate::raft::topology::{ClusterTopology, ReplicationMode};

// ── DegradeTimer ──────────────────────────────────────────────────────────────

/// What the `sync` fallback should do on this tick.
#[derive(Debug, PartialEq, Eq)]
pub enum Fallback {
    Stay,
    /// No standby for `sync_degrade_timeout_seconds`: fall back to async.
    Degrade,
    /// A standby is back: require synchronous commit again.
    Restore,
}

/// Times how long `sync` has had no standby streaming.
#[derive(Default)]
pub struct DegradeTimer {
    no_standby_since: Option<Instant>,
}

impl DegradeTimer {
    pub fn step(
        &mut self,
        topology: &ClusterTopology,
        standby_streaming: bool,
        timeout: Duration,
        now: Instant,
    ) -> Fallback {
        if topology.replication_mode != ReplicationMode::Sync || standby_streaming {
            self.no_standby_since = None;
            let restore = topology.replication_mode == ReplicationMode::Sync
                && topology.sync_degraded
                && standby_streaming;
            return if restore {
                Fallback::Restore
            } else {
                Fallback::Stay
            };
        }
        if topology.sync_degraded {
            return Fallback::Stay;
        }
        let since = *self.no_standby_since.get_or_insert(now);
        if now.duration_since(since) >= timeout {
            self.no_standby_since = None;
            Fallback::Degrade
        } else {
            Fallback::Stay
        }
    }
}

// ── StalePrimaries ────────────────────────────────────────────────────────────

/// Decides when a node that acts as primary, but is not the cluster's, should
/// be rewound. A rewind discards its diverged commits, so this waits for
/// repeated sightings: a node promoted a moment ago also looks like this until
/// the topology catches up.
#[derive(Default)]
pub struct StalePrimaries {
    sightings: HashMap<String, u32>,
    last_attempt: HashMap<String, Instant>,
}

impl StalePrimaries {
    /// Record one sighting. Returns `true` when it is time to act on the node.
    pub fn sighted(
        &mut self,
        node_id: &str,
        needed: u32,
        cooldown: Duration,
        now: Instant,
    ) -> bool {
        let sightings = self.sightings.entry(node_id.to_string()).or_insert(0);
        *sightings += 1;
        if *sightings < needed {
            return false;
        }
        let cooled = self
            .last_attempt
            .get(node_id)
            .is_none_or(|last| now.duration_since(*last) >= cooldown);
        if cooled {
            self.last_attempt.insert(node_id.to_string(), now);
        }
        cooled
    }

    /// The node no longer looks like a stale primary.
    pub fn cleared(&mut self, node_id: &str) {
        self.sightings.remove(node_id);
    }
}

// ── Reconcile ─────────────────────────────────────────────────────────────────

/// Node IDs from slots that lost WAL, for slots named `<prefix><node_id>`.
fn lost_nodes<'a>(slots: &'a [String], prefix: &str) -> Vec<&'a str> {
    slots
        .iter()
        .filter_map(|slot| slot.strip_prefix(prefix))
        .collect()
}

impl NodeMonitor {
    /// Bring the primary, reported by `primary`, in line with `topology`.
    pub(super) async fn reconcile_replication(
        &self,
        topology: &ClusterTopology,
        primary: &HealthCheckResult,
        timer: &mut DegradeTimer,
    ) {
        let standby_streaming = primary
            .streaming_standbys
            .iter()
            .any(|name| topology.node_configs.contains_key(name));
        let timeout = Duration::from_secs(self.sync_degrade_timeout_seconds);

        let mut standby_names = topology.synchronous_standby_names();
        let fallback = timer.step(topology, standby_streaming, timeout, Instant::now());
        match fallback {
            Fallback::Stay => {}
            Fallback::Degrade => {
                // Record this first: once the setting clears, commits stop
                // waiting, and a failover must know no standby is current.
                let cmd = TopologyCommand::SetSyncDegraded { degraded: true };
                if let Err(e) = self.raft.raft.client_write(cmd).await {
                    error!(err = %e, "could not record the sync fallback; commits stay stalled");
                    return;
                }
                warn!("no standby available: sync falls back to async");
                standby_names = String::new();
            }
            Fallback::Restore => standby_names = topology.standby_names(),
        }

        let mut desired = HashMap::new();
        desired.insert("synchronous_standby_names".to_string(), standby_names);
        desired.insert(
            "max_slot_wal_keep_size".to_string(),
            self.max_slot_wal_keep_size_mb.to_string(),
        );
        desired.retain(|name, value| primary.parameters.get(name) != Some(value));

        if !desired.is_empty() && !self.apply_parameters(topology, desired).await {
            return;
        }

        // Clear the flag only once commits wait for a standby again.
        if fallback == Fallback::Restore {
            let cmd = TopologyCommand::SetSyncDegraded { degraded: false };
            match self.raft.raft.client_write(cmd).await {
                Ok(_) => info!("standby available: sync restored"),
                Err(e) => warn!(err = %e, "could not record that sync recovered"),
            }
        }

        self.track_reseeds(topology, primary).await;
    }

    async fn apply_parameters(
        &self,
        topology: &ClusterTopology,
        parameters: HashMap<String, String>,
    ) -> bool {
        let primary_id = &topology.primary_node_id;
        let Some(cfg) = topology.node_configs.get(primary_id) else {
            return false;
        };
        let result = match self.pool.get_or_connect(primary_id, &cfg.agent_addr).await {
            Ok(mut client) => client.set_parameters(parameters.clone()).await,
            Err(e) => Err(e),
        };
        match result {
            Ok(response) if response.success => {
                info!(?parameters, "applied replication settings to the primary");
                true
            }
            Ok(response) => {
                warn!(err = %response.error, "primary refused replication settings");
                false
            }
            Err(e) => {
                warn!(err = %e, "could not apply replication settings to the primary");
                false
            }
        }
    }

    /// Flag standbys whose slot lost WAL, and clear the flag once they stream.
    async fn track_reseeds(&self, topology: &ClusterTopology, primary: &HealthCheckResult) {
        let lost = lost_nodes(&primary.lost_slots, &self.slot_prefix);
        for node_id in &lost {
            if topology.node_configs.contains_key(*node_id)
                && !topology.needs_reseed.contains(*node_id)
            {
                error!(
                    node_id,
                    "standby is past max_slot_wal_keep_size: re-seed it"
                );
                self.write_needs_reseed(node_id, true).await;
            }
        }
        for node_id in &topology.needs_reseed {
            if !lost.contains(&node_id.as_str()) && primary.streaming_standbys.contains(node_id) {
                info!(node_id, "standby streams again");
                self.write_needs_reseed(node_id, false).await;
            }
        }
    }

    async fn write_needs_reseed(&self, node_id: &str, needs_reseed: bool) {
        let cmd = TopologyCommand::SetNeedsReseed {
            node_id: node_id.to_string(),
            needs_reseed,
        };
        if let Err(e) = self.raft.raft.client_write(cmd).await {
            warn!(node_id, err = %e, "could not record the re-seed flag");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(30);

    fn sync(degraded: bool) -> ClusterTopology {
        ClusterTopology {
            replication_mode: ReplicationMode::Sync,
            sync_degraded: degraded,
            ..Default::default()
        }
    }

    #[test]
    fn sync_degrades_only_after_the_timeout() {
        let mut timer = DegradeTimer::default();
        let start = Instant::now();
        let t = sync(false);
        assert_eq!(timer.step(&t, false, TIMEOUT, start), Fallback::Stay);
        let before = start + TIMEOUT - Duration::from_secs(1);
        assert_eq!(timer.step(&t, false, TIMEOUT, before), Fallback::Stay);
        assert_eq!(
            timer.step(&t, false, TIMEOUT, start + TIMEOUT),
            Fallback::Degrade
        );
    }

    #[test]
    fn a_returning_standby_resets_the_timer() {
        let mut timer = DegradeTimer::default();
        let start = Instant::now();
        let t = sync(false);
        timer.step(&t, false, TIMEOUT, start);
        timer.step(&t, true, TIMEOUT, start + TIMEOUT - Duration::from_secs(1));
        assert_eq!(
            timer.step(&t, false, TIMEOUT, start + TIMEOUT),
            Fallback::Stay
        );
    }

    #[test]
    fn degraded_sync_restores_when_a_standby_streams() {
        let mut timer = DegradeTimer::default();
        let now = Instant::now();
        assert_eq!(timer.step(&sync(true), false, TIMEOUT, now), Fallback::Stay);
        assert_eq!(
            timer.step(&sync(true), true, TIMEOUT, now),
            Fallback::Restore
        );
    }

    #[test]
    fn other_modes_never_degrade() {
        for mode in [ReplicationMode::Async, ReplicationMode::SyncStrict] {
            let t = ClusterTopology {
                replication_mode: mode,
                ..Default::default()
            };
            let mut timer = DegradeTimer::default();
            let start = Instant::now();
            timer.step(&t, false, TIMEOUT, start);
            assert_eq!(
                timer.step(&t, false, TIMEOUT, start + TIMEOUT * 2),
                Fallback::Stay
            );
        }
    }

    const COOLDOWN: Duration = Duration::from_secs(60);

    #[test]
    fn a_stale_primary_needs_repeated_sightings() {
        let mut stale = StalePrimaries::default();
        let now = Instant::now();
        assert!(!stale.sighted("pg1", 3, COOLDOWN, now));
        assert!(!stale.sighted("pg1", 3, COOLDOWN, now));
        assert!(stale.sighted("pg1", 3, COOLDOWN, now));
    }

    #[test]
    fn a_node_that_stops_looking_stale_starts_over() {
        let mut stale = StalePrimaries::default();
        let now = Instant::now();
        stale.sighted("pg1", 2, COOLDOWN, now);
        stale.cleared("pg1");
        assert!(!stale.sighted("pg1", 2, COOLDOWN, now));
    }

    #[test]
    fn attempts_wait_for_the_cooldown() {
        let mut stale = StalePrimaries::default();
        let now = Instant::now();
        assert!(stale.sighted("pg1", 1, COOLDOWN, now));
        assert!(!stale.sighted("pg1", 1, COOLDOWN, now + COOLDOWN / 2));
        assert!(stale.sighted("pg1", 1, COOLDOWN, now + COOLDOWN));
    }

    #[test]
    fn lost_slots_map_to_nodes_by_prefix() {
        let slots = vec!["pgcluster_pg2".to_string(), "other".to_string()];
        assert_eq!(lost_nodes(&slots, "pgcluster_"), vec!["pg2"]);
    }
}
