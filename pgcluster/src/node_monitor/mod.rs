pub mod failure_state;
pub mod health_check;
pub mod replication;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time;
use tracing::{debug, error, info, warn};

use std::sync::atomic::{AtomicBool, Ordering};

use crate::agent_clients::AgentClientPool;
use crate::config::{FailoverConfig, ReplicationConfig};
use crate::failover::seed::SeedPlan;
use crate::metrics_registry::Metrics;
use crate::proxy::auth::AuthRole;
use crate::raft::commands::TopologyCommand;
use crate::raft::lease::QuorumLease;
use crate::raft::topology::{ClusterTopology, NodeRole};
use crate::raft::{RaftNode, TopologyWatch};
use failure_state::FailureTracker;
use health_check::HealthCheckResult;
use health_check::{check_node, send_heartbeat};
use replication::{DegradeTimer, StalePrimaries};

/// How long to wait before trying again to rejoin an old primary.
const REJOIN_COOLDOWN: Duration = Duration::from_secs(60);

// ── NodeMonitor ───────────────────────────────────────────────────────────────

/// Polls every vk-agent on the Raft leader and triggers failover when the
/// primary misses enough consecutive health checks.
pub struct NodeMonitor {
    raft: Arc<RaftNode>,
    topology_rx: TopologyWatch,
    pool: Arc<AgentClientPool>,
    config: FailoverConfig,
    metrics: Arc<Metrics>,
    repl_user: String,
    repl_password: String,
    slot_prefix: String,
    /// Shared with the API handlers: prevents concurrent automatic + manual operations.
    op_in_progress: Arc<AtomicBool>,
    /// A leader cut off from the majority does not know it has lost leadership
    /// yet, so this instance acts only while it holds the lease.
    quorum: Arc<QuorumLease>,
    sync_degrade_timeout_seconds: u64,
    max_slot_wal_keep_size_mb: u64,
    replication: ReplicationConfig,
    /// The role the proxy reads verifiers with, and a version for its settings.
    auth_role: AuthRole,
    auth_version: String,
    /// The backup tool's settings and their version, when backups are on.
    backup_settings: Option<(HashMap<String, String>, String)>,
    /// How many health-check ticks between orphaned-slot audits on the primary.
    /// Default: 120 ticks × 500 ms interval ≈ once per minute.
    slot_audit_ticks: u64,
}

impl NodeMonitor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        raft: Arc<RaftNode>,
        topology_rx: TopologyWatch,
        pool: Arc<AgentClientPool>,
        config: FailoverConfig,
        metrics: Arc<Metrics>,
        replication: &ReplicationConfig,
        auth_role: &AuthRole,
        backup_settings: Option<HashMap<String, String>>,
        repl_password: String,
        op_in_progress: Arc<AtomicBool>,
        quorum: Arc<QuorumLease>,
    ) -> Self {
        Self {
            raft,
            topology_rx,
            pool,
            config,
            metrics,
            repl_user: replication.replication_user.clone(),
            repl_password,
            slot_prefix: replication.slot_prefix.clone(),
            op_in_progress,
            quorum,
            sync_degrade_timeout_seconds: replication.sync_degrade_timeout_seconds,
            max_slot_wal_keep_size_mb: replication.max_slot_wal_keep_size_mb,
            replication: replication.clone(),
            auth_role: auth_role.clone(),
            auth_version: crate::backup::walg::version(&HashMap::from([(
                auth_role.user.clone(),
                auth_role.password.clone(),
            )])),
            backup_settings: backup_settings.map(|settings| {
                let version = crate::backup::walg::version(&settings);
                (settings, version)
            }),
            slot_audit_ticks: 120,
        }
    }

    /// Run the monitor loop.  Spawn this as a background task.
    ///
    /// The loop:
    /// 1. Skip if this pgcluster instance is not the Raft leader.
    /// 2. Poll every agent in the cluster.
    /// 3. Propose `UpdateFlushLsn` for nodes that answer OK.
    /// 4. If the primary misses `failures_before_failover` consecutive checks,
    ///    mark it offline and call `failover::trigger_failover`.
    pub async fn run(&mut self) {
        let mut tracker = FailureTracker::default();
        let mut degrade_timer = DegradeTimer::default();
        let mut stale_primaries = StalePrimaries::default();
        let mut unseeded = StalePrimaries::default();
        let interval = Duration::from_millis(self.config.health_check_interval_ms);
        let mut tick = time::interval(interval);
        let mut tick_count: u64 = 0;

        info!("node monitor started");
        self.spawn_heartbeats(interval);

        loop {
            tick.tick().await;
            tick_count += 1;

            // Only the Raft leader should drive health checks.
            let leader_opt = self.raft.raft.current_leader().await;
            let Some(leader_id) = leader_opt else {
                debug!("no Raft leader elected yet, skipping health check");
                continue;
            };

            let my_id = self.raft.raft.metrics().borrow().id;
            let is_leader = leader_id == my_id;
            self.metrics.raft_leader.set(if is_leader { 1 } else { 0 });
            if !is_leader {
                continue;
            }
            if !self.quorum.is_held() {
                debug!("Raft leader without quorum, skipping health check");
                continue;
            }

            let topology = self.topology_rx.current();
            self.poll_all_nodes(
                &topology,
                &mut tracker,
                &mut degrade_timer,
                &mut stale_primaries,
                &mut unseeded,
            )
            .await;

            // Update topology_version metric
            self.metrics.topology_version.set(topology.version as i64);

            // Periodically audit the primary for orphaned WAL replication slots.
            if tick_count.is_multiple_of(self.slot_audit_ticks)
                && !topology.primary_node_id.is_empty()
            {
                self.audit_slots(&topology).await;
            }
        }
    }

    /// Heartbeat every agent while this instance is the Raft leader with quorum.
    /// A stale leader must go quiet, or its isolated agent would never fence.
    ///
    /// Runs apart from the health checks, because a status call that hangs on
    /// one busy Postgres must not starve the others and fence them.
    fn spawn_heartbeats(&self, interval: Duration) {
        let raft = self.raft.clone();
        let topology_rx = self.topology_rx.clone();
        let pool = self.pool.clone();
        let quorum = self.quorum.clone();
        tokio::spawn(async move {
            let mut tick = time::interval(interval);
            loop {
                tick.tick().await;
                let metrics = raft.raft.metrics().borrow().clone();
                if metrics.current_leader != Some(metrics.id) || !quorum.is_held() {
                    continue;
                }
                for (node_id, cfg) in &topology_rx.current().node_configs {
                    let node_id = node_id.clone();
                    let addr = cfg.agent_addr.clone();
                    let pool = pool.clone();
                    tokio::spawn(async move { send_heartbeat(&node_id, &addr, &pool).await });
                }
            }
        });
    }

    /// A node accepting writes that the cluster does not count as its primary.
    /// Reads the latest topology, and holds back during a failover or
    /// switchover, when a new primary can run ahead of the topology.
    fn is_stale_primary(&self, node_id: &str, result: &HealthCheckResult) -> bool {
        let primary = self.topology_rx.current().primary_node_id.clone();
        !result.in_recovery
            && !primary.is_empty()
            && primary != node_id
            && !self.op_in_progress.load(Ordering::SeqCst)
    }

    /// Bring a node's backup settings up to date. Every node gets them, so that
    /// archiving works the moment one is promoted.
    async fn push_backup_settings(
        &self,
        node_id: &str,
        agent_addr: &str,
        result: &HealthCheckResult,
    ) {
        let Some((settings, version)) = &self.backup_settings else {
            return;
        };
        if result.backup_config_version == *version {
            return;
        }
        let pushed = match self.pool.get_or_connect(node_id, agent_addr).await {
            Ok(mut client) => {
                client
                    .configure_backups(settings.clone(), version.clone())
                    .await
            }
            Err(e) => Err(e),
        };
        match pushed {
            Ok(response) if response.success => {
                info!(node_id, "backup settings applied");
            }
            Ok(response) => warn!(node_id, err = %response.error, "backup settings refused"),
            Err(e) => warn!(node_id, err = %e, "could not apply backup settings"),
        }
    }

    /// Seed an empty node in the background, which can take hours.
    fn spawn_seed(&self, node_id: &str) {
        let topology = self.topology_rx.current();
        let plan = match SeedPlan::new(&topology, node_id, &self.replication) {
            Ok(plan) => plan,
            Err(e) => {
                warn!(node_id, err = %e, "cannot seed the node");
                return;
            }
        };
        info!(node_id, "seeding an empty node from the primary");
        let node_id = node_id.to_string();
        let pool = self.pool.clone();
        tokio::spawn(async move {
            match plan.run(&pool, false).await {
                Ok(()) => info!(node_id, "node seeded from the primary"),
                Err(e) => error!(node_id, err = %e, "seeding failed"),
            }
        });
    }

    /// Rejoin a diverged old primary in the background: a rewind is quick, but
    /// the re-seed it can fall back to is not.
    fn spawn_rejoin(&self, node_id: &str) {
        let topology = self.topology_rx.current();
        let plan = match SeedPlan::new(&topology, node_id, &self.replication) {
            Ok(plan) => plan,
            Err(e) => {
                warn!(node_id, err = %e, "cannot rejoin the old primary");
                return;
            }
        };
        warn!(node_id, "old primary diverged: rejoining it as a standby");
        let node_id = node_id.to_string();
        let pool = self.pool.clone();
        tokio::spawn(async move {
            match plan.rejoin(&pool).await {
                Ok(()) => info!(node_id, "old primary rejoined as a standby"),
                Err(e) => error!(node_id, err = %e, "old primary could not rejoin"),
            }
        });
    }

    async fn audit_slots(&self, topology: &ClusterTopology) {
        let primary_cfg = match topology.node_configs.get(&topology.primary_node_id) {
            Some(c) => c,
            None => return,
        };
        let primary_url = format!(
            "postgres://{}:{}@{}/postgres",
            self.repl_user, self.repl_password, primary_cfg.postgres_addr
        );
        // Slots more than 1 GiB behind are considered orphaned.
        const ORPHAN_LAG_THRESHOLD: u64 = 1024 * 1024 * 1024;
        match crate::failover::slots::drop_orphaned_slots(
            &primary_url,
            &self.slot_prefix,
            ORPHAN_LAG_THRESHOLD,
        )
        .await
        {
            Ok(dropped) if !dropped.is_empty() => {
                info!(
                    primary = topology.primary_node_id,
                    count = dropped.len(),
                    "dropped orphaned replication slots"
                );
            }
            Ok(_) => {} // nothing to drop
            Err(e) => {
                warn!(
                    primary = topology.primary_node_id,
                    err = %e,
                    "slot audit failed"
                );
            }
        }
    }

    async fn poll_all_nodes(
        &self,
        topology: &ClusterTopology,
        tracker: &mut FailureTracker,
        degrade_timer: &mut DegradeTimer,
        stale_primaries: &mut StalePrimaries,
        unseeded: &mut StalePrimaries,
    ) {
        let primary_lsn = topology
            .last_flush_lsns
            .get(&topology.primary_node_id)
            .copied()
            .unwrap_or(0);

        // Fan out all health-check gRPC calls in parallel so that failover
        // detection latency is O(max_single_timeout) instead of O(N × timeout).
        let mut join_set = tokio::task::JoinSet::new();
        for (node_id, cfg) in &topology.node_configs {
            let id = node_id.clone();
            let addr = cfg.agent_addr.clone();
            let pool = self.pool.clone();
            join_set.spawn(async move { check_node(&id, &addr, &pool).await });
        }
        let mut check_results: Vec<health_check::HealthCheckResult> =
            Vec::with_capacity(topology.node_configs.len());
        while let Some(outcome) = join_set.join_next().await {
            match outcome {
                Ok(r) => check_results.push(r),
                Err(e) => tracing::warn!(err = %e, "health-check task panicked"),
            }
        }

        if let Some(primary) = check_results.iter().find(|r| {
            r.node_id == topology.primary_node_id && r.reachable && r.pg_running && !r.in_recovery
        }) {
            self.reconcile_replication(topology, primary, degrade_timer)
                .await;
        }

        // Process results sequentially — tracker updates and Raft writes must not race.
        for result in check_results {
            let node_id = &result.node_id;
            let cfg = match topology.node_configs.get(node_id) {
                Some(c) => c,
                None => continue,
            };

            // An empty node has nothing to lose, so seed it without being asked.
            if result.reachable && !result.has_data && *node_id != topology.primary_node_id {
                let threshold = self.config.health_check_failures_before_failover;
                if unseeded.sighted(node_id, threshold, REJOIN_COOLDOWN, Instant::now()) {
                    self.spawn_seed(node_id);
                }
                continue;
            }

            if result.reachable && result.pg_running {
                tracker.record_success(node_id);

                if self.is_stale_primary(node_id, &result) {
                    let threshold = self.config.health_check_failures_before_failover;
                    if stale_primaries.sighted(node_id, threshold, REJOIN_COOLDOWN, Instant::now())
                    {
                        self.spawn_rejoin(node_id);
                    }
                    continue;
                }
                stale_primaries.cleared(node_id);
                self.push_backup_settings(node_id, &cfg.agent_addr, &result)
                    .await;

                // The primary came back before a failover could complete.
                if *node_id == topology.primary_node_id
                    && topology.node_roles.get(node_id) == Some(&NodeRole::Offline)
                    && !result.in_recovery
                {
                    info!(node_id, "primary recovered before failover completed");
                    let _ = self
                        .raft
                        .raft
                        .client_write(TopologyCommand::SetPrimary {
                            node_id: node_id.clone(),
                            at_lsn: result.flush_lsn,
                            new_timeline: result.timeline,
                        })
                        .await;
                }

                // If the node has no effective role (Unknown, Offline, or Maintenance) but is
                // healthy, promote it to Replica. This handles recovery from failure, nodes
                // returning from maintenance, and the initial bootstrap period.
                let current_role = topology.node_roles.get(node_id);
                if matches!(
                    current_role,
                    Some(&NodeRole::Offline)
                        | Some(&NodeRole::Unknown)
                        | Some(&NodeRole::Maintenance)
                ) && node_id != &topology.primary_node_id
                {
                    info!(node_id, role = ?current_role, "healthy node has non-replica role, marking as replica");
                    let _ = self
                        .raft
                        .raft
                        .client_write(TopologyCommand::MarkReplica {
                            node_id: node_id.clone(),
                            flush_lsn: result.flush_lsn,
                        })
                        .await;

                    // Repoint this node if its primary_conninfo is stale.
                    // A replica that was offline during a failover still points at the old
                    // primary; we must demote it to the current primary so it can rejoin.
                    if result.in_recovery && !topology.primary_node_id.is_empty() {
                        let primary_id = &topology.primary_node_id;
                        let primary_host = topology
                            .node_configs
                            .get(primary_id)
                            .map(|c| {
                                c.postgres_addr
                                    .split(':')
                                    .next()
                                    .unwrap_or(&c.postgres_addr)
                                    .to_string()
                            })
                            .unwrap_or_default();

                        // Only repoint if the WAL receiver is not already talking to the
                        // current primary (conninfo will contain host=<primary_host>).
                        let already_correct = !primary_host.is_empty()
                            && result.replication_conninfo.contains(&primary_host);

                        if !already_correct {
                            info!(
                                node_id,
                                primary_host,
                                conninfo = %result.replication_conninfo,
                                "recovered replica has stale primary_conninfo — repointing"
                            );
                            if let Ok(mut client) =
                                self.pool.get_or_connect(node_id, &cfg.agent_addr).await
                            {
                                let conninfo = format!(
                                    "host={} port=5432 user={} password={}",
                                    primary_host, self.repl_user, self.repl_password
                                );
                                let slot_name = format!("{}{}", self.slot_prefix, node_id);
                                match client.demote(&conninfo, &slot_name, node_id).await {
                                    Ok(r) if r.success => {
                                        info!(
                                            node_id,
                                            "recovered replica repointed to new primary"
                                        );
                                    }
                                    Ok(r) => {
                                        warn!(node_id, err = %r.error, "repoint Demote RPC returned failure");
                                    }
                                    Err(e) => {
                                        warn!(node_id, err = %e, "repoint Demote RPC failed");
                                    }
                                }
                            }
                        }
                    }
                }

                // Update replication lag metric for replicas
                if node_id != &topology.primary_node_id {
                    let lag = primary_lsn.saturating_sub(result.flush_lsn);
                    self.metrics
                        .replica_lag_bytes
                        .with_label_values(&[node_id])
                        .set(lag as f64);
                }

                // Propose an LSN update so the topology stays current.
                let cmd = TopologyCommand::UpdateFlushLsn {
                    node_id: node_id.clone(),
                    flush_lsn: result.flush_lsn,
                    replay_lsn: result.replay_lsn,
                };
                if let Err(e) = self.raft.raft.client_write(cmd).await {
                    warn!(node_id, err = %e, "failed to propose LSN update");
                }
            } else {
                // Nodes in Maintenance mode are intentionally stopped; ignore health failures.
                if topology.node_roles.get(node_id) == Some(&NodeRole::Maintenance) {
                    debug!(
                        node_id,
                        "skipping health-check failure for node in maintenance mode"
                    );
                    continue;
                }

                tracker.record_failure(node_id);
                self.metrics
                    .health_check_failures
                    .with_label_values(&[node_id])
                    .inc();

                let threshold = self.config.health_check_failures_before_failover;
                if tracker.is_failed(node_id, threshold) {
                    // By ID, not role: the first attempt marks the primary offline,
                    // and an attempt that aborts must be able to run again.
                    let is_primary = *node_id == topology.primary_node_id;

                    if is_primary {
                        // Guard: skip automatic failover if an operator-initiated
                        // failover or switchover is already running (API handler holds
                        // the flag). Try to acquire; if the API beat us to it, back off.
                        if self
                            .op_in_progress
                            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                            .is_err()
                        {
                            warn!(
                                node_id,
                                "automatic failover skipped: operator operation in progress"
                            );
                            continue;
                        }

                        // Mark the node offline in Raft before promoting a replacement.
                        // Skip failover if the Raft write fails (we may have lost leadership).
                        if let Err(e) = self
                            .raft
                            .raft
                            .client_write(TopologyCommand::MarkOffline {
                                node_id: node_id.clone(),
                            })
                            .await
                        {
                            warn!(
                                node_id,
                                err = %e,
                                "MarkOffline write failed — skipping failover to avoid split-brain"
                            );
                            self.op_in_progress.store(false, Ordering::SeqCst);
                            continue;
                        }

                        warn!(
                            node_id,
                            threshold, "primary has failed health checks, triggering failover"
                        );
                        crate::failover::trigger_failover(
                            &self.raft,
                            topology,
                            node_id,
                            &self.pool,
                            &self.metrics,
                            &self.repl_user,
                            &self.repl_password,
                            &self.slot_prefix,
                        )
                        .await;
                        self.op_in_progress.store(false, Ordering::SeqCst);
                        // Reset tracker so we don't re-trigger on the same node.
                        tracker.record_success(node_id);
                    } else {
                        error!(
                            node_id,
                            threshold, "replica has failed health checks, marked offline"
                        );
                    }
                }
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::HealthCheckResult;

    /// Extract the "should we repoint?" decision from poll_all_nodes so it can be
    /// unit-tested without spinning up Raft or gRPC.
    ///
    /// Returns `false` when `primary_host` is empty (no primary elected yet —
    /// matches the outer `!topology.primary_node_id.is_empty()` guard in the real code).
    fn needs_repoint(primary_host: &str, conninfo: &str) -> bool {
        if primary_host.is_empty() {
            return false;
        }
        !conninfo.contains(primary_host)
    }

    #[test]
    fn stale_conninfo_triggers_repoint() {
        // Replica still points at old-primary after a failover.
        let result = HealthCheckResult {
            node_id: "pg2".into(),
            reachable: true,
            in_recovery: true,
            flush_lsn: 0,
            replay_lsn: 0,
            timeline: 1,
            pg_running: true,
            replication_conninfo: "host=pg-primary port=5432 user=replicator".into(),
            streaming_standbys: Vec::new(),
            parameters: Default::default(),
            lost_slots: Vec::new(),
            has_data: true,
            backup_config_version: String::new(),
            proxy_auth_version: String::new(),
        };
        let primary_host = "pg-replica-1"; // new primary after failover
        assert!(
            needs_repoint(primary_host, &result.replication_conninfo),
            "stale conninfo should trigger repoint"
        );
    }

    #[test]
    fn current_conninfo_skips_repoint() {
        // Replica already points at the current primary — no repoint needed.
        let result = HealthCheckResult {
            node_id: "pg3".into(),
            reachable: true,
            in_recovery: true,
            flush_lsn: 0,
            replay_lsn: 0,
            timeline: 2,
            pg_running: true,
            replication_conninfo: "host=pg-replica-1 port=5432 user=replicator".into(),
            streaming_standbys: Vec::new(),
            parameters: Default::default(),
            lost_slots: Vec::new(),
            has_data: true,
            backup_config_version: String::new(),
            proxy_auth_version: String::new(),
        };
        let primary_host = "pg-replica-1";
        assert!(
            !needs_repoint(primary_host, &result.replication_conninfo),
            "correct conninfo should skip repoint"
        );
    }

    #[test]
    fn empty_conninfo_triggers_repoint() {
        // WAL receiver not yet started (e.g. postgres just restarted) — treat as stale.
        let result = HealthCheckResult {
            node_id: "pg2".into(),
            reachable: true,
            in_recovery: true,
            flush_lsn: 0,
            replay_lsn: 0,
            timeline: 0,
            pg_running: true,
            replication_conninfo: String::new(),
            streaming_standbys: Vec::new(),
            parameters: Default::default(),
            lost_slots: Vec::new(),
            has_data: true,
            backup_config_version: String::new(),
            proxy_auth_version: String::new(),
        };
        let primary_host = "pg-replica-1";
        assert!(
            needs_repoint(primary_host, &result.replication_conninfo),
            "empty conninfo should trigger repoint"
        );
    }

    #[test]
    fn empty_primary_host_skips_repoint() {
        // No primary in topology yet — don't repoint to an unknown host.
        let result = HealthCheckResult {
            node_id: "pg2".into(),
            reachable: true,
            in_recovery: true,
            flush_lsn: 0,
            replay_lsn: 0,
            timeline: 1,
            pg_running: true,
            replication_conninfo: "host=pg-primary port=5432 user=replicator".into(),
            streaming_standbys: Vec::new(),
            parameters: Default::default(),
            lost_slots: Vec::new(),
            has_data: true,
            backup_config_version: String::new(),
            proxy_auth_version: String::new(),
        };
        let primary_host = ""; // no primary elected yet
        assert!(
            !needs_repoint(primary_host, &result.replication_conninfo),
            "unknown primary should skip repoint"
        );
    }
}
