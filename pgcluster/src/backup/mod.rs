//! Backup coordinator.
//!
//! A `BackupScheduler` runs on every pgcluster node but acts only on the Raft
//! leader, with quorum. Each backup runs on a node's agent, preferring the
//! least-lagged standby, and its manifest goes through Raft, so a new leader
//! knows when the next is due. Scheduling, retention and manifests live here;
//! the agent's backup RPCs carry out the work and name no tool.
//!
//! WAL archiving runs inside Postgres, from whichever node is primary, so it
//! keeps going without this module. The leader only pushes the configuration.

pub mod walg;

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Result};
use tracing::{error, info, warn};

use crate::agent_clients::AgentClientPool;
use crate::config::BackupConfig;
use crate::raft::commands::TopologyCommand;
use crate::raft::topology::{BackupManifest, BackupStatus, ClusterTopology, NodeRole};
use crate::raft::{RaftNode, TopologyWatch};

/// How often to check whether a backup is due.
const CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// How long to wait after a failed backup.
const RETRY_DELAY: Duration = Duration::from_secs(60);

// ── BackupScheduler ────────────────────────────────────────────────────────────

pub struct BackupScheduler {
    raft: Arc<RaftNode>,
    topology_rx: TopologyWatch,
    config: BackupConfig,
    pool: Arc<AgentClientPool>,
}

impl BackupScheduler {
    pub fn new(
        raft: Arc<RaftNode>,
        topology_rx: TopologyWatch,
        config: BackupConfig,
        pool: Arc<AgentClientPool>,
    ) -> Self {
        Self {
            raft,
            topology_rx,
            config,
            pool,
        }
    }

    /// Run the scheduler loop.  Spawn this as a background task.
    pub async fn run(&mut self) {
        let mut tick = tokio::time::interval(CHECK_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_failure: Option<Instant> = None;

        info!("backup scheduler started");

        loop {
            tick.tick().await;

            if !self.config.enabled {
                continue;
            }
            if last_failure.is_some_and(|at| at.elapsed() < RETRY_DELAY) {
                continue;
            }

            // Only the Raft leader runs backups to avoid duplicate uploads.
            let metrics = self.raft.raft.metrics().borrow().clone();
            if metrics.current_leader != Some(metrics.id) {
                continue;
            }

            let topology = self.topology_rx.current();
            if !is_due(
                self.config.interval_seconds,
                last_backup_ts(&topology),
                unix_now(),
            ) {
                continue;
            }

            let label = format!("scheduled-{}", chrono::Utc::now().format("%Y-%m-%d"));
            match run_backup(
                &self.raft,
                &topology,
                &self.pool,
                &self.config,
                "scheduled",
                &label,
            )
            .await
            {
                Ok(manifest) => {
                    last_failure = None;
                    info!(
                        backup_id = manifest.backup_id,
                        source_node = manifest.source_node,
                        "scheduled backup complete"
                    );
                }
                Err(e) => {
                    last_failure = Some(Instant::now());
                    error!(err = %e, "scheduled backup failed");
                }
            }
        }
    }
}

// ── Core execution ─────────────────────────────────────────────────────────────

/// Take a backup on the best node, record it through Raft and apply retention.
pub async fn run_backup(
    raft: &Arc<RaftNode>,
    topology: &ClusterTopology,
    pool: &AgentClientPool,
    config: &BackupConfig,
    frequency: &str,
    label: &str,
) -> Result<BackupManifest> {
    let (mut source_node, agent_addr) = select_source(topology, config.prefer_replica)
        .ok_or_else(|| anyhow!("no suitable backup source"))?;

    let started_at = unix_now();
    info!(source_node, label, "starting backup");
    let mut client = pool.get_or_connect(&source_node, &agent_addr).await?;
    let mut response = client.backup_push().await?;

    // A standby can refuse, for one when an idle cluster has written no new WAL
    // since the last backup. The primary can always start a new segment.
    if !response.success && source_node != topology.primary_node_id {
        warn!(source_node, err = %response.error, "backup failed on a standby, using the primary");
        let (primary, primary_addr) =
            select_source(topology, false).ok_or_else(|| anyhow!("the cluster has no primary"))?;
        client = pool.get_or_connect(&primary, &primary_addr).await?;
        response = client.backup_push().await?;
        source_node = primary;
    }
    if !response.success {
        bail!("backup on {source_node} failed: {}", response.error);
    }

    let manifest = BackupManifest {
        backup_id: response.backup_name.clone(),
        label: label.to_string(),
        frequency: frequency.to_string(),
        source_node,
        started_at,
        completed_at: unix_now(),
        size_bytes: response.size_bytes,
        location: response.backup_name,
        status: BackupStatus::Completed,
    };
    raft.raft
        .client_write(TopologyCommand::AddBackupManifest(manifest.clone()))
        .await
        .map_err(|e| anyhow!("record backup manifest: {e}"))?;

    // Retention failing does not undo a good backup.
    match client.backup_delete(config.retain_full, "").await {
        Ok(response) if response.success => {
            remove_manifests_except(raft, topology, &manifest, &response.remaining).await;
        }
        Ok(response) => warn!(err = %response.error, "backup retention failed"),
        Err(e) => warn!(err = %e, "backup retention failed"),
    }
    Ok(manifest)
}

/// Drop manifests of backups retention removed from the repository.
async fn remove_manifests_except(
    raft: &Arc<RaftNode>,
    topology: &ClusterTopology,
    newest: &BackupManifest,
    remaining: &[String],
) {
    for old in &topology.backups {
        if old.backup_id == newest.backup_id || remaining.contains(&old.backup_id) {
            continue;
        }
        let backup_id = old.backup_id.clone();
        if let Err(e) = raft
            .raft
            .client_write(TopologyCommand::RemoveBackupManifest {
                backup_id: backup_id.clone(),
            })
            .await
        {
            warn!(backup_id, err = %e, "failed to remove backup manifest");
        }
    }
}

// ── Scheduling helpers ─────────────────────────────────────────────────────────

/// Pick the node to back up, as `(node_id, agent_addr)`.
/// Prefers the replica with the smallest replication lag; falls back to the
/// primary when `prefer_replica` is false or no replica is healthy.
pub fn select_source(topology: &ClusterTopology, prefer_replica: bool) -> Option<(String, String)> {
    if prefer_replica {
        let best = topology
            .node_roles
            .iter()
            .filter(|(_, role)| **role == NodeRole::Replica)
            .map(|(id, _)| {
                let lag = topology
                    .replica_lag_bytes
                    .get(id)
                    .copied()
                    .unwrap_or(u64::MAX);
                (id.clone(), lag)
            })
            .min_by_key(|(_, lag)| *lag)
            .map(|(id, _)| id);

        if let Some(node_id) = best {
            if let Some(cfg) = topology.node_configs.get(&node_id) {
                return Some((node_id, cfg.agent_addr.clone()));
            }
        }
    }

    let cfg = topology.node_configs.get(&topology.primary_node_id)?;
    Some((topology.primary_node_id.clone(), cfg.agent_addr.clone()))
}

fn last_backup_ts(topology: &ClusterTopology) -> Option<i64> {
    topology
        .backups
        .iter()
        .filter(|b| b.status == BackupStatus::Completed)
        .map(|b| b.completed_at)
        .max()
}

fn is_due(interval_seconds: u64, last: Option<i64>, now: i64) -> bool {
    last.is_none_or(|last| now - last >= interval_seconds as i64)
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::topology::NodeConfig;

    fn node(id: &str) -> NodeConfig {
        NodeConfig {
            node_id: id.into(),
            agent_addr: format!("{id}-agent:7001"),
            postgres_addr: format!("{id}-db:5432"),
            priority: 100,
            tags: Default::default(),
        }
    }

    fn topology() -> ClusterTopology {
        let mut t = ClusterTopology {
            primary_node_id: "pg1".into(),
            ..Default::default()
        };
        t.node_roles.insert("pg1".into(), NodeRole::Primary);
        t.node_roles.insert("pg2".into(), NodeRole::Replica);
        t.node_roles.insert("pg3".into(), NodeRole::Replica);
        t.replica_lag_bytes.insert("pg2".into(), 500);
        t.replica_lag_bytes.insert("pg3".into(), 100);
        for id in ["pg1", "pg2", "pg3"] {
            t.node_configs.insert(id.into(), node(id));
        }
        t
    }

    fn manifest(completed_at: i64, status: BackupStatus) -> BackupManifest {
        BackupManifest {
            backup_id: format!("base_{completed_at}"),
            label: String::new(),
            frequency: "scheduled".into(),
            source_node: "pg2".into(),
            started_at: 0,
            completed_at,
            size_bytes: 0,
            location: String::new(),
            status,
        }
    }

    #[test]
    fn source_is_the_least_lagged_standby_agent() {
        assert_eq!(
            select_source(&topology(), true),
            Some(("pg3".into(), "pg3-agent:7001".into()))
        );
    }

    #[test]
    fn source_can_be_forced_to_the_primary() {
        let (node, _) = select_source(&topology(), false).unwrap();
        assert_eq!(node, "pg1");
    }

    #[test]
    fn source_falls_back_to_the_primary_with_no_standby() {
        let mut t = topology();
        t.node_roles.insert("pg2".into(), NodeRole::Offline);
        t.node_roles.insert("pg3".into(), NodeRole::Offline);
        let (node, _) = select_source(&t, true).unwrap();
        assert_eq!(node, "pg1");
    }

    #[test]
    fn no_primary_and_no_standby_means_no_source() {
        assert_eq!(select_source(&ClusterTopology::default(), true), None);
    }

    #[test]
    fn a_first_backup_is_due_at_once() {
        assert!(is_due(86400, None, 1000));
    }

    #[test]
    fn backups_come_due_after_the_interval() {
        assert!(!is_due(100, Some(1000), 1099));
        assert!(is_due(100, Some(1000), 1100));
    }

    #[test]
    fn last_backup_ignores_failures() {
        let mut t = topology();
        t.backups = vec![
            manifest(100, BackupStatus::Completed),
            manifest(200, BackupStatus::Failed),
        ];
        assert_eq!(last_backup_ts(&t), Some(100));
    }
}
