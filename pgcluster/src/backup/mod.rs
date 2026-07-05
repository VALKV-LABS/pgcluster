//! Scheduled backup coordinator.
//!
//! # How it works
//!
//! A `BackupScheduler` task runs on every pgcluster node but acts only when
//! the node is the Raft leader.  It wakes once per hour, checks each configured
//! frequency tier (daily / weekly / monthly) against the last completed backup
//! timestamp stored in Raft topology, and runs pg_basebackup when a tier is due.
//!
//! ## Backup execution
//!
//! pgcluster invokes `pg_basebackup` as a subprocess, connecting directly to the
//! selected Postgres node's address over the standard replication protocol.
//! pg_basebackup must be installed on the pgcluster host
//! (available in the `postgresql-client` package).
//!
//! The preferred source is the replica with the smallest replication lag;
//! the primary is used as a fallback to avoid interfering with read traffic.
//!
//! Output is written to a temporary directory in tar+gzip format
//! (`base.tar.gz` + `pg_wal.tar.gz`) and then uploaded to S3 before the
//! temp dir is cleaned up.
//!
//! ## Credentials
//!
//! S3 credentials are read from the environment:
//!   `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` (optional).
//! IAM instance roles / IRSA are also supported — no env vars needed in that case.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use futures::StreamExt as _;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use tracing::{error, info, warn};

use crate::config::{BackupConfig, BackupFrequency, S3Config};
use crate::raft::commands::TopologyCommand;
use crate::raft::topology::{BackupManifest, BackupStatus, ClusterTopology, NodeRole};
use crate::raft::{RaftNode, TopologyWatch};

// ── BackupScheduler ────────────────────────────────────────────────────────────

pub struct BackupScheduler {
    raft: Arc<RaftNode>,
    topology_rx: TopologyWatch,
    config: BackupConfig,
    repl_user: String,
    repl_password: String,
    store: Arc<dyn ObjectStore>,
}

impl BackupScheduler {
    pub fn new(
        raft: Arc<RaftNode>,
        topology_rx: TopologyWatch,
        config: BackupConfig,
        repl_user: String,
        repl_password: String,
    ) -> Result<Self> {
        let store = build_s3_store(&config.s3)?;
        Ok(Self {
            raft,
            topology_rx,
            config,
            repl_user,
            repl_password,
            store,
        })
    }

    /// Run the scheduler loop.  Spawn this as a background task.
    pub async fn run(&mut self) {
        let mut tick = tokio::time::interval(Duration::from_secs(3600));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        info!("backup scheduler started");

        loop {
            tick.tick().await;

            if !self.config.enabled {
                continue;
            }

            // Only the Raft leader runs backups to avoid duplicate uploads.
            let my_id = self.raft.raft.metrics().borrow().id;
            let Some(leader) = self.raft.raft.current_leader().await else {
                continue;
            };
            if leader != my_id {
                continue;
            }

            let topology = self.topology_rx.current();
            let schedules = self.config.schedule.clone();

            for entry in &schedules {
                let freq_str = entry.frequency.to_string();
                let last = last_backup_ts(&topology, &freq_str);

                if !is_due(&entry.frequency, last) {
                    continue;
                }

                let Some((source_node, source_addr)) =
                    select_source(&topology, self.config.prefer_replica)
                else {
                    warn!(
                        frequency = %freq_str,
                        "no suitable backup source — skipping scheduled backup"
                    );
                    continue;
                };

                let label = format!("{}-{}", freq_str, chrono::Utc::now().format("%Y-%m-%d"));

                info!(frequency = %freq_str, source_node, label, "starting scheduled backup");

                match execute_backup(
                    &source_node,
                    &source_addr,
                    &self.repl_user,
                    &self.repl_password,
                    &freq_str,
                    &label,
                    &self.config,
                    &self.store,
                )
                .await
                {
                    Ok(manifest) => {
                        let backup_id = manifest.backup_id.clone();
                        if let Err(e) = self
                            .raft
                            .raft
                            .client_write(TopologyCommand::AddBackupManifest(manifest))
                            .await
                        {
                            error!(err = %e, backup_id, "failed to record backup manifest in Raft");
                            continue;
                        }
                        info!(backup_id, frequency = %freq_str, "scheduled backup complete");
                        self.prune(&freq_str, entry.retain as usize).await;
                    }
                    Err(e) => {
                        error!(frequency = %freq_str, err = %e, "scheduled backup failed");
                    }
                }
            }
        }
    }

    async fn prune(&self, frequency: &str, retain: usize) {
        let topology = self.topology_rx.current();
        let mut completed: Vec<BackupManifest> = topology
            .backups
            .iter()
            .filter(|b| b.frequency == frequency && b.status == BackupStatus::Completed)
            .cloned()
            .collect();
        // Topology keeps backups sorted most-recent-first; stable.
        completed.sort_by_key(|m| std::cmp::Reverse(m.completed_at));

        if completed.len() <= retain {
            return;
        }

        for old in &completed[retain..] {
            let bid = old.backup_id.clone();
            delete_from_s3(&self.store, &self.config.s3.bucket, &old.s3_uri).await;
            match self
                .raft
                .raft
                .client_write(TopologyCommand::RemoveBackupManifest {
                    backup_id: bid.clone(),
                })
                .await
            {
                Ok(_) => info!(backup_id = bid, frequency, "pruned old backup"),
                Err(e) => warn!(backup_id = bid, err = %e, "failed to remove backup manifest"),
            }
        }
    }
}

// ── Core execution ─────────────────────────────────────────────────────────────

/// Run pg_basebackup against `source_addr`, upload each tar file to S3,
/// and return a completed `BackupManifest`.
///
/// Requires `pg_basebackup` to be available on the pgcluster host PATH
/// (installed via the `postgresql-client` OS package).
#[allow(clippy::too_many_arguments)]
pub async fn execute_backup(
    source_node: &str,
    source_addr: &str,
    repl_user: &str,
    repl_password: &str,
    frequency: &str,
    label: &str,
    backup_cfg: &BackupConfig,
    store: &Arc<dyn ObjectStore>,
) -> Result<BackupManifest> {
    let backup_id = uuid::Uuid::new_v4().to_string();
    let started_at = unix_now();

    let (host, port) = parse_host_port(source_addr)?;

    // pg_basebackup writes base.tar.gz + pg_wal.tar.gz into the temp dir.
    let tmp = tempfile::tempdir()?;

    let out = tokio::process::Command::new("pg_basebackup")
        .args([
            "-h",
            &host,
            "-p",
            &port.to_string(),
            "-U",
            repl_user,
            "-F",
            "tar",
            "-z",
            "--wal-method=stream",
            "-D",
            tmp.path().to_str().unwrap_or("/tmp/pgcluster-backup"),
            "--no-password",
        ])
        .env("PGPASSWORD", repl_password)
        .output()
        .await?;

    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        anyhow::bail!(
            "pg_basebackup exited {:?}: {}",
            out.status.code(),
            stderr.trim()
        );
    }

    // Upload every file the backup wrote to S3.
    let s3_key_base = format!(
        "{}/{}/{}",
        backup_cfg.s3.prefix.trim_end_matches('/'),
        frequency,
        backup_id,
    );
    let mut total_bytes: u64 = 0;

    let mut dir = tokio::fs::read_dir(tmp.path()).await?;
    while let Some(entry) = dir.next_entry().await? {
        let path = entry.path();
        let filename = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let data = tokio::fs::read(&path).await?;
        total_bytes += data.len() as u64;
        let key = format!("{}/{}", s3_key_base, filename);
        let location = object_store::path::Path::from(key.as_str());
        store
            .put(&location, bytes::Bytes::from(data).into())
            .await
            .map_err(|e| anyhow::anyhow!("S3 put {} failed: {}", key, e))?;
        info!(key, bytes = total_bytes, "uploaded backup file to S3");
    }
    // tmp dir cleaned up on drop here.

    Ok(BackupManifest {
        backup_id,
        label: label.to_string(),
        frequency: frequency.to_string(),
        source_node: source_node.to_string(),
        started_at,
        completed_at: unix_now(),
        size_bytes: total_bytes,
        s3_uri: format!("s3://{}/{}", backup_cfg.s3.bucket, s3_key_base),
        status: BackupStatus::Completed,
    })
}

// ── S3 helpers ─────────────────────────────────────────────────────────────────

/// Build an S3 object store from config.
/// Credentials are sourced from the environment (AWS_ACCESS_KEY_ID /
/// AWS_SECRET_ACCESS_KEY / IAM role) via `AmazonS3Builder::from_env()`.
pub fn build_s3_store(cfg: &S3Config) -> Result<Arc<dyn ObjectStore>> {
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(&cfg.bucket);
    if let Some(region) = &cfg.region {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = &cfg.endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    if cfg.path_style {
        // Path-style addressing: required for MinIO and Ceph.
        builder = builder.with_virtual_hosted_style_request(false);
    }
    Ok(Arc::new(builder.build().map_err(|e| {
        anyhow::anyhow!("S3 store init failed: {}", e)
    })?))
}

/// Delete all objects under the backup's S3 prefix.
/// Non-fatal: logs warnings on individual delete failures.
pub async fn delete_from_s3(store: &Arc<dyn ObjectStore>, bucket: &str, s3_uri: &str) {
    // s3_uri = "s3://bucket/prefix/frequency/backup_id"
    let prefix_str = s3_uri
        .strip_prefix(&format!("s3://{}/", bucket))
        .unwrap_or(s3_uri);
    let prefix = object_store::path::Path::from(prefix_str);

    let mut stream = store.list(Some(&prefix));
    while let Some(result) = stream.next().await {
        match result {
            Ok(meta) => {
                if let Err(e) = store.delete(&meta.location).await {
                    warn!(key = %meta.location, err = %e, "S3 delete failed during backup prune");
                }
            }
            Err(e) => warn!(err = %e, "S3 list error during backup delete"),
        }
    }
}

// ── Scheduling helpers ─────────────────────────────────────────────────────────

/// Pick the best Postgres node to back up from.
/// Prefers the replica with the smallest replication lag (fewest bytes behind);
/// falls back to the primary when `prefer_replica` is false or no replica is healthy.
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
                return Some((node_id, cfg.postgres_addr.clone()));
            }
        }
    }

    if !topology.primary_node_id.is_empty() {
        if let Some(cfg) = topology.node_configs.get(&topology.primary_node_id) {
            return Some((topology.primary_node_id.clone(), cfg.postgres_addr.clone()));
        }
    }
    None
}

fn last_backup_ts(topology: &ClusterTopology, frequency: &str) -> Option<i64> {
    topology
        .backups
        .iter()
        .filter(|b| b.frequency == frequency && b.status == BackupStatus::Completed)
        .map(|b| b.completed_at)
        .max()
}

fn is_due(frequency: &BackupFrequency, last: Option<i64>) -> bool {
    let Some(last_ts) = last else {
        return true; // never backed up
    };
    let elapsed = unix_now() - last_ts;
    match frequency {
        // 1-hour slack so backups don't drift across the day boundary.
        BackupFrequency::Daily => elapsed >= 23 * 3600,
        BackupFrequency::Weekly => elapsed >= (7 * 24 - 1) * 3600,
        BackupFrequency::Monthly => elapsed >= 28 * 24 * 3600,
    }
}

// ── Misc helpers ───────────────────────────────────────────────────────────────

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn parse_host_port(addr: &str) -> Result<(String, u16)> {
    let colon = addr
        .rfind(':')
        .ok_or_else(|| anyhow::anyhow!("missing port in address {:?}", addr))?;
    let host = addr[..colon].to_string();
    let port = addr[colon + 1..]
        .parse::<u16>()
        .map_err(|_| anyhow::anyhow!("invalid port in address {:?}", addr))?;
    Ok((host, port))
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::topology::{
        BackupManifest, BackupStatus, ClusterTopology, NodeConfig, NodeRole,
    };
    use std::collections::HashMap;

    fn make_topology_with_backups(backups: Vec<BackupManifest>) -> ClusterTopology {
        let mut t = ClusterTopology {
            primary_node_id: "pg1".into(),
            ..Default::default()
        };
        t.node_roles.insert("pg1".into(), NodeRole::Primary);
        t.node_roles.insert("pg2".into(), NodeRole::Replica);
        t.node_roles.insert("pg3".into(), NodeRole::Replica);
        t.replica_lag_bytes.insert("pg2".into(), 500);
        t.replica_lag_bytes.insert("pg3".into(), 100); // pg3 is closer
        for id in ["pg1", "pg2", "pg3"] {
            t.node_configs.insert(
                id.into(),
                NodeConfig {
                    node_id: id.into(),
                    agent_addr: format!("{}:7001", id),
                    postgres_addr: format!("{}:5432", id),
                    priority: 100,
                    tags: HashMap::new(),
                },
            );
        }
        t.backups = backups;
        t
    }

    fn completed_manifest(frequency: &str, completed_at: i64) -> BackupManifest {
        BackupManifest {
            backup_id: uuid::Uuid::new_v4().to_string(),
            label: format!("{}-test", frequency),
            frequency: frequency.to_string(),
            source_node: "pg2".into(),
            started_at: completed_at - 60,
            completed_at,
            size_bytes: 1024 * 1024,
            s3_uri: format!("s3://test-bucket/backups/{}/backup-id/", frequency),
            status: BackupStatus::Completed,
        }
    }

    #[test]
    fn select_source_prefers_lowest_lag_replica() {
        let t = make_topology_with_backups(vec![]);
        let (node_id, _) = select_source(&t, true).unwrap();
        assert_eq!(node_id, "pg3", "pg3 has lower lag (100) than pg2 (500)");
    }

    #[test]
    fn select_source_falls_back_to_primary_when_prefer_replica_false() {
        let t = make_topology_with_backups(vec![]);
        let (node_id, _) = select_source(&t, false).unwrap();
        assert_eq!(node_id, "pg1");
    }

    #[test]
    fn select_source_falls_back_to_primary_when_no_replicas() {
        let mut t = ClusterTopology {
            primary_node_id: "pg1".into(),
            ..Default::default()
        };
        t.node_roles.insert("pg1".into(), NodeRole::Primary);
        t.node_configs.insert(
            "pg1".into(),
            NodeConfig {
                node_id: "pg1".into(),
                agent_addr: "pg1:7001".into(),
                postgres_addr: "pg1:5432".into(),
                priority: 100,
                tags: HashMap::new(),
            },
        );
        let (node_id, _) = select_source(&t, true).unwrap();
        assert_eq!(node_id, "pg1");
    }

    #[test]
    fn is_due_when_never_backed_up() {
        assert!(is_due(&BackupFrequency::Daily, None));
        assert!(is_due(&BackupFrequency::Weekly, None));
        assert!(is_due(&BackupFrequency::Monthly, None));
    }

    #[test]
    fn is_due_daily_after_threshold() {
        let now = unix_now();
        let just_backed_up = now - 22 * 3600;
        assert!(!is_due(&BackupFrequency::Daily, Some(just_backed_up)));

        let old_backup = now - 24 * 3600;
        assert!(is_due(&BackupFrequency::Daily, Some(old_backup)));
    }

    #[test]
    fn is_due_weekly_after_threshold() {
        let now = unix_now();
        let recent = now - 6 * 24 * 3600; // 6 days ago — not yet due
        assert!(!is_due(&BackupFrequency::Weekly, Some(recent)));

        let old = now - 7 * 24 * 3600; // 7 days ago — due
        assert!(is_due(&BackupFrequency::Weekly, Some(old)));
    }

    #[test]
    fn is_due_monthly_after_threshold() {
        let now = unix_now();
        let recent = now - 27 * 24 * 3600; // 27 days — not yet due
        assert!(!is_due(&BackupFrequency::Monthly, Some(recent)));

        let old = now - 29 * 24 * 3600; // 29 days — due
        assert!(is_due(&BackupFrequency::Monthly, Some(old)));
    }

    #[test]
    fn last_backup_ts_returns_most_recent_completed() {
        let now = unix_now();
        let t = make_topology_with_backups(vec![
            completed_manifest("daily", now - 100),
            completed_manifest("daily", now - 50), // most recent
            completed_manifest("weekly", now - 200),
        ]);
        assert_eq!(last_backup_ts(&t, "daily"), Some(now - 50));
        assert_eq!(last_backup_ts(&t, "weekly"), Some(now - 200));
        assert_eq!(last_backup_ts(&t, "monthly"), None);
    }

    #[test]
    fn last_backup_ts_ignores_failed_backups() {
        let now = unix_now();
        let mut failed = completed_manifest("daily", now - 30);
        failed.status = BackupStatus::Failed;
        let t = make_topology_with_backups(vec![failed, completed_manifest("daily", now - 100)]);
        // The failed manifest (now - 30) must not count.
        assert_eq!(last_backup_ts(&t, "daily"), Some(now - 100));
    }

    #[test]
    fn parse_host_port_valid() {
        let (h, p) = parse_host_port("pg-replica:5432").unwrap();
        assert_eq!(h, "pg-replica");
        assert_eq!(p, 5432);
    }

    #[test]
    fn parse_host_port_missing_port() {
        assert!(parse_host_port("pg-replica").is_err());
    }
}
