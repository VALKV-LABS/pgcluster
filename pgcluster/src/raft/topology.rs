use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

// ── Core types ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    Primary,
    Replica,
    Offline,
    Maintenance,
    Unknown,
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeRole::Primary => write!(f, "primary"),
            NodeRole::Replica => write!(f, "replica"),
            NodeRole::Offline => write!(f, "offline"),
            NodeRole::Maintenance => write!(f, "maintenance"),
            NodeRole::Unknown => write!(f, "unknown"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub node_id: String,
    pub agent_addr: String,
    pub postgres_addr: String,
    pub priority: u32,
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BackupStatus {
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    pub backup_id: String,
    /// Human-readable label, e.g. "daily-2026-07-04" or operator-provided.
    pub label: String,
    /// "daily" | "weekly" | "monthly" | "manual"
    pub frequency: String,
    /// Node ID of the postgres instance that served pg_basebackup.
    pub source_node: String,
    pub started_at: i64,
    pub completed_at: i64,
    /// Total bytes uploaded across all tar files.
    pub size_bytes: u64,
    /// S3 URI prefix for this backup, e.g. "s3://bucket/prefix/daily/backup-id/".
    pub s3_uri: String,
    pub status: BackupStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailoverEvent {
    pub old_primary: String,
    pub new_primary: String,
    pub triggered_at: i64, // Unix seconds
    pub duration_ms: u64,
    pub reason: String, // "health_check_failure" | "operator_switchover"
}

// ── ClusterTopology — the Raft state machine ──────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterTopology {
    /// Which node is currently primary
    pub primary_node_id: String,
    /// Role of each node
    pub node_roles: HashMap<String, NodeRole>,
    /// Replication slot name for each replica
    pub replica_slots: HashMap<String, String>,
    /// primary_conninfo for each replica (written to postgresql.auto.conf)
    pub primary_conninfos: HashMap<String, String>,
    /// Last confirmed flush_lsn per node (updated on every health check)
    pub last_flush_lsns: HashMap<String, u64>,
    /// Last confirmed replay_lsn per node
    pub last_replay_lsns: HashMap<String, u64>,
    /// Replication lag in bytes per replica (primary_lsn - replica_flush_lsn)
    pub replica_lag_bytes: HashMap<String, u64>,
    /// Static node configuration (address + priority)
    pub node_configs: HashMap<String, NodeConfig>,
    /// Last 10 failover events
    pub failover_history: VecDeque<FailoverEvent>,
    /// All known backup manifests, sorted most-recent first. Capped at 1 000.
    #[serde(default)]
    pub backups: Vec<BackupManifest>,
    /// Monotonically increasing version; incremented on every command
    pub version: u64,
    /// Unix seconds of last topology change
    pub last_changed_at: i64,
}

impl ClusterTopology {
    pub fn new(cluster_name: &str) -> Self {
        let t = Self::default();
        // Store cluster name in a simple way
        let _ = cluster_name; // used by callers for display
        t
    }

    /// Returns the Postgres address for the current primary, if any.
    pub fn primary_postgres_addr(&self) -> Option<&str> {
        self.node_configs
            .get(&self.primary_node_id)
            .map(|c| c.postgres_addr.as_str())
    }

    /// Returns postgres addresses of replicas within the lag threshold (bytes).
    pub fn replica_addrs_within_lag(&self, max_lag_bytes: u64) -> Vec<String> {
        let primary_lsn = self
            .last_flush_lsns
            .get(&self.primary_node_id)
            .copied()
            .unwrap_or(0);

        self.node_roles
            .iter()
            .filter(|(_, role)| **role == NodeRole::Replica)
            .filter(|(id, _)| {
                let lag =
                    primary_lsn.saturating_sub(self.last_flush_lsns.get(*id).copied().unwrap_or(0));
                lag <= max_lag_bytes
            })
            .filter_map(|(id, _)| self.node_configs.get(id).map(|c| c.postgres_addr.clone()))
            .collect()
    }

    /// Returns the node ID of the best failover candidate (highest LSN, then priority).
    /// Excludes `failed_id` and offline/maintenance nodes.
    pub fn best_failover_candidate(&self, failed_id: &str) -> Option<String> {
        let mut candidates: Vec<_> = self
            .node_roles
            .iter()
            .filter(|(id, role)| *id != failed_id && **role == NodeRole::Replica)
            .collect();

        candidates.sort_by(|(id_a, _), (id_b, _)| {
            let lsn_a = self.last_flush_lsns.get(*id_a).copied().unwrap_or(0);
            let lsn_b = self.last_flush_lsns.get(*id_b).copied().unwrap_or(0);
            let pri_a = self
                .node_configs
                .get(*id_a)
                .map(|c| c.priority)
                .unwrap_or(0);
            let pri_b = self
                .node_configs
                .get(*id_b)
                .map(|c| c.priority)
                .unwrap_or(0);
            lsn_b.cmp(&lsn_a).then(pri_b.cmp(&pri_a))
        });

        candidates.first().map(|(id, _)| (*id).clone())
    }

    /// Maximum number of failover events kept in history.
    pub const FAILOVER_HISTORY_MAX: usize = 100;

    /// Append a failover event, keeping only the last `FAILOVER_HISTORY_MAX`.
    pub fn record_failover(&mut self, event: FailoverEvent) {
        self.failover_history.push_back(event);
        while self.failover_history.len() > Self::FAILOVER_HISTORY_MAX {
            self.failover_history.pop_front();
        }
    }
}

// ── LSN helpers ───────────────────────────────────────────────────────────────

/// Parse a Postgres LSN string like "A/1B2C3D" into a u64.
pub fn parse_lsn(s: &str) -> anyhow::Result<u64> {
    let parts: Vec<&str> = s.splitn(2, '/').collect();
    if parts.len() != 2 {
        anyhow::bail!("invalid LSN {:?}", s);
    }
    let hi = u64::from_str_radix(parts[0], 16)
        .map_err(|_| anyhow::anyhow!("invalid LSN hi {:?}", parts[0]))?;
    let lo = u64::from_str_radix(parts[1], 16)
        .map_err(|_| anyhow::anyhow!("invalid LSN lo {:?}", parts[1]))?;
    Ok((hi << 32) | lo)
}

/// Format a u64 LSN as "A/1B2C3D" (matches PostgreSQL's pg_lsn output format).
pub fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_lsn_roundtrip() {
        let cases = ["0/1A2B3C", "A/DEADBEEF", "0/0", "1/0"];
        for s in &cases {
            let lsn = parse_lsn(s).unwrap();
            assert_eq!(&format_lsn(lsn), s);
        }
    }

    #[test]
    fn parse_lsn_invalid() {
        assert!(parse_lsn("not-an-lsn").is_err());
        assert!(parse_lsn("XYZ/123").is_err());
    }

    #[test]
    fn best_candidate_picks_highest_lsn() {
        let mut t = ClusterTopology::default();
        t.node_roles.insert("pg1".into(), NodeRole::Primary);
        t.node_roles.insert("pg2".into(), NodeRole::Replica);
        t.node_roles.insert("pg3".into(), NodeRole::Replica);
        t.last_flush_lsns.insert("pg2".into(), 100);
        t.last_flush_lsns.insert("pg3".into(), 200); // higher
        for id in ["pg1", "pg2", "pg3"] {
            t.node_configs.insert(
                id.into(),
                NodeConfig {
                    node_id: id.into(),
                    agent_addr: format!("127.0.0.1:700{}", id.chars().last().unwrap()),
                    postgres_addr: format!("127.0.0.1:543{}", id.chars().last().unwrap()),
                    priority: 100,
                    tags: Default::default(),
                },
            );
        }
        assert_eq!(t.best_failover_candidate("pg1").as_deref(), Some("pg3"));
    }

    #[test]
    fn best_candidate_priority_breaks_tie() {
        let mut t = ClusterTopology::default();
        t.node_roles.insert("pg1".into(), NodeRole::Primary);
        t.node_roles.insert("pg2".into(), NodeRole::Replica);
        t.node_roles.insert("pg3".into(), NodeRole::Replica);
        t.last_flush_lsns.insert("pg2".into(), 100);
        t.last_flush_lsns.insert("pg3".into(), 100); // tied LSN
        t.node_configs.insert(
            "pg2".into(),
            NodeConfig {
                node_id: "pg2".into(),
                agent_addr: "x".into(),
                postgres_addr: "y".into(),
                priority: 90,
                tags: Default::default(),
            },
        );
        t.node_configs.insert(
            "pg3".into(),
            NodeConfig {
                node_id: "pg3".into(),
                agent_addr: "x".into(),
                postgres_addr: "y".into(),
                priority: 110,
                tags: Default::default(), // higher priority
            },
        );
        t.node_configs.insert(
            "pg1".into(),
            NodeConfig {
                node_id: "pg1".into(),
                agent_addr: "x".into(),
                postgres_addr: "y".into(),
                priority: 100,
                tags: Default::default(),
            },
        );
        assert_eq!(t.best_failover_candidate("pg1").as_deref(), Some("pg3"));
    }
}
