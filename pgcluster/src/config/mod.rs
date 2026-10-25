use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub mod validate;

// ── Top-level config ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgClusterConfig {
    pub cluster: ClusterConfig,
    pub raft: RaftConfig,
    #[serde(default)]
    pub nodes: NodesConfig,
    pub replication: ReplicationConfig,
    pub proxy: ProxyConfig,
    #[serde(default)]
    pub failover: FailoverConfig,
    #[serde(default)]
    pub tls: TlsConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    pub api: ApiConfig,
    /// Backup policy. When present, [backup.s3] is mandatory.
    #[serde(default)]
    pub backup: Option<BackupConfig>,
}

// ── [cluster] ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub name: String,
    pub data_dir: String,
    #[serde(default = "default_mode")]
    pub mode: ClusterMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ClusterMode {
    #[default]
    Cluster,
    Coordinator,
}

fn default_mode() -> ClusterMode {
    ClusterMode::Cluster
}

// ── [raft] ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    pub node_id: u64,
    pub peers: Vec<RaftPeer>,
    #[serde(default = "default_heartbeat")]
    pub heartbeat_interval_ms: u64,
    #[serde(default = "default_election_timeout")]
    pub election_timeout_ms: u64,
    /// Set true on exactly one node for initial cluster bootstrap.
    #[serde(default)]
    pub bootstrap: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftPeer {
    pub id: u64,
    pub addr: String, // e.g. "10.0.0.1:7000"
}

fn default_heartbeat() -> u64 {
    150
}
fn default_election_timeout() -> u64 {
    500
}

// ── [[nodes.node]] ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NodesConfig {
    #[serde(default)]
    pub node: Vec<NodeConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub id: String,
    pub agent_addr: String,    // vk-agent gRPC address, e.g. "10.0.0.1:7001"
    pub postgres_addr: String, // Postgres TCP address,  e.g. "10.0.0.1:5432"
    #[serde(default = "default_priority")]
    pub priority: u32,
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

fn default_priority() -> u32 {
    100
}

// ── [replication] ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    pub replication_user: String,
    #[serde(default = "default_replication_password_env")]
    pub replication_password_env: String,
    #[serde(default = "default_slot_prefix")]
    pub slot_prefix: String,
    #[serde(default)]
    pub synchronous_standby_names: String,
}

fn default_replication_password_env() -> String {
    "PG_REPLICATION_PASSWORD".into()
}
fn default_slot_prefix() -> String {
    "pgcluster_".into()
}

// ── [proxy] ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyConfig {
    #[serde(default = "default_proxy_listen")]
    pub listen_addr: String,
    #[serde(default = "default_admin_listen")]
    pub admin_listen_addr: String,
    #[serde(default = "default_health_listen")]
    pub health_listen_addr: String,
    #[serde(default)]
    pub pool: PoolConfig,
    #[serde(default)]
    pub read_routing: ReadRoutingConfig,
}

fn default_proxy_listen() -> String {
    "0.0.0.0:5432".into()
}
fn default_admin_listen() -> String {
    "0.0.0.0:5433".into()
}
fn default_health_listen() -> String {
    "0.0.0.0:8008".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    #[serde(default = "default_max_conn")]
    pub max_connections_per_db_user: u32,
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout_seconds: u64,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_seconds: u64,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections_per_db_user: default_max_conn(),
            idle_timeout_seconds: default_idle_timeout(),
            connect_timeout_seconds: default_connect_timeout(),
        }
    }
}

fn default_max_conn() -> u32 {
    25
}
fn default_idle_timeout() -> u64 {
    600
}
fn default_connect_timeout() -> u64 {
    5
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadRoutingConfig {
    #[serde(default = "default_read_routing_enabled")]
    pub enabled: bool,
    #[serde(default = "default_max_lag_bytes")]
    pub max_replica_lag_bytes: u64,
}

impl Default for ReadRoutingConfig {
    fn default() -> Self {
        Self {
            enabled: default_read_routing_enabled(),
            max_replica_lag_bytes: default_max_lag_bytes(),
        }
    }
}

fn default_read_routing_enabled() -> bool {
    true
}
fn default_max_lag_bytes() -> u64 {
    10 * 1024 * 1024
} // 10 MB

// ── [failover] ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailoverConfig {
    #[serde(default = "default_health_check_interval")]
    pub health_check_interval_ms: u64,
    #[serde(default = "default_health_check_failures")]
    pub health_check_failures_before_failover: u32,
    #[serde(default = "default_agent_heartbeat_timeout")]
    pub agent_heartbeat_timeout_seconds: u64,
    #[serde(default = "default_promote_timeout")]
    pub promote_timeout_secs: u64,
    #[serde(default = "default_repoint_timeout")]
    pub repoint_timeout_secs: u64,
}

impl Default for FailoverConfig {
    fn default() -> Self {
        Self {
            health_check_interval_ms: default_health_check_interval(),
            health_check_failures_before_failover: default_health_check_failures(),
            agent_heartbeat_timeout_seconds: default_agent_heartbeat_timeout(),
            promote_timeout_secs: default_promote_timeout(),
            repoint_timeout_secs: default_repoint_timeout(),
        }
    }
}

fn default_health_check_interval() -> u64 {
    500
}
fn default_health_check_failures() -> u32 {
    3
}
fn default_agent_heartbeat_timeout() -> u64 {
    10
}
fn default_promote_timeout() -> u64 {
    30
}
fn default_repoint_timeout() -> u64 {
    60
}

// ── [tls] ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    pub ca_cert: Option<String>,
    pub cert: Option<String>,
    pub key: Option<String>,
    /// If true and cert/key are None, generate self-signed dev certs at startup.
    #[serde(default = "default_auto_cert")]
    pub auto_generate: bool,
}

fn default_auto_cert() -> bool {
    true
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            ca_cert: None,
            cert: None,
            key: None,
            auto_generate: default_auto_cert(),
        }
    }
}

// ── [metrics] ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    #[serde(default = "default_metrics_listen")]
    pub listen_addr: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            listen_addr: default_metrics_listen(),
        }
    }
}

fn default_metrics_listen() -> String {
    "0.0.0.0:9190".into()
}

// ── [api] ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    #[serde(default = "default_api_listen")]
    pub listen_addr: String,
    #[serde(default)]
    pub api_keys: Vec<String>,
    /// If true, status/metrics endpoints skip auth.
    #[serde(default = "default_public_read")]
    pub public_read_endpoints: bool,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            listen_addr: default_api_listen(),
            api_keys: vec![],
            public_read_endpoints: default_public_read(),
        }
    }
}

fn default_api_listen() -> String {
    "0.0.0.0:8009".into()
}
fn default_public_read() -> bool {
    false
}

// ── [backup] ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BackupFrequency {
    Daily,
    Weekly,
    Monthly,
}

impl std::fmt::Display for BackupFrequency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Daily => write!(f, "daily"),
            Self::Weekly => write!(f, "weekly"),
            Self::Monthly => write!(f, "monthly"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupScheduleEntry {
    pub frequency: BackupFrequency,
    /// Number of completed backups of this frequency to retain.
    pub retain: u32,
}

/// S3-compatible object storage configuration.
/// Credentials are read from the environment: AWS_ACCESS_KEY_ID,
/// AWS_SECRET_ACCESS_KEY, and optionally AWS_SESSION_TOKEN.
/// IAM instance roles are also supported (no env vars needed).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    /// Key prefix for all backups, e.g. "backups/my-cluster".
    #[serde(default)]
    pub prefix: String,
    /// AWS region, e.g. "us-east-1". Reads AWS_REGION env var if absent.
    pub region: Option<String>,
    /// Custom endpoint for S3-compatible stores (MinIO, Ceph, Cloudflare R2).
    pub endpoint: Option<String>,
    /// Use path-style addressing instead of virtual-hosted-style.
    /// Required for MinIO and other self-hosted S3-compatible stores.
    #[serde(default)]
    pub path_style: bool,
}

/// Backup policy configuration. When this section is present, [backup.s3]
/// is mandatory — pgcluster refuses to start without a valid S3 destination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupConfig {
    #[serde(default = "default_backup_enabled")]
    pub enabled: bool,
    pub s3: S3Config,
    /// Prefer running pg_basebackup against a replica (reduces primary load).
    /// Falls back to the primary if no healthy replica is available.
    #[serde(default = "default_prefer_replica")]
    pub prefer_replica: bool,
    /// One entry per frequency tier. At least one entry is required.
    pub schedule: Vec<BackupScheduleEntry>,
}

fn default_backup_enabled() -> bool {
    true
}
fn default_prefer_replica() -> bool {
    true
}

// ── Load helper ───────────────────────────────────────────────────────────────

impl PgClusterConfig {
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("cannot read config {}: {}", path, e))?;
        let cfg: Self = toml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("invalid config {}: {}", path, e))?;
        validate::validate(&cfg)?;
        Ok(cfg)
    }

    /// Returns the Postgres address for a given node ID.
    pub fn postgres_addr(&self, node_id: &str) -> Option<String> {
        self.nodes
            .node
            .iter()
            .find(|n| n.id == node_id)
            .map(|n| n.postgres_addr.clone())
    }

    /// Returns the vk-agent gRPC address for a given node ID.
    pub fn agent_addr(&self, node_id: &str) -> Option<String> {
        self.nodes
            .node
            .iter()
            .find(|n| n.id == node_id)
            .map(|n| n.agent_addr.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_TOML: &str = r#"
[cluster]
name     = "test"
data_dir = "/tmp/pgcluster"

[raft]
node_id   = 1
bootstrap = true
peers     = [
  { id = 1, addr = "127.0.0.1:7000" },
  { id = 2, addr = "127.0.0.1:7001" },
  { id = 3, addr = "127.0.0.1:7002" },
]

[[nodes.node]]
id            = "pg1"
agent_addr    = "127.0.0.1:7010"
postgres_addr = "127.0.0.1:5432"
priority      = 100

[[nodes.node]]
id            = "pg2"
agent_addr    = "127.0.0.1:7011"
postgres_addr = "127.0.0.1:5433"
priority      = 90

[[nodes.node]]
id            = "pg3"
agent_addr    = "127.0.0.1:7012"
postgres_addr = "127.0.0.1:5434"
priority      = 80

[replication]
replication_user = "replicator"

[proxy]
listen_addr        = "0.0.0.0:5432"
admin_listen_addr  = "0.0.0.0:5433"
health_listen_addr = "0.0.0.0:8008"

[failover]
health_check_interval_ms              = 500
health_check_failures_before_failover = 3
agent_heartbeat_timeout_seconds       = 10

[api]
listen_addr = "0.0.0.0:8009"
api_keys    = ["test-key"]
"#;

    #[test]
    fn parses_full_config() {
        let cfg: PgClusterConfig = toml::from_str(VALID_TOML).unwrap();
        assert_eq!(cfg.cluster.name, "test");
        assert_eq!(cfg.raft.node_id, 1);
        assert_eq!(cfg.nodes.node.len(), 3);
        assert_eq!(cfg.nodes.node[0].id, "pg1");
        assert_eq!(cfg.failover.health_check_failures_before_failover, 3);
    }

    #[test]
    fn defaults_applied_for_optional_sections() {
        let cfg: PgClusterConfig = toml::from_str(VALID_TOML).unwrap();
        assert_eq!(cfg.proxy.pool.max_connections_per_db_user, 25);
        assert!(cfg.proxy.read_routing.enabled);
        assert_eq!(cfg.metrics.listen_addr, "0.0.0.0:9190");
        assert!(cfg.tls.auto_generate);
    }

    #[test]
    fn rejects_duplicate_node_ids() {
        let bad = VALID_TOML.replace(r#"id            = "pg3""#, r#"id            = "pg1""#);
        let cfg: PgClusterConfig = toml::from_str(&bad).unwrap();
        assert!(validate::validate(&cfg).is_err());
    }
}
