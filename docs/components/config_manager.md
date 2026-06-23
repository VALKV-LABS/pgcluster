# Component: Config Manager (`config_manager`)

## High-Level Function

The config manager loads, validates, and hot-reloads `pgcluster.toml`. It provides a typed, validated view of all configuration to every other component. Config changes that don't require a restart (pool sizes, timeouts, lag thresholds) can be applied without downtime via `pgcluster config reload`.

---

## Architecture

### Config Sections

| Section | Scope | Hot-reloadable |
|---------|-------|---------------|
| `[cluster]` | Cluster identity | No |
| `[raft]` | Raft peers, timeouts | No (restart required) |
| `[nodes]` | Node addresses, priorities | Via API (`pgcluster node add/remove`) |
| `[replication]` | User, slot config | Yes |
| `[proxy]` | Proxy listen addr, pool sizes, read routing | Partial (pool sizes yes; listen addr no) |
| `[failover]` | Health check intervals, thresholds | Yes |
| `[tls]` | Cert paths | Yes (triggers cert reload) |
| `[metrics]` | Metrics listen addr | No |
| `[api]` | API listen addr, API keys | Yes (keys only) |

---

## Detailed Implementation Plan

### 1. Module Layout

```
src/
  config/
    mod.rs        # PgClusterConfig struct, load(), validate()
    reload.rs     # ConfigReloader — watch file for changes, apply hot-reload
    schema.rs     # All config sub-structs with serde + validation
```

### 2. Config Struct

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct PgClusterConfig {
    pub cluster: ClusterConfig,
    pub raft: RaftConfig,
    pub nodes: NodesConfig,
    pub replication: ReplicationConfig,
    pub proxy: ProxyConfig,
    pub failover: FailoverConfig,
    pub tls: TlsConfig,
    pub metrics: MetricsConfig,
    pub api: ApiConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClusterConfig {
    pub name: String,
    pub data_dir: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RaftConfig {
    pub node_id: u64,
    pub peers: Vec<RaftPeer>,
    pub heartbeat_interval_ms: u64,     // default: 150
    pub election_timeout_ms: u64,        // default: 500
}

#[derive(Debug, Clone, Deserialize)]
pub struct RaftPeer {
    pub id: u64,
    pub addr: String,   // e.g. "10.0.0.1:7000"
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodesConfig {
    pub node: Vec<NodeEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeEntry {
    pub id: String,
    pub agent_addr: String,
    pub postgres_addr: String,
    pub priority: u32,
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReplicationConfig {
    pub replication_user: String,
    pub replication_password_env: String,
    pub slot_prefix: String,            // default: "pgcluster_"
    #[serde(default)]
    pub synchronous_standby_names: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProxyConfig {
    pub listen_addr: String,            // default: "0.0.0.0:5432"
    pub admin_listen_addr: String,      // default: "0.0.0.0:5433"
    pub health_listen_addr: String,     // default: "0.0.0.0:8008"
    pub pool: PoolConfig,
    pub read_routing: ReadRoutingConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PoolConfig {
    pub max_connections_per_db_user: usize,  // default: 25
    pub idle_timeout_seconds: u64,           // default: 600
    pub connect_timeout_seconds: u64,        // default: 5
    pub keepalive_interval_seconds: u64,     // default: 60
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReadRoutingConfig {
    pub enabled: bool,
    pub max_replica_lag_bytes: u64,     // default: 10MB
}

#[derive(Debug, Clone, Deserialize)]
pub struct FailoverConfig {
    pub health_check_interval_ms: u64,          // default: 500
    pub health_check_failures_before_failover: u32, // default: 3
    pub agent_heartbeat_timeout_seconds: u64,   // default: 10
    pub promote_timeout_seconds: u64,           // default: 30
    pub repoint_timeout_seconds: u64,           // default: 60
    pub drain_timeout_ms: u64,                  // default: 30_000
    pub lag_wait_timeout_ms: u64,               // default: 60_000
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApiConfig {
    pub listen_addr: String,            // default: "0.0.0.0:8009"
    pub api_keys: Vec<String>,
    pub public_status: bool,            // Allow unauthenticated status reads
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    pub listen_addr: String,            // default: "0.0.0.0:9190"
}
```

### 3. Load and Validate

```rust
impl PgClusterConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Cannot read config file: {:?}", path))?;
        let config: PgClusterConfig = toml::from_str(&content)
            .with_context(|| "Config parse error")?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        // Raft requires odd number of voters >= 3 for meaningful quorum
        if self.raft.peers.len() < 3 {
            return Err(anyhow::anyhow!(
                "raft.peers must have at least 3 entries for quorum (got {})",
                self.raft.peers.len()
            ));
        }
        if self.raft.peers.len() % 2 == 0 {
            log::warn!("Even number of Raft peers ({}) — recommend odd number for faster elections",
                self.raft.peers.len());
        }
        if self.nodes.node.is_empty() {
            return Err(anyhow::anyhow!("nodes.node must have at least 1 entry"));
        }
        // Replication password must be in environment
        if std::env::var(&self.replication.replication_password_env).is_err() {
            return Err(anyhow::anyhow!(
                "Environment variable '{}' not set (required for replication password)",
                self.replication.replication_password_env
            ));
        }
        Ok(())
    }
}
```

### 4. Hot Reload

```rust
pub struct ConfigReloader {
    path: PathBuf,
    current: Arc<RwLock<PgClusterConfig>>,
    tls: Arc<TlsManager>,
    pool: Arc<ConnectionPool>,
}

impl ConfigReloader {
    pub async fn reload(&self) -> Result<()> {
        let new_config = PgClusterConfig::load(&self.path)?;
        let old_config = self.current.read().await.clone();

        // Apply hot-reloadable changes
        if new_config.tls != old_config.tls {
            self.tls.reload()?;
        }
        if new_config.proxy.pool.max_connections_per_db_user
            != old_config.proxy.pool.max_connections_per_db_user
        {
            self.pool.update_max_per_db_user(new_config.proxy.pool.max_connections_per_db_user);
        }

        *self.current.write().await = new_config;
        log::info!("Config reloaded from {:?}", self.path);
        Ok(())
    }
}
```

### 5. Integration Points

- All components receive `Arc<RwLock<PgClusterConfig>>` at startup and read it on demand.
- `rest_api` calls `config_reloader.reload()` on `POST /api/v1/config/reload`.
- `tls_manager` is notified on TLS config changes.
- `connection_pool` is updated on pool size changes.
- `pgcluster config validate` loads and validates without starting any services.
