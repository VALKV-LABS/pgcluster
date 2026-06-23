# Component: CLI (`pgcluster` binary)

## High-Level Function

The `pgcluster` binary serves as both the server daemon and the operator CLI tool, using subcommands to distinguish the two modes. This follows the pattern of tools like `etcd` / `etcdctl` shipped as one binary.

---

## Command Reference

### Server Mode

```bash
pgcluster server --config /etc/pgcluster/pgcluster.toml
```

Starts the pgcluster daemon (Raft + proxy + REST API + health endpoint). This is what runs in production, typically as a systemd unit.

### Cluster Status

```bash
pgcluster status
pgcluster status --format json

# Output:
Cluster: prod-cluster
Raft leader: pg1 (this node)
Primary:     pg1  (10.0.0.1:5432)  LSN 0/4A00000  Connections: 42

Replicas:
  pg2  (10.0.0.2:5432)  lag=2KB    lag=120ms  ✓ streaming
  pg3  (10.0.0.3:5432)  OFFLINE    last seen 45s ago
```

### Switchover

```bash
pgcluster switchover pg2              # Switch primary to pg2
pgcluster switchover pg2 --dry-run   # Show what would happen, don't execute
pgcluster switchover pg2 --wait      # Block until complete (default: background)
```

### Node Management

```bash
pgcluster node add   --id pg4 --agent 10.0.0.4:7001 --postgres 10.0.0.4:5432 --priority 70
pgcluster node remove pg4
pgcluster node maintenance pg3 --enable   # Take pg3 out of failover candidate pool
pgcluster node maintenance pg3 --disable
```

### Replication

```bash
pgcluster replication lag        # Show lag for all replicas
pgcluster replication slots      # Show replication slot state

# Output:
Replication Lag:
  pg2  flush=0/49FF800  lag=2048 bytes  lag=120ms  slot=pgcluster_pg2  active=true
  pg3  OFFLINE          slot=pgcluster_pg3  retained_wal=1.2GB (DANGER)
```

### Config

```bash
pgcluster config show            # Print current running config
pgcluster config reload          # Reload config from file (non-restart fields)
pgcluster config validate        # Validate pgcluster.toml without starting
```

### Failover (manual)

```bash
pgcluster failover               # Let pgcluster pick best candidate
pgcluster failover --target pg2  # Force specific target
```

### Init (bootstrap)

```bash
pgcluster init --config /etc/pgcluster/pgcluster.toml
# Creates Raft data directory, generates TLS certs, validates Postgres connectivity
```

### Coordinator Mode (M5-A)

```bash
# Start coordinator daemon
pgcluster coordinator --config /etc/pgcluster/coordinator.toml

# List all shards and their health
pgcluster shard list
# Output:
# SHARD     STRATEGY  RANGE          STATE    PRIMARY  LAG
# shard-0   range     [0, 10000)     serving  pg1      1KB
# shard-1   range     [10000,20000)  serving  pg4      512B

# Resolve a shard key to a proxy address
pgcluster shard resolve --key 12345
# → shard-1  proxy=10.0.2.1:5432  range=[10000, 20000)

# Add a new shard
pgcluster shard add \
  --id shard-2 \
  --range-low 20000 --range-high 30000 \
  --proxy-addrs 10.0.3.1:5432,10.0.3.2:5432,10.0.3.3:5432 \
  --api-addrs   10.0.3.1:8009,10.0.3.2:8009,10.0.3.3:8009

# Take a shard offline before removing
pgcluster shard set-state shard-2 --state offline
pgcluster shard remove shard-2

# Per-shard status (proxied from shard's own API)
pgcluster shard status shard-1
# Output identical to `pgcluster status` but for shard-1's cluster
```

---

## Detailed Implementation Plan

### 1. Module Layout

```
src/
  cli/
    mod.rs         # Clap root command, dispatch to subcommands
    server.rs      # pgcluster server subcommand
    status.rs      # pgcluster status subcommand
    switchover.rs  # pgcluster switchover subcommand
    failover.rs    # pgcluster failover subcommand
    node.rs        # pgcluster node subcommands
    replication.rs # pgcluster replication subcommands
    config.rs      # pgcluster config subcommands
    init.rs        # pgcluster init subcommand
    output.rs      # Table/JSON formatting helpers
    client.rs      # HTTP client for REST API calls
```

### 2. Clap Structure

```rust
#[derive(Parser)]
#[command(name = "pgcluster", version, about = "PostgreSQL HA cluster manager")]
pub struct Cli {
    #[arg(long, default_value = "http://localhost:8009")]
    pub api_url: String,
    #[arg(long, env = "PGCLUSTER_API_KEY")]
    pub api_key: Option<String>,
    #[arg(long, default_value = "table")]
    pub format: OutputFormat,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Start the pgcluster daemon
    Server { #[arg(long)] config: PathBuf },
    /// Show cluster status
    Status,
    /// Trigger a planned switchover
    Switchover { target: String, #[arg(long)] dry_run: bool },
    /// Trigger failover to a replica
    Failover { #[arg(long)] target: Option<String> },
    /// Manage cluster nodes
    Node {
        #[command(subcommand)]
        cmd: NodeCommand,
    },
    /// View replication state
    Replication {
        #[command(subcommand)]
        cmd: ReplicationCommand,
    },
    /// Manage configuration
    Config {
        #[command(subcommand)]
        cmd: ConfigCommand,
    },
    /// Initialize pgcluster data directory
    Init { #[arg(long)] config: PathBuf },
}
```

### 3. CLI Client (calls REST API)

All CLI subcommands (except `server`) call the pgcluster REST API:

```rust
pub struct PgClusterClient {
    base_url: String,
    api_key: Option<String>,
    client: reqwest::Client,
}

impl PgClusterClient {
    pub async fn get_status(&self) -> Result<ClusterStatusResponse> {
        self.get("/api/v1/cluster/status").await
    }

    pub async fn switchover(&self, target: &str) -> Result<SwitchoverResponse> {
        self.post("/api/v1/cluster/switchover", json!({ "target": target })).await
    }

    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let mut req = self.client.get(format!("{}{}", self.base_url, path));
        if let Some(key) = &self.api_key {
            req = req.bearer_auth(key);
        }
        Ok(req.send().await?.error_for_status()?.json().await?)
    }
}
```

### 4. Output Formatting

```rust
#[derive(Clone, ValueEnum)]
pub enum OutputFormat { Table, Json, Yaml }

pub fn print_status(status: &ClusterStatusResponse, format: OutputFormat) {
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(status).unwrap()),
        OutputFormat::Table => {
            println!("Cluster: {}", status.cluster_name);
            println!("Primary: {} ({})", status.primary, status.nodes.iter()
                .find(|n| n.id == status.primary)
                .map(|n| n.addr.as_str()).unwrap_or("?"));
            // ... table rendering
        }
        OutputFormat::Yaml => { /* serde_yaml */ }
    }
}
```

### 5. Integration Points

- All operator commands go through the REST API (`api_url`, default `localhost:8009`).
- `pgcluster server` starts the full daemon in-process (no separate binary needed).
- `pgcluster init` generates TLS certs via `tls_manager::generate_self_signed()` and validates `pgcluster.toml`.
- `pgcluster config validate` parses and validates the config without starting any services.
