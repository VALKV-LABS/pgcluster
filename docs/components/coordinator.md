# Component: Coordinator (`coordinator`)

## High-Level Function

The coordinator is a new **operating mode** of the pgcluster binary (`mode = "coordinator"` in config). Instead of managing a single Postgres HA cluster, it manages a **map of shard clusters** — making pgcluster a cluster-manager-of-clusters.

Each shard is a standard M1 pgcluster HA cluster, entirely unchanged. The coordinator holds the global shard map in its own Raft group, exposes a shard resolution API, and runs a connection-hint proxy that routes client connections to the correct shard without any SQL parsing.

---

## Architecture

### Two-Tier Raft

```
Coordinator Raft Group (3 coordinator nodes)
  └── ShardMapStateMachine
        ├── shard-0: key range [0, 9999]  → shard-0 pgcluster proxy addrs
        ├── shard-1: key range [10000, 19999] → shard-1 pgcluster proxy addrs
        └── shard-N: ...

Shard-0 Raft Group (3 pgcluster nodes)   ← standard M1, unmodified
  └── ClusterTopology (pg1=primary, pg2/pg3=replicas)

Shard-1 Raft Group (3 pgcluster nodes)   ← standard M1, unmodified
  └── ClusterTopology (pg4=primary, pg5/pg6=replicas)
```

The two tiers are completely independent. The coordinator doesn't participate in per-shard Raft, and per-shard pgcluster nodes don't know the coordinator exists. Per-shard failover runs autonomously.

### Connection-Hint Routing (no SQL parsing)

```
Client connects to coordinator:5432
  │
  ├── Startup handshake (coordinator proxies auth to shard once resolved)
  │
  ├── Waits for: SET pgcluster.shard_key = '12345'
  │     ↓
  │   Resolve: 12345 → range [0, 9999] → shard-0
  │     ↓
  │   Pipe: all subsequent bytes ↔ shard-0 pgcluster proxy
  │
  └── OR: client skips coordinator and uses shard resolution API directly
         GET /api/v1/shards/resolve?key=12345 → { "proxy_addr": "shard0:5432" }
         then connects to shard0:5432 directly
```

### Sharding Strategies

```rust
pub enum ShardingStrategy {
    /// key range → shard (key is a numeric user-supplied value)
    Range,
    /// murmur3(key) % num_shards → shard (key can be any string/int)
    Hash { buckets: u32 },
}
```

### Global Tables

Tables that are small, rarely written, and needed on every shard (config, feature flags, reference data):

```
Write to global table → coordinator broadcasts INSERT/UPDATE/DELETE to all shards
Read from global table → served from whichever shard the client is connected to
```

---

## Detailed Implementation Plan

### 1. Module Layout

```
pgcluster/src/
  coordinator/
    mod.rs          # CoordinatorServer: entry point when config.mode="coordinator"
    shard_map.rs    # ShardMap, ShardDef, KeyRange, ShardingStrategy
    commands.rs     # ShardMapCommand: AddShard, RemoveShard, UpdateShardState
    state_machine.rs# ShardMapStateMachine: openraft RaftStateMachine<ShardMapCommand>
    resolver.rs     # resolve(key, strategy) → &ShardDef
    proxy.rs        # ShardProxy: SET interception + connection pipe to shard
    broadcaster.rs  # GlobalTableBroadcaster: fan-out writes to all shard proxies
    health.rs       # Poll each shard's /api/v1/cluster/status; aggregate
    api.rs          # Shard-specific REST handlers (mounted on main axum router)
```

### 2. ShardMap State Machine

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardMap {
    pub keyspace_name: String,
    pub strategy: ShardingStrategy,
    pub shards: Vec<ShardDef>,
    pub global_tables: Vec<String>,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardDef {
    pub shard_id: String,
    /// Range strategy: [range_low, range_high)
    pub range_low: Option<i64>,
    pub range_high: Option<i64>,
    /// Hash strategy: which bucket(s) this shard owns
    pub hash_buckets: Vec<u32>,
    /// Addresses of the 3 pgcluster proxy nodes for this shard
    pub pgcluster_proxy_addrs: Vec<String>,
    /// Addresses of the 3 pgcluster API nodes for this shard
    pub pgcluster_api_addrs: Vec<String>,
    pub state: ShardState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ShardState {
    Serving,
    Offline,
    // Draining,  // M5-B resharding: transition state
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ShardMapCommand {
    AddShard(ShardDef),
    RemoveShard { shard_id: String },
    UpdateShardState { shard_id: String, state: ShardState },
}

pub struct ShardMapStateMachine {
    pub state: Arc<RwLock<ShardMap>>,
}

impl openraft::RaftStateMachine<ShardMapCommand> for ShardMapStateMachine {
    async fn apply(&mut self, entries: &[Entry<ShardMapCommand>]) -> Vec<()> {
        let mut map = self.state.write().await;
        for entry in entries {
            if let EntryPayload::Normal(cmd) = &entry.payload {
                match cmd {
                    ShardMapCommand::AddShard(def) => {
                        map.shards.retain(|s| s.shard_id != def.shard_id);
                        map.shards.push(def.clone());
                    }
                    ShardMapCommand::RemoveShard { shard_id } => {
                        map.shards.retain(|s| &s.shard_id != shard_id);
                    }
                    ShardMapCommand::UpdateShardState { shard_id, state } => {
                        if let Some(s) = map.shards.iter_mut().find(|s| &s.shard_id == shard_id) {
                            s.state = state.clone();
                        }
                    }
                }
                map.version += 1;
            }
        }
        vec![(); entries.len()]
    }
}
```

### 3. Resolver

```rust
pub fn resolve_range(key: i64, map: &ShardMap) -> Option<&ShardDef> {
    map.shards.iter()
        .filter(|s| s.state == ShardState::Serving)
        .find(|s| {
            let lo = s.range_low.unwrap_or(i64::MIN);
            let hi = s.range_high.unwrap_or(i64::MAX);
            key >= lo && key < hi
        })
}

pub fn resolve_hash(key: &str, map: &ShardMap) -> Option<&ShardDef> {
    if let ShardingStrategy::Hash { buckets } = map.strategy {
        let bucket = murmur3_32(key.as_bytes()) % buckets;
        map.shards.iter()
            .filter(|s| s.state == ShardState::Serving)
            .find(|s| s.hash_buckets.contains(&bucket))
    } else { None }
}
```

### 4. Connection-Hint Proxy

```rust
pub struct ShardProxy {
    listener: TcpListener,             // coordinator:5432
    shard_map: Arc<RwLock<ShardMap>>,
}

impl ShardProxy {
    pub async fn handle_connection(&self, mut client: TcpStream) {
        // 1. Speak enough PG wire protocol to accept the client startup
        //    and wait for a SET pgcluster.shard_key message
        let mut buf = [0u8; 4096];
        loop {
            let n = client.read(&mut buf).await?;
            let msg = parse_frontend_message(&buf[..n]);

            if let Some(ShardKeySet { key }) = extract_shard_key_set(&msg) {
                // 2. Resolve key → shard
                let map = self.shard_map.read().await;
                let shard = resolve(&key, &map)
                    .ok_or_else(|| anyhow!("no shard for key {}", key))?;

                // 3. Pick a healthy proxy addr (round-robin or least-conn)
                let backend_addr = pick_proxy_addr(&shard.pgcluster_proxy_addrs);

                // 4. Pipe bytes bidirectionally
                let mut backend = TcpStream::connect(backend_addr).await?;
                tokio::io::copy_bidirectional(&mut client, &mut backend).await?;
                return;
            }
            // Forward non-shard-key messages back as an error or buffer them
        }
    }
}
```

### 5. Shard-Specific API Endpoints

Mounted alongside the standard M1 API endpoints on the coordinator's axum router:

```
GET  /api/v1/shards                       → list all shards with ranges + health
GET  /api/v1/shards/{id}                  → single shard status (proxied from shard API)
GET  /api/v1/shards/resolve?key=N         → resolve key → { shard_id, proxy_addr }
POST /api/v1/shards                       → add a shard (committed to Raft)
DELETE /api/v1/shards/{id}                → remove a shard (must be empty / drained)
PUT  /api/v1/shards/{id}/state            → set state: Serving | Offline
```

```rust
// GET /api/v1/shards/resolve?key=12345
async fn resolve_shard(
    Query(params): Query<ResolveParams>,
    State(state): State<Arc<CoordinatorState>>,
) -> Json<ResolveResponse> {
    let map = state.shard_map.read().await;
    let shard = resolve_range(params.key, &map)
        .or_else(|| resolve_hash(&params.key.to_string(), &map));
    match shard {
        Some(s) => Json(ResolveResponse {
            shard_id: s.shard_id.clone(),
            proxy_addr: pick_proxy_addr(&s.pgcluster_proxy_addrs).to_string(),
            range_low: s.range_low,
            range_high: s.range_high,
        }),
        None => /* 404 */ todo!(),
    }
}
```

### 6. Global Table Broadcaster

```rust
pub struct GlobalTableBroadcaster {
    shard_map: Arc<RwLock<ShardMap>>,
    // One persistent connection pool per shard proxy
    shard_pools: HashMap<String, PgPool>,
}

impl GlobalTableBroadcaster {
    // Called when coordinator intercepts a DML on a global table
    pub async fn broadcast(&self, sql: &str) -> Result<()> {
        let map = self.shard_map.read().await;
        let futs = map.shards.iter()
            .filter(|s| s.state == ShardState::Serving)
            .map(|s| {
                let pool = self.shard_pools[&s.shard_id].clone();
                let sql = sql.to_string();
                async move { sqlx::query(&sql).execute(&pool).await }
            });
        // Fire all in parallel; fail if any shard rejects
        futures::future::try_join_all(futs).await?;
        Ok(())
    }
}
```

### 7. Coordinator Config (`pgcluster.toml` coordinator mode)

```toml
[cluster]
name    = "prod-coordinator"
mode    = "coordinator"          # "cluster" (default) | "coordinator"
data_dir = "/var/lib/pgcluster-coord"

[raft]
node_id = 1
peers = [
  { id = 1, addr = "coord1:7000" },
  { id = 2, addr = "coord2:7000" },
  { id = 3, addr = "coord3:7000" },
]

[coordinator]
proxy_listen_addr   = "0.0.0.0:5432"
api_listen_addr     = "0.0.0.0:8009"
metrics_listen_addr = "0.0.0.0:9190"
sharding_strategy   = "range"        # "range" | "hash"

[[coordinator.shard]]
id                    = "shard-0"
range_low             = 0
range_high            = 10000        # [0, 10000)
pgcluster_proxy_addrs = ["10.0.1.1:5432", "10.0.1.2:5432", "10.0.1.3:5432"]
pgcluster_api_addrs   = ["10.0.1.1:8009", "10.0.1.2:8009", "10.0.1.3:8009"]

[[coordinator.shard]]
id                    = "shard-1"
range_low             = 10000
range_high            = 20000        # [10000, 20000)
pgcluster_proxy_addrs = ["10.0.2.1:5432", "10.0.2.2:5432", "10.0.2.3:5432"]
pgcluster_api_addrs   = ["10.0.2.1:8009", "10.0.2.2:8009", "10.0.2.3:8009"]

[coordinator.global_tables]
tables = ["config", "feature_flags", "migrations"]

[tls]
ca_cert = "/etc/pgcluster/ca.crt"
cert    = "/etc/pgcluster/server.crt"
key     = "/etc/pgcluster/server.key"

[api]
api_keys = ["ops-key-1"]
```

### 8. CLI Additions

```bash
# Start coordinator mode
pgcluster coordinator --config /etc/pgcluster/coordinator.toml

# Shard management
pgcluster shard list
pgcluster shard add   --id shard-2 --range-low 20000 --range-high 30000 \
                      --proxy-addrs 10.0.3.1:5432,10.0.3.2:5432,10.0.3.3:5432 \
                      --api-addrs   10.0.3.1:8009,10.0.3.2:8009,10.0.3.3:8009
pgcluster shard remove shard-2
pgcluster shard status shard-1   # shows per-shard primary, replicas, lag

# Resolve a key
pgcluster shard resolve --key 12345
# → shard-1  proxy=10.0.1.1:5432  range=[10000,20000)
```

---

## Integration Points

- Reuses `raft/network.rs` and `raft/storage.rs` from M1 unchanged (different state machine only).
- Reuses `tls/` from M1 for Raft peer and client TLS.
- Reuses `api/` axum router from M1; shard endpoints added via `coordinator/api.rs`.
- Per-shard pgcluster clusters are **completely unmodified M1** — the coordinator is a consumer of their existing REST and proxy interfaces.
- `config_manager` extended with `CoordinatorConfig` struct and `mode` field.
- `ShardMapStateMachine` runs instead of `TopologyStateMachine` in coordinator mode.
