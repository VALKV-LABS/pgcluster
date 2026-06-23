# Component: Topology Store (`topology_store`)

## High-Level Function

The topology store is the replicated state that the Raft consensus module keeps synchronized across all pgcluster instances. It is the single source of truth for the cluster's configuration and current state. Every component that needs to know "who is the primary" reads from the topology store rather than querying Postgres or vk-agents directly.

---

## Architecture

The topology store is the **state machine** in the Raft log. When the Raft leader commits a `TopologyCommand`, every pgcluster instance applies it to its local copy of the topology. All reads are local (no network hop). Writes go through Raft (linearizable).

### Contents

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterTopology {
    /// Cluster name (informational)
    pub cluster_name: String,
    /// Which node is currently primary
    pub primary_node_id: String,
    /// Role of each node
    pub node_roles: HashMap<String, NodeRole>,
    /// Replication slot name per replica
    pub replica_slots: HashMap<String, String>,
    /// primary_conninfo string for each replica
    pub primary_conninfos: HashMap<String, String>,
    /// Last confirmed flush_lsn per node (updated every health check)
    pub last_flush_lsns: HashMap<String, u64>,
    /// Last confirmed replay_lsn per node
    pub last_replay_lsns: HashMap<String, u64>,
    /// Lag in bytes per replica (primary_lsn - replica_flush_lsn)
    pub replica_lag_bytes: HashMap<String, u64>,
    /// Node configuration (addresses, priority — immutable once added)
    pub node_configs: HashMap<String, NodeConfig>,
    /// Failover history (last 10 events)
    pub failover_history: VecDeque<FailoverEvent>,
    /// Config version (incremented on every topology change)
    pub version: u64,
    /// Timestamp of last topology change
    pub last_changed_at: i64,   // Unix seconds
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeRole { Primary, Replica, Offline, Maintenance, Unknown }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub node_id: String,
    pub agent_addr: String,       // vk-agent gRPC address
    pub postgres_addr: String,    // Postgres TCP address
    pub priority: u32,            // Higher = preferred primary candidate
    pub tags: HashMap<String, String>,  // e.g. {"region": "us-east-1", "az": "a"}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailoverEvent {
    pub old_primary: String,
    pub new_primary: String,
    pub triggered_at: i64,
    pub duration_ms: u64,
    pub reason: String,  // "health_check_failure" | "operator_switchover"
}
```

### Read Access Pattern

All reads from the topology are local — no Raft consensus round-trip needed:

```rust
pub struct TopologyStore {
    state: Arc<RwLock<ClusterTopology>>,
}

impl TopologyStore {
    pub fn current_primary_addr(&self) -> Option<String> {
        let t = self.state.read().unwrap();
        t.node_configs.get(&t.primary_node_id).map(|c| c.postgres_addr.clone())
    }

    pub fn replica_addrs_within_lag(&self, max_lag_bytes: u64) -> Vec<String> {
        let t = self.state.read().unwrap();
        let primary_lsn = t.last_flush_lsns.get(&t.primary_node_id).copied().unwrap_or(0);
        t.node_roles.iter()
            .filter(|(_, role)| **role == NodeRole::Replica)
            .filter(|(id, _)| {
                let lag = primary_lsn.saturating_sub(
                    t.last_flush_lsns.get(*id).copied().unwrap_or(0)
                );
                lag <= max_lag_bytes
            })
            .filter_map(|(id, _)| t.node_configs.get(id).map(|c| c.postgres_addr.clone()))
            .collect()
    }

    pub fn best_failover_candidate(&self, failed_id: &str) -> Option<String> {
        let t = self.state.read().unwrap();
        let mut candidates: Vec<_> = t.node_roles.iter()
            .filter(|(id, role)| **role == NodeRole::Replica && *id != failed_id)
            .collect();
        candidates.sort_by(|(id_a, _), (id_b, _)| {
            let lsn_a = t.last_flush_lsns.get(*id_a).copied().unwrap_or(0);
            let lsn_b = t.last_flush_lsns.get(*id_b).copied().unwrap_or(0);
            let pri_a = t.node_configs.get(*id_a).map(|c| c.priority).unwrap_or(0);
            let pri_b = t.node_configs.get(*id_b).map(|c| c.priority).unwrap_or(0);
            lsn_b.cmp(&lsn_a).then(pri_b.cmp(&pri_a))
        });
        candidates.first().map(|(id, _)| (*id).clone())
    }
}
```

### Change Notification

Components that need to react to topology changes (proxy_layer, connection_pool) subscribe via a broadcast channel:

```rust
pub struct TopologyWatch {
    rx: tokio::sync::watch::Receiver<Arc<ClusterTopology>>,
}

impl TopologyWatch {
    pub async fn wait_for_primary_change(&mut self) -> Arc<ClusterTopology> {
        self.rx.changed().await.ok();
        self.rx.borrow().clone()
    }
}
```

The Raft state machine's `apply()` method sends on the watch channel after each topology update.

### Persistence

The topology is persisted through Raft log snapshots (see `raft_consensus.md`). At startup, the Raft state machine restores the last snapshot and replays any log entries not yet snapshotted. There is no separate topology file — Raft is the source of truth.

### Integration Points

- `raft_consensus` is the write path; all topology changes go through `raft.propose(TopologyCommand)`.
- `proxy_layer` reads `current_primary_addr()` and `replica_addrs_within_lag()` on every routing decision.
- `failover_engine` calls `best_failover_candidate()` to select who to promote.
- `node_monitor` proposes `UpdateFlushLsn` on every health check.
- `rest_api` reads the full `ClusterTopology` to serve `/api/v1/cluster/status`.
- `connection_pool` subscribes to `TopologyWatch` to drain pools when primary changes.

---

## M5-A: ShardMap (Coordinator Mode)

In `mode = "coordinator"`, the Raft state machine stores a `ShardMap` instead of `ClusterTopology`. The `ShardMapStateMachine` is a parallel implementation that reuses the same Raft infrastructure (network, storage, log) but with a different state type and command set.

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardMap {
    pub keyspace_name: String,
    pub strategy: ShardingStrategy,   // Range | Hash { buckets }
    pub shards: Vec<ShardDef>,
    pub global_tables: Vec<String>,   // tables broadcast to all shards
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardDef {
    pub shard_id: String,
    pub range_low: Option<i64>,
    pub range_high: Option<i64>,
    pub hash_buckets: Vec<u32>,
    pub pgcluster_proxy_addrs: Vec<String>,
    pub pgcluster_api_addrs: Vec<String>,
    pub state: ShardState,            // Serving | Offline
}
```

`ShardMapCommand` variants: `AddShard`, `RemoveShard`, `UpdateShardState`. Full implementation: see [coordinator.md](coordinator.md).
