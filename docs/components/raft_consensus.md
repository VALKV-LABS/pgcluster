# Component: Raft Consensus (`raft_consensus`)

## High-Level Function

The Raft consensus module is the brain of pgcluster. It maintains agreement across all 3 pgcluster instances on the single most critical fact: **who is the current primary Postgres node**. All failover decisions flow from the Raft leader — no distributed decision-making, no split-brain.

pgcluster uses the `openraft` crate (a battle-tested Raft implementation in Rust) rather than building Raft from scratch.

---

## Architecture

### Why Raft Instead of External DCS

Patroni externalizes consensus to etcd/ZooKeeper/Consul. This means:
- Three processes to manage instead of one
- etcd failure = Patroni cannot make any decisions (even if Postgres is healthy)
- Network partition between Patroni and etcd stalls the entire system

pgcluster embeds Raft directly. The pgcluster binary **is** the consensus node. No external process needed.

### Raft State Machine

The replicated state machine stores the cluster topology — the ground truth that all pgcluster instances agree on:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterTopology {
    /// Which node is currently primary
    pub primary_node_id: String,
    /// Role of each node
    pub node_roles: HashMap<String, NodeRole>,
    /// Replication slot names for each replica
    pub replica_slots: HashMap<String, String>,
    /// primary_conninfo string for each replica
    pub primary_conninfos: HashMap<String, String>,
    /// Last confirmed flush_lsn per node (for replica candidate ranking)
    pub last_flush_lsns: HashMap<String, u64>,
    /// Config version (incremented on every topology change)
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeRole { Primary, Replica, Offline, Unknown }
```

### Raft Log Entries (Commands)

Every topology change is a Raft log entry:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TopologyCommand {
    /// Declare a node as primary (after promotion)
    SetPrimary { node_id: String, at_lsn: u64, new_tli: u32 },
    /// Mark a node as offline (failed health checks)
    MarkOffline { node_id: String },
    /// Mark a node as online replica
    MarkReplica { node_id: String, flush_lsn: u64 },
    /// Update primary_conninfo for a replica
    UpdatePrimaryConninfo { node_id: String, conninfo: String },
    /// Register a new node
    AddNode { node_id: String, agent_addr: String, postgres_addr: String, priority: u32 },
    /// Remove a node
    RemoveNode { node_id: String },
    /// Update flush LSN from health check
    UpdateFlushLsn { node_id: String, flush_lsn: u64 },
}
```

### Leader vs Follower Roles

Only the **Raft leader** pgcluster instance:
- Runs the failover engine (makes promotion decisions)
- Runs the switchover engine
- Executes node monitor health checks that trigger topology changes
- Sends commands to vk-agents

All pgcluster instances:
- Proxy client connections (any instance can proxy; all read topology from local Raft state)
- Serve the REST API (followers forward mutation requests to leader)
- Expose the health endpoint

---

## Detailed Implementation Plan

### 1. Module Layout

```
src/
  raft/
    mod.rs           # RaftNode — openraft setup, start(), propose()
    state_machine.rs # TopologyStateMachine — apply() Raft log entries
    storage.rs       # RaftStorage — persist Raft log to local disk (sled or flat files)
    network.rs       # RaftNetwork — gRPC transport between Raft peers
    topology.rs      # ClusterTopology struct, NodeRole enum
    commands.rs      # TopologyCommand enum
```

### 2. openraft Setup

```rust
use openraft::{Config, Raft, RaftMetrics};

pub type PgClusterRaft = Raft<TopologyCommand, TopologyStateMachine, RaftNetwork, RaftStorage>;

pub async fn start_raft_node(
    config: &RaftConfig,
    network: RaftNetwork,
    storage: RaftStorage,
) -> Result<Arc<PgClusterRaft>> {
    let raft_config = Arc::new(Config {
        heartbeat_interval: config.heartbeat_interval_ms,
        election_timeout_min: config.election_timeout_ms,
        election_timeout_max: config.election_timeout_ms * 2,
        ..Default::default()
    });

    let raft = Raft::new(
        config.node_id,
        raft_config,
        network,
        storage,
    ).await?;

    // Bootstrap if first node, or join existing cluster
    if config.bootstrap {
        raft.initialize(BTreeMap::from([(config.node_id, config.peers[0].clone())])).await?;
    }

    Ok(Arc::new(raft))
}
```

### 3. State Machine Apply

```rust
pub struct TopologyStateMachine {
    pub state: Arc<RwLock<ClusterTopology>>,
}

impl openraft::RaftStateMachine<TopologyCommand> for TopologyStateMachine {
    async fn apply(&mut self, entries: &[Entry<TopologyCommand>]) -> Vec<()> {
        let mut topology = self.state.write().await;
        for entry in entries {
            match &entry.payload {
                EntryPayload::Normal(cmd) => {
                    topology.apply_command(cmd);
                    topology.version += 1;
                }
                _ => {}
            }
        }
        vec![(); entries.len()]
    }
}

impl ClusterTopology {
    fn apply_command(&mut self, cmd: &TopologyCommand) {
        match cmd {
            TopologyCommand::SetPrimary { node_id, at_lsn, new_tli } => {
                // Demote old primary to Unknown, set new
                if let Some(old) = &self.primary_node_id.clone().into() {
                    self.node_roles.insert(old, NodeRole::Unknown);
                }
                self.primary_node_id = node_id.clone();
                self.node_roles.insert(node_id.clone(), NodeRole::Primary);
            }
            TopologyCommand::MarkOffline { node_id } => {
                self.node_roles.insert(node_id.clone(), NodeRole::Offline);
            }
            TopologyCommand::MarkReplica { node_id, flush_lsn } => {
                self.node_roles.insert(node_id.clone(), NodeRole::Replica);
                self.last_flush_lsns.insert(node_id.clone(), *flush_lsn);
            }
            TopologyCommand::UpdateFlushLsn { node_id, flush_lsn } => {
                self.last_flush_lsns.insert(node_id.clone(), *flush_lsn);
            }
            TopologyCommand::UpdatePrimaryConninfo { node_id, conninfo } => {
                self.primary_conninfos.insert(node_id.clone(), conninfo.clone());
            }
            TopologyCommand::AddNode { node_id, .. } => {
                self.node_roles.insert(node_id.clone(), NodeRole::Unknown);
            }
            TopologyCommand::RemoveNode { node_id } => {
                self.node_roles.remove(node_id);
                self.last_flush_lsns.remove(node_id);
            }
        }
    }
}
```

### 4. Raft Storage (Log Persistence)

Raft log entries must survive pgcluster restarts. Stored in a local directory (`data_dir/raft/`):

```rust
pub struct RaftStorage {
    log_dir: PathBuf,
    log: sled::Db,          // sled embedded KV for log entries
    vote: sled::Db,         // current vote (persisted)
    snapshot: Option<ClusterTopology>,
}
```

Snapshots are taken when the log exceeds `max_log_entries` (default 10,000) to prevent unbounded growth.

### 5. Raft Network (gRPC Transport)

```rust
// Defined in proto/raft.proto:
// service RaftService {
//   rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
//   rpc RequestVote(VoteRequest) returns (VoteResponse);
//   rpc InstallSnapshot(SnapshotRequest) returns (SnapshotResponse);
// }

pub struct RaftNetwork {
    peers: HashMap<u64, RaftServiceClient<Channel>>,
}

impl openraft::RaftNetwork<TopologyCommand> for RaftNetwork {
    async fn send_append_entries(&mut self, target: u64, req: AppendEntriesRequest<TopologyCommand>)
        -> Result<AppendEntriesResponse>
    {
        let client = self.peers.get_mut(&target).unwrap();
        client.append_entries(req.into()).await?.into_inner().try_into()
    }
    // ... vote, snapshot similarly
}
```

### 6. Proposing a Topology Change

Any component (failover engine, node monitor, REST API) proposes changes through:

```rust
pub async fn propose(&self, cmd: TopologyCommand) -> Result<()> {
    // Only the Raft leader can commit entries
    // If this node is a follower, it must forward to the leader
    let leader_id = self.raft.current_leader().await;
    if leader_id == Some(self.node_id) {
        self.raft.client_write(ClientWriteRequest::new(EntryPayload::Normal(cmd))).await?;
    } else {
        // Forward to leader via gRPC
        self.forward_to_leader(cmd).await?;
    }
    Ok(())
}
```

### 7. Integration Points

- `node_monitor` calls `propose(UpdateFlushLsn)` on every health check cycle.
- `failover_engine` calls `propose(MarkOffline)` then `propose(SetPrimary)` during failover.
- `proxy_layer` reads topology from `TopologyStateMachine::state` (local read, no network hop).
- `switchover_engine` proposes `SetPrimary` after the zero-lag wait completes.
- `rest_api` forwards mutation requests to the Raft leader; serves read-only status from local state.

---

## M5-A: Coordinator Mode

In coordinator mode (`config.mode = "coordinator"`), the same Raft infrastructure (network, storage, log persistence, snapshot) is reused with a different state machine:

```
Cluster mode:     Raft → TopologyStateMachine  → ClusterTopology
Coordinator mode: Raft → ShardMapStateMachine  → ShardMap
```

`ShardMapStateMachine` implements the same `openraft::RaftStateMachine` trait. The network layer (`RaftNetwork`, gRPC transport) and storage layer (`RaftStorage`, sled-backed) are identical — only the applied state and command types differ.

The coordinator runs its own independent Raft group. It does not join or participate in any per-shard Raft group. Full implementation: see [coordinator.md](coordinator.md).
