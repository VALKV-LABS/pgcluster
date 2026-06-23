# Component: Node Monitor (`node_monitor`)

## High-Level Function

The node monitor continuously polls every Postgres node's health via its `vk-agent`. It collects role, LSN, replication lag, and connection count. It feeds this data into two consumers: the Raft topology store (LSN updates) and the failover engine (failure signals). Only the Raft leader runs the active monitoring loop.

---

## Architecture

### Monitoring Loop

```
Every health_check_interval_ms (default 500ms):

For each node in topology:
  │
  ├── Call vk-agent::GetStatus() [gRPC, 1s timeout]
  │     Success → reset failure_count[node]; update flush_lsn in topology
  │     Timeout/Error → increment failure_count[node]
  │
  └── if failure_count[node] >= health_check_failures_before_failover (default 3):
        if node.role == Primary:
          → trigger failover_engine::on_primary_failed(node_id)
        else:
          → propose(MarkOffline { node_id })
```

### Consecutive Failure Logic

Single missed health checks are ignored (transient network blips). Three consecutive failures (default) declare a node dead. At 500ms intervals and 3 failures, detection takes ~1.5s.

```rust
pub struct NodeHealthState {
    pub consecutive_failures: u32,
    pub last_success: Instant,
    pub last_status: Option<StatusResponse>,
}
```

### LSN Tracking

Every successful health check updates `last_flush_lsns` in the topology via Raft. This data is used by the failover engine to pick the most caught-up replica.

---

## Detailed Implementation Plan

### 1. Module Layout

```
src/
  node_monitor/
    mod.rs          # NodeMonitor struct, run() loop
    health_check.rs # Single-node health check via vk-agent gRPC call
    failure_state.rs # Per-node consecutive failure counter
```

### 2. NodeMonitor

```rust
pub struct NodeMonitor {
    topology: Arc<RwLock<ClusterTopology>>,
    raft: Arc<PgClusterRaft>,
    agents: Arc<AgentClients>,
    failover: Arc<FailoverEngine>,
    config: MonitorConfig,
    health_states: HashMap<String, NodeHealthState>,
}

pub struct MonitorConfig {
    pub interval_ms: u64,              // default: 500
    pub failure_threshold: u32,        // default: 3
    pub health_check_timeout_ms: u64,  // default: 1000
}

impl NodeMonitor {
    pub async fn run(&mut self) {
        let mut interval = tokio::time::interval(
            Duration::from_millis(self.config.interval_ms)
        );
        loop {
            interval.tick().await;

            // Only Raft leader runs active monitoring
            if !self.raft.is_leader().await { continue; }

            let topology = self.topology.read().await.clone();
            let checks = topology.node_roles.keys()
                .map(|node_id| self.check_node(node_id.clone()))
                .collect::<Vec<_>>();

            let results = futures::future::join_all(checks).await;
            for result in results {
                self.handle_check_result(result).await;
            }
        }
    }

    async fn check_node(&self, node_id: String) -> NodeCheckResult {
        let timeout = Duration::from_millis(self.config.health_check_timeout_ms);
        match tokio::time::timeout(timeout, self.agents.get_status(&node_id)).await {
            Ok(Ok(status)) => NodeCheckResult::Healthy { node_id, status },
            Ok(Err(e))     => NodeCheckResult::Failed { node_id, reason: e.to_string() },
            Err(_)         => NodeCheckResult::Failed { node_id, reason: "timeout".into() },
        }
    }

    async fn handle_check_result(&mut self, result: NodeCheckResult) {
        match result {
            NodeCheckResult::Healthy { node_id, status } => {
                let state = self.health_states.entry(node_id.clone()).or_default();
                state.consecutive_failures = 0;
                state.last_success = Instant::now();
                state.last_status = Some(status.clone());

                // Propose LSN update to topology
                let _ = self.raft.propose(TopologyCommand::UpdateFlushLsn {
                    node_id,
                    flush_lsn: status.received_lsn.max(status.sent_lsn),
                }).await;
            }
            NodeCheckResult::Failed { node_id, reason } => {
                let state = self.health_states.entry(node_id.clone()).or_default();
                state.consecutive_failures += 1;
                log::warn!("Node {} health check failed ({}/{}): {}",
                    node_id, state.consecutive_failures, self.config.failure_threshold, reason);

                if state.consecutive_failures >= self.config.failure_threshold {
                    let topology = self.topology.read().await;
                    if topology.node_roles.get(&node_id) == Some(&NodeRole::Primary) {
                        log::error!("PRIMARY {} declared failed — triggering failover", node_id);
                        self.failover.on_primary_failed(node_id).await;
                    } else {
                        let _ = self.raft.propose(TopologyCommand::MarkOffline { node_id }).await;
                    }
                    state.consecutive_failures = 0; // Reset to avoid re-triggering
                }
            }
        }
    }
}
```

### 3. Integration Points

- `node_monitor` is started only on the Raft leader.
- On Raft leader change, the new leader restarts the monitor loop.
- `failover_engine::on_primary_failed()` is called directly (not through Raft) to initiate failover.
- `proxy_layer` reads topology from the Raft state machine directly (not from node_monitor).
