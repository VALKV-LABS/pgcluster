# Component: Switchover Engine (`switchover_engine`)

## High-Level Function

The switchover engine handles **planned, zero-data-loss primary handoff**. Unlike the failover engine (which responds to crashes), the switchover engine is operator-triggered and proceeds gracefully: it drains in-flight transactions, waits for zero replication lag, then promotes the target replica and demotes the old primary to a replica.

---

## Architecture

### Switchover vs Failover

| Aspect | Switchover | Failover |
|--------|------------|----------|
| Trigger | Operator (`pgcluster switchover pg2`) | Automatic (primary dead) |
| Data loss | Zero — guaranteed | Possible (depends on sync mode) |
| Old primary | Becomes a replica | Left dead / needs recovery |
| Write pause | Yes — explicit drain | N/A |
| Duration | ~5–15 seconds | ~3–5 seconds (Milestone 1) |

### Sequence

```
1. Validate target replica is healthy and streaming
2. Pause new write transactions on proxy (WritePause → Draining)
3. Wait for in-flight transactions to complete (max: drain_timeout)
4. Force StandbyStatusUpdate from target: send keepalive with reply_requested=true
5. Wait for target flush_lsn == current primary LSN (zero lag)
6. Send Promote RPC to target vk-agent
7. Confirm target is now primary (poll vk-agent status)
8. Propose SetPrimary to Raft → topology updated
9. Send Demote RPC to old primary vk-agent (restart as replica)
10. Update proxy WritePause → Accepting (write to new primary)
11. Emit switchover complete event
```

---

## Detailed Implementation Plan

### 1. Module Layout

```
src/
  switchover/
    mod.rs            # SwitchoverEngine, run_switchover()
    drain.rs          # WritePause state, drain wait loop
    lag_wait.rs       # Poll target flush_lsn until == primary LSN
    promote.rs        # Promote RPC to target vk-agent + confirmation poll
    demote.rs         # Demote RPC to old primary vk-agent
```

### 2. SwitchoverEngine

```rust
pub struct SwitchoverEngine {
    raft: Arc<PgClusterRaft>,
    topology: Arc<RwLock<ClusterTopology>>,
    agents: Arc<AgentClients>,
    node_configs: HashMap<String, NodeConfig>,
    write_pause: Arc<Mutex<WritePauseState>>,
    config: SwitchoverConfig,
    event_tx: mpsc::Sender<ClusterEvent>,
}

pub struct SwitchoverConfig {
    pub drain_timeout_ms: u64,       // default: 30_000
    pub lag_wait_timeout_ms: u64,    // default: 60_000
    pub promote_timeout_secs: u64,   // default: 30
}

#[derive(Clone)]
pub enum WritePauseState { Accepting, Draining { deadline: Instant }, Paused }

impl SwitchoverEngine {
    pub async fn run_switchover(&self, target_id: String) -> Result<SwitchoverResult> {
        log::info!("Switchover initiated: target = {}", target_id);
        let start = Instant::now();

        // Step 1: Validate target
        let topology = self.topology.read().await;
        if topology.node_roles.get(&target_id) != Some(&NodeRole::Replica) {
            return Err(anyhow::anyhow!("{} is not a streaming replica", target_id));
        }
        let old_primary = topology.primary_node_id.clone();
        let primary_lsn = topology.last_flush_lsns.get(&old_primary).copied().unwrap_or(0);
        drop(topology);

        // Step 2: Pause new writes at proxy
        *self.write_pause.lock().await = WritePauseState::Draining {
            deadline: Instant::now() + Duration::from_millis(self.config.drain_timeout_ms),
        };

        // Step 3: Wait for drain
        self.wait_for_drain().await;
        *self.write_pause.lock().await = WritePauseState::Paused;
        log::info!("Write pause active — proxy rejecting new BEGIN");

        // Step 4-5: Wait for zero lag
        self.wait_for_zero_lag(&target_id, primary_lsn).await?;
        log::info!("Zero lag confirmed on {} at LSN {:X}/{:08X}",
            target_id, primary_lsn >> 32, primary_lsn as u32);

        // Step 6-7: Promote target
        let promote_result = tokio::time::timeout(
            Duration::from_secs(self.config.promote_timeout_secs),
            self.agents.promote(&target_id, primary_lsn)
        ).await??;
        if !promote_result.success {
            // Unpause on failure
            *self.write_pause.lock().await = WritePauseState::Accepting;
            return Err(anyhow::anyhow!("Promote failed: {}", promote_result.error));
        }

        // Step 8: Commit to Raft
        self.raft.propose(TopologyCommand::SetPrimary {
            node_id: target_id.clone(),
            at_lsn: promote_result.promoted_at_lsn,
            new_tli: promote_result.new_timeline,
        }).await?;

        // Step 9: Demote old primary
        let new_conninfo = self.build_conninfo(&target_id);
        let slot_name = format!("pgcluster_{}", old_primary.replace('-', "_"));
        self.agents.demote(&old_primary, &new_conninfo, &slot_name).await
            .unwrap_or_else(|e| log::warn!("Demote old primary failed (non-fatal): {:?}", e));

        // Step 10: Re-open writes on new primary
        *self.write_pause.lock().await = WritePauseState::Accepting;

        let duration = start.elapsed();
        log::info!("Switchover complete: {} → {} in {:?}", old_primary, target_id, duration);

        let result = SwitchoverResult {
            old_primary,
            new_primary: target_id,
            duration_ms: duration.as_millis() as u64,
            data_loss: false,
        };
        let _ = self.event_tx.send(ClusterEvent::SwitchoverComplete(result.clone())).await;
        Ok(result)
    }

    async fn wait_for_drain(&self) {
        let deadline = match *self.write_pause.lock().await {
            WritePauseState::Draining { deadline } => deadline,
            _ => return,
        };
        // Poll until active transaction count drops to zero or deadline passes
        loop {
            // In-flight txn count tracked by proxy layer
            let active = ACTIVE_TRANSACTIONS.load(Ordering::Relaxed);
            if active == 0 || Instant::now() > deadline { return; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_for_zero_lag(&self, target_id: &str, target_lsn: u64) -> Result<()> {
        let deadline = Instant::now() + Duration::from_millis(self.config.lag_wait_timeout_ms);
        loop {
            let status = self.agents.get_status(target_id).await?;
            if status.received_lsn >= target_lsn { return Ok(()); }
            if Instant::now() > deadline {
                let lag = target_lsn.saturating_sub(status.received_lsn);
                return Err(anyhow::anyhow!("Lag wait timeout: {} bytes remaining", lag));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
```

### 3. Write Pause in Proxy

The proxy checks `WritePauseState` before acquiring a backend for any write-intent statement:

```rust
// In proxy router, before routing a write:
match *write_pause.lock().await {
    WritePauseState::Paused | WritePauseState::Draining { .. } => {
        send_error_to_client(client, "57P01",
            "cluster is switching over — retry in a moment").await?;
        return Ok(());
    }
    WritePauseState::Accepting => { /* proceed */ }
}
```

### 4. Integration Points

- `rest_api` and `cli` trigger `run_switchover()` on the Raft leader.
- `proxy_layer` checks `WritePauseState` before routing write transactions.
- `vk_agent` receives Promote and Demote gRPC calls.
- `raft_consensus` commits the `SetPrimary` topology change at step 8.
- `event_tx` channel delivers the event to REST API push and alerting webhook.
