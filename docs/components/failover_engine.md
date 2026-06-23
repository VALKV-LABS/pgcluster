# Component: Failover Engine (`failover_engine`)

## High-Level Function

The failover engine handles unplanned primary failure. It selects the best available replica, coordinates its promotion via vk-agent, updates the topology in Raft, and re-points remaining replicas to the new primary. It runs exclusively on the Raft leader.

---

## Architecture

### Failover Sequence

```
node_monitor declares primary dead
  │
  ▼
1. Acquire failover lock (prevent concurrent failovers)
  │
  ▼
2. Mark old primary Offline in Raft topology
  │
  ▼
3. Select best candidate replica:
   rank by: (a) highest flush_lsn  (b) node priority  (c) lag_seconds
  │
  ▼
4. Send Promote RPC to candidate's vk-agent
   → vk-agent writes promote.signal
   → waits for pg_is_in_recovery() = false
  │
  ▼
5. Confirm promotion: poll until vk-agent reports is_in_recovery=false
  │
  ▼
6. Propose SetPrimary to Raft → topology updated cluster-wide
  │
  ▼
7. Proxy layer reads new topology → routes writes to new primary
  │
  ▼
8. For each remaining replica:
   Build new primary_conninfo string
   Send Demote RPC to vk-agent (update auto.conf + restart)
  │
  ▼
9. Create replication slots on new primary for each replica
  │
  ▼
10. Emit failover event (REST API, metrics, alerting webhook)
```

### Replica Candidate Ranking

```rust
fn rank_candidates(
    replicas: &[(String, NodeHealthState)],
    topology: &ClusterTopology,
    node_configs: &HashMap<String, NodeConfig>,
) -> Vec<String> {
    let mut candidates: Vec<_> = replicas.iter()
        .filter(|(id, state)| topology.node_roles.get(*id) == Some(&NodeRole::Replica))
        .filter(|(id, state)| state.consecutive_failures == 0)  // Must be healthy
        .collect();

    candidates.sort_by(|(id_a, _), (id_b, _)| {
        let lsn_a = topology.last_flush_lsns.get(*id_a).copied().unwrap_or(0);
        let lsn_b = topology.last_flush_lsns.get(*id_b).copied().unwrap_or(0);
        let pri_a = node_configs.get(*id_a).map(|n| n.priority).unwrap_or(0);
        let pri_b = node_configs.get(*id_b).map(|n| n.priority).unwrap_or(0);

        // Primary sort: highest flush_lsn
        lsn_b.cmp(&lsn_a)
            // Secondary: highest configured priority
            .then(pri_b.cmp(&pri_a))
    });

    candidates.into_iter().map(|(id, _)| id.clone()).collect()
}
```

---

## Detailed Implementation Plan

### 1. Module Layout

```
src/
  failover/
    mod.rs            # FailoverEngine, on_primary_failed()
    candidate.rs      # rank_candidates(), select_best_replica()
    promote.rs        # Send Promote RPC, wait for confirmation
    repoint.rs        # Build primary_conninfo, send Demote RPC to remaining replicas
    slots.rs          # Create replication slots on new primary via SQL
    events.rs         # Emit failover event to webhook, metrics
```

### 2. FailoverEngine

```rust
pub struct FailoverEngine {
    raft: Arc<PgClusterRaft>,
    topology: Arc<RwLock<ClusterTopology>>,
    agents: Arc<AgentClients>,
    health_states: Arc<RwLock<HashMap<String, NodeHealthState>>>,
    node_configs: HashMap<String, NodeConfig>,
    failover_lock: Arc<Mutex<()>>,
    config: FailoverConfig,
    event_tx: mpsc::Sender<ClusterEvent>,
}

pub struct FailoverConfig {
    pub promote_timeout_secs: u64,    // default: 30
    pub repoint_timeout_secs: u64,    // default: 60
    pub max_failover_attempts: u32,   // Try next candidate if first fails
}

impl FailoverEngine {
    pub async fn on_primary_failed(&self, failed_node_id: String) {
        let _lock = self.failover_lock.lock().await;

        log::error!("Starting failover: primary {} is dead", failed_node_id);
        let start = Instant::now();

        // Step 2: Mark old primary offline
        let _ = self.raft.propose(TopologyCommand::MarkOffline {
            node_id: failed_node_id.clone(),
        }).await;

        // Step 3: Select best candidate
        let health = self.health_states.read().await;
        let topology = self.topology.read().await;
        let candidates = rank_candidates(&health, &topology, &self.node_configs);
        drop(health);
        drop(topology);

        // Step 4-5: Promote — try candidates in order
        let mut new_primary = None;
        for candidate_id in &candidates {
            log::info!("Attempting to promote {}", candidate_id);
            match self.promote_node(candidate_id).await {
                Ok(result) => {
                    new_primary = Some((candidate_id.clone(), result));
                    break;
                }
                Err(e) => {
                    log::warn!("Failed to promote {}: {:?} — trying next candidate", candidate_id, e);
                }
            }
        }

        let (new_primary_id, promote_result) = match new_primary {
            Some(r) => r,
            None => {
                log::error!("FAILOVER FAILED: no candidate could be promoted");
                let _ = self.event_tx.send(ClusterEvent::FailoverFailed { failed_node: failed_node_id }).await;
                return;
            }
        };

        // Step 6: Commit to Raft
        let _ = self.raft.propose(TopologyCommand::SetPrimary {
            node_id: new_primary_id.clone(),
            at_lsn: promote_result.promoted_at_lsn,
            new_tli: promote_result.new_timeline,
        }).await;

        // Step 7: Proxy automatically picks up new topology via Raft state machine

        // Step 8: Re-point remaining replicas
        let topology = self.topology.read().await;
        let new_conninfo = self.build_primary_conninfo(&new_primary_id);
        let replicas: Vec<_> = topology.node_roles.iter()
            .filter(|(id, role)| **role == NodeRole::Replica && *id != &new_primary_id)
            .map(|(id, _)| id.clone())
            .collect();
        drop(topology);

        for replica_id in replicas {
            let slot_name = format!("pgcluster_{}", replica_id.replace('-', "_"));
            if let Err(e) = self.agents.demote(&replica_id, &new_conninfo, &slot_name).await {
                log::error!("Failed to re-point replica {}: {:?}", replica_id, e);
            }
        }

        // Step 9: Create replication slots on new primary
        if let Err(e) = self.create_replica_slots(&new_primary_id).await {
            log::warn!("Failed to create replication slots: {:?}", e);
        }

        // Step 10: Emit event
        let duration = start.elapsed();
        log::info!("Failover complete: {} promoted in {:?}", new_primary_id, duration);
        let _ = self.event_tx.send(ClusterEvent::FailoverComplete {
            old_primary: failed_node_id,
            new_primary: new_primary_id,
            duration_ms: duration.as_millis() as u64,
        }).await;
    }

    async fn promote_node(&self, node_id: &str) -> Result<PromoteResponse> {
        let topology = self.topology.read().await;
        let expected_lsn = topology.last_flush_lsns.get(node_id).copied().unwrap_or(0);
        drop(topology);

        let response = tokio::time::timeout(
            Duration::from_secs(self.config.promote_timeout_secs),
            self.agents.promote(node_id, expected_lsn)
        ).await??;

        if !response.success {
            return Err(anyhow::anyhow!("vk-agent promote rejected: {}", response.error));
        }
        Ok(response)
    }

    async fn create_replica_slots(&self, primary_id: &str) -> Result<()> {
        let pg_addr = self.node_configs[primary_id].postgres_addr.clone();
        let conn = connect_postgres(&pg_addr).await?;
        let topology = self.topology.read().await;
        for (node_id, role) in &topology.node_roles {
            if *role == NodeRole::Replica {
                let slot = format!("pgcluster_{}", node_id.replace('-', "_"));
                sqlx::query(&format!(
                    "SELECT pg_create_physical_replication_slot('{}', true, false)",
                    slot
                ))
                .execute(&conn).await
                .ok(); // Ignore if already exists
            }
        }
        Ok(())
    }
}
```

### 3. Integration Points

- `node_monitor` calls `failover_engine::on_primary_failed()` directly when primary death is confirmed.
- `raft_consensus` is updated at step 2 (MarkOffline) and step 6 (SetPrimary).
- `proxy_layer` reads new topology from Raft state machine — no direct call needed.
- `vk_agent` receives Promote (step 4) and Demote (step 8) gRPC calls.
- `rest_api` and alerting webhook receive `ClusterEvent::FailoverComplete`.
