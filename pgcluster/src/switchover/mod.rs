pub mod handoff;
pub mod lag_wait;

use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;

use crate::agent_clients::AgentClientPool;
use crate::raft::{RaftNode, TopologyWatch};

// ── planned_switchover ────────────────────────────────────────────────────────

/// Public API for a planned (operator-initiated) switchover.
///
/// Waits until `new_primary_id` is within `max_lag_bytes` of the current
/// primary, then executes the hand-off atomically.
pub async fn planned_switchover(
    raft: Arc<RaftNode>,
    topology_rx: TopologyWatch,
    new_primary_id: &str,
    pool: Arc<AgentClientPool>,
    max_lag_bytes: u64,
    sync_timeout_secs: u64,
) -> Result<()> {
    // Step 1: Wait for the target replica to catch up.
    lag_wait::wait_for_replica_sync(
        &topology_rx,
        new_primary_id,
        max_lag_bytes,
        Duration::from_secs(sync_timeout_secs),
    )
    .await?;

    // Snapshot the topology right before we start the hand-off.
    let topology = topology_rx.current();

    // Steps 2–4: Execute the switchover.
    handoff::execute_switchover(&raft, &*topology, new_primary_id, &pool).await
}
