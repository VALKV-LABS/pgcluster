pub mod handoff;
pub mod lag_wait;

use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;

use crate::agent_clients::AgentClientPool;
use crate::proxy::Router as ProxyRouter;
use crate::raft::{RaftNode, TopologyWatch};

// ── SwitchoverParams ──────────────────────────────────────────────────────────

/// Parameters for a planned switchover, grouped to avoid the too-many-arguments
/// clippy lint.
pub struct SwitchoverParams {
    pub raft: Arc<RaftNode>,
    pub topology_rx: TopologyWatch,
    pub new_primary_id: String,
    pub pool: Arc<AgentClientPool>,
    pub max_lag_bytes: u64,
    pub sync_timeout_secs: u64,
    pub replication_user: String,
    pub replication_password: String,
    /// Proxy router for connection draining before the hand-off begins.
    /// `None` when no local proxy is running on this node.
    pub proxy_drain: Option<Arc<ProxyRouter>>,
    /// How long to wait for in-flight transactions to complete after
    /// signalling the drain. Default: 3 seconds.
    pub drain_timeout: Duration,
    /// Replication slot name prefix from config (e.g. `"pgcluster_"`).
    pub slot_prefix: String,
}

// ── planned_switchover ────────────────────────────────────────────────────────

/// Public API for a planned (operator-initiated) switchover.
///
/// Waits until `new_primary_id` is within `max_lag_bytes` of the current
/// primary, then executes the hand-off atomically.
pub async fn planned_switchover(p: SwitchoverParams) -> Result<()> {
    // Step 1: Wait for the target replica to catch up.
    lag_wait::wait_for_replica_sync(
        &p.topology_rx,
        &p.new_primary_id,
        p.max_lag_bytes,
        Duration::from_secs(p.sync_timeout_secs),
    )
    .await?;

    // Snapshot the topology right before we start the hand-off.
    let topology = p.topology_rx.current();

    // Steps 2–5: Execute the switchover (with optional proxy drain in step 0).
    handoff::execute_switchover(
        &p.raft,
        &topology,
        &p.new_primary_id,
        &p.pool,
        &p.replication_user,
        &p.replication_password,
        p.proxy_drain.as_ref(),
        p.drain_timeout,
        &p.slot_prefix,
    )
    .await
}
