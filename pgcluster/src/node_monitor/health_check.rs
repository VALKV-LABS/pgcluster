use tracing::warn;

use crate::agent_clients::AgentClientPool;

// ── HealthCheckResult ─────────────────────────────────────────────────────────

/// The normalised result of a single health-check poll against one vk-agent.
pub struct HealthCheckResult {
    pub node_id: String,
    /// Whether the agent responded at all.
    pub reachable: bool,
    /// `true` when Postgres is in recovery (i.e. acting as a standby).
    pub in_recovery: bool,
    /// Bytes flushed to WAL on a primary (`sent_lsn`), or bytes received on a
    /// standby (`received_lsn`).  We store the best available LSN here.
    pub flush_lsn: u64,
    /// Bytes replayed on a standby (`replayed_lsn`).
    pub replay_lsn: u64,
    pub timeline: u32,
    pub pg_running: bool,
}

// ── check_node ────────────────────────────────────────────────────────────────

/// Poll the vk-agent at `agent_addr` and return a normalised health-check
/// result.  Never returns an `Err`; connection / RPC failures are represented
/// as `reachable: false` so the caller's failure tracker can account for them.
pub async fn check_node(
    node_id: &str,
    agent_addr: &str,
    pool: &AgentClientPool,
) -> HealthCheckResult {
    let mut client = match pool.get_or_connect(node_id, agent_addr).await {
        Ok(c) => c,
        Err(e) => {
            warn!(node_id, err = %e, "failed to connect to agent");
            // Evict the cached slot so the next tick retries the handshake.
            pool.remove(node_id);
            return unreachable_result(node_id);
        }
    };

    match client.get_status().await {
        Ok(status) => {
            // Use `sent_lsn` for primaries and `received_lsn` for standbys as
            // the "flush" proxy.  Both collapse into a single monotonic number
            // stored in the topology.
            let flush_lsn = if status.is_in_recovery {
                status.received_lsn
            } else {
                status.sent_lsn
            };

            HealthCheckResult {
                node_id: node_id.to_string(),
                reachable: true,
                in_recovery: status.is_in_recovery,
                flush_lsn,
                replay_lsn: status.replayed_lsn,
                timeline: status.timeline,
                pg_running: status.postgres_running,
            }
        }
        Err(e) => {
            warn!(node_id, err = %e, "GetStatus RPC failed");
            // Evict so the next attempt re-establishes the channel.
            pool.remove(node_id);
            unreachable_result(node_id)
        }
    }
}

fn unreachable_result(node_id: &str) -> HealthCheckResult {
    HealthCheckResult {
        node_id: node_id.to_string(),
        reachable: false,
        in_recovery: false,
        flush_lsn: 0,
        replay_lsn: 0,
        timeline: 0,
        pg_running: false,
    }
}
