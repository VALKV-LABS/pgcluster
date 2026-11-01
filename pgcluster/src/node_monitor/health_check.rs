use std::collections::HashMap;
use std::time::Duration;

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
    /// The `conninfo` string from `pg_stat_wal_receiver` (standby only; empty on primary).
    /// Used to detect a stale `primary_conninfo` after failover.
    pub replication_conninfo: String,
    /// Primary only: `application_name` of each standby that is streaming.
    pub streaming_standbys: Vec<String>,
    /// Current values of the settings pgcluster manages.
    pub parameters: HashMap<String, String>,
    /// Primary only: slots whose WAL has been removed.
    pub lost_slots: Vec<String>,
    /// Whether the node has a data directory. Assumed true until it answers.
    pub has_data: bool,
    /// The version of the backup settings this node last applied.
    pub backup_config_version: String,
    /// The version of the proxy's auth role on this node.
    pub proxy_auth_version: String,
}

// ── send_heartbeat ────────────────────────────────────────────────────────────

/// Upper bound on one health check.
const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(2);

/// Upper bound on one heartbeat, so a dead agent cannot pile up tasks.
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(2);

/// Tell the agent at `agent_addr` that a Raft leader can see it. Without
/// heartbeats an agent enters safe mode and then self-fences.
pub async fn send_heartbeat(node_id: &str, agent_addr: &str, pool: &AgentClientPool) {
    let attempt = async {
        let mut client = pool.get_or_connect(node_id, agent_addr).await?;
        client.heartbeat().await
    };
    let err = match tokio::time::timeout(HEARTBEAT_TIMEOUT, attempt).await {
        Ok(Ok(_)) => return,
        Ok(Err(e)) => e.to_string(),
        Err(_) => "timed out".to_string(),
    };
    warn!(node_id, err, "Heartbeat RPC failed");
    pool.remove(node_id);
}

// ── check_node ────────────────────────────────────────────────────────────────

/// Poll the vk-agent at `agent_addr` and return a normalised health-check
/// result.  Never returns an `Err`; connection / RPC failures are represented
/// as `reachable: false` so the caller's failure tracker can account for them.
///
/// Time-boxed, because a node that vanishes without a connection reset would
/// otherwise hang the call, and with it the leader's whole monitor loop.
pub async fn check_node(
    node_id: &str,
    agent_addr: &str,
    pool: &AgentClientPool,
) -> HealthCheckResult {
    match tokio::time::timeout(HEALTH_CHECK_TIMEOUT, query_node(node_id, agent_addr, pool)).await {
        Ok(result) => result,
        Err(_) => {
            warn!(node_id, "health check timed out");
            pool.remove(node_id);
            unreachable_result(node_id)
        }
    }
}

async fn query_node(node_id: &str, agent_addr: &str, pool: &AgentClientPool) -> HealthCheckResult {
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
                replication_conninfo: status.replication_conninfo,
                streaming_standbys: status
                    .replicas
                    .into_iter()
                    .filter(|r| r.state == "streaming")
                    .map(|r| r.application_name)
                    .collect(),
                parameters: status.parameters,
                lost_slots: status.lost_slots,
                has_data: status.has_data,
                backup_config_version: status.backup_config_version,
                proxy_auth_version: status.proxy_auth_version,
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
        replication_conninfo: String::new(),
        streaming_standbys: Vec::new(),
        parameters: HashMap::new(),
        lost_slots: Vec::new(),
        has_data: true,
        backup_config_version: String::new(),
        proxy_auth_version: String::new(),
    }
}
