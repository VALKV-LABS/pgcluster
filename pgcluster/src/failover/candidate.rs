use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinSet;
use tracing::warn;

use crate::agent_clients::AgentClientPool;
use crate::raft::topology::{ClusterTopology, NodeRole};

/// How long a standby has to report its position during failover.
const LIVE_STATUS_TIMEOUT: Duration = Duration::from_secs(2);

/// Pick the failover candidate from the live status of reachable standbys.
///
/// The positions in the topology trail the health checks, and promoting from
/// them can lose commits that a more current standby holds. Returns the node
/// and its live position, or `None` if no standby answered.
pub async fn pick_live_candidate(
    topology: &ClusterTopology,
    failed_node_id: &str,
    pool: &Arc<AgentClientPool>,
) -> Option<(String, u64)> {
    let positions = live_positions(topology, failed_node_id, pool).await;
    rank_positions(topology, &positions)
}

/// Ask every standby except `failed_node_id` for its position. One that does
/// not answer, or is not in recovery, is left out.
async fn live_positions(
    topology: &ClusterTopology,
    failed_node_id: &str,
    pool: &Arc<AgentClientPool>,
) -> Vec<(String, u64)> {
    let mut checks = JoinSet::new();
    for (node_id, role) in &topology.node_roles {
        if node_id == failed_node_id || *role != NodeRole::Replica {
            continue;
        }
        let Some(cfg) = topology.node_configs.get(node_id) else {
            continue;
        };
        let node_id = node_id.clone();
        let addr = cfg.agent_addr.clone();
        let pool = pool.clone();
        checks.spawn(async move {
            let query = async {
                let mut client = pool.get_or_connect(&node_id, &addr).await?;
                client.get_status().await
            };
            match tokio::time::timeout(LIVE_STATUS_TIMEOUT, query).await {
                Ok(Ok(status)) if status.postgres_running && status.is_in_recovery => {
                    Some((node_id, status.received_lsn.max(status.replayed_lsn)))
                }
                Ok(Ok(_)) => None,
                Ok(Err(e)) => {
                    warn!(node_id, err = %e, "standby did not report its position");
                    pool.remove(&node_id);
                    None
                }
                Err(_) => {
                    warn!(node_id, "standby timed out reporting its position");
                    pool.remove(&node_id);
                    None
                }
            }
        });
    }

    let mut positions = Vec::new();
    while let Some(result) = checks.join_next().await {
        if let Ok(Some(position)) = result {
            positions.push(position);
        }
    }
    positions
}

/// Pick the highest position, breaking ties by node priority.
fn rank_positions(
    topology: &ClusterTopology,
    positions: &[(String, u64)],
) -> Option<(String, u64)> {
    let priority = |id: &str| topology.node_configs.get(id).map_or(0, |c| c.priority);
    positions
        .iter()
        .max_by_key(|(id, lsn)| (*lsn, priority(id)))
        .cloned()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::topology::NodeConfig;

    fn topology(priorities: &[(&str, u32)]) -> ClusterTopology {
        let mut t = ClusterTopology::default();
        for (id, priority) in priorities {
            t.node_configs.insert(
                id.to_string(),
                NodeConfig {
                    node_id: id.to_string(),
                    agent_addr: "x".into(),
                    postgres_addr: "y".into(),
                    priority: *priority,
                    tags: Default::default(),
                },
            );
        }
        t
    }

    #[test]
    fn no_positions_means_no_candidate() {
        assert_eq!(rank_positions(&topology(&[]), &[]), None);
    }

    #[test]
    fn highest_position_wins_over_priority() {
        let t = topology(&[("pg2", 200), ("pg3", 100)]);
        let positions = [("pg2".to_string(), 50), ("pg3".to_string(), 500)];
        assert_eq!(rank_positions(&t, &positions), Some(("pg3".into(), 500)));
    }

    #[test]
    fn priority_breaks_a_tie() {
        let t = topology(&[("pg2", 80), ("pg3", 120)]);
        let positions = [("pg2".to_string(), 100), ("pg3".to_string(), 100)];
        assert_eq!(rank_positions(&t, &positions), Some(("pg3".into(), 100)));
    }
}
