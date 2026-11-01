use anyhow::{anyhow, bail, Result};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;

use super::SwitchoverParams;
use crate::agent_clients::AgentClientPool;
use crate::failover::candidate::pick_live_candidate;
use crate::node_monitor::health_check::check_node;
use crate::raft::topology::{ClusterTopology, NodeRole};
use crate::raft::TopologyWatch;

/// How long a restarted node has to stream from the primary again.
const CATCH_UP_TIMEOUT: Duration = Duration::from_secs(120);

/// Restart every node with at most one failover-free interruption: standbys one
/// at a time, then a switchover, which restarts the old primary as a standby.
///
/// `switchover` starts with no target: this picks the most current standby.
/// Stops at the first failure, because each step relies on the last one leaving
/// the cluster fully replicated.
pub async fn rolling_restart(mut switchover: SwitchoverParams) -> Result<()> {
    let pool = switchover.pool.clone();
    let topology_rx = switchover.topology_rx.clone();
    let topology = topology_rx.current();
    let old_primary = topology.primary_node_id.clone();
    let standbys = standbys_in_order(&topology)?;

    for node_id in &standbys {
        info!(node_id, "rolling restart: restarting standby");
        restart_node(&topology, node_id, &pool).await?;
        wait_until_streaming(&topology_rx, node_id, &pool).await?;
    }

    if standbys.is_empty() {
        info!("rolling restart: no standbys, restarting the only node");
        return restart_node(&topology, &old_primary, &pool).await;
    }

    let (target, _) = pick_live_candidate(&topology, &old_primary, &pool)
        .await
        .ok_or_else(|| anyhow!("no standby can take over"))?;
    info!(target, "rolling restart: switching over");
    switchover.new_primary_id = target;
    super::planned_switchover(switchover).await?;

    wait_until_streaming(&topology_rx, &old_primary, &pool).await
}

/// Every standby, sorted. Refuses a cluster that is already missing one, because
/// restarting another would leave no standby at all.
fn standbys_in_order(topology: &ClusterTopology) -> Result<Vec<String>> {
    if topology.primary_node_id.is_empty() {
        bail!("the cluster has no primary");
    }
    let mut standbys = Vec::new();
    for node_id in topology.node_configs.keys() {
        if *node_id == topology.primary_node_id {
            continue;
        }
        match topology.node_roles.get(node_id) {
            Some(NodeRole::Replica) => standbys.push(node_id.clone()),
            role => bail!("{node_id} is not a healthy standby ({role:?})"),
        }
    }
    standbys.sort();
    Ok(standbys)
}

async fn restart_node(
    topology: &ClusterTopology,
    node_id: &str,
    pool: &Arc<AgentClientPool>,
) -> Result<()> {
    let cfg = topology
        .node_configs
        .get(node_id)
        .ok_or_else(|| anyhow!("node {node_id} not found"))?;
    let mut client = pool.get_or_connect(node_id, &cfg.agent_addr).await?;
    let response = client.stop_postgres("fast").await?;
    if !response.success {
        bail!("could not restart {node_id}: {}", response.error);
    }
    Ok(())
}

/// Wait until the current primary reports `node_id` as streaming.
async fn wait_until_streaming(
    topology_rx: &TopologyWatch,
    node_id: &str,
    pool: &Arc<AgentClientPool>,
) -> Result<()> {
    let deadline = Instant::now() + CATCH_UP_TIMEOUT;
    while Instant::now() < deadline {
        let topology = topology_rx.current();
        if let Some(primary) = topology.node_configs.get(&topology.primary_node_id) {
            let status = check_node(&primary.node_id, &primary.agent_addr, pool).await;
            if status.streaming_standbys.iter().any(|n| n == node_id) {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    bail!("{node_id} did not stream again within {CATCH_UP_TIMEOUT:?}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::topology::NodeConfig;

    fn cluster(roles: &[(&str, NodeRole)]) -> ClusterTopology {
        let mut t = ClusterTopology {
            primary_node_id: "pg1".into(),
            ..Default::default()
        };
        for (id, role) in roles {
            t.node_roles.insert(id.to_string(), role.clone());
            t.node_configs.insert(
                id.to_string(),
                NodeConfig {
                    node_id: id.to_string(),
                    agent_addr: "x".into(),
                    postgres_addr: "y".into(),
                    priority: 100,
                    tags: Default::default(),
                },
            );
        }
        t
    }

    #[test]
    fn standbys_come_in_a_stable_order_without_the_primary() {
        let t = cluster(&[
            ("pg1", NodeRole::Primary),
            ("pg3", NodeRole::Replica),
            ("pg2", NodeRole::Replica),
        ]);
        assert_eq!(standbys_in_order(&t).unwrap(), vec!["pg2", "pg3"]);
    }

    #[test]
    fn a_cluster_missing_a_standby_is_refused() {
        let t = cluster(&[
            ("pg1", NodeRole::Primary),
            ("pg2", NodeRole::Replica),
            ("pg3", NodeRole::Offline),
        ]);
        let err = standbys_in_order(&t).unwrap_err();
        assert!(err.to_string().contains("pg3"), "{err}");
    }

    #[test]
    fn a_cluster_without_a_primary_is_refused() {
        let mut t = cluster(&[("pg2", NodeRole::Replica)]);
        t.primary_node_id.clear();
        assert!(standbys_in_order(&t).is_err());
    }
}
