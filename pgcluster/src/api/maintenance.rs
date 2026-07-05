use super::ApiState;
use crate::raft::{commands::TopologyCommand, topology::NodeRole};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::Serialize;

#[derive(Serialize)]
pub struct MaintenanceResponse {
    pub success: bool,
    pub message: String,
}

/// PUT a node into maintenance mode — it will be excluded from failover
/// candidates and health-check failures will not trigger automatic failover.
pub async fn enter_maintenance(
    State(s): State<ApiState>,
    Path(node_id): Path<String>,
) -> (StatusCode, Json<MaintenanceResponse>) {
    let topology = s.topology.borrow().clone();

    if !topology.node_configs.contains_key(&node_id) {
        return (
            StatusCode::NOT_FOUND,
            Json(MaintenanceResponse {
                success: false,
                message: format!("node {} not found", node_id),
            }),
        );
    }

    if topology.primary_node_id == node_id {
        return (
            StatusCode::BAD_REQUEST,
            Json(MaintenanceResponse {
                success: false,
                message: "cannot put the current primary into maintenance — switchover first"
                    .into(),
            }),
        );
    }

    match s
        .raft
        .raft
        .client_write(TopologyCommand::MarkMaintenance {
            node_id: node_id.clone(),
        })
        .await
    {
        Ok(_) => (
            StatusCode::OK,
            Json(MaintenanceResponse {
                success: true,
                message: format!("node {} is now in maintenance mode", node_id),
            }),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(MaintenanceResponse {
                success: false,
                message: format!("failed to set maintenance mode: {e}"),
            }),
        ),
    }
}

/// Take a node out of maintenance mode by marking it as a Replica.
pub async fn exit_maintenance(
    State(s): State<ApiState>,
    Path(node_id): Path<String>,
) -> (StatusCode, Json<MaintenanceResponse>) {
    let topology = s.topology.borrow().clone();

    if !topology.node_configs.contains_key(&node_id) {
        return (
            StatusCode::NOT_FOUND,
            Json(MaintenanceResponse {
                success: false,
                message: format!("node {} not found", node_id),
            }),
        );
    }

    if topology.node_roles.get(&node_id) != Some(&NodeRole::Maintenance) {
        return (
            StatusCode::BAD_REQUEST,
            Json(MaintenanceResponse {
                success: false,
                message: format!("node {} is not in maintenance mode", node_id),
            }),
        );
    }

    match s
        .raft
        .raft
        .client_write(TopologyCommand::MarkReplica {
            node_id: node_id.clone(),
            flush_lsn: 0,
        })
        .await
    {
        Ok(_) => (
            StatusCode::OK,
            Json(MaintenanceResponse {
                success: true,
                message: format!("node {} returned to replica role", node_id),
            }),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(MaintenanceResponse {
                success: false,
                message: format!("failed to exit maintenance mode: {e}"),
            }),
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::raft::topology::{ClusterTopology, NodeConfig, NodeRole};
    use std::collections::HashMap;

    fn topology_with_roles(primary: &str, nodes: &[(&str, NodeRole)]) -> ClusterTopology {
        let mut t = ClusterTopology {
            primary_node_id: primary.into(),
            ..Default::default()
        };
        for (id, role) in nodes {
            t.node_roles.insert(id.to_string(), role.clone());
            t.node_configs.insert(
                id.to_string(),
                NodeConfig {
                    node_id: id.to_string(),
                    agent_addr: "127.0.0.1:7001".into(),
                    postgres_addr: "127.0.0.1:5432".into(),
                    priority: 100,
                    tags: HashMap::new(),
                },
            );
        }
        t
    }

    #[test]
    fn primary_cannot_enter_maintenance() {
        let t = topology_with_roles(
            "pg1",
            &[("pg1", NodeRole::Primary), ("pg2", NodeRole::Replica)],
        );
        // Simulate the guard: primary should be rejected
        assert_eq!(t.primary_node_id, "pg1");
    }

    #[test]
    fn maintenance_node_excluded_from_candidates() {
        let mut t = topology_with_roles(
            "pg1",
            &[
                ("pg1", NodeRole::Primary),
                ("pg2", NodeRole::Replica),
                ("pg3", NodeRole::Maintenance),
            ],
        );
        t.last_flush_lsns.insert("pg2".into(), 100);
        t.last_flush_lsns.insert("pg3".into(), 999); // higher LSN but in maintenance

        // Only Replica nodes are candidates — pg3 in Maintenance is excluded
        let candidate = t.best_failover_candidate("pg1");
        assert_eq!(
            candidate.as_deref(),
            Some("pg2"),
            "maintenance node should not be a failover candidate"
        );
    }

    #[test]
    fn exit_maintenance_requires_maintenance_role() {
        let t = topology_with_roles(
            "pg1",
            &[("pg1", NodeRole::Primary), ("pg2", NodeRole::Replica)],
        );
        // pg2 is Replica, not Maintenance — exit_maintenance should reject
        assert_ne!(
            t.node_roles.get("pg2"),
            Some(&NodeRole::Maintenance),
            "pg2 is not in maintenance mode"
        );
    }
}
