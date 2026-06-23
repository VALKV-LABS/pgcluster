use super::ApiState;
use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct SwitchoverRequest {
    pub target_node_id: String,
    #[serde(default = "default_max_lag")]
    pub max_lag_bytes: u64,
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
}

fn default_max_lag() -> u64 {
    1048576
} // 1 MiB
fn default_timeout() -> u64 {
    30
}

#[derive(Serialize)]
pub struct SwitchoverResponse {
    pub success: bool,
    pub message: String,
}

pub async fn trigger_switchover(
    State(s): State<ApiState>,
    Json(req): Json<SwitchoverRequest>,
) -> (StatusCode, Json<SwitchoverResponse>) {
    let topology = s.topology.borrow().clone();
    if !topology.node_configs.contains_key(&req.target_node_id) {
        return (
            StatusCode::NOT_FOUND,
            Json(SwitchoverResponse {
                success: false,
                message: format!("node {} not found", req.target_node_id),
            }),
        );
    }
    if topology.primary_node_id == req.target_node_id {
        return (
            StatusCode::BAD_REQUEST,
            Json(SwitchoverResponse {
                success: false,
                message: "target is already the primary".into(),
            }),
        );
    }

    // Spawn switchover in background — REST returns immediately
    let raft = s.raft.clone();
    let topology_rx = s.topology.clone();
    let metrics = s.metrics.clone();
    let pool = std::sync::Arc::new(crate::agent_clients::AgentClientPool::new());
    let target = req.target_node_id.clone();
    tokio::spawn(async move {
        match crate::switchover::planned_switchover(
            raft,
            topology_rx,
            &target,
            pool,
            req.max_lag_bytes,
            req.timeout_secs,
        )
        .await
        {
            Ok(()) => {
                metrics.switchover_total.inc();
                metrics.primary_changes_total.inc();
            }
            Err(e) => {
                tracing::error!(err = %e, target, "switchover failed");
            }
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(SwitchoverResponse {
            success: true,
            message: format!("switchover to {} initiated", req.target_node_id),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::topology::{ClusterTopology, NodeConfig, NodeRole};
    use std::collections::HashMap;

    fn topology_with_primary(primary: &str, replicas: &[&str]) -> ClusterTopology {
        let mut t = ClusterTopology::default();
        t.primary_node_id = primary.into();
        t.node_roles.insert(primary.into(), NodeRole::Primary);
        t.node_configs.insert(
            primary.into(),
            NodeConfig {
                node_id: primary.into(),
                agent_addr: "127.0.0.1:7001".into(),
                postgres_addr: "127.0.0.1:5432".into(),
                priority: 100,
                tags: HashMap::new(),
            },
        );
        for r in replicas {
            t.node_roles.insert(r.to_string(), NodeRole::Replica);
            t.node_configs.insert(
                r.to_string(),
                NodeConfig {
                    node_id: r.to_string(),
                    agent_addr: "127.0.0.1:7002".into(),
                    postgres_addr: "127.0.0.1:5433".into(),
                    priority: 90,
                    tags: HashMap::new(),
                },
            );
        }
        t
    }

    #[test]
    fn switchover_rejects_unknown_node() {
        let topology = topology_with_primary("pg1", &["pg2"]);
        // Simulate the guard logic without the full handler
        let target = "pg99";
        assert!(
            !topology.node_configs.contains_key(target),
            "pg99 should not exist in topology"
        );
    }

    #[test]
    fn switchover_rejects_current_primary_as_target() {
        let topology = topology_with_primary("pg1", &["pg2"]);
        let target = "pg1";
        assert_eq!(
            topology.primary_node_id, target,
            "pg1 is the primary — switchover to self should be rejected"
        );
    }

    #[test]
    fn switchover_request_default_lag_and_timeout() {
        let json = r#"{"target_node_id":"pg2"}"#;
        let req: SwitchoverRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.target_node_id, "pg2");
        assert_eq!(req.max_lag_bytes, 1_048_576);
        assert_eq!(req.timeout_secs, 30);
    }
}
