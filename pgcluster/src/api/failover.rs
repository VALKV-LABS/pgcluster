use super::ApiState;
use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct FailoverRequest {
    pub failed_node_id: String,
}

#[derive(Serialize)]
pub struct FailoverResponse {
    pub triggered: bool,
    pub message: String,
}

pub async fn trigger_failover(
    State(s): State<ApiState>,
    Json(req): Json<FailoverRequest>,
) -> (StatusCode, Json<FailoverResponse>) {
    let topology = s.topology.borrow().clone();
    // Validate that the node exists
    if !topology.node_configs.contains_key(&req.failed_node_id) {
        return (
            StatusCode::NOT_FOUND,
            Json(FailoverResponse {
                triggered: false,
                message: format!("node {} not found", req.failed_node_id),
            }),
        );
    }
    // Note: actual failover logic lives in crate::failover; here we just
    // mark the node offline and return. The monitor loop will pick it up.
    let cmd = crate::raft::commands::TopologyCommand::MarkOffline {
        node_id: req.failed_node_id.clone(),
    };
    if let Err(e) = s.raft.raft.client_write(cmd).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(FailoverResponse {
                triggered: false,
                message: e.to_string(),
            }),
        );
    }
    (
        StatusCode::ACCEPTED,
        Json(FailoverResponse {
            triggered: true,
            message: format!("failover initiated for {}", req.failed_node_id),
        }),
    )
}
