use super::ApiState;
use crate::raft::{commands::TopologyCommand, topology::NodeConfig};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub struct NodeListResponse {
    pub nodes: Vec<NodeInfo>,
}

#[derive(Serialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub role: String,
    pub postgres_addr: String,
    pub agent_addr: String,
    pub priority: u32,
    pub flush_lsn: u64,
    pub replay_lsn: u64,
    pub lag_bytes: u64,
}

pub async fn list_nodes(State(s): State<ApiState>) -> Json<NodeListResponse> {
    let t = s.topology.borrow().clone();
    let nodes = t
        .node_configs
        .values()
        .map(|cfg| NodeInfo {
            node_id: cfg.node_id.clone(),
            role: t
                .node_roles
                .get(&cfg.node_id)
                .map(|r| r.to_string())
                .unwrap_or_else(|| "unknown".into()),
            postgres_addr: cfg.postgres_addr.clone(),
            agent_addr: cfg.agent_addr.clone(),
            priority: cfg.priority,
            flush_lsn: t.last_flush_lsns.get(&cfg.node_id).copied().unwrap_or(0),
            replay_lsn: t.last_replay_lsns.get(&cfg.node_id).copied().unwrap_or(0),
            lag_bytes: t.replica_lag_bytes.get(&cfg.node_id).copied().unwrap_or(0),
        })
        .collect();
    Json(NodeListResponse { nodes })
}

pub async fn get_node(
    State(s): State<ApiState>,
    Path(node_id): Path<String>,
) -> Result<Json<NodeInfo>, StatusCode> {
    let t = s.topology.borrow().clone();
    let cfg = t
        .node_configs
        .get(&node_id)
        .ok_or(StatusCode::NOT_FOUND)?
        .clone();
    Ok(Json(NodeInfo {
        node_id: cfg.node_id.clone(),
        role: t
            .node_roles
            .get(&cfg.node_id)
            .map(|r| r.to_string())
            .unwrap_or_default(),
        postgres_addr: cfg.postgres_addr.clone(),
        agent_addr: cfg.agent_addr.clone(),
        priority: cfg.priority,
        flush_lsn: t.last_flush_lsns.get(&cfg.node_id).copied().unwrap_or(0),
        replay_lsn: t.last_replay_lsns.get(&cfg.node_id).copied().unwrap_or(0),
        lag_bytes: t.replica_lag_bytes.get(&cfg.node_id).copied().unwrap_or(0),
    }))
}

#[derive(Deserialize)]
pub struct AddNodeRequest {
    pub node_id: String,
    pub agent_addr: String,
    pub postgres_addr: String,
    pub priority: Option<u32>,
}

pub async fn add_node(
    State(s): State<ApiState>,
    Json(req): Json<AddNodeRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let cmd = TopologyCommand::AddNode(NodeConfig {
        node_id: req.node_id,
        agent_addr: req.agent_addr,
        postgres_addr: req.postgres_addr,
        priority: req.priority.unwrap_or(100),
        tags: Default::default(),
    });
    s.raft
        .raft
        .client_write(cmd)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::CREATED)
}

pub async fn remove_node(
    State(s): State<ApiState>,
    Path(node_id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let cmd = TopologyCommand::RemoveNode { node_id };
    s.raft
        .raft
        .client_write(cmd)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}
