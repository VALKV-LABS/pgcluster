use super::ApiState;
use axum::{extract::State, http::StatusCode, Json};
use serde::Serialize;

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub primary: Option<String>,
    pub raft_leader: bool,
}

pub async fn health(State(s): State<ApiState>) -> (StatusCode, Json<HealthResponse>) {
    let topology = s.topology.borrow().clone();
    let metrics = s.raft.raft.metrics().borrow().clone();
    let is_leader = metrics.current_leader == Some(metrics.id);
    let primary = if topology.primary_node_id.is_empty() {
        None
    } else {
        Some(topology.primary_node_id.clone())
    };
    let code = if primary.is_some() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        code,
        Json(HealthResponse {
            status: if primary.is_some() {
                "ok"
            } else {
                "no_primary"
            },
            primary,
            raft_leader: is_leader,
        }),
    )
}

#[derive(Serialize)]
pub struct ClusterStatusResponse {
    pub cluster_name: String,
    pub primary_node_id: String,
    pub topology_version: u64,
    pub raft_leader_id: Option<u64>,
    pub node_count: usize,
    pub failover_history_count: usize,
}

pub async fn cluster_status(State(s): State<ApiState>) -> Json<ClusterStatusResponse> {
    let topology = s.topology.borrow().clone();
    let metrics = s.raft.raft.metrics().borrow().clone();
    Json(ClusterStatusResponse {
        cluster_name: s.config.cluster.name.clone(),
        primary_node_id: topology.primary_node_id.clone(),
        topology_version: topology.version,
        raft_leader_id: metrics.current_leader,
        node_count: topology.node_configs.len(),
        failover_history_count: topology.failover_history.len(),
    })
}

pub async fn topology(State(s): State<ApiState>) -> Json<crate::raft::topology::ClusterTopology> {
    Json(s.topology.borrow().clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_response_serializes_correctly() {
        let r = ClusterStatusResponse {
            cluster_name: "prod".into(),
            primary_node_id: "pg1".into(),
            topology_version: 7,
            raft_leader_id: Some(2),
            node_count: 3,
            failover_history_count: 1,
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"cluster_name\":\"prod\""));
        assert!(json.contains("\"primary_node_id\":\"pg1\""));
        assert!(json.contains("\"topology_version\":7"));
        assert!(json.contains("\"raft_leader_id\":2"));
    }

    #[test]
    fn health_response_no_primary_serializes_null() {
        let r = HealthResponse {
            status: "no_primary",
            primary: None,
            raft_leader: false,
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"primary\":null"));
        assert!(json.contains("\"status\":\"no_primary\""));
    }
}
