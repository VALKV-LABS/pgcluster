use super::ApiState;
use axum::{extract::State, Json};
use serde::Serialize;

#[derive(Serialize)]
pub struct ConfigResponse {
    pub cluster_name: String,
    pub node_id: u64,
    pub peers: Vec<PeerInfo>,
    pub proxy_addr: String,
    pub api_addr: String,
    pub metrics_addr: String,
}

#[derive(Serialize)]
pub struct PeerInfo {
    pub id: u64,
    pub addr: String,
}

pub async fn get_config(State(s): State<ApiState>) -> Json<ConfigResponse> {
    let cfg = &s.config;
    Json(ConfigResponse {
        cluster_name: cfg.cluster.name.clone(),
        node_id: cfg.raft.node_id,
        peers: cfg
            .raft
            .peers
            .iter()
            .map(|p| PeerInfo {
                id: p.id,
                addr: p.addr.clone(),
            })
            .collect(),
        proxy_addr: cfg.proxy.listen_addr.clone(),
        api_addr: cfg.api.listen_addr.clone(),
        metrics_addr: cfg.metrics.listen_addr.clone(),
    })
}
