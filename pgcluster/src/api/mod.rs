pub mod config;
pub mod failover;
pub mod forward;
pub mod metrics;
pub mod nodes;
pub mod replication;
pub mod status;
pub mod switchover;

use crate::config::PgClusterConfig;
use crate::metrics_registry::Metrics;
use crate::raft::{RaftNode, TopologyWatch};
use axum::{
    routing::{get, post},
    Router,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct ApiState {
    pub raft: Arc<RaftNode>,
    pub topology: TopologyWatch,
    pub metrics: Arc<Metrics>,
    pub config: Arc<PgClusterConfig>,
}

pub fn router(state: ApiState) -> Router {
    Router::new()
        // /health stays at root for Docker/LB health checks
        .route("/health", get(status::health))
        .nest(
            "/api",
            Router::new()
                .route("/status", get(status::cluster_status))
                .route("/topology", get(status::topology))
                .route("/nodes", get(nodes::list_nodes))
                .route(
                    "/nodes/:node_id",
                    get(nodes::get_node).delete(nodes::remove_node),
                )
                .route("/nodes/add", post(nodes::add_node))
                .route("/failover", post(failover::trigger_failover))
                .route("/switchover", post(switchover::trigger_switchover))
                .route("/replication/slots", get(replication::list_slots))
                .route("/config", get(config::get_config)),
        )
        .with_state(state)
}

pub async fn serve(state: ApiState, listen_addr: &str) -> anyhow::Result<()> {
    let addr: std::net::SocketAddr = listen_addr.parse()?;
    let app = router(state);
    tracing::info!(%addr, "REST API listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
