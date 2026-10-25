pub mod auth;
pub mod backup;
pub mod config;
pub mod failover;
pub mod forward;
pub mod maintenance;
pub mod metrics;
pub mod nodes;
pub mod pg_hba;
pub mod replication;
pub mod status;
pub mod switchover;

use crate::agent_clients::AgentClientPool;
use crate::config::PgClusterConfig;
use crate::metrics_registry::Metrics;
use crate::proxy::Router as ProxyRouter;
use crate::raft::{RaftNode, TopologyWatch};
use axum::{
    middleware,
    routing::{get, post},
    Router,
};
use std::sync::{atomic::AtomicBool, Arc};

#[derive(Clone)]
pub struct ApiState {
    pub raft: Arc<RaftNode>,
    pub topology: TopologyWatch,
    pub metrics: Arc<Metrics>,
    pub config: Arc<PgClusterConfig>,
    /// Prevents concurrent switchover / failover operations.
    /// `true` while a switchover task is running; API returns 409 if already set.
    pub op_in_progress: Arc<AtomicBool>,
    /// Shared reference to the proxy router for connection draining during
    /// planned switchovers. `None` when the proxy is not running on this node.
    pub proxy_drain: Option<Arc<ProxyRouter>>,
    /// TLS-aware agent client pool shared by all API handlers.
    pub pool: Arc<AgentClientPool>,
}

impl ApiState {
    pub fn new(
        raft: Arc<RaftNode>,
        topology: TopologyWatch,
        metrics: Arc<Metrics>,
        config: Arc<PgClusterConfig>,
        proxy_drain: Option<Arc<ProxyRouter>>,
        pool: Arc<AgentClientPool>,
        op_in_progress: Arc<AtomicBool>,
    ) -> Self {
        Self {
            raft,
            topology,
            metrics,
            config,
            op_in_progress,
            proxy_drain,
            pool,
        }
    }
}

pub fn router(state: ApiState) -> Router {
    Router::new()
        // /health stays at root for Docker/LB health checks (always public)
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
                .route("/failover/history", get(failover::failover_history))
                .route("/switchover", post(switchover::trigger_switchover))
                .route("/replication/slots", get(replication::list_slots))
                .route("/config", get(config::get_config))
                .route(
                    "/maintenance/:node_id",
                    post(maintenance::enter_maintenance).delete(maintenance::exit_maintenance),
                )
                .route("/pg-hba/reload", post(pg_hba::reload_pg_hba))
                .route(
                    "/backups",
                    get(backup::list_backups).post(backup::trigger_backup),
                )
                .route(
                    "/backups/:backup_id",
                    axum::routing::delete(backup::delete_backup),
                )
                .layer(middleware::from_fn_with_state(
                    state.clone(),
                    auth::require_auth,
                )),
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
