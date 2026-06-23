//! Proxy layer: PostgreSQL wire protocol proxy with connection pooling
//! and read/write routing.

pub mod backend;
pub mod connection;
pub mod pool;
pub mod protocol;
pub mod router;
pub mod session;
pub mod ssl;

use std::sync::Arc;

use anyhow::{Context, Result};
use axum::{
    extract::State, http::StatusCode, response::IntoResponse, routing::get, Router as AxumRouter,
};
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::{config::ProxyConfig, metrics_registry::Metrics, raft::TopologyWatch, tls::TlsManager};

use pool::ConnectionPool;
use router::Router;

// ── ProxyServer ───────────────────────────────────────────────────────────────

/// Listens for Postgres client connections and routes them to backends.
pub struct ProxyServer {
    config: ProxyConfig,
    router: Arc<Router>,
    pool: Arc<ConnectionPool>,
    tls: Option<Arc<TlsManager>>,
    metrics: Arc<Metrics>,
}

impl ProxyServer {
    pub fn new(
        config: ProxyConfig,
        topology: TopologyWatch,
        tls: Option<Arc<TlsManager>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let pool = Arc::new(ConnectionPool::new(config.pool.clone()));
        let router = Arc::new(Router::new(topology, config.read_routing.clone()));

        Self {
            config,
            router,
            pool,
            tls,
            metrics,
        }
    }

    /// Bind the proxy TCP listener and enter the accept loop.
    ///
    /// Spawns a new `ProxyConnection::run` task for each incoming client.
    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.config.listen_addr)
            .await
            .with_context(|| format!("bind proxy listener on {}", self.config.listen_addr))?;

        info!(addr = %self.config.listen_addr, "proxy listening");

        // Build TLS acceptor once, share via Arc.
        let tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>> = self
            .tls
            .as_ref()
            .map(|tm| Arc::new(tm.server_tls_acceptor()));

        // Idle connection reaper: evict stale pool connections every 30 s.
        let pool_reaper = Arc::clone(&self.pool);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                pool_reaper.evict_idle().await;
            }
        });

        loop {
            let (stream, peer_addr) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    error!("accept error: {e}");
                    continue;
                }
            };

            let router = Arc::clone(&self.router);
            let pool = Arc::clone(&self.pool);
            let tls_clone = tls_acceptor.clone();
            let m = Arc::clone(&self.metrics);

            tokio::spawn(async move {
                m.proxy_connections_total.inc();
                m.connected_clients.inc();
                tracing::debug!(%peer_addr, "client connected");
                if let Err(e) =
                    connection::ProxyConnection::run(stream, router, pool, tls_clone).await
                {
                    tracing::debug!(%peer_addr, "connection ended: {e}");
                }
                m.connected_clients.dec();
            });
        }
    }

    /// Bind a health-check HTTP server on `config.health_listen_addr`.
    ///
    /// Routes:
    /// - `GET /health` → 200 OK if a primary is known, 503 if not.
    pub async fn run_health_endpoint(&self) -> Result<()> {
        let router_state = Arc::clone(&self.router);

        let app = AxumRouter::new()
            .route("/health", get(health_handler))
            .with_state(router_state);

        let listener = TcpListener::bind(&self.config.health_listen_addr)
            .await
            .with_context(|| {
                format!("bind health endpoint on {}", self.config.health_listen_addr)
            })?;

        info!(addr = %self.config.health_listen_addr, "health endpoint listening");

        axum::serve(listener, app)
            .await
            .context("health endpoint server error")
    }
}

// ── Health handler ────────────────────────────────────────────────────────────

async fn health_handler(State(router): State<Arc<Router>>) -> impl IntoResponse {
    if router.primary_addr().is_some() {
        (StatusCode::OK, "ok")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "no primary")
    }
}
