//! pgcluster — High-availability PostgreSQL proxy and cluster manager.

#![allow(dead_code)]

pub mod agent_clients;
pub mod api;
pub mod cli;
pub mod config;
pub mod failover;
pub mod logging;
pub mod metrics_registry;
pub mod node_monitor;
pub mod proxy;
pub mod raft;
pub mod switchover;
pub mod tls;

use anyhow::Result;
use std::sync::Arc;
use tracing::{error, info};

/// Start the full pgcluster server from the given config file path.
pub async fn run_server(config_path: &str) -> Result<()> {
    // sqlx's runtime-tokio-rustls pulls in aws-lc-rs alongside our ring feature;
    // rustls panics if it can't auto-select, so pin ring explicitly.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cfg = config::PgClusterConfig::from_file(config_path)?;
    config::validate::validate(&cfg)?;
    let cfg = Arc::new(cfg);

    logging::init();
    info!(cluster = %cfg.cluster.name, node_id = cfg.raft.node_id, "pgcluster starting");

    let raft_node = Arc::new(raft::RaftNode::start(&cfg).await?);
    let topology_rx = raft_node.topology_rx.clone();

    // On bootstrap, seed the Raft topology from the TOML [[nodes.node]] list.
    // This runs once: if nodes are already in the topology (restart), the
    // AddNode commands are idempotent (HashMap::insert overwrites, safe).
    if cfg.raft.bootstrap && !cfg.nodes.node.is_empty() {
        let seed_raft = raft_node.clone();
        let seed_nodes = cfg.nodes.node.clone();
        let single_node = seed_nodes.len() == 1;
        tokio::spawn(async move {
            // Wait until this instance is the Raft leader (fast in single-node bootstrap).
            for _ in 0..50u32 {
                let m = seed_raft.raft.metrics().borrow().clone();
                if m.current_leader == Some(m.id) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            for node in &seed_nodes {
                let cmd = raft::commands::TopologyCommand::AddNode(raft::topology::NodeConfig {
                    node_id: node.id.clone(),
                    agent_addr: node.agent_addr.clone(),
                    postgres_addr: node.postgres_addr.clone(),
                    priority: node.priority,
                    tags: node.tags.clone(),
                });
                if let Err(e) = seed_raft.raft.client_write(cmd).await {
                    tracing::warn!(node_id = node.id, err = %e, "bootstrap seed: AddNode failed");
                }
            }
            // Single-node cluster: set the only node as primary immediately.
            if single_node {
                let cmd = raft::commands::TopologyCommand::SetPrimary {
                    node_id: seed_nodes[0].id.clone(),
                    at_lsn: 0,
                    new_timeline: 1,
                };
                if let Err(e) = seed_raft.raft.client_write(cmd).await {
                    tracing::warn!(err = %e, "bootstrap seed: SetPrimary failed");
                }
            }
            tracing::info!(count = seed_nodes.len(), "bootstrap seed complete");
        });
    }
    let metrics = metrics_registry::Metrics::new()?;
    let pool = Arc::new(agent_clients::AgentClientPool::new());

    let tls_mgr: Option<Arc<tls::TlsManager>> = if cfg.tls.auto_generate || cfg.tls.cert.is_some() {
        Some(Arc::new(tls::TlsManager::new(
            &cfg.tls,
            cfg.raft.node_id,
            std::path::Path::new(&cfg.cluster.data_dir),
        )?))
    } else {
        None
    };

    let api_state = api::ApiState {
        raft: raft_node.clone(),
        topology: topology_rx.clone(),
        metrics: metrics.clone(),
        config: cfg.clone(),
    };

    let raft_grpc = raft::RaftGrpcServer::new(raft_node.raft.clone());
    let (raft_svc, topo_svc) = raft_grpc.into_services();
    let raft_grpc_addr = cfg
        .raft
        .peers
        .iter()
        .find(|p| p.id == cfg.raft.node_id)
        .map(|p| p.addr.clone())
        .unwrap_or_else(|| "0.0.0.0:7000".into());

    let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // Raft gRPC peer server
    {
        let addr_str = raft_grpc_addr.clone();
        tasks.push(tokio::spawn(async move {
            let addr: std::net::SocketAddr = match addr_str.parse() {
                Ok(a) => a,
                Err(e) => {
                    error!("invalid raft addr: {e}");
                    return;
                }
            };
            if let Err(e) = tonic::transport::Server::builder()
                .add_service(raft_svc)
                .add_service(topo_svc)
                .serve(addr)
                .await
            {
                error!(err = %e, "Raft gRPC server failed");
            }
        }));
    }

    // REST API
    {
        let api_addr = cfg.api.listen_addr.clone();
        let api_state = api_state.clone();
        tasks.push(tokio::spawn(async move {
            if let Err(e) = api::serve(api_state, &api_addr).await {
                error!(err = %e, "REST API failed");
            }
        }));
    }

    // Dedicated Prometheus metrics server (separate port, no auth)
    {
        let metrics_addr = cfg.metrics.listen_addr.clone();
        let metrics_clone = metrics.clone();
        tasks.push(tokio::spawn(async move {
            use axum::{extract::State, routing::get, Router};
            let app = Router::new()
                .route(
                    "/metrics",
                    get(|State(m): State<Arc<metrics_registry::Metrics>>| async move {
                        m.render().unwrap_or_else(|e| e.to_string())
                    }),
                )
                .with_state(metrics_clone);
            let addr: std::net::SocketAddr = match metrics_addr.parse() {
                Ok(a) => a,
                Err(e) => {
                    error!("invalid metrics addr: {e}");
                    return;
                }
            };
            tracing::info!(%addr, "Prometheus metrics listening");
            let listener = match tokio::net::TcpListener::bind(addr).await {
                Ok(l) => l,
                Err(e) => {
                    error!("metrics bind failed: {e}");
                    return;
                }
            };
            if let Err(e) = axum::serve(listener, app).await {
                error!(err = %e, "metrics server failed");
            }
        }));
    }

    // Proxy server + health endpoint
    {
        let proxy_cfg = cfg.proxy.clone();
        let proxy_tls = tls_mgr.clone();
        let proxy_topo = topology_rx.clone();
        let proxy_metrics = metrics.clone();
        let srv = Arc::new(proxy::ProxyServer::new(
            proxy_cfg,
            proxy_topo,
            proxy_tls,
            proxy_metrics,
        ));

        let srv2 = Arc::clone(&srv);
        tasks.push(tokio::spawn(async move {
            if let Err(e) = srv2.run().await {
                error!(err = %e, "proxy server failed");
            }
        }));

        let srv3 = Arc::clone(&srv);
        tasks.push(tokio::spawn(async move {
            if let Err(e) = srv3.run_health_endpoint().await {
                error!(err = %e, "proxy health endpoint failed");
            }
        }));
    }

    // Node monitor
    {
        let raft = raft_node.clone();
        let topo = topology_rx.clone();
        let pool = pool.clone();
        let fcfg = cfg.failover.clone();
        let mon_metrics = metrics.clone();
        tasks.push(tokio::spawn(async move {
            let mut monitor =
                node_monitor::NodeMonitor::new(raft, topo, pool, fcfg, mon_metrics);
            monitor.run().await;
        }));
    }

    info!(raft_grpc = %raft_grpc_addr, api = %cfg.api.listen_addr,
          proxy = %cfg.proxy.listen_addr, "all subsystems started");

    let _ = futures::future::select_all(tasks).await;
    Ok(())
}
