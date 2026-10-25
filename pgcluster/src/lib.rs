//! pgcluster — High-availability PostgreSQL proxy and cluster manager.

#![allow(dead_code)]

pub mod agent_clients;
pub mod api;
pub mod backup;
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

    // Build TLS manager before Raft so we can share the CA cert for peer connections.
    let tls_mgr: Option<Arc<tls::TlsManager>> = if cfg.tls.auto_generate || cfg.tls.cert.is_some() {
        Some(Arc::new(tls::TlsManager::new(
            &cfg.tls,
            cfg.raft.node_id,
            std::path::Path::new(&cfg.cluster.data_dir),
        )?))
    } else {
        None
    };

    let raft_ca_pem = tls_mgr.as_ref().map(|t| t.ca_cert_pem().to_vec());
    let raft_node = Arc::new(raft::RaftNode::start(&cfg, raft_ca_pem).await?);
    let topology_rx = raft_node.topology_rx.clone();

    // On bootstrap, seed the Raft topology from the TOML [[nodes.node]] list.
    // This runs once: if nodes are already in the topology (restart), the
    // AddNode commands are idempotent (HashMap::insert overwrites, safe).
    if cfg.raft.bootstrap && !cfg.nodes.node.is_empty() {
        let seed_raft = raft_node.clone();
        let seed_nodes = cfg.nodes.node.clone();
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
            // Set the highest-priority node as the initial primary.
            if let Some(primary_node) = seed_nodes.iter().max_by_key(|n| n.priority) {
                let cmd = raft::commands::TopologyCommand::SetPrimary {
                    node_id: primary_node.id.clone(),
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

    // Use TLS-verified connections to vk-agents when a CA cert is available.
    let pool = Arc::new(match tls_mgr.as_ref() {
        Some(t) => agent_clients::AgentClientPool::new_with_ca(t.ca_cert_pem().to_vec()),
        None => agent_clients::AgentClientPool::new(),
    });

    // Build the proxy Router early so it can be shared between the ProxyServer
    // (which uses it for routing) and ApiState (which uses it for drain control).
    let proxy_router = Arc::new(proxy::Router::new(
        topology_rx.clone(),
        cfg.proxy.read_routing.clone(),
    ));

    // Shared op_in_progress flag: prevents concurrent automatic failover (node_monitor)
    // and operator-initiated failover/switchover (API handlers) from running simultaneously.
    let op_in_progress = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let api_state = api::ApiState::new(
        raft_node.clone(),
        topology_rx.clone(),
        metrics.clone(),
        cfg.clone(),
        Some(proxy_router.clone()),
        pool.clone(),
        op_in_progress.clone(),
    );

    let raft_grpc = raft::RaftGrpcServer::new(raft_node.raft.clone());
    let (raft_svc, topo_svc) = raft_grpc.into_services();
    let raft_peer_addr = cfg
        .raft
        .peers
        .iter()
        .find(|p| p.id == cfg.raft.node_id)
        .map(|p| p.addr.clone())
        .unwrap_or_else(|| "0.0.0.0:7000".into());
    // Peer addrs use hostnames (e.g. "pgcluster-1:7000"); extract port to bind on all interfaces.
    let raft_grpc_addr = raft_peer_addr
        .rsplit(':')
        .next()
        .map(|port| format!("0.0.0.0:{port}"))
        .unwrap_or_else(|| raft_peer_addr.clone());

    let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // Raft gRPC peer server
    {
        let addr_str = raft_grpc_addr.clone();
        let raft_tls = tls_mgr.as_ref().map(|t| t.tonic_server_tls_config());
        tasks.push(tokio::spawn(async move {
            let addr: std::net::SocketAddr = match addr_str.parse() {
                Ok(a) => a,
                Err(e) => {
                    error!("invalid raft addr: {e}");
                    return;
                }
            };
            let mut builder = tonic::transport::Server::builder();
            if let Some(tls_cfg) = raft_tls {
                builder = match builder.tls_config(tls_cfg) {
                    Ok(b) => b,
                    Err(e) => {
                        error!(err = %e, "Raft gRPC TLS config failed");
                        return;
                    }
                };
            }
            if let Err(e) = builder
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
                    get(
                        |State(m): State<Arc<metrics_registry::Metrics>>| async move {
                            m.render().unwrap_or_else(|e| e.to_string())
                        },
                    ),
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
        let proxy_metrics = metrics.clone();
        let srv = Arc::new(proxy::ProxyServer::new(
            proxy_cfg,
            proxy_router.clone(),
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
        let repl_user = cfg.replication.replication_user.clone();
        let repl_password =
            std::env::var(&cfg.replication.replication_password_env).unwrap_or_default();
        let slot_prefix = cfg.replication.slot_prefix.clone();
        tasks.push(tokio::spawn(async move {
            let mut monitor = node_monitor::NodeMonitor::new(
                raft,
                topo,
                pool,
                fcfg,
                mon_metrics,
                repl_user,
                repl_password,
                slot_prefix,
                op_in_progress.clone(),
            );
            monitor.run().await;
        }));
    }

    // Backup scheduler (only started when [backup] section is present in config)
    if let Some(backup_cfg) = cfg.backup.clone() {
        let raft = raft_node.clone();
        let topo = topology_rx.clone();
        let repl_user = cfg.replication.replication_user.clone();
        let repl_password =
            std::env::var(&cfg.replication.replication_password_env).unwrap_or_default();
        tasks.push(tokio::spawn(async move {
            match backup::BackupScheduler::new(raft, topo, backup_cfg, repl_user, repl_password) {
                Ok(mut scheduler) => scheduler.run().await,
                Err(e) => {
                    error!(err = %e, "backup scheduler failed to initialize (check S3 config)")
                }
            }
        }));
    }

    info!(raft_grpc = %raft_grpc_addr, api = %cfg.api.listen_addr,
          proxy = %cfg.proxy.listen_addr, "all subsystems started");

    let _ = futures::future::select_all(tasks).await;
    Ok(())
}
