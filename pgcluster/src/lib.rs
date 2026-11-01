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
use tracing::{error, info, warn};

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
    if cfg.api.insecure_no_auth {
        warn!("REST API is running with no credentials (insecure_no_auth = true)");
    }

    // Build TLS manager before Raft so peer connections can use it.
    let tls_mgr: Option<Arc<tls::TlsManager>> = if cfg.tls.insecure_plaintext {
        warn!("gRPC and Postgres connections are in plaintext (insecure_plaintext = true)");
        None
    } else {
        Some(Arc::new(tls::TlsManager::new(&cfg.tls)?))
    };

    let auth_password = std::env::var(&cfg.proxy.auth_password_env).unwrap_or_default();
    if auth_password.is_empty() {
        anyhow::bail!(
            "[proxy] set {} to the password for the {} role",
            cfg.proxy.auth_password_env,
            cfg.proxy.auth_user
        );
    }
    let auth_role = proxy::auth::AuthRole {
        user: cfg.proxy.auth_user.clone(),
        password: auth_password,
        ca_pem: tls_mgr.as_ref().map(|t| t.ca_cert_pem().to_vec()),
    };

    let raft_tls = tls_mgr.as_ref().map(|t| t.tonic_client_tls_config());
    let raft_node = Arc::new(raft::RaftNode::start(&cfg, raft_tls).await?);
    let topology_rx = raft_node.topology_rx.clone();

    // On bootstrap, seed the Raft topology from the TOML [[nodes.node]] list.
    // This runs once: if nodes are already in the topology (restart), the
    // AddNode commands are idempotent (HashMap::insert overwrites, safe).
    if cfg.raft.bootstrap && !cfg.nodes.node.is_empty() {
        let seed_raft = raft_node.clone();
        let seed_nodes = cfg.nodes.node.clone();
        let seed_topology = topology_rx.clone();
        let seed_mode = cfg.replication.replication_mode;
        tokio::spawn(async move {
            // Wait until this instance is the Raft leader and has applied its
            // log (fast in single-node bootstrap).
            for _ in 0..50u32 {
                let m = seed_raft.raft.metrics().borrow().clone();
                let applied = m.last_applied.map(|id| id.index) >= m.last_log_index;
                if m.current_leader == Some(m.id) && applied {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            // A restart must not repeat the seed: it would move the primary back
            // to the highest-priority node and undo any online mode change.
            if !seed_topology.current().node_configs.is_empty() {
                return;
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
            let cmd = raft::commands::TopologyCommand::SetReplicationMode { mode: seed_mode };
            if let Err(e) = seed_raft.raft.client_write(cmd).await {
                tracing::warn!(err = %e, "bootstrap seed: SetReplicationMode failed");
            }
            tracing::info!(count = seed_nodes.len(), "bootstrap seed complete");
        });
    }
    let metrics = metrics_registry::Metrics::new()?;

    let pool = Arc::new(match tls_mgr.as_ref() {
        Some(t) => agent_clients::AgentClientPool::new_with_tls(t.tonic_client_tls_config()),
        None => agent_clients::AgentClientPool::new(),
    });

    // Build the proxy Router early so it can be shared between the ProxyServer
    // (which uses it for routing) and ApiState (which uses it for drain control).
    let quorum = Arc::new(raft::lease::QuorumLease::new(
        std::time::Duration::from_millis(cfg.proxy.quorum_lease_ms),
    ));
    raft::lease::spawn_renewal(
        quorum.clone(),
        raft_node.raft.clone(),
        tls_mgr.as_ref().map(|t| t.tonic_client_tls_config()),
    );
    let proxy_router = Arc::new(
        proxy::Router::new(topology_rx.clone(), cfg.proxy.read_routing.clone())
            .with_quorum(quorum.clone()),
    );

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
        let raft = raft_node.raft.clone();
        tasks.push(tokio::spawn(async move {
            let addr: std::net::SocketAddr = match addr_str.parse() {
                Ok(a) => a,
                Err(e) => {
                    error!("invalid raft addr: {e}");
                    return;
                }
            };
            if let Err(e) = raft::serve_grpc(raft, addr, raft_tls).await {
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
            Arc::new(proxy::auth::AuthQuery::new(auth_role.clone())),
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
        let replication = cfg.replication.clone();
        let backup_settings = cfg
            .backup
            .as_ref()
            .filter(|b| b.enabled)
            .map(backup::walg::settings);
        let repl_password =
            std::env::var(&cfg.replication.replication_password_env).unwrap_or_default();
        tasks.push(tokio::spawn(async move {
            let mut monitor = node_monitor::NodeMonitor::new(
                raft,
                topo,
                pool,
                fcfg,
                mon_metrics,
                &replication,
                &auth_role,
                backup_settings,
                repl_password,
                op_in_progress.clone(),
                quorum,
            );
            monitor.run().await;
        }));
    }

    // Backup scheduler (only started when [backup] section is present in config)
    if let Some(backup_cfg) = cfg.backup.clone() {
        let mut scheduler = backup::BackupScheduler::new(
            raft_node.clone(),
            topology_rx.clone(),
            backup_cfg,
            pool.clone(),
        );
        tasks.push(tokio::spawn(async move { scheduler.run().await }));
    }

    info!(raft_grpc = %raft_grpc_addr, api = %cfg.api.listen_addr,
          proxy = %cfg.proxy.listen_addr, "all subsystems started");

    let _ = futures::future::select_all(tasks).await;
    Ok(())
}
