//! pgcluster — High-availability PostgreSQL proxy and cluster manager.

#![allow(dead_code)]

pub mod agent_clients;
pub mod api;
pub mod backup;
pub mod cli;
pub mod config;
pub mod console;
pub mod credentials;
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

/// Where to bind Raft, given this node's address in `peers`. A hostname such as
/// "pgcluster-1:7000" binds every interface, because it may not resolve to one
/// this process can bind. A loopback address stays on loopback, so a one-node
/// cluster has no Raft port reachable from outside.
fn raft_bind_addr(peer_addr: &str) -> String {
    match peer_addr.parse::<std::net::SocketAddr>() {
        Ok(addr) if addr.ip().is_loopback() => peer_addr.to_string(),
        _ => peer_addr
            .rsplit(':')
            .next()
            .map(|port| format!("0.0.0.0:{port}"))
            .unwrap_or_else(|| peer_addr.to_string()),
    }
}

/// Read passwords, API keys and retention again on SIGHUP, none of which needs a
/// restart. A file that does not load leaves the old values in use.
fn spawn_settings_reload(
    config_path: String,
    secrets_file: Option<String>,
    runtime: Arc<config::runtime::Runtime>,
) -> Result<()> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut hangup = signal(SignalKind::hangup())?;
    tokio::spawn(async move {
        while hangup.recv().await.is_some() {
            if let Some(path) = &secrets_file {
                if let Err(e) = credentials::load(path) {
                    error!(err = format!("{e:#}"), "kept the old passwords");
                }
            }
            match config::PgClusterConfig::from_file(&config_path) {
                Ok(cfg) => runtime.apply(&cfg),
                Err(e) => error!(err = format!("{e:#}"), "kept the old settings"),
            }
        }
    });
    Ok(())
}

/// Read the certificate files again on SIGHUP, and apply them to everything that
/// holds one. Connections stay up. Files that do not load leave the old ones in
/// use, which still work: they are renewed well before they expire.
#[allow(clippy::too_many_arguments)]
fn spawn_certificate_reload(
    cfg: Arc<config::PgClusterConfig>,
    client_tls: tls::SharedClientTls,
    acceptor: tls::SharedAcceptor,
    backend_tls: tls::SharedConnector,
    ca_pem: tls::SharedCa,
    server_tls: tokio::sync::watch::Sender<tonic::transport::ServerTlsConfig>,
    pool: Arc<agent_clients::AgentClientPool>,
) -> Result<()> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut hangup = signal(SignalKind::hangup())?;
    tokio::spawn(async move {
        while hangup.recv().await.is_some() {
            match tls::TlsManager::new(&cfg.tls) {
                Ok(manager) => {
                    client_tls.store(Arc::new(manager.tonic_client_tls_config()));
                    acceptor.store(Arc::new(manager.server_tls_acceptor()));
                    backend_tls.store(Arc::new(manager.client_tls_connector()));
                    ca_pem.store(Arc::new(manager.ca_cert_pem().to_vec()));
                    credentials::bump();
                    server_tls.send_replace(manager.tonic_server_tls_config());
                    pool.clear();
                    info!("reloaded certificates");
                }
                Err(e) => error!(err = format!("{e:#}"), "kept the old certificates"),
            }
        }
    });
    Ok(())
}

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
    if let Some(path) = &cfg.cluster.secrets_file {
        credentials::load(path)?;
    }
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

    if credentials::get(&cfg.proxy.auth_password_env).is_empty() {
        anyhow::bail!(
            "[proxy] set {} to the password for the {} role",
            cfg.proxy.auth_password_env,
            cfg.proxy.auth_user
        );
    }
    let ca_pem: Option<tls::SharedCa> = tls_mgr
        .as_ref()
        .map(|t| Arc::new(arc_swap::ArcSwap::from_pointee(t.ca_cert_pem().to_vec())));
    let auth_role = proxy::auth::AuthRole {
        user: cfg.proxy.auth_user.clone(),
        password_env: cfg.proxy.auth_password_env.clone(),
        ca_pem: ca_pem.clone(),
    };

    // Each of these is swapped when certificates reload.
    let client_tls = tls_mgr
        .as_ref()
        .map(|t| tls::shared_client_tls(t.tonic_client_tls_config()));
    let acceptor: Option<tls::SharedAcceptor> = tls_mgr
        .as_ref()
        .map(|t| Arc::new(arc_swap::ArcSwap::from_pointee(t.server_tls_acceptor())));
    let backend_tls: Option<tls::SharedConnector> = tls_mgr
        .as_ref()
        .map(|t| Arc::new(arc_swap::ArcSwap::from_pointee(t.client_tls_connector())));
    let server_tls = tls_mgr
        .as_ref()
        .map(|t| tokio::sync::watch::channel(t.tonic_server_tls_config()));

    let raft_node = Arc::new(raft::RaftNode::start(&cfg, client_tls.clone()).await?);
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
            // log. That takes a second peer, which may start well after this
            // one on another host.
            for _ in 0..3000u32 {
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

    let pool = Arc::new(match client_tls.as_ref() {
        Some(tls) => agent_clients::AgentClientPool::new_with_tls(tls.clone()),
        None => agent_clients::AgentClientPool::new(),
    });

    // Build the proxy Router early so it can be shared between the ProxyServer
    // (which uses it for routing) and ApiState (which uses it for drain control).
    let quorum = Arc::new(raft::lease::QuorumLease::new(
        std::time::Duration::from_millis(cfg.proxy.quorum_lease_ms),
    ));
    raft::lease::spawn_renewal(quorum.clone(), raft_node.raft.clone(), client_tls.clone());
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
    let runtime = api_state.runtime.clone();
    spawn_settings_reload(
        config_path.to_string(),
        cfg.cluster.secrets_file.clone(),
        runtime.clone(),
    )?;

    let raft_peer_addr = cfg
        .raft
        .peers
        .iter()
        .find(|p| p.id == cfg.raft.node_id)
        .map(|p| p.addr.clone())
        .unwrap_or_else(|| "0.0.0.0:7000".into());
    let raft_grpc_addr = raft_bind_addr(&raft_peer_addr);

    let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // Raft gRPC peer server
    {
        let addr_str = raft_grpc_addr.clone();
        let raft_tls = server_tls.as_ref().map(|(_, rx)| rx.clone());
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
        let allowlist = proxy::allowlist::shared(cfg.proxy.allowed_cidrs.as_deref())?;
        proxy::allowlist::spawn_reload(config_path.to_string(), allowlist.clone())?;
        let proxy_cfg = cfg.proxy.clone();
        let proxy_metrics = metrics.clone();
        let srv = Arc::new(proxy::ProxyServer::new(
            proxy_cfg,
            proxy_router.clone(),
            acceptor.clone(),
            backend_tls.clone(),
            proxy_metrics,
            Arc::new(proxy::auth::AuthQuery::new(auth_role.clone())),
            allowlist,
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
            runtime.clone(),
        );
        tasks.push(tokio::spawn(async move { scheduler.run().await }));
    }

    if let (Some(client_tls), Some(acceptor), Some(backend_tls), Some(ca_pem), Some((tx, _))) =
        (client_tls, acceptor, backend_tls, ca_pem, server_tls)
    {
        spawn_certificate_reload(
            cfg.clone(),
            client_tls,
            acceptor,
            backend_tls,
            ca_pem,
            tx,
            pool.clone(),
        )?;
    }

    info!(raft_grpc = %raft_grpc_addr, api = %cfg.api.listen_addr,
          proxy = %cfg.proxy.listen_addr, "all subsystems started");

    let _ = futures::future::select_all(tasks).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::raft_bind_addr;

    #[test]
    fn loopback_stays_on_loopback() {
        assert_eq!(raft_bind_addr("127.0.0.1:20003"), "127.0.0.1:20003");
    }

    #[test]
    fn other_addresses_bind_every_interface() {
        assert_eq!(raft_bind_addr("pgcluster-1:7000"), "0.0.0.0:7000");
        assert_eq!(raft_bind_addr("10.0.0.1:7000"), "0.0.0.0:7000");
    }
}
