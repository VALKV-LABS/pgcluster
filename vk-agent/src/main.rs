use anyhow::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;
use tracing::info;

use vk_agent::{
    config::AgentConfig,
    heartbeat::HeartbeatTracker,
    postgres::LocalPostgres,
    process::PgCtl,
    server::{agent::agent_service_server::AgentServiceServer, AgentServiceImpl},
};

#[tokio::main]
async fn main() -> Result<()> {
    // Init tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Handle --version before anything else.
    let first_arg = std::env::args().nth(1).unwrap_or_default();
    if first_arg == "--version" || first_arg == "-V" {
        println!("vk-agent {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    // Load config from first CLI arg, or default path
    let config_path = if first_arg.is_empty() {
        "/etc/vk-agent/config.toml".into()
    } else {
        first_arg
    };

    let config = AgentConfig::from_file(&config_path)?;
    info!(listen = %config.listen_addr, "vk-agent starting");

    // Connect to postgres (retry up to 30 times with 1s sleep)
    let pg_url = config.postgres_url();
    let pg = {
        let mut last_err = None;
        let mut pg_opt = None;
        for attempt in 1..=30u32 {
            match LocalPostgres::connect(&pg_url).await {
                Ok(p) => {
                    pg_opt = Some(p);
                    break;
                }
                Err(e) => {
                    tracing::warn!(attempt, "postgres not ready: {e}");
                    last_err = Some(e);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
        pg_opt.ok_or_else(|| last_err.unwrap())?
    };

    let heartbeat = HeartbeatTracker::new(std::time::Duration::from_secs(
        config.heartbeat_timeout_seconds,
    ));
    heartbeat.clone().spawn_watchdog();

    let pg_ctl = Arc::new(PgCtl::new(&config.pg_ctl_path, &config.data_dir));

    let svc = AgentServiceImpl::new(Arc::new(config.clone()), heartbeat, Arc::new(pg), pg_ctl);

    let addr: SocketAddr = config.listen_addr.parse()?;
    info!(%addr, "gRPC server listening");

    Server::builder()
        .add_service(AgentServiceServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}
