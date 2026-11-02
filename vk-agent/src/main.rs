use anyhow::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

use vk_agent::{
    config::AgentConfig,
    heartbeat::HeartbeatTracker,
    postgres::LocalPostgres,
    process::PgCtl,
    server::{serve, AgentServiceImpl},
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

    // Connect on first use: a node waiting to be seeded has no Postgres yet.
    let pg = LocalPostgres::connect_lazy(&config.postgres_url())?;

    let heartbeat = HeartbeatTracker::new(Duration::from_secs(config.heartbeat_timeout_seconds));
    heartbeat.clone().spawn_watchdog();
    if std::path::Path::new(&config.data_dir)
        .join(vk_agent::server::DETACHED_MARKER)
        .exists()
    {
        heartbeat.detach();
    }

    let pg_ctl = Arc::new(PgCtl::new(&config.pg_ctl_path, &config.data_dir));

    // Self-fencing watchdog: if pgcluster heartbeats are absent for fence_timeout seconds
    // after entering safe mode, stop postgres immediately to prevent split-brain writes.
    let fence_secs = config.fence_timeout_seconds;
    if fence_secs == 0 {
        tracing::warn!("self-fencing is off (fence_timeout_seconds = 0)");
    } else {
        let fence_timeout = Duration::from_secs(fence_secs);
        let hb = heartbeat.clone();
        let ctl = pg_ctl.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if let Some(elapsed) = hb.time_in_safe_mode() {
                    // No Postgres means nothing to fence, as on an unseeded node.
                    if elapsed >= fence_timeout && ctl.is_running().await.unwrap_or(false) {
                        tracing::error!(
                            elapsed_secs = elapsed.as_secs(),
                            fence_timeout_secs = fence_secs,
                            "pgcluster contact lost — self-fencing: stopping postgres immediately"
                        );
                        if let Err(e) = ctl.stop_immediate().await {
                            tracing::error!(err = %e, "pg_ctl stop -m immediate failed during self-fence");
                        }
                        std::process::exit(1);
                    }
                }
            }
        });
    }

    // After a restart, bring back the Postgres this agent owns. An unseeded node
    // has none yet, and pgcluster seeds it.
    if config.manage_postgres
        && vk_agent::seed::has_data(std::path::Path::new(&config.data_dir)).await?
        && !pg_ctl.is_running().await.unwrap_or(false)
    {
        let ctl = pg_ctl.clone();
        tokio::spawn(async move {
            if let Err(e) = ctl.start().await {
                tracing::error!(err = %e, "could not start postgres");
            }
        });
    }

    let svc = AgentServiceImpl::new(Arc::new(config.clone()), heartbeat, Arc::new(pg), pg_ctl);

    let addr: SocketAddr = config.listen_addr.parse()?;

    let tls = if config.tls.insecure_plaintext {
        tracing::warn!(%addr, "gRPC server listening in plaintext (insecure_plaintext = true)");
        None
    } else {
        info!(%addr, "gRPC server listening (TLS, client certificates required)");
        Some(vk_agent::tls::server_tls_config(&config.tls)?)
    };

    serve(svc, addr, tls).await?;

    Ok(())
}
