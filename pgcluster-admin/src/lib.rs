//! pgcluster-admin: one API for every cluster in a fleet.
//!
//! It holds what each database should look like and hands that to the
//! `vk-agent host` on each machine. It never decides who is primary, runs no SQL
//! on user tables, and the data plane keeps working when it is down.

pub mod agent;
pub mod api;
pub mod ca;
pub mod config;
pub mod error;
pub mod model;
pub mod operations;
pub mod reconciler;
pub mod secrets;
pub mod state;
pub mod tls;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio::net::TcpListener;

use config::AdminConfig;
use state::AdminState;

const RECONCILE_TICK: Duration = Duration::from_millis(500);

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Create the hosts in `bootstrap_hosts`. A host or token that exists is left
/// alone, so a restart cannot revive a used or expired token.
pub async fn load_bootstrap_hosts(pool: &PgPool, config: &AdminConfig) -> Result<()> {
    for host in &config.bootstrap_hosts {
        let mut tx = pool.begin().await?;
        sqlx::query(
            "INSERT INTO hosts (id, name, failure_domain) VALUES ($1, $2, $3) \
             ON CONFLICT (name) DO NOTHING",
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(&host.name)
        .bind(&host.failure_domain)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO enrollment_tokens (token_hash, host_id, expires_at) \
             SELECT $1, id, now() + make_interval(mins => $3) FROM hosts WHERE name = $2 \
             ON CONFLICT (token_hash) DO NOTHING",
        )
        .bind(host.token_sha256.to_lowercase())
        .bind(&host.name)
        .bind(host.token_ttl_minutes as i32)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
    }
    Ok(())
}

pub async fn run(config_path: &str) -> Result<()> {
    // sqlx's rustls pulls in aws-lc-rs too, so pin ring, as pgcluster does.
    let _ = rustls::crypto::ring::default_provider().install_default();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = AdminConfig::from_file(config_path)?;
    let database_url = std::env::var(&config.database_url_env).with_context(|| {
        format!(
            "set {} to the metadata database URL",
            config.database_url_env
        )
    })?;
    let secret_key = std::env::var(&config.secret_key_env)
        .with_context(|| format!("set {} to a base64 32-byte key", config.secret_key_env))?;
    let secrets = secrets::SecretBox::from_base64(&secret_key)?;
    let ca = Arc::new(ca::Ca::load_or_create(Path::new(&config.tls.ca_dir))?);

    let pool = PgPoolOptions::new()
        .max_connections(20)
        .acquire_timeout(Duration::from_secs(30))
        .connect(&database_url)
        .await
        .context("connect to the metadata database")?;
    MIGRATOR.run(&pool).await.context("run migrations")?;
    load_bootstrap_hosts(&pool, &config).await?;

    let state = AdminState::new(pool, ca.clone(), secrets, &config.api.api_keys);
    let server_cert = ca.issue_server(&config.tls.server_names)?;

    let agent_listener = TcpListener::bind(&config.agent_api.listen_addr)
        .await
        .with_context(|| format!("bind {}", config.agent_api.listen_addr))?;
    tracing::info!(addr = %config.agent_api.listen_addr, "agent API listening");
    let agent_app = agent::router(state.clone());
    let agent_acceptor = tls::agent_tls(&ca, &server_cert)?;
    let agent_task = tokio::spawn(tls::serve(agent_listener, agent_acceptor, agent_app));

    let api_listener = TcpListener::bind(&config.api.listen_addr)
        .await
        .with_context(|| format!("bind {}", config.api.listen_addr))?;
    let api_app = api::router(state.clone());
    let api_task = if config.api.insecure_plaintext {
        tracing::warn!(addr = %config.api.listen_addr,
            "admin API listening in plaintext (insecure_plaintext = true)");
        tokio::spawn(async move {
            if let Err(e) = axum::serve(api_listener, api_app).await {
                tracing::error!(err = %e, "admin API failed");
            }
        })
    } else {
        tracing::info!(addr = %config.api.listen_addr, "admin API listening");
        let acceptor = tls::server_tls(&server_cert)?;
        tokio::spawn(tls::serve(api_listener, acceptor, api_app))
    };

    let reconciler = reconciler::Reconciler::new(state, RECONCILE_TICK);
    let reconcile_task = tokio::spawn(reconciler.run());

    tokio::select! {
        result = agent_task => result.context("agent API stopped")?,
        result = api_task => result.context("admin API stopped")?,
        result = reconcile_task => result.context("reconciler stopped")?,
    }
    anyhow::bail!("a server task stopped")
}
