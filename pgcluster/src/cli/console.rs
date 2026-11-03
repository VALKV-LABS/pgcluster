use anyhow::{Context, Result};
use clap::Args;

use crate::console::{serve, ConsoleConfig};

#[derive(Args)]
pub struct ConsoleArgs {
    /// Address to listen on
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub listen: String,
    /// URL of the admin API
    #[arg(long, env = "PGCLUSTER_ADMIN_URL")]
    pub admin_url: Option<String>,
    /// API key for pgcluster-admin
    #[arg(long, env = "PGCLUSTER_ADMIN_KEY", hide_env_values = true)]
    pub admin_key: Option<String>,
    /// CA certificate that signed the admin API's certificate
    #[arg(long, env = "PGCLUSTER_ADMIN_CA")]
    pub admin_ca: Option<String>,
    /// REST API of a pgcluster node. Repeat for each node
    #[arg(long = "cluster-url")]
    pub cluster_urls: Vec<String>,
    /// API key for pgcluster
    #[arg(long, env = "PGCLUSTER_API_KEY", hide_env_values = true)]
    pub cluster_key: Option<String>,
    /// Postgres URL of the cluster's proxy, for running SQL against it
    #[arg(long, env = "PGCLUSTER_CONSOLE_SQL_URL", hide_env_values = true)]
    pub cluster_sql_url: Option<String>,
}

pub async fn run(args: ConsoleArgs) -> Result<()> {
    let admin_ca = args
        .admin_ca
        .as_ref()
        .map(|path| std::fs::read(path).with_context(|| format!("read {path}")))
        .transpose()?;
    let trim = |url: String| url.trim_end_matches('/').to_string();
    serve(
        &args.listen,
        ConsoleConfig {
            admin_url: args.admin_url.map(trim),
            admin_key: args.admin_key,
            admin_ca,
            cluster_urls: args.cluster_urls.into_iter().map(trim).collect(),
            cluster_key: args.cluster_key,
            cluster_sql_url: args.cluster_sql_url,
        },
    )
    .await
}
