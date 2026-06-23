use super::output;
use anyhow::Result;
use clap::{Args, Subcommand};

#[derive(Args)]
pub struct ConfigArgs {
    #[command(subcommand)]
    pub command: ConfigCommands,
}

#[derive(Subcommand)]
pub enum ConfigCommands {
    /// Validate a config file without starting the server
    Validate {
        /// Path to config file
        #[arg(default_value = "/etc/pgcluster/config.toml")]
        path: String,
    },
    /// Show the current running config from the API
    Show {
        #[arg(long, default_value = "127.0.0.1:8080")]
        api: String,
        #[arg(long)]
        json: bool,
    },
}

pub async fn run(args: ConfigArgs) -> Result<()> {
    match args.command {
        ConfigCommands::Validate { path } => {
            let cfg = crate::config::PgClusterConfig::from_file(&path)?;
            crate::config::validate::validate(&cfg)?;
            println!("config OK: {}", path);
        }
        ConfigCommands::Show { api, json } => {
            let client = super::client::ApiClient::new(&api);
            let resp: serde_json::Value = client.get("/api/config").await?;
            if json {
                output::print_json(&resp);
            } else {
                output::print_kv(&[
                    ("cluster_name", resp["cluster_name"].as_str().unwrap_or("")),
                    (
                        "node_id",
                        &resp["node_id"].as_u64().unwrap_or(0).to_string(),
                    ),
                    ("proxy_addr", resp["proxy_addr"].as_str().unwrap_or("")),
                    ("api_addr", resp["api_addr"].as_str().unwrap_or("")),
                ]);
            }
        }
    }
    Ok(())
}
