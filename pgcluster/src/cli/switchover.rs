use super::client::ApiClient;
use anyhow::Result;
use clap::Args;
use serde::Serialize;

#[derive(Args)]
pub struct SwitchoverArgs {
    /// Node ID to promote as the new primary
    pub target: String,
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub api: String,
    /// Maximum tolerable replica lag in bytes
    #[arg(long, default_value = "1048576")]
    pub max_lag: u64,
    /// Timeout waiting for replica sync (seconds)
    #[arg(long, default_value = "30")]
    pub timeout: u64,
}

#[derive(Serialize)]
struct SwitchoverReq {
    target_node_id: String,
    max_lag_bytes: u64,
    timeout_secs: u64,
}

pub async fn run(args: SwitchoverArgs) -> Result<()> {
    let client = ApiClient::new(&args.api);
    let resp: serde_json::Value = client
        .post(
            "/api/switchover",
            &SwitchoverReq {
                target_node_id: args.target.clone(),
                max_lag_bytes: args.max_lag,
                timeout_secs: args.timeout,
            },
        )
        .await?;
    println!("{}", resp["message"].as_str().unwrap_or("initiated"));
    Ok(())
}
