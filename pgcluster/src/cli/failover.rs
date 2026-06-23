use super::client::ApiClient;
use anyhow::Result;
use clap::Args;
use serde::Serialize;

#[derive(Args)]
pub struct FailoverArgs {
    /// Node ID that has failed (will be marked offline)
    pub failed_node: String,
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub api: String,
}

#[derive(Serialize)]
struct FailoverReq {
    failed_node_id: String,
}

pub async fn run(args: FailoverArgs) -> Result<()> {
    let client = ApiClient::new(&args.api);
    let resp: serde_json::Value = client
        .post(
            "/api/failover",
            &FailoverReq {
                failed_node_id: args.failed_node,
            },
        )
        .await?;
    println!("{}", resp["message"].as_str().unwrap_or("triggered"));
    Ok(())
}
