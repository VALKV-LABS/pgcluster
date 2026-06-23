use super::{client::ApiClient, output};
use anyhow::Result;
use clap::{Args, Subcommand};

#[derive(Args)]
pub struct ReplicationArgs {
    #[command(subcommand)]
    pub command: ReplicationCommands,
}

#[derive(Subcommand)]
pub enum ReplicationCommands {
    /// List replication slots
    Slots {
        #[arg(long, default_value = "127.0.0.1:8080")]
        api: String,
        #[arg(long)]
        json: bool,
    },
}

pub async fn run(args: ReplicationArgs) -> Result<()> {
    match args.command {
        ReplicationCommands::Slots { api, json } => {
            let client = ApiClient::new(&api);
            let resp: serde_json::Value = client.get("/api/replication/slots").await?;
            if json {
                output::print_json(&resp);
            } else {
                let slots = resp.as_array().cloned().unwrap_or_default();
                let rows: Vec<Vec<String>> = slots
                    .iter()
                    .map(|s| {
                        vec![
                            s["node_id"].as_str().unwrap_or("").into(),
                            s["slot_name"].as_str().unwrap_or("").into(),
                        ]
                    })
                    .collect();
                output::print_table(&["NODE_ID", "SLOT_NAME"], &rows);
            }
        }
    }
    Ok(())
}
