use super::{client::ApiClient, output};
use anyhow::Result;
use clap::{Args, Subcommand};
use serde::Serialize;

#[derive(Args)]
pub struct NodeArgs {
    #[command(subcommand)]
    pub command: NodeCommands,
}

#[derive(Subcommand)]
pub enum NodeCommands {
    /// List all nodes
    List {
        #[arg(long, default_value = "127.0.0.1:8080")]
        api: String,
        #[arg(long)]
        json: bool,
    },
    /// Get details for a specific node
    Get {
        node_id: String,
        #[arg(long, default_value = "127.0.0.1:8080")]
        api: String,
        #[arg(long)]
        json: bool,
    },
    /// Add a new node to the cluster
    Add {
        node_id: String,
        agent_addr: String,
        postgres_addr: String,
        #[arg(long, default_value = "100")]
        priority: u32,
        #[arg(long, default_value = "127.0.0.1:8080")]
        api: String,
    },
    /// Remove a node from the cluster
    Remove {
        node_id: String,
        #[arg(long, default_value = "127.0.0.1:8080")]
        api: String,
    },
}

#[derive(Serialize)]
struct AddNodeReq {
    node_id: String,
    agent_addr: String,
    postgres_addr: String,
    priority: u32,
}

pub async fn run(args: NodeArgs) -> Result<()> {
    match args.command {
        NodeCommands::List { api, json } => {
            let client = ApiClient::new(&api);
            let resp: serde_json::Value = client.get("/api/nodes").await?;
            if json {
                output::print_json(&resp);
            } else {
                let nodes = resp["nodes"].as_array().cloned().unwrap_or_default();
                let rows: Vec<Vec<String>> = nodes
                    .iter()
                    .map(|n| {
                        vec![
                            n["node_id"].as_str().unwrap_or("").into(),
                            n["role"].as_str().unwrap_or("").into(),
                            n["postgres_addr"].as_str().unwrap_or("").into(),
                            n["lag_bytes"].as_u64().unwrap_or(0).to_string(),
                        ]
                    })
                    .collect();
                output::print_table(&["NODE_ID", "ROLE", "POSTGRES_ADDR", "LAG_BYTES"], &rows);
            }
        }
        NodeCommands::Get { node_id, api, json } => {
            let client = ApiClient::new(&api);
            let resp: serde_json::Value = client.get(&format!("/api/nodes/{}", node_id)).await?;
            if json {
                output::print_json(&resp);
            } else {
                output::print_kv(&[
                    ("node_id", resp["node_id"].as_str().unwrap_or("")),
                    ("role", resp["role"].as_str().unwrap_or("")),
                    (
                        "postgres_addr",
                        resp["postgres_addr"].as_str().unwrap_or(""),
                    ),
                    ("agent_addr", resp["agent_addr"].as_str().unwrap_or("")),
                ]);
            }
        }
        NodeCommands::Add {
            node_id,
            agent_addr,
            postgres_addr,
            priority,
            api,
        } => {
            let client = ApiClient::new(&api);
            client
                .post::<_, serde_json::Value>(
                    "/api/nodes/add",
                    &AddNodeReq {
                        node_id,
                        agent_addr,
                        postgres_addr,
                        priority,
                    },
                )
                .await?;
            println!("node added");
        }
        NodeCommands::Remove { node_id, api } => {
            let client = ApiClient::new(&api);
            client.delete(&format!("/api/nodes/{}", node_id)).await?;
            println!("node removed");
        }
    }
    Ok(())
}
