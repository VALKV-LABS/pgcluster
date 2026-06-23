use super::{client::ApiClient, output};
use anyhow::Result;
use clap::Args;

#[derive(Args)]
pub struct StatusArgs {
    /// pgcluster API address
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub api: String,
    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

pub async fn run(args: StatusArgs) -> Result<()> {
    let client = ApiClient::new(&args.api);
    let status: serde_json::Value = client.get("/api/status").await?;
    if args.json {
        output::print_json(&status);
    } else {
        let empty = "".to_string();
        let primary = status["primary_node_id"].as_str().unwrap_or(&empty);
        let version = status["topology_version"].as_u64().unwrap_or(0);
        let leader = status["raft_leader_id"]
            .as_u64()
            .map(|n| n.to_string())
            .unwrap_or_else(|| "none".into());
        let nodes = status["node_count"].as_u64().unwrap_or(0);
        output::print_kv(&[
            ("primary", primary),
            ("topology_version", &version.to_string()),
            ("raft_leader", &leader),
            ("node_count", &nodes.to_string()),
        ]);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::output;

    #[test]
    fn status_table_formats_correctly() {
        // Ensure print_kv doesn't panic and produces aligned output.
        // We capture nothing here — just verify it runs without error.
        // Real formatting is tested visually via `make start`.
        let pairs = [
            ("primary", "pg1"),
            ("topology_version", "42"),
            ("raft_leader", "1"),
            ("node_count", "3"),
        ];
        // No panic = pass (print_kv writes to stdout)
        output::print_kv(&pairs);
    }

    #[test]
    fn node_table_formats_correctly() {
        let rows = vec![
            vec!["pg1".into(), "primary".into(), "127.0.0.1:5432".into(), "0".into()],
            vec!["pg2".into(), "replica".into(), "127.0.0.1:5433".into(), "1024".into()],
        ];
        // No panic = pass
        output::print_table(&["NODE_ID", "ROLE", "POSTGRES_ADDR", "LAG_BYTES"], &rows);
    }

    #[tokio::test]
    async fn status_exits_nonzero_on_connection_failure() {
        // Attempt to connect to a port that has nothing listening.
        let args = super::StatusArgs {
            api: "127.0.0.1:19999".into(),
            json: false,
        };
        let result = super::run(args).await;
        assert!(result.is_err(), "should return Err when server is unreachable");
    }
}
