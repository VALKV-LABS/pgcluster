use super::ApiState;
use axum::{extract::State, http::StatusCode, Json};
use serde::Serialize;

#[derive(Serialize)]
pub struct ReloadResponse {
    pub reloaded: Vec<String>,
    pub failed: Vec<ReloadFailure>,
}

#[derive(Serialize)]
pub struct ReloadFailure {
    pub node_id: String,
    pub error: String,
}

/// Reload Postgres configuration (SIGHUP / pg_ctl reload) on every node.
///
/// This lets operators update `pg_hba.conf` (or any GUC) and apply it without
/// restarting containers.  The call is fire-and-report: it attempts all nodes
/// and returns a summary of which succeeded and which failed.
pub async fn reload_pg_hba(State(s): State<ApiState>) -> (StatusCode, Json<ReloadResponse>) {
    let topology = s.topology.borrow().clone();
    let pool = s.pool.clone();

    let mut reloaded = Vec::new();
    let mut failed = Vec::new();

    for (node_id, cfg) in &topology.node_configs {
        let mut client = match pool.get_or_connect(node_id, &cfg.agent_addr).await {
            Ok(c) => c,
            Err(e) => {
                failed.push(ReloadFailure {
                    node_id: node_id.clone(),
                    error: format!("connect failed: {e}"),
                });
                continue;
            }
        };

        match client.reload_config().await {
            Ok(resp) if resp.success => {
                reloaded.push(node_id.clone());
            }
            Ok(resp) => {
                failed.push(ReloadFailure {
                    node_id: node_id.clone(),
                    error: resp.error,
                });
            }
            Err(e) => {
                failed.push(ReloadFailure {
                    node_id: node_id.clone(),
                    error: format!("RPC failed: {e}"),
                });
            }
        }
    }

    let status = if failed.is_empty() {
        StatusCode::OK
    } else if reloaded.is_empty() {
        StatusCode::INTERNAL_SERVER_ERROR
    } else {
        StatusCode::MULTI_STATUS
    };

    (status, Json(ReloadResponse { reloaded, failed }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reload_response_serializes() {
        let r = ReloadResponse {
            reloaded: vec!["pg1".into(), "pg2".into()],
            failed: vec![],
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"reloaded\":[\"pg1\",\"pg2\"]"));
        assert!(json.contains("\"failed\":[]"));
    }

    #[test]
    fn partial_failure_serializes() {
        let r = ReloadResponse {
            reloaded: vec!["pg1".into()],
            failed: vec![ReloadFailure {
                node_id: "pg2".into(),
                error: "connect failed".into(),
            }],
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"node_id\":\"pg2\""));
        assert!(json.contains("\"error\":\"connect failed\""));
    }
}
