use super::ApiState;
use axum::{extract::State, http::StatusCode, Json};
use serde::Serialize;

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub primary: Option<String>,
    pub raft_leader: bool,
}

pub async fn health(State(s): State<ApiState>) -> (StatusCode, Json<HealthResponse>) {
    let topology = s.topology.borrow().clone();
    let metrics = s.raft.raft.metrics().borrow().clone();
    let is_leader = metrics.current_leader == Some(metrics.id);
    let primary = if topology.primary_node_id.is_empty() {
        None
    } else {
        Some(topology.primary_node_id.clone())
    };
    let code = if primary.is_some() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        code,
        Json(HealthResponse {
            status: if primary.is_some() {
                "ok"
            } else {
                "no_primary"
            },
            primary,
            raft_leader: is_leader,
        }),
    )
}

#[derive(Serialize)]
pub struct ClusterStatusResponse {
    pub cluster_name: String,
    pub primary_node_id: String,
    pub topology_version: u64,
    pub raft_leader_id: Option<u64>,
    pub node_count: usize,
    pub failover_history_count: usize,
}

pub async fn cluster_status(State(s): State<ApiState>) -> Json<ClusterStatusResponse> {
    let topology = s.topology.borrow().clone();
    let metrics = s.raft.raft.metrics().borrow().clone();
    Json(ClusterStatusResponse {
        cluster_name: s.config.cluster.name.clone(),
        primary_node_id: topology.primary_node_id.clone(),
        topology_version: topology.version,
        raft_leader_id: metrics.current_leader,
        node_count: topology.node_configs.len(),
        failover_history_count: topology.failover_history.len(),
    })
}

/// Format a Unix-seconds timestamp as RFC 3339 (UTC, no external deps).
pub fn unix_to_rfc3339(secs: i64) -> String {
    if secs < 0 {
        return "1970-01-01T00:00:00Z".into();
    }
    let s = secs as u64;
    let sec = s % 60;
    let min = (s / 60) % 60;
    let hour = (s / 3600) % 24;
    let days = s / 86400;
    let (y, mo, d) = days_to_ymd(days);
    format!("{y:04}-{mo:02}-{d:02}T{hour:02}:{min:02}:{sec:02}Z")
}

fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z % 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if mo <= 2 { y + 1 } else { y };
    (y, mo, d)
}

pub async fn topology(State(s): State<ApiState>) -> Json<serde_json::Value> {
    let topo = s.topology.borrow().clone();
    // Serialize topology to JSON, then rewrite failover_history timestamps to RFC 3339.
    let mut json = serde_json::to_value(&topo).unwrap_or(serde_json::Value::Null);
    if let Some(history) = json
        .get_mut("failover_history")
        .and_then(|h| h.as_array_mut())
    {
        for event in history.iter_mut() {
            if let Some(ts) = event.get("triggered_at").and_then(|t| t.as_i64()) {
                event["triggered_at"] = serde_json::Value::String(unix_to_rfc3339(ts));
            }
        }
    }
    Json(json)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_response_serializes_correctly() {
        let r = ClusterStatusResponse {
            cluster_name: "prod".into(),
            primary_node_id: "pg1".into(),
            topology_version: 7,
            raft_leader_id: Some(2),
            node_count: 3,
            failover_history_count: 1,
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"cluster_name\":\"prod\""));
        assert!(json.contains("\"primary_node_id\":\"pg1\""));
        assert!(json.contains("\"topology_version\":7"));
        assert!(json.contains("\"raft_leader_id\":2"));
    }

    #[test]
    fn health_response_no_primary_serializes_null() {
        let r = HealthResponse {
            status: "no_primary",
            primary: None,
            raft_leader: false,
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"primary\":null"));
        assert!(json.contains("\"status\":\"no_primary\""));
    }
}
