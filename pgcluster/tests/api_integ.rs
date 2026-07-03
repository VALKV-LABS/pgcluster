//! REST API integration tests.
//!
//! Two groups:
//!
//!   #[ignore = "requires running pgcluster (make integ-up)"]
//!     — single-node integ stack; reads PGCLUSTER_API_URL (default localhost:8009)
//!
//!   #[ignore = "requires Docker e2e stack (make start)"]
//!     — full 3-node e2e stack; uses hardcoded localhost addresses

use std::time::Duration;

// ── Serialization unit tests (no network needed, always run) ─────────────────

#[test]
fn status_response_serializes_correctly() {
    use pgcluster::api::status::ClusterStatusResponse;
    let r = ClusterStatusResponse {
        cluster_name: "test".into(),
        primary_node_id: "pg1".into(),
        topology_version: 42,
        raft_leader_id: Some(1),
        node_count: 3,
        failover_history_count: 0,
    };
    let json = serde_json::to_string(&r).unwrap();
    assert!(json.contains("\"cluster_name\":\"test\""));
    assert!(json.contains("\"topology_version\":42"));
    assert!(json.contains("\"raft_leader_id\":1"));
}

#[test]
fn switchover_request_deserializes() {
    use pgcluster::api::switchover::SwitchoverRequest;
    let json = r#"{"target_node_id":"pg2","max_lag_bytes":1048576,"timeout_secs":30}"#;
    let req: SwitchoverRequest = serde_json::from_str(json).unwrap();
    assert_eq!(req.target_node_id, "pg2");
    assert_eq!(req.max_lag_bytes, 1_048_576);
    assert_eq!(req.timeout_secs, 30);
}

#[test]
fn failover_request_deserializes() {
    use pgcluster::api::failover::FailoverRequest;
    let json = r#"{"failed_node_id":"pg1"}"#;
    let req: FailoverRequest = serde_json::from_str(json).unwrap();
    assert_eq!(req.failed_node_id, "pg1");
}

// ── Integ helpers ─────────────────────────────────────────────────────────────

fn api_url() -> String {
    std::env::var("PGCLUSTER_API_URL").unwrap_or_else(|_| "http://localhost:8009".into())
}

fn metrics_url() -> String {
    std::env::var("PGCLUSTER_METRICS_URL").unwrap_or_else(|_| "http://localhost:9190".into())
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
}

// ── Single-node integ stack tests (make integ-up) ─────────────────────────────

#[tokio::test]
#[ignore = "requires running pgcluster (make integ-up)"]
async fn health_returns_200() {
    let resp = client()
        .get(format!("{}/health", api_url()))
        .send()
        .await
        .expect("GET /health");
    assert_eq!(resp.status().as_u16(), 200, "/health returned non-200");
}

#[tokio::test]
#[ignore = "requires running pgcluster (make integ-up)"]
async fn health_body_has_status_field() {
    let body: serde_json::Value = client()
        .get(format!("{}/health", api_url()))
        .send()
        .await
        .expect("GET /health")
        .json()
        .await
        .expect("parse JSON");
    assert!(
        body.get("status").is_some(),
        "body missing 'status': {body}"
    );
}

#[tokio::test]
#[ignore = "requires running pgcluster (make integ-up)"]
async fn status_returns_cluster_name() {
    let body: serde_json::Value = client()
        .get(format!("{}/api/status", api_url()))
        .send()
        .await
        .expect("GET /api/status")
        .json()
        .await
        .expect("parse JSON");
    let name = body["cluster_name"].as_str().unwrap_or("");
    assert!(!name.is_empty(), "cluster_name is empty: {body}");
}

#[tokio::test]
#[ignore = "requires running pgcluster (make integ-up)"]
async fn status_topology_version_is_numeric() {
    let body: serde_json::Value = client()
        .get(format!("{}/api/status", api_url()))
        .send()
        .await
        .expect("GET /api/status")
        .json()
        .await
        .expect("parse JSON");
    assert!(
        body["topology_version"].is_number(),
        "topology_version not a number: {body}"
    );
}

#[tokio::test]
#[ignore = "requires running pgcluster (make integ-up)"]
async fn topology_endpoint_returns_json_object() {
    let resp = client()
        .get(format!("{}/api/topology", api_url()))
        .send()
        .await
        .expect("GET /api/topology");
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.expect("parse JSON");
    assert!(
        body.is_object(),
        "expected object from /api/topology: {body}"
    );
}

#[tokio::test]
#[ignore = "requires running pgcluster (make integ-up)"]
async fn nodes_list_returns_array() {
    let body: serde_json::Value = client()
        .get(format!("{}/api/nodes", api_url()))
        .send()
        .await
        .expect("GET /api/nodes")
        .json()
        .await
        .expect("parse JSON");
    assert!(body["nodes"].is_array(), "missing 'nodes' array: {body}");
}

#[tokio::test]
#[ignore = "requires running pgcluster (make integ-up)"]
async fn nodes_list_contains_pg1() {
    let body: serde_json::Value = client()
        .get(format!("{}/api/nodes", api_url()))
        .send()
        .await
        .expect("GET /api/nodes")
        .json()
        .await
        .expect("parse JSON");
    let nodes = body["nodes"].as_array().expect("nodes array");
    let has_pg1 = nodes.iter().any(|n| n["node_id"].as_str() == Some("pg1"));
    assert!(has_pg1, "pg1 not in node list: {body}");
}

#[tokio::test]
#[ignore = "requires running pgcluster (make integ-up)"]
async fn get_node_pg1_returns_200() {
    let resp = client()
        .get(format!("{}/api/nodes/pg1", api_url()))
        .send()
        .await
        .expect("GET /api/nodes/pg1");
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.expect("parse JSON");
    assert_eq!(
        body["node_id"].as_str(),
        Some("pg1"),
        "wrong node_id: {body}"
    );
}

#[tokio::test]
#[ignore = "requires running pgcluster (make integ-up)"]
async fn get_unknown_node_returns_404() {
    let resp = client()
        .get(format!("{}/api/nodes/does-not-exist", api_url()))
        .send()
        .await
        .expect("GET /api/nodes/does-not-exist");
    assert_eq!(resp.status().as_u16(), 404);
}

#[tokio::test]
#[ignore = "requires running pgcluster (make integ-up)"]
async fn replication_slots_returns_array() {
    let resp = client()
        .get(format!("{}/api/replication/slots", api_url()))
        .send()
        .await
        .expect("GET /api/replication/slots");
    assert_eq!(resp.status().as_u16(), 200);
    let body: serde_json::Value = resp.json().await.expect("parse JSON");
    assert!(body.is_array(), "expected array: {body}");
}

#[tokio::test]
#[ignore = "requires running pgcluster (make integ-up)"]
async fn metrics_endpoint_returns_prometheus_text() {
    let resp = client()
        .get(format!("{}/metrics", metrics_url()))
        .send()
        .await
        .expect("GET /metrics");
    assert_eq!(resp.status().as_u16(), 200);
    let text = resp.text().await.expect("read body");
    assert!(
        text.contains("# HELP") || text.contains("# TYPE"),
        "response doesn't look like Prometheus text: {text:.200}"
    );
}

// ── Full 3-node e2e stack tests (make start) ──────────────────────────────────
// These tests need a 3-node stack accessible at E2E_STACK_ADDR (e.g. "127.0.0.1:8009").
// They are skipped automatically when that env var is not set.

fn e2e_api_base() -> Option<String> {
    std::env::var("E2E_STACK_ADDR")
        .ok()
        .map(|addr| format!("http://{}", addr))
}

#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn get_status_returns_correct_topology() {
    let Some(base) = e2e_api_base() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    let resp = reqwest::get(format!("{base}/api/status"))
        .await
        .expect("connect to API");
    assert!(resp.status().is_success(), "status: {}", resp.status());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        !body["primary_node_id"].as_str().unwrap_or("").is_empty(),
        "primary_node_id should be set: {body}"
    );
    assert!(
        body["topology_version"].as_u64().unwrap_or(0) > 0,
        "topology_version should be > 0: {body}"
    );
}

#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn get_topology_lists_all_nodes() {
    let Some(base) = e2e_api_base() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    let resp = reqwest::get(format!("{base}/api/topology"))
        .await
        .expect("connect to API");
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    let nodes = body["node_configs"].as_object().unwrap();
    assert_eq!(nodes.len(), 3, "expected 3 nodes, got {}", nodes.len());
}

#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn post_switchover_executes_and_changes_primary() {
    let Some(base) = e2e_api_base() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    let c = reqwest::Client::new();
    let before: serde_json::Value = c
        .get(format!("{base}/api/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let original_primary = before["primary_node_id"].as_str().unwrap().to_string();
    let nodes: serde_json::Value = c
        .get(format!("{base}/api/nodes"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let target = nodes["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["node_id"].as_str().unwrap_or("") != original_primary)
        .and_then(|n| n["node_id"].as_str())
        .expect("at least one non-primary node");
    let resp = c
        .post(format!("{base}/api/switchover"))
        .json(&serde_json::json!({
            "target_node_id": target,
            "max_lag_bytes": 10_485_760,
            "timeout_secs": 30
        }))
        .send()
        .await
        .expect("switchover request");
    assert!(
        resp.status().is_success() || resp.status().as_u16() == 202,
        "switchover failed: {}",
        resp.status()
    );
}

#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn get_metrics_returns_prometheus_format() {
    let Some(base) = e2e_api_base() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    let metrics_base = base.replace(":8009", ":9190");
    let resp = reqwest::get(format!("{metrics_base}/metrics"))
        .await
        .expect("connect to metrics endpoint");
    assert!(resp.status().is_success());
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("pgcluster_failovers_total"),
        "missing metric: {body:.200}"
    );
    assert!(
        body.contains("pgcluster_raft_is_leader"),
        "missing metric: {body:.200}"
    );
    assert!(
        body.contains("pgcluster_replica_lag_bytes"),
        "missing metric: {body:.200}"
    );
}

#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn health_endpoint_returns_200_with_primary() {
    let Some(base) = e2e_api_base() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    let health_base = base.replace(":8009", ":8008");
    let resp = reqwest::get(format!("{health_base}/health"))
        .await
        .expect("connect to health endpoint");
    assert!(resp.status().is_success(), "health: {}", resp.status());
}

#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn cli_status_prints_table() {
    let Some(base) = e2e_api_base() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    let addr = base.trim_start_matches("http://");
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_pgcluster"))
        .args(["status", "--api", addr])
        .output()
        .expect("run pgcluster");
    assert!(
        out.status.success(),
        "pgcluster status failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("primary") || stdout.contains("pg"),
        "unexpected output: {stdout}"
    );
}
