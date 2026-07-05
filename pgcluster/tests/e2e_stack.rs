//! End-to-end automated test (todo.md item 14).
//!
//! Starts the Docker Compose e2e stack, exercises switchover + failover, then
//! tears it down.  Must be run with:
//!
//!   cargo test --test e2e_stack -- --ignored --nocapture
//!
//! Requirements:
//!   - Docker and Docker Compose (v2) installed and the Docker daemon running.
//!   - The machine must have enough resources to run 9 containers.
//!
//! Environment variables:
//!   E2E_SKIP_BUILD=1  — reuse existing images (faster if already built)
//!   E2E_NO_TEARDOWN=1 — leave the stack running after the test (for debugging)

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Path to the e2e compose file, relative to the project root.
const COMPOSE_FILE: &str = "docker/e2e-compose.yml";

/// Default API address for pgcluster-1 on the e2e stack.
const DEFAULT_API: &str = "http://localhost:8009";

/// Return the project-root directory (two levels up from `target/`).
fn project_root() -> std::path::PathBuf {
    // In cargo tests, CARGO_MANIFEST_DIR points to the crate (pgcluster/).
    // The project root is one level up.
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    std::path::Path::new(manifest_dir)
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .to_path_buf()
}

/// Run a `docker compose` command from the project root.  Panics on failure.
fn compose(args: &[&str]) -> std::process::Output {
    let root = project_root();
    let output = Command::new("docker")
        .arg("compose")
        .arg("-f")
        .arg(COMPOSE_FILE)
        .args(args)
        .current_dir(&root)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to run docker compose");
    if !output.status.success() {
        eprintln!(
            "docker compose {:?} failed:\nstdout: {}\nstderr: {}",
            args,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        panic!("docker compose command failed");
    }
    output
}

/// Poll `GET /api/status` until the cluster has a primary, or `timeout` elapses.
async fn wait_for_primary(api: &str, timeout: Duration) -> serde_json::Value {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let deadline = Instant::now() + timeout;
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        if let Ok(resp) = client.get(format!("{api}/api/status")).send().await {
            if let Ok(body) = resp.json::<serde_json::Value>().await {
                let primary = body["primary_node_id"].as_str().unwrap_or("");
                if !primary.is_empty() {
                    return body;
                }
            }
        }
        if Instant::now() >= deadline {
            panic!("cluster did not elect a primary within {:?}", timeout);
        }
    }
}

/// Poll until the primary changes away from `old_primary`, or timeout.
async fn wait_for_new_primary(api: &str, old_primary: &str, timeout: Duration) -> String {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let deadline = Instant::now() + timeout;
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        if let Ok(resp) = client.get(format!("{api}/api/status")).send().await {
            if let Ok(body) = resp.json::<serde_json::Value>().await {
                let primary = body["primary_node_id"].as_str().unwrap_or("").to_string();
                if !primary.is_empty() && primary != old_primary {
                    return primary;
                }
            }
        }
        if Instant::now() >= deadline {
            panic!(
                "primary did not change from '{}' within {:?}",
                old_primary, timeout
            );
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Full automated e2e test: starts the stack, exercises switchover and
/// failover, then tears down.
///
/// Run with:
///   cargo test --test e2e_stack e2e_switchover_and_failover -- --ignored --nocapture
#[tokio::test]
#[ignore = "requires Docker daemon and sufficient system resources"]
async fn e2e_switchover_and_failover() {
    let skip_build = std::env::var("E2E_SKIP_BUILD").as_deref() == Ok("1");
    let no_teardown = std::env::var("E2E_NO_TEARDOWN").as_deref() == Ok("1");
    let api = std::env::var("E2E_API_URL").unwrap_or_else(|_| DEFAULT_API.to_string());

    // ── Step 1: Start the stack ───────────────────────────────────────────────
    eprintln!("==> Starting e2e stack...");
    if skip_build {
        compose(&["up", "-d", "--wait"]);
    } else {
        compose(&["up", "-d", "--build", "--wait"]);
    }
    eprintln!("==> Stack started");

    // ── Step 2: Wait for cluster to form ─────────────────────────────────────
    eprintln!("==> Waiting for cluster to elect a primary...");
    let status = wait_for_primary(&api, Duration::from_secs(90)).await;
    let original_primary = status["primary_node_id"]
        .as_str()
        .expect("primary_node_id missing")
        .to_string();
    let topo_version = status["topology_version"].as_u64().unwrap_or(0);
    eprintln!("==> Primary: {original_primary}  topology_version: {topo_version}");

    assert!(!original_primary.is_empty(), "primary_node_id is empty");
    assert!(
        topo_version > 0,
        "topology_version should be > 0 after bootstrap"
    );

    // ── Step 3: Verify topology lists 3 nodes ────────────────────────────────
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let topo: serde_json::Value = client
        .get(format!("{api}/api/topology"))
        .send()
        .await
        .expect("GET /api/topology")
        .json()
        .await
        .expect("parse topology JSON");
    let node_configs = topo["node_configs"]
        .as_object()
        .expect("node_configs must be an object");
    assert_eq!(
        node_configs.len(),
        3,
        "expected 3 nodes in topology, got {}",
        node_configs.len()
    );
    eprintln!("==> Topology has {} nodes", node_configs.len());

    // ── Step 4: Planned switchover ────────────────────────────────────────────
    // Pick a replica as the switchover target.
    let nodes_resp: serde_json::Value = client
        .get(format!("{api}/api/nodes"))
        .send()
        .await
        .expect("GET /api/nodes")
        .json()
        .await
        .expect("parse nodes JSON");
    let switchover_target = nodes_resp["nodes"]
        .as_array()
        .expect("nodes array")
        .iter()
        .find(|n| {
            n["node_id"].as_str().unwrap_or("") != original_primary
                && n["role"].as_str().unwrap_or("") == "replica"
        })
        .and_then(|n| n["node_id"].as_str())
        .expect("no replica node to switchover to");

    eprintln!("==> Triggering switchover to {switchover_target}...");
    let sw_resp = client
        .post(format!("{api}/api/switchover"))
        .json(&serde_json::json!({
            "target_node_id": switchover_target,
            "max_lag_bytes": 10_485_760,
            "timeout_secs": 30
        }))
        .send()
        .await
        .expect("POST /api/switchover");
    assert!(
        sw_resp.status().is_success() || sw_resp.status().as_u16() == 202,
        "switchover returned unexpected status: {}",
        sw_resp.status()
    );

    // Wait for switchover to complete: primary must change.
    eprintln!("==> Waiting for switchover to complete...");
    let new_primary = wait_for_new_primary(&api, &original_primary, Duration::from_secs(60)).await;
    assert_eq!(
        new_primary, switchover_target,
        "new primary should be the switchover target"
    );
    eprintln!("==> Switchover complete: new primary is {new_primary}");

    // Verify the old primary is now a replica.
    let after_sw: serde_json::Value = client
        .get(format!("{api}/api/nodes/{original_primary}"))
        .send()
        .await
        .expect("GET /api/nodes/{original_primary}")
        .json()
        .await
        .expect("parse node JSON");
    let old_role = after_sw["role"].as_str().unwrap_or("");
    assert_eq!(
        old_role, "replica",
        "old primary should now be a replica, got: {old_role}"
    );
    eprintln!("==> Old primary ({original_primary}) is now: {old_role}");

    // ── Step 5: Automatic failover ────────────────────────────────────────────
    // Determine which Docker container hosts the new primary and kill it.
    let primary_container = match new_primary.as_str() {
        "pg1" => "pg-primary",
        "pg2" => "pg-replica-1",
        "pg3" => "pg-replica-2",
        other => panic!("unexpected node ID: {other}"),
    };

    eprintln!("==> Killing container {primary_container} to trigger automatic failover...");
    let root = project_root();
    let kill_out = Command::new("docker")
        .args(["kill", primary_container])
        .current_dir(&root)
        .output()
        .expect("docker kill");
    if !kill_out.status.success() {
        // Container might have a different name in compose. Try compose kill.
        compose(&["kill", primary_container]);
    }

    // Wait for failover: a different primary must emerge.
    eprintln!("==> Waiting for automatic failover...");
    let failover_primary = wait_for_new_primary(&api, &new_primary, Duration::from_secs(90)).await;
    assert_ne!(
        failover_primary, new_primary,
        "failover primary must differ from the killed node"
    );
    eprintln!("==> Failover complete: new primary is {failover_primary}");

    // Verify failover history contains at least one record.
    let history: serde_json::Value = client
        .get(format!("{api}/api/failover/history"))
        .send()
        .await
        .expect("GET /api/failover/history")
        .json()
        .await
        .expect("parse failover history");
    let events = history.as_array().unwrap_or(&vec![]).len();
    assert!(
        events > 0,
        "failover history should be non-empty after a failover"
    );
    eprintln!("==> Failover history has {events} event(s)");

    // ── Step 6: Teardown ─────────────────────────────────────────────────────
    if no_teardown {
        eprintln!("==> E2E_NO_TEARDOWN=1 — leaving stack running");
    } else {
        eprintln!("==> Tearing down e2e stack...");
        compose(&["down", "-v", "--remove-orphans"]);
        eprintln!("==> Stack torn down");
    }
}

/// Lighter smoke test: just starts the stack, verifies basic API health, then
/// tears down.  Faster than the full switchover+failover test.
#[tokio::test]
#[ignore = "requires Docker daemon"]
async fn e2e_cluster_forms_and_has_primary() {
    let skip_build = std::env::var("E2E_SKIP_BUILD").as_deref() == Ok("1");
    let api = std::env::var("E2E_API_URL").unwrap_or_else(|_| DEFAULT_API.to_string());

    // Start the stack.
    if skip_build {
        compose(&["up", "-d", "--wait"]);
    } else {
        compose(&["up", "-d", "--build", "--wait"]);
    }

    // Wait for a primary to be elected.
    let status = wait_for_primary(&api, Duration::from_secs(90)).await;
    let primary = status["primary_node_id"].as_str().unwrap_or("");
    assert!(!primary.is_empty(), "cluster must elect a primary");

    let node_count = status["node_count"].as_u64().unwrap_or(0);
    assert_eq!(node_count, 3, "expected 3 nodes in the cluster");

    // Teardown.
    compose(&["down", "-v", "--remove-orphans"]);
}

/// Runs switchover only — useful when the stack is already up (E2E_SKIP_BUILD=1).
#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR or localhost:8009"]
async fn e2e_switchover_against_running_stack() {
    let api = std::env::var("E2E_API_URL").unwrap_or_else(|_| DEFAULT_API.to_string());

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    // Get current status.
    let status: serde_json::Value = client
        .get(format!("{api}/api/status"))
        .send()
        .await
        .expect("connect to API")
        .json()
        .await
        .expect("parse status");
    let original_primary = status["primary_node_id"]
        .as_str()
        .expect("primary_node_id")
        .to_string();

    // Pick a replica.
    let nodes: serde_json::Value = client
        .get(format!("{api}/api/nodes"))
        .send()
        .await
        .expect("GET /api/nodes")
        .json()
        .await
        .expect("parse nodes");
    let target = nodes["nodes"]
        .as_array()
        .expect("nodes array")
        .iter()
        .find(|n| {
            n["role"].as_str().unwrap_or("") == "replica"
                && n["node_id"].as_str().unwrap_or("") != original_primary
        })
        .and_then(|n| n["node_id"].as_str())
        .expect("no replica to switchover to");

    // Trigger switchover.
    let resp = client
        .post(format!("{api}/api/switchover"))
        .json(&serde_json::json!({
            "target_node_id": target,
            "max_lag_bytes": 10_485_760,
            "timeout_secs": 30
        }))
        .send()
        .await
        .expect("POST /api/switchover");
    assert!(
        resp.status().is_success() || resp.status().as_u16() == 202,
        "switchover: {}",
        resp.status()
    );

    // Wait for primary to change.
    let new_primary = wait_for_new_primary(&api, &original_primary, Duration::from_secs(60)).await;
    assert_eq!(new_primary, target, "primary should switch to target node");
    eprintln!("Switchover OK: {original_primary} → {new_primary}");
}
