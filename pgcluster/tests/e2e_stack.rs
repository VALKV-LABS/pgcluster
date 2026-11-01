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
        .bearer_auth("dev-api-key")
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

    // The cluster formed, so Raft and vk-agent accepted mutual TLS. Writes need
    // a key.
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{api}/api/switchover"))
        .json(&serde_json::json!({ "target_node_id": "pg2" }))
        .send()
        .await
        .expect("POST /api/switchover");
    assert_eq!(response.status(), 401, "a write without a key must fail");

    let dsn = "host=localhost port=5432 user=postgres password=test dbname=postgres";
    let (pg, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("connect through the proxy");
    tokio::spawn(connection);
    pg.simple_query("SELECT 1").await.expect("query the proxy");

    // Heartbeats keep the agents out of safe mode, so nothing self-fences. The
    // stack's agents would otherwise stop Postgres 40 seconds after starting.
    tokio::time::sleep(Duration::from_secs(45)).await;
    let restarts = Command::new("docker")
        .args([
            "compose",
            "-f",
            COMPOSE_FILE,
            "ps",
            "--format",
            "{{.Status}}",
        ])
        .current_dir(project_root())
        .output()
        .expect("docker compose ps");
    let restarts = String::from_utf8_lossy(&restarts.stdout);
    assert!(
        !restarts.contains("Up 1") && !restarts.contains("Up 2 seconds"),
        "containers restarted during the wait:\n{restarts}"
    );
    pg.simple_query("SELECT 1")
        .await
        .expect("the session must outlive the fence timeout");

    // Teardown.
    compose(&["down", "-v", "--remove-orphans"]);
}

/// The containers on the minority side, which starts with the primary.
const MINORITY: [&str; 3] = ["pg-primary", "vk-agent-1", "pgcluster-1"];

fn psql(service: &str, proxy: &str, sql: &str) -> std::process::Output {
    let conninfo =
        format!("host={proxy} user=postgres dbname=postgres sslmode=disable connect_timeout=2");
    Command::new("docker")
        .args(["compose", "-f", COMPOSE_FILE, "exec", "-T"])
        .args(["-e", "PGPASSWORD=test", service])
        .args(["psql", &conninfo, "-v", "ON_ERROR_STOP=1", "-Atc", sql])
        .current_dir(project_root())
        .output()
        .expect("docker compose exec psql")
}

fn write_through(service: &str, proxy: &str, value: u32) -> bool {
    psql(
        service,
        proxy,
        &format!("INSERT INTO partition_test VALUES ({value})"),
    )
    .status
    .success()
}

fn container_id(service: &str) -> String {
    let id = compose(&["ps", "-a", "-q", service]);
    String::from_utf8_lossy(&id.stdout).trim().to_string()
}

/// Stop or start one container. `docker compose start` would also start the
/// services it depends on, which could revive a node a test has taken down.
fn docker(action: &str, service: &str) {
    let status = Command::new("docker")
        .args([action, &container_id(service)])
        .stdout(Stdio::null())
        .status()
        .expect("docker stop or start");
    assert!(status.success(), "docker {action} {service}");
}

fn cut_off_from_default_network(service: &str) {
    let id = container_id(service);
    let status = Command::new("docker")
        .args(["network", "disconnect", "docker_default", &id])
        .status()
        .expect("docker network disconnect");
    assert!(status.success(), "could not disconnect {service}");
}

/// Splits the cluster with the primary on the minority side. The isolated proxy
/// must stop accepting writes before the majority starts its failover, so no
/// write lands on both sides, and the isolated agent must then fence Postgres.
#[tokio::test]
#[ignore = "requires Docker daemon and sufficient system resources"]
async fn e2e_partition_stops_writes_on_both_sides() {
    let skip_build = std::env::var("E2E_SKIP_BUILD").as_deref() == Ok("1");
    let majority_api = "http://localhost:18009";

    if skip_build {
        compose(&["up", "-d", "--wait"]);
    } else {
        compose(&["up", "-d", "--build", "--wait"]);
    }
    let status = wait_for_primary(DEFAULT_API, Duration::from_secs(90)).await;
    assert_eq!(
        status["primary_node_id"], "pg1",
        "pg1 must start as primary"
    );

    assert!(
        psql(
            "pg-primary",
            "pgcluster-1",
            "CREATE TABLE partition_test (value int)"
        )
        .status
        .success(),
        "create the test table"
    );
    assert!(write_through("pg-primary", "pgcluster-1", 0));

    for service in MINORITY {
        cut_off_from_default_network(service);
    }
    eprintln!("==> Partitioned");

    // Keep writing on the isolated side until it refuses.
    let mut last_accepted = chrono::Utc::now();
    let mut refused = false;
    for value in 1..100 {
        if write_through("pg-primary", "pgcluster-1", value) {
            last_accepted = chrono::Utc::now();
        } else {
            refused = true;
            break;
        }
    }
    assert!(refused, "the isolated proxy kept accepting writes");
    eprintln!("==> Isolated proxy refused writes after {last_accepted}");

    let new_primary = wait_for_new_primary(majority_api, "pg1", Duration::from_secs(60)).await;
    eprintln!("==> Majority promoted {new_primary}");

    let history: serde_json::Value = reqwest::get(format!("{majority_api}/api/failover/history"))
        .await
        .expect("GET /api/failover/history")
        .json()
        .await
        .expect("parse failover history");
    eprintln!("==> History: {history}");
    let triggered_at = history
        .as_array()
        .and_then(|events| events.last())
        .and_then(|event| event["triggered_at"].as_str())
        .expect("failover history must record the failover");
    let triggered_at = chrono::DateTime::parse_from_rfc3339(triggered_at)
        .expect("parse triggered_at")
        .with_timezone(&chrono::Utc);
    // triggered_at has whole seconds, so compare at that resolution.
    assert!(
        last_accepted.timestamp() <= triggered_at.timestamp(),
        "the isolated side accepted a write at {last_accepted}, \
         after failover began at {triggered_at}"
    );

    assert!(
        !write_through("pg-primary", "pgcluster-1", 1000),
        "the isolated side must still refuse writes"
    );
    assert!(
        write_through("pg-replica-2", "pgcluster-2", 2000),
        "the majority must accept writes on the new primary"
    );

    // heartbeat_timeout_seconds + fence_timeout_seconds, plus a margin.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let logs = compose(&["logs", "--no-log-prefix", "vk-agent-1"]);
        if String::from_utf8_lossy(&logs.stdout).contains("self-fencing") {
            eprintln!("==> Isolated agent self-fenced");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the isolated agent did not fence"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    compose(&["down", "-v", "--remove-orphans"]);
}

// ── Replication modes ─────────────────────────────────────────────────────────

fn start_stack() {
    if std::env::var("E2E_SKIP_BUILD").as_deref() == Ok("1") {
        compose(&["up", "-d", "--wait"]);
    } else {
        compose(&["up", "-d", "--build", "--wait"]);
    }
}

async fn set_mode(mode: &str) {
    let response = reqwest::Client::new()
        .post(format!("{DEFAULT_API}/api/replication/mode"))
        .bearer_auth("dev-api-key")
        .json(&serde_json::json!({ "mode": mode }))
        .send()
        .await
        .expect("POST /api/replication/mode");
    assert_eq!(response.status(), 204, "set mode {mode}");
}

async fn replication(api: &str) -> serde_json::Value {
    reqwest::get(format!("{api}/api/replication"))
        .await
        .expect("GET /api/replication")
        .json()
        .await
        .expect("parse replication status")
}

fn show(service: &str, setting: &str) -> String {
    let out = Command::new("docker")
        .args(["compose", "-f", COMPOSE_FILE, "exec", "-T", service])
        .args(["psql", "-U", "postgres", "-Atc", &format!("SHOW {setting}")])
        .current_dir(project_root())
        .output()
        .expect("docker compose exec psql");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Wait until `check` holds, or fail naming `what`.
async fn wait_until<F, Fut>(what: &str, timeout: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    while !check().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_for_standby_names(expected: &str) {
    wait_until(
        &format!("synchronous_standby_names = {expected:?}"),
        Duration::from_secs(20),
        || async { show("pg-primary", "synchronous_standby_names") == expected },
    )
    .await;
}

/// Start an insert on the primary in the background, for a commit that may stall.
fn spawn_write(value: u32) -> std::process::Child {
    Command::new("docker")
        .args(["compose", "-f", COMPOSE_FILE, "exec", "-T", "pg-primary"])
        .args(["psql", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c"])
        .arg(format!("INSERT INTO mode_test VALUES ({value})"))
        .current_dir(project_root())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn psql")
}

fn is_running(write: &mut std::process::Child) -> bool {
    write.try_wait().expect("poll write").is_none()
}

async fn wait_for_exit(write: &mut std::process::Child, timeout: Duration) {
    wait_until("the write to finish", timeout, || {
        let done = !is_running(write);
        async move { done }
    })
    .await;
    assert!(write.wait().expect("write status").success());
}

async fn create_mode_test_table() {
    wait_for_primary(DEFAULT_API, Duration::from_secs(90)).await;
    let out = psql(
        "pg-primary",
        "pgcluster-1",
        "CREATE TABLE mode_test (value int)",
    );
    assert!(out.status.success(), "create the test table");
    wait_until(
        "both standbys to stream",
        Duration::from_secs(30),
        || async {
            let out = psql(
                "pg-primary",
                "pgcluster-1",
                "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'",
            );
            String::from_utf8_lossy(&out.stdout).trim() == "2"
        },
    )
    .await;
}

const STANDBY_NAMES: &str = r#"ANY 1 ("pg2", "pg3")"#;

/// `sync` commits wait for a standby, fall back after the timeout with both
/// standbys down, and recover when one returns. `sync_strict` never falls back.
#[tokio::test]
#[ignore = "requires Docker daemon and sufficient system resources"]
async fn e2e_sync_modes_control_commits() {
    start_stack();
    create_mode_test_table().await;

    // Changing the mode needs no restart.
    assert_eq!(show("pg-primary", "synchronous_standby_names"), "");
    set_mode("sync").await;
    wait_for_standby_names(STANDBY_NAMES).await;
    assert_eq!(replication(DEFAULT_API).await["mode"], "sync");
    wait_for_exit(&mut spawn_write(1), Duration::from_secs(10)).await;

    // With both standbys down, the commit waits, then the fallback releases it.
    compose(&["stop", "pg-replica-1", "pg-replica-2"]);
    let mut write = spawn_write(2);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        is_running(&mut write),
        "a sync commit must wait for a standby"
    );
    wait_for_exit(&mut write, Duration::from_secs(30)).await;
    assert_eq!(replication(DEFAULT_API).await["degraded"], true);
    assert_eq!(show("pg-primary", "synchronous_standby_names"), "");
    wait_for_exit(&mut spawn_write(3), Duration::from_secs(5)).await;

    // A returning standby restores sync.
    compose(&["start", "pg-replica-1", "pg-replica-2"]);
    wait_for_standby_names(STANDBY_NAMES).await;
    wait_until("sync to recover", Duration::from_secs(20), || async {
        replication(DEFAULT_API).await["degraded"] == false
    })
    .await;

    // sync_strict holds the commit past the timeout, until a standby returns.
    set_mode("sync_strict").await;
    compose(&["stop", "pg-replica-1", "pg-replica-2"]);
    let mut write = spawn_write(4);
    tokio::time::sleep(Duration::from_secs(10)).await;
    assert!(is_running(&mut write), "sync_strict must never fall back");
    assert_eq!(replication(DEFAULT_API).await["degraded"], false);
    assert_eq!(
        show("pg-primary", "synchronous_standby_names"),
        STANDBY_NAMES
    );
    compose(&["start", "pg-replica-1", "pg-replica-2"]);
    wait_for_exit(&mut write, Duration::from_secs(30)).await;

    compose(&["down", "-v", "--remove-orphans"]);
}

/// `sync_strict` stays down until both standbys answer.
#[tokio::test]
#[ignore = "requires Docker daemon and sufficient system resources"]
async fn e2e_sync_strict_promotes_only_when_both_standbys_answer() {
    let majority_api = "http://localhost:18009";
    start_stack();
    create_mode_test_table().await;
    set_mode("sync_strict").await;
    wait_for_standby_names(STANDBY_NAMES).await;

    // pg3 keeps running, but its agent cannot answer.
    docker("stop", "vk-agent-3");
    docker("stop", "pg-primary");
    tokio::time::sleep(Duration::from_secs(12)).await;
    let status: serde_json::Value = reqwest::get(format!("{majority_api}/api/status"))
        .await
        .expect("GET /api/status")
        .json()
        .await
        .expect("parse status");
    assert_eq!(status["primary_node_id"], "pg1", "must not promote yet");

    docker("start", "vk-agent-3");
    let new_primary = wait_for_new_primary(majority_api, "pg1", Duration::from_secs(60)).await;
    eprintln!("==> Promoted {new_primary} once both standbys answered");

    compose(&["down", "-v", "--remove-orphans"]);
}

/// A failover after the fallback records that no standby was guaranteed current.
#[tokio::test]
#[ignore = "requires Docker daemon and sufficient system resources"]
async fn e2e_failover_after_fallback_is_recorded() {
    let majority_api = "http://localhost:18009";
    start_stack();
    create_mode_test_table().await;
    set_mode("sync").await;
    wait_for_standby_names(STANDBY_NAMES).await;

    compose(&["stop", "pg-replica-1", "pg-replica-2"]);
    wait_until("the fallback", Duration::from_secs(30), || async {
        replication(DEFAULT_API).await["degraded"] == true
    })
    .await;

    // Stopping Postgres takes its agent with it, because they share a PID
    // namespace, so each standby's agent needs starting too.
    docker("stop", "pg-primary");
    for service in ["pg-replica-1", "vk-agent-2", "pg-replica-2", "vk-agent-3"] {
        docker("start", service);
    }
    wait_for_new_primary(majority_api, "pg1", Duration::from_secs(60)).await;

    let history: serde_json::Value = reqwest::get(format!("{majority_api}/api/failover/history"))
        .await
        .expect("GET /api/failover/history")
        .json()
        .await
        .expect("parse failover history");
    assert_eq!(
        history.as_array().and_then(|e| e.last()).unwrap()["ran_degraded"],
        true
    );

    compose(&["down", "-v", "--remove-orphans"]);
}

/// A dead standby holds back at most `max_slot_wal_keep_size`, then is flagged.
#[tokio::test]
#[ignore = "requires Docker daemon and sufficient system resources"]
async fn e2e_dead_standby_past_the_limit_is_flagged() {
    start_stack();
    create_mode_test_table().await;
    wait_until("the slot limit", Duration::from_secs(20), || async {
        show("pg-primary", "max_slot_wal_keep_size") == "32MB"
    })
    .await;

    compose(&["stop", "pg-replica-2"]);
    for _ in 0..8 {
        let out = psql(
            "pg-primary",
            "pgcluster-1",
            "BEGIN; CREATE TABLE IF NOT EXISTS wal_fill AS SELECT 1 AS a; \
             INSERT INTO wal_fill SELECT generate_series(1, 300000); COMMIT; \
             SELECT pg_switch_wal(); CHECKPOINT",
        );
        assert!(
            out.status.success(),
            "generate WAL: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    wait_until("the re-seed flag", Duration::from_secs(30), || async {
        let flagged = replication(DEFAULT_API).await["needs_reseed"].clone();
        flagged
            .as_array()
            .is_some_and(|f| f.iter().any(|n| n == "pg3"))
    })
    .await;

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
        .bearer_auth("dev-api-key")
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
