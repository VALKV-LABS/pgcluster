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
    let no_teardown = std::env::var("E2E_NO_TEARDOWN").as_deref() == Ok("1");
    let api = std::env::var("E2E_API_URL").unwrap_or_else(|_| DEFAULT_API.to_string());

    // ── Step 1: Start the stack ───────────────────────────────────────────────
    eprintln!("==> Starting e2e stack...");
    start_stack();
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
    let api = std::env::var("E2E_API_URL").unwrap_or_else(|_| DEFAULT_API.to_string());

    // Start the stack.
    start_stack();

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
const MINORITY: [&str; 2] = ["pg-primary", "pgcluster-1"];

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
    let majority_api = "http://localhost:18009";

    start_stack();
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
        let logs = compose(&["logs", "--no-log-prefix", "pg-primary"]);
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

/// Start the stack, and wait until the proxy can log clients in, which needs the
/// leader to have set up its role on the primary.
fn start_stack() {
    if std::env::var("E2E_SKIP_BUILD").as_deref() == Ok("1") {
        compose(&["up", "-d", "--wait"]);
    } else {
        compose(&["up", "-d", "--build", "--wait"]);
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    while !psql("pg-primary", "pgcluster-1", "SELECT 1")
        .status
        .success()
    {
        assert!(
            Instant::now() < deadline,
            "the proxy never accepted a login"
        );
        std::thread::sleep(Duration::from_millis(500));
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

    // pg3 is frozen, so its agent cannot answer.
    docker("pause", "pg-replica-2");
    docker("stop", "pg-primary");
    tokio::time::sleep(Duration::from_secs(12)).await;
    let status: serde_json::Value = reqwest::get(format!("{majority_api}/api/status"))
        .await
        .expect("GET /api/status")
        .json()
        .await
        .expect("parse status");
    assert_eq!(status["primary_node_id"], "pg1", "must not promote yet");

    docker("unpause", "pg-replica-2");
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

    docker("stop", "pg-primary");
    for service in ["pg-replica-1", "pg-replica-2"] {
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

/// Add the spare node and wait until it streams from the primary.
async fn add_and_seed_pg4() {
    add_and_seed_pg4_from(false).await;
}

async fn add_and_seed_pg4_from(from_backup: bool) {
    add_pg4(from_backup, false).await;
}

async fn add_pg4(from_backup: bool, external: bool) {
    compose(&[
        "--profile",
        "spare",
        "up",
        "-d",
        "--no-deps",
        "--build",
        "vk-agent-4",
    ]);

    let client = reqwest::Client::new();
    let added = client
        .post(format!("{DEFAULT_API}/api/nodes/add"))
        .bearer_auth("dev-api-key")
        .json(&serde_json::json!({
            "node_id": "pg4",
            "agent_addr": "vk-agent-4:7001",
            "postgres_addr": "pg-new:5432",
            "priority": 1000,
            "external": external,
        }))
        .send()
        .await
        .expect("POST /api/nodes/add");
    assert_eq!(added.status(), 201);

    let seeded = client
        .post(format!("{DEFAULT_API}/api/nodes/pg4/seed"))
        .bearer_auth("dev-api-key")
        .json(&serde_json::json!({ "from_backup": from_backup }))
        .send()
        .await
        .expect("POST /api/nodes/pg4/seed");
    assert_eq!(seeded.status(), 202);

    wait_until("pg4 to stream", Duration::from_secs(60), || async {
        let out = psql(
            "pg-primary",
            "pgcluster-1",
            "SELECT count(*) FROM pg_stat_replication \
             WHERE application_name = 'pg4' AND state = 'streaming'",
        );
        String::from_utf8_lossy(&out.stdout).trim() == "1"
    })
    .await;
}

fn pg4_query(sql: &str) -> String {
    let out = Command::new("docker")
        .args(["compose", "-f", COMPOSE_FILE, "--profile", "spare"])
        .args(["exec", "-T", "-e", "PGPASSWORD=test", "vk-agent-4"])
        .args(["psql", "-h", "127.0.0.1", "-U", "postgres", "-Atc", sql])
        .current_dir(project_root())
        .output()
        .expect("query pg4");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// A new node with an empty data directory seeds from the primary through an
/// RPC, then streams from it.
#[tokio::test]
#[ignore = "requires Docker daemon and sufficient system resources"]
async fn e2e_new_node_seeds_through_the_agent() {
    start_stack();
    create_mode_test_table().await;
    wait_for_exit(&mut spawn_write(7), Duration::from_secs(10)).await;

    add_and_seed_pg4().await;
    assert_eq!(pg4_query("SELECT value FROM mode_test"), "7");

    compose(&["--profile", "spare", "down", "-v", "--remove-orphans"]);
}

/// An old primary that took writes after losing its role comes back as a
/// standby of the new primary, with those writes discarded.
#[tokio::test]
#[ignore = "requires Docker daemon and sufficient system resources"]
async fn e2e_diverged_primary_rejoins_with_rewind() {
    let majority_api = "http://localhost:18009";
    start_stack();
    create_mode_test_table().await;
    add_and_seed_pg4().await;

    let switched = reqwest::Client::new()
        .post(format!("{DEFAULT_API}/api/switchover"))
        .bearer_auth("dev-api-key")
        .json(&serde_json::json!({
            "target_node_id": "pg4",
            "max_lag_bytes": 1048576,
            "timeout_secs": 30,
        }))
        .send()
        .await
        .expect("POST /api/switchover");
    assert!(switched.status().is_success(), "{}", switched.status());
    wait_for_new_primary(majority_api, "pg1", Duration::from_secs(60)).await;
    wait_until("pg4 to accept writes", Duration::from_secs(20), || async {
        pg4_query("SELECT pg_is_in_recovery()") == "f"
    })
    .await;

    // A standby must follow pg4 first, so that the next promotion gets a new
    // timeline and this exercises a true rewind.
    wait_until(
        "a standby to follow pg4",
        Duration::from_secs(40),
        || async {
            pg4_query("SELECT count(*) > 0 FROM pg_stat_replication WHERE state = 'streaming'")
                == "t"
        },
    )
    .await;

    // Isolate it, and let it take a write only it will ever have.
    let id = container_id("vk-agent-4");
    let network = |args: &[&str]| {
        let status = Command::new("docker")
            .arg("network")
            .args(args)
            .args(["docker_default", &id])
            .status()
            .expect("docker network");
        assert!(status.success(), "docker network {args:?}");
    };
    network(&["disconnect"]);
    assert_eq!(
        pg4_query("INSERT INTO mode_test VALUES (999) RETURNING value")
            .lines()
            .next(),
        Some("999")
    );

    wait_for_new_primary(majority_api, "pg4", Duration::from_secs(30)).await;
    network(&["connect", "--alias", "vk-agent-4", "--alias", "pg-new"]);

    wait_until(
        "pg4 to rejoin as a standby",
        Duration::from_secs(60),
        || async { pg4_query("SELECT pg_is_in_recovery()") == "t" },
    )
    .await;
    assert_eq!(
        pg4_query("SELECT count(*) FROM mode_test WHERE value = 999"),
        "0",
        "the diverged write must go"
    );
    let logs = compose(&["logs"]);
    assert!(
        !String::from_utf8_lossy(&logs.stdout).contains("rewind failed"),
        "this should rewind, without falling back to a seed"
    );

    compose(&["--profile", "spare", "down", "-v", "--remove-orphans"]);
}

/// A rolling restart under client load interrupts commits only around the
/// switchover, and loses none that were acknowledged.
#[tokio::test]
#[ignore = "requires Docker daemon and sufficient system resources"]
async fn e2e_rolling_restart_under_load() {
    let api = "http://localhost:18009";
    start_stack();
    create_mode_test_table().await;

    // An application: one connection, reconnecting after any failure.
    let dsn = "host=localhost port=15432 user=postgres password=test dbname=postgres";
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let stop = stop.clone();
        tokio::spawn(async move {
            let mut acknowledged = Vec::new();
            let mut failures = Vec::new();
            let mut session = None;
            let mut value = 10_000u32;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                value += 1;
                if session.is_none() {
                    match tokio_postgres::connect(dsn, tokio_postgres::NoTls).await {
                        Ok((client, connection)) => {
                            tokio::spawn(connection);
                            session = Some(client);
                        }
                        Err(_) => {
                            failures.push(Instant::now());
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    }
                }
                let sql = format!("INSERT INTO mode_test VALUES ({value})");
                match session.as_ref().unwrap().simple_query(&sql).await {
                    Ok(_) => acknowledged.push(value),
                    Err(_) => {
                        failures.push(Instant::now());
                        session = None;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            (acknowledged, failures)
        })
    };

    // The leader runs it, and any of the three may lead.
    let client = reqwest::Client::new();
    let mut leader = None;
    for port in [8009, 18009, 28009] {
        let response = client
            .post(format!("http://localhost:{port}/api/restart"))
            .bearer_auth("dev-api-key")
            .send()
            .await
            .expect("POST /api/restart");
        if response.status() == 202 {
            leader = Some(port);
            break;
        }
    }
    let leader = leader.expect("one instance must lead");

    wait_until("the rolling restart", Duration::from_secs(240), || {
        let client = client.clone();
        async move {
            let status: serde_json::Value = client
                .get(format!("http://localhost:{leader}/api/restart"))
                .send()
                .await
                .expect("GET /api/restart")
                .json()
                .await
                .expect("parse restart status");
            assert!(status["error"].is_null(), "restart stopped: {status}");
            status["running"] == false
        }
    })
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let (acknowledged, failures) = writer.await.expect("writer");
    eprintln!(
        "==> {} commits, {} failures",
        acknowledged.len(),
        failures.len()
    );
    assert!(acknowledged.len() > 100, "the load must have run");

    // Failures must be one short window, the switchover.
    if let (Some(first), Some(last)) = (failures.first(), failures.last()) {
        let window = last.duration_since(*first);
        eprintln!("==> Failures spanned {window:?}");
        assert!(
            window < Duration::from_secs(15),
            "commits failed for {window:?}"
        );
    }

    let status: serde_json::Value = reqwest::get(format!("{api}/api/status"))
        .await
        .expect("GET /api/status")
        .json()
        .await
        .expect("parse status");
    assert_ne!(status["primary_node_id"], "pg1", "the primary must move");

    let (pg, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("connect after the restart");
    tokio::spawn(connection);
    let rows = pg
        .simple_query("SELECT value FROM mode_test WHERE value > 10000")
        .await
        .expect("read the writes back");
    let stored: std::collections::HashSet<u32> = rows
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0)?.parse().ok(),
            _ => None,
        })
        .collect();
    let lost = acknowledged.iter().filter(|v| !stored.contains(v)).count();
    assert_eq!(lost, 0, "acknowledged commits were lost");

    compose(&["down", "-v", "--remove-orphans"]);
}

// ── Backups ───────────────────────────────────────────────────────────────────

async fn backups(api: &str) -> Vec<serde_json::Value> {
    let Ok(response) = reqwest::get(format!("{api}/api/backups")).await else {
        return Vec::new();
    };
    let body: serde_json::Value = response.json().await.unwrap_or_default();
    body["backups"].as_array().cloned().unwrap_or_default()
}

async fn wait_for_backup(api: &str) -> serde_json::Value {
    wait_until("a backup", Duration::from_secs(90), || async {
        !backups(api).await.is_empty()
    })
    .await;
    backups(api).await.remove(0)
}

fn service_of(node_id: &str) -> &'static str {
    match node_id {
        "pg1" => "pg-primary",
        "pg2" => "pg-replica-1",
        "pg3" => "pg-replica-2",
        other => panic!("unknown node {other}"),
    }
}

fn exec_sql(service: &str, sql: &str) -> String {
    let out = Command::new("docker")
        .args(["compose", "-f", COMPOSE_FILE, "--profile", "spare"])
        .args(["exec", "-T", service])
        .args(["psql", "-U", "postgres", "-Atc", sql])
        .current_dir(project_root())
        .output()
        .expect("docker compose exec psql");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Scheduled backups come from a standby, and carry on under a new leader.
#[tokio::test]
#[ignore = "requires Docker daemon and sufficient system resources"]
async fn e2e_backups_run_from_a_standby_and_survive_a_leader_change() {
    start_stack();
    create_mode_test_table().await;

    wait_until(
        "a backup from a standby",
        Duration::from_secs(90),
        || async {
            backups(DEFAULT_API)
                .await
                .iter()
                .any(|b| b["source_node"] != "pg1")
        },
    )
    .await;

    let status: serde_json::Value = reqwest::get(format!("{DEFAULT_API}/api/status"))
        .await
        .expect("GET /api/status")
        .json()
        .await
        .expect("parse status");
    let leader = status["raft_leader_id"].as_u64().expect("a leader");
    let survivor = if leader == 1 {
        "http://localhost:18009"
    } else {
        DEFAULT_API
    };
    let newest_before = backups(survivor).await[0]["completed_at"].clone();
    docker("stop", &format!("pgcluster-{leader}"));
    eprintln!("==> Stopped the leader, pgcluster-{leader}");

    wait_until(
        "a backup under the new leader",
        Duration::from_secs(90),
        || {
            let newest_before = newest_before.as_i64();
            async move {
                backups(survivor)
                    .await
                    .first()
                    .is_some_and(|b| b["completed_at"].as_i64() > newest_before)
            }
        },
    )
    .await;

    compose(&["down", "-v", "--remove-orphans"]);
}

/// WAL keeps archiving from whichever node is primary, with no gaps.
#[tokio::test]
#[ignore = "requires Docker daemon and sufficient system resources"]
async fn e2e_wal_archiving_follows_failover() {
    let api = "http://localhost:18009";
    start_stack();
    create_mode_test_table().await;
    wait_for_backup(api).await;

    exec_sql(
        "pg-primary",
        "INSERT INTO mode_test VALUES (1); SELECT pg_switch_wal()",
    );
    wait_until(
        "the old primary to archive",
        Duration::from_secs(30),
        || async {
            exec_sql(
                "pg-primary",
                "SELECT archived_count > 0 FROM pg_stat_archiver",
            ) == "t"
        },
    )
    .await;

    docker("stop", "pg-primary");
    let new_primary = wait_for_new_primary(api, "pg1", Duration::from_secs(60)).await;
    let service = service_of(&new_primary);
    eprintln!("==> Promoted {new_primary}");

    exec_sql(
        service,
        "INSERT INTO mode_test VALUES (2); SELECT pg_switch_wal()",
    );
    wait_until(
        "the new primary to archive",
        Duration::from_secs(60),
        || async { exec_sql(service, "SELECT archived_count > 0 FROM pg_stat_archiver") == "t" },
    )
    .await;
    assert_eq!(
        exec_sql(service, "SELECT failed_count FROM pg_stat_archiver"),
        "0"
    );

    // Archived WAL must be unreadable without the key. With it, the same fetch
    // proves the file is there.
    let fetch = |config: &str| {
        let script = format!(
            "{config} wal-g wal-fetch \
             \"$(ls /backups/wal_005 | head -1 | cut -d. -f1)\" /tmp/fetched"
        );
        Command::new("docker")
            .args(["compose", "-f", COMPOSE_FILE, "exec", "-T", service])
            .args(["sh", "-c", &script])
            .current_dir(project_root())
            .output()
            .expect("docker compose exec wal-g")
    };
    let with_key = fetch("WALG_CONFIG_PATH=/var/lib/postgresql/walg.json");
    assert!(
        with_key.status.success(),
        "{}",
        String::from_utf8_lossy(&with_key.stderr)
    );
    let without_key = fetch("WALG_CONFIG_PATH=/nonexistent WALG_FILE_PREFIX=/backups");
    assert!(
        !without_key.status.success(),
        "archived WAL was readable without the key"
    );

    // The control file names the new timeline after the first checkpoint. A
    // WARNING only covers segments still on their way, and FAILURE means a gap.
    exec_sql(service, "CHECKPOINT");
    let report: serde_json::Value = reqwest::get(format!("{api}/api/backups/verify"))
        .await
        .expect("GET /api/backups/verify")
        .json()
        .await
        .expect("parse the verify report");
    eprintln!("==> Verify: {report}");
    assert_ne!(report["integrity"]["status"], "FAILURE", "{report}");
    assert!(!report.to_string().contains("MISSING_LOST"), "{report}");
    assert_eq!(report["timeline"]["status"], "OK", "{report}");

    compose(&["down", "-v", "--remove-orphans"]);
}

/// A restore to a chosen time holds what was committed before it and nothing
/// after, accepts writes, and leaves the source running.
#[tokio::test]
#[ignore = "requires Docker daemon and sufficient system resources"]
async fn e2e_point_in_time_restore() {
    start_stack();
    create_mode_test_table().await;
    exec_sql("pg-primary", "INSERT INTO mode_test VALUES (100)");
    // The restore starts from a backup taken before the target time.
    let since = backups(DEFAULT_API).await.len();
    wait_until(
        "a backup after the first write",
        Duration::from_secs(90),
        || async { backups(DEFAULT_API).await.len() > since },
    )
    .await;

    exec_sql("pg-primary", "INSERT INTO mode_test VALUES (200)");
    let target = exec_sql("pg-primary", "SELECT now()::text");
    tokio::time::sleep(Duration::from_secs(2)).await;
    exec_sql("pg-primary", "INSERT INTO mode_test VALUES (300)");
    let segment = exec_sql("pg-primary", "SELECT pg_walfile_name(pg_switch_wal())");
    wait_until("the WAL to archive", Duration::from_secs(90), || async {
        exec_sql(
            "pg-primary",
            "SELECT last_archived_wal FROM pg_stat_archiver",
        ) >= segment
    })
    .await;

    compose(&[
        "--profile",
        "spare",
        "up",
        "-d",
        "--no-deps",
        "--build",
        "vk-agent-4",
    ]);
    let response = reqwest::Client::new()
        .post(format!("{DEFAULT_API}/api/backups/restore"))
        .bearer_auth("dev-api-key")
        .json(&serde_json::json!({
            "agent_addr": "vk-agent-4:7001",
            "target_time": target,
        }))
        .send()
        .await
        .expect("POST /api/backups/restore");
    assert_eq!(response.status(), 202);

    wait_until("the restore", Duration::from_secs(120), || async {
        let status: serde_json::Value = reqwest::get(format!("{DEFAULT_API}/api/backups/restore"))
            .await
            .expect("GET /api/backups/restore")
            .json()
            .await
            .expect("parse restore status");
        assert!(status["error"].is_null(), "restore failed: {status}");
        status["running"] == false
    })
    .await;

    wait_until("the restored database", Duration::from_secs(30), || async {
        pg4_query("SELECT pg_is_in_recovery()") == "f"
    })
    .await;
    assert_eq!(
        pg4_query("SELECT string_agg(value::text, ',' ORDER BY value) FROM mode_test"),
        "100,200",
        "the restore must stop at the target time"
    );
    assert_eq!(
        pg4_query("INSERT INTO mode_test VALUES (400) RETURNING value")
            .lines()
            .next(),
        Some("400"),
        "a restored database must accept writes"
    );
    assert_eq!(
        exec_sql(
            "pg-primary",
            "SELECT count(*) FROM mode_test WHERE value = 300"
        ),
        "1",
        "the source must stay up"
    );

    compose(&["--profile", "spare", "down", "-v", "--remove-orphans"]);
}

/// A new node can seed from a backup instead of from the primary.
#[tokio::test]
#[ignore = "requires Docker daemon and sufficient system resources"]
async fn e2e_new_node_seeds_from_a_backup() {
    start_stack();
    create_mode_test_table().await;
    exec_sql("pg-primary", "INSERT INTO mode_test VALUES (7)");
    let since = backups(DEFAULT_API).await.len();
    wait_until("a backup", Duration::from_secs(90), || async {
        backups(DEFAULT_API).await.len() > since
    })
    .await;

    add_and_seed_pg4_from(true).await;

    assert_eq!(pg4_query("SELECT value FROM mode_test"), "7");
    assert!(
        pg4_query("SHOW restore_command").contains("wal-fetch"),
        "pg4 must have come from a backup"
    );

    compose(&["--profile", "spare", "down", "-v", "--remove-orphans"]);
}

/// An external standby follows every primary but is never promoted by a
/// failover, and detaching it gives an independent database.
#[tokio::test]
#[ignore = "requires Docker daemon and sufficient system resources"]
async fn e2e_external_standby_stays_out_and_can_detach() {
    let api = "http://localhost:18009";
    start_stack();
    create_mode_test_table().await;
    add_pg4(false, true).await;

    // It takes no part in synchronous commit.
    set_mode("sync").await;
    wait_for_standby_names(STANDBY_NAMES).await;
    set_mode("async").await;
    wait_for_standby_names("").await;

    // With the highest priority and every commit, it is still passed over, and
    // it follows whoever is promoted.
    docker("stop", "pg-primary");
    let new_primary = wait_for_new_primary(api, "pg1", Duration::from_secs(60)).await;
    assert_ne!(
        new_primary, "pg4",
        "an external standby must not be promoted"
    );
    let service = service_of(&new_primary);
    wait_until(
        "pg4 to follow the new primary",
        Duration::from_secs(60),
        || async {
            exec_sql(
                service,
                "SELECT count(*) FROM pg_stat_replication \
             WHERE application_name = 'pg4' AND state = 'streaming'",
            ) == "1"
        },
    )
    .await;

    // Only the leader can detach a node, and any instance could be leading.
    let client = reqwest::Client::new();
    let mut detached = false;
    for port in [8009, 18009, 28009] {
        let response = client
            .post(format!("http://localhost:{port}/api/nodes/pg4/detach"))
            .bearer_auth("dev-api-key")
            .send()
            .await
            .expect("POST /api/nodes/pg4/detach");
        if response.status() == 204 {
            detached = true;
            break;
        }
        assert_eq!(response.status(), 503, "{}", response.text().await.unwrap());
    }
    assert!(detached, "the leader must detach pg4");

    wait_until("pg4 to accept writes", Duration::from_secs(30), || async {
        pg4_query("SELECT pg_is_in_recovery()") == "f"
    })
    .await;
    assert_eq!(
        pg4_query("INSERT INTO mode_test VALUES (500) RETURNING value")
            .lines()
            .next(),
        Some("500")
    );
    assert_eq!(
        exec_sql(service, "SELECT count(*) FROM mode_test WHERE value = 500"),
        "0",
        "a detached node must be independent"
    );
    assert_eq!(
        exec_sql(
            service,
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'pgcluster_pg4'"
        ),
        "0",
        "its slot must not hold WAL on the old primary"
    );

    // With no heartbeats, a node still in a cluster would fence by now.
    tokio::time::sleep(Duration::from_secs(45)).await;
    assert_eq!(
        pg4_query("SELECT pg_is_in_recovery()"),
        "f",
        "a detached node must not fence itself"
    );

    compose(&["--profile", "spare", "down", "-v", "--remove-orphans"]);
}

const SINGLE_COMPOSE: &str = "docker/single-compose.yml";

fn single(args: &[&str]) -> std::process::Output {
    let out = Command::new("docker")
        .args(["compose", "-p", "pgcluster-single", "-f", SINGLE_COMPOSE])
        .args(args)
        .current_dir(project_root())
        .output()
        .expect("docker compose");
    assert!(
        out.status.success(),
        "docker compose {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

/// Connect, allowing for the proxy's lease and login setup.
async fn connect_with_retries(dsn: &str) -> tokio_postgres::Client {
    for _ in 0..30 {
        if let Ok((client, connection)) = tokio_postgres::connect(dsn, tokio_postgres::NoTls).await
        {
            tokio::spawn(connection);
            return client;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    panic!("could not connect through the proxy");
}

/// One node has no failover: it recovers from its volume. Removing the
/// containers and network stands for losing the VM, with its disk reattached.
#[tokio::test]
#[ignore = "requires Docker daemon and sufficient system resources"]
async fn e2e_one_node_recovers_after_its_vm_is_replaced() {
    let api = "http://localhost:8109";
    let dsn = "host=localhost port=5532 user=postgres password=test dbname=postgres";

    single(&["up", "-d", "--build", "--wait"]);
    wait_for_primary(api, Duration::from_secs(60)).await;
    let pg = connect_with_retries(dsn).await;
    pg.simple_query("CREATE TABLE survives (value int); INSERT INTO survives VALUES (1)")
        .await
        .expect("write before the replacement");
    drop(pg);

    single(&["down"]);
    single(&["up", "-d", "--wait"]);

    let status = wait_for_primary(api, Duration::from_secs(60)).await;
    assert_eq!(status["primary_node_id"], "pg1");

    let pg = connect_with_retries(dsn).await;
    let rows = pg
        .simple_query("SELECT value FROM survives")
        .await
        .expect("read after the replacement");
    assert!(
        rows.iter()
            .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))),
        "the data must survive"
    );
    pg.simple_query("INSERT INTO survives VALUES (2)")
        .await
        .expect("write after the replacement");

    single(&["down", "-v", "--remove-orphans"]);
}

// ── Proxy login ───────────────────────────────────────────────────────────────

fn psql_as(
    user: &str,
    password: &str,
    proxy: &str,
    sslmode: &str,
    sql: &str,
) -> std::process::Output {
    let conninfo = format!("host={proxy} user={user} dbname=postgres sslmode={sslmode}");
    Command::new("docker")
        .args(["compose", "-f", COMPOSE_FILE, "exec", "-T"])
        .args(["-e", &format!("PGPASSWORD={password}"), "pg-primary"])
        .args(["psql", &conninfo, "-v", "ON_ERROR_STOP=1", "-Atc", sql])
        .current_dir(project_root())
        .output()
        .expect("docker compose exec psql")
}

/// The proxy runs SCRAM itself and logs in to Postgres over verified TLS, for any
/// role, any client TLS mode and both query protocols.
#[tokio::test]
#[ignore = "requires Docker daemon and sufficient system resources"]
async fn e2e_proxy_authenticates_clients_and_uses_tls_to_postgres() {
    start_stack();
    create_mode_test_table().await;
    exec_sql("pg-primary", "CREATE ROLE app LOGIN PASSWORD 'app-secret'");
    exec_sql("pg-primary", "GRANT ALL ON mode_test TO app");

    // A client that sees TLS sends a different SCRAM header, which is what
    // broke passthrough. The link to Postgres must be encrypted either way.
    for sslmode in ["disable", "prefer", "require"] {
        let out = psql_as(
            "app",
            "app-secret",
            "pgcluster-1",
            sslmode,
            "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
        );
        assert!(
            out.status.success(),
            "sslmode={sslmode}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&out.stdout).trim(),
            "t",
            "{sslmode}"
        );
    }

    // Wrong passwords and unknown roles must look the same.
    let wrong = psql_as("app", "nope", "pgcluster-1", "disable", "SELECT 1");
    let unknown = psql_as("nobody", "nope", "pgcluster-1", "disable", "SELECT 1");
    assert!(!wrong.status.success() && !unknown.status.success());
    for (out, user) in [(&wrong, "app"), (&unknown, "nobody")] {
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains(&format!(
                "password authentication failed for user \"{user}\""
            )),
            "{stderr}"
        );
    }

    // The auth role reads verifiers through its one function, and no other way.
    assert_eq!(
        exec_sql(
            "pg-primary",
            "SELECT has_table_privilege('pgcluster_auth', 'pg_authid', 'SELECT'), \
             has_function_privilege('public', 'pgcluster.get_auth(name)', 'EXECUTE')"
        ),
        "f|f"
    );

    // The extended protocol, as drivers use it.
    let dsn = "host=localhost port=5432 user=app password=app-secret dbname=postgres";
    let (pg, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("connect with the extended protocol");
    tokio::spawn(connection);
    let row = tokio::time::timeout(
        Duration::from_secs(10),
        pg.query_one("SELECT $1::int + 1", &[&41i32]),
    )
    .await
    .expect("a prepared statement must not hang")
    .expect("run a prepared statement");
    assert_eq!(row.get::<_, i32>(0), 42);

    // COPY waits on the client, so the proxy must not wait on the backend.
    let copy = "printf '1\\n2\\n' | PGPASSWORD=test timeout 10 psql \
        'host=pgcluster-1 user=postgres dbname=postgres sslmode=disable' \
        -c 'COPY mode_test FROM STDIN'";
    let out = Command::new("docker")
        .args(["compose", "-f", COMPOSE_FILE, "exec", "-T", "pg-primary"])
        .args(["sh", "-c", copy])
        .current_dir(project_root())
        .output()
        .expect("docker compose exec");
    assert!(
        out.status.success(),
        "COPY: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // pgcluster-3 has read routing on, so it logs in to a replica for the client.
    let dsn = "host=localhost port=25432 user=app password=app-secret dbname=postgres";
    let (pg, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("connect to the routing proxy");
    tokio::spawn(connection);
    let value = |rows: &[tokio_postgres::SimpleQueryMessage]| {
        rows.iter().find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
    };
    let rows = pg
        .simple_query("SELECT pg_is_in_recovery()")
        .await
        .expect("read through the routing proxy");
    assert_eq!(
        value(&rows).as_deref(),
        Some("t"),
        "reads must reach a replica"
    );
    let rows = pg
        .simple_query("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
        .await
        .expect("read TLS status");
    assert_eq!(value(&rows).as_deref(), Some("t"), "replica links use TLS");
    pg.simple_query("INSERT INTO mode_test VALUES (900)")
        .await
        .expect("writes go to the primary");
    pg.simple_query("SET application_name = 'x'")
        .await
        .expect("SET");
    let rows = pg
        .simple_query("SELECT pg_is_in_recovery()")
        .await
        .expect("read after SET");
    assert_eq!(
        value(&rows).as_deref(),
        Some("f"),
        "SET pins a session to the primary"
    );

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
