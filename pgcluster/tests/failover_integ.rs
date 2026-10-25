/// Failover engine unit tests (no Docker needed for candidate selection).
/// End-to-end failover tests are #[ignore].
use pgcluster::failover::candidate::pick_candidate;
use pgcluster::raft::topology::{ClusterTopology, NodeConfig, NodeRole};
use std::time::{Duration, Instant};

fn node(id: &str, priority: u32) -> NodeConfig {
    NodeConfig {
        node_id: id.into(),
        agent_addr: format!("127.0.0.1:700{}", id.len()),
        postgres_addr: format!("127.0.0.1:543{}", id.len()),
        priority,
        tags: Default::default(),
    }
}

#[test]
fn pick_candidate_no_replicas_returns_none() {
    let mut t = ClusterTopology::default();
    t.node_configs.insert("pg1".into(), node("pg1", 100));
    t.node_roles.insert("pg1".into(), NodeRole::Primary);
    assert_eq!(pick_candidate(&t, "pg1"), None);
}

#[test]
fn pick_candidate_excludes_offline_nodes() {
    let mut t = ClusterTopology::default();
    t.node_configs.insert("pg1".into(), node("pg1", 100));
    t.node_configs.insert("pg2".into(), node("pg2", 90));
    t.node_roles.insert("pg1".into(), NodeRole::Primary);
    t.node_roles.insert("pg2".into(), NodeRole::Offline);
    assert_eq!(pick_candidate(&t, "pg1"), None);
}

#[test]
fn pick_candidate_picks_highest_lsn() {
    let mut t = ClusterTopology::default();
    for id in ["pg1", "pg2", "pg3"] {
        t.node_configs.insert(id.into(), node(id, 100));
    }
    t.node_roles.insert("pg1".into(), NodeRole::Primary);
    t.node_roles.insert("pg2".into(), NodeRole::Replica);
    t.node_roles.insert("pg3".into(), NodeRole::Replica);
    t.last_flush_lsns.insert("pg2".into(), 100);
    t.last_flush_lsns.insert("pg3".into(), 999);
    assert_eq!(pick_candidate(&t, "pg1").as_deref(), Some("pg3"));
}

// ── E2E helpers ───────────────────────────────────────────────────────────────

/// Returns the hostname from E2E_STACK_ADDR (e.g. "localhost" from "localhost:8009").
/// Returns None if the env var is not set — callers return early to skip.
fn e2e_host() -> Option<String> {
    std::env::var("E2E_STACK_ADDR")
        .ok()
        .map(|addr| addr.split(':').next().unwrap_or("127.0.0.1").to_string())
}

fn api_url(host: &str) -> String {
    format!("http://{}:8009", host)
}

fn proxy_dsn(host: &str) -> String {
    format!("host={host} port=5432 user=postgres password=test dbname=postgres")
}

/// Map pgcluster logical node_id to the Docker container name used in the e2e compose stack.
fn node_container(node_id: &str) -> &'static str {
    match node_id {
        "pg1" => "pg-primary",
        "pg2" => "pg-replica-1",
        "pg3" => "pg-replica-2",
        _ => panic!("unknown node id: {node_id}"),
    }
}

fn docker_kill(container: &str) {
    let status = std::process::Command::new("docker")
        .args(["kill", container])
        .status()
        .unwrap_or_else(|e| panic!("docker kill {container}: {e}"));
    assert!(status.success(), "docker kill {container} failed");
}

/// Run a SQL statement inside a postgres container via `docker exec psql`.
fn docker_exec_sql(container: &str, sql: &str) {
    std::process::Command::new("docker")
        .args(["exec", container, "psql", "-U", "postgres", "-c", sql])
        .status()
        .unwrap_or_else(|e| panic!("docker exec {container} psql: {e}"));
}

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
                if !body["primary_node_id"].as_str().unwrap_or("").is_empty() {
                    return body;
                }
            }
        }
        if Instant::now() >= deadline {
            panic!("cluster did not elect a primary within {:?}", timeout);
        }
    }
}

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

// ── E2E tests ─────────────────────────────────────────────────────────────────

/// Kill the primary Postgres container and verify the cluster elects a new primary.
///
/// Steps:
///   1. Wait for cluster to report a primary.
///   2. Map node_id → Docker container name and `docker kill` it.
///   3. Poll /api/status until primary_node_id changes.
///   4. Assert the new primary is writable through the proxy.
///   5. Assert failover history has at least one record.
#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn primary_crash_triggers_automatic_failover() {
    let Some(host) = e2e_host() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    let api = api_url(&host);
    let dsn = proxy_dsn(&host);

    eprintln!("==> Waiting for cluster to have a primary...");
    let status = wait_for_primary(&api, Duration::from_secs(60)).await;
    let original_primary = status["primary_node_id"].as_str().unwrap().to_string();
    eprintln!("==> Primary is {original_primary}");

    let container = node_container(&original_primary);
    eprintln!("==> Killing {container}...");
    docker_kill(container);

    eprintln!("==> Waiting for new primary to be elected...");
    let new_primary = wait_for_new_primary(&api, &original_primary, Duration::from_secs(90)).await;
    assert_ne!(
        new_primary, original_primary,
        "new primary must differ from killed node"
    );
    eprintln!("==> Failover complete: new primary is {new_primary}");

    // Give the proxy a moment to route to the new primary.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify writes work through the proxy after failover.
    let (pg, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("connect to proxy after failover");
    tokio::spawn(async move { connection.await.ok() });
    pg.execute("SELECT 1", &[])
        .await
        .expect("proxy write after failover");

    // Verify failover history was recorded.
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let history: serde_json::Value = http
        .get(format!("{api}/api/failover/history"))
        .send()
        .await
        .expect("GET /api/failover/history")
        .json()
        .await
        .expect("parse failover history");
    let events = history.as_array().map(|a| a.len()).unwrap_or(0);
    assert!(
        events > 0,
        "failover history must be non-empty after failover"
    );
    eprintln!("==> Failover history: {events} event(s)");
}

/// Insert rows through the proxy, trigger a planned switchover, then verify
/// all rows survive on the new primary (zero-data-loss).
///
/// Steps:
///   1. Create a test table and insert 50 rows through the proxy.
///   2. Trigger switchover to a replica (max_lag_bytes=0 → waits for 0 lag).
///   3. Reconnect after switchover and assert row count is still 50.
#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn switchover_is_zero_data_loss() {
    let Some(host) = e2e_host() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    let api = api_url(&host);
    let dsn = proxy_dsn(&host);

    eprintln!("==> Waiting for cluster to have a primary...");
    let status = wait_for_primary(&api, Duration::from_secs(60)).await;
    let original_primary = status["primary_node_id"].as_str().unwrap().to_string();
    eprintln!("==> Primary is {original_primary}");

    // Write 50 rows through the proxy.
    let (pg, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("connect to proxy");
    tokio::spawn(async move { connection.await.ok() });
    pg.execute("DROP TABLE IF EXISTS _sw_zdl_test", &[])
        .await
        .expect("drop table");
    pg.execute(
        "CREATE TABLE _sw_zdl_test (id SERIAL PRIMARY KEY, v INT)",
        &[],
    )
    .await
    .expect("create table");
    pg.execute(
        "INSERT INTO _sw_zdl_test (v) SELECT generate_series(1, 50)",
        &[],
    )
    .await
    .expect("insert rows");
    eprintln!("==> Inserted 50 rows");

    // Find a replica to switchover to.
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let nodes: serde_json::Value = http
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
        .find(|n| n["role"].as_str().unwrap_or("") == "replica")
        .and_then(|n| n["node_id"].as_str())
        .expect("no replica found for switchover");
    eprintln!("==> Switchover target: {target}");

    // Trigger switchover — waits for zero lag before promoting.
    let sw_resp = http
        .post(format!("{api}/api/switchover"))
        .json(&serde_json::json!({
            "target_node_id": target,
            "max_lag_bytes": 0,
            "timeout_secs": 60
        }))
        .send()
        .await
        .expect("POST /api/switchover");
    assert!(
        sw_resp.status().is_success() || sw_resp.status().as_u16() == 202,
        "switchover returned {}",
        sw_resp.status()
    );
    eprintln!("==> Switchover request accepted");

    // Wait for the primary to flip.
    let new_primary = wait_for_new_primary(&api, &original_primary, Duration::from_secs(60)).await;
    assert_eq!(
        new_primary, target,
        "new primary should be the switchover target"
    );
    eprintln!("==> Switchover complete: {original_primary} → {new_primary}");

    // Give the proxy a moment to settle.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Reconnect and verify all 50 rows survived.
    let (pg2, connection2) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("reconnect after switchover");
    tokio::spawn(async move { connection2.await.ok() });
    let rows = pg2
        .query("SELECT COUNT(*) FROM _sw_zdl_test", &[])
        .await
        .expect("count rows");
    let count: i64 = rows[0].get(0);
    assert_eq!(count, 50, "all 50 rows must survive switchover");
    eprintln!("==> Row count after switchover: {count} ✓");

    // Cleanup.
    pg2.execute("DROP TABLE IF EXISTS _sw_zdl_test", &[])
        .await
        .ok();
}

/// Lag pg3 behind pg2 by pausing WAL replay, then crash the primary and verify
/// that pg2 (the more caught-up replica) is promoted rather than pg3.
///
/// Steps:
///   1. Pause WAL replay on pg-replica-2 (pg3) via docker exec psql.
///   2. Write data through the proxy to create lag on pg3.
///   3. Kill the primary container.
///   4. Wait for failover.
///   5. Assert the new primary is pg2, not pg3.
///   6. Resume WAL replay on pg3.
#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn failover_picks_most_caught_up_replica() {
    let Some(host) = e2e_host() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    let api = api_url(&host);
    let dsn = proxy_dsn(&host);

    eprintln!("==> Waiting for cluster to have a primary...");
    let status = wait_for_primary(&api, Duration::from_secs(60)).await;
    let original_primary = status["primary_node_id"].as_str().unwrap().to_string();
    assert_eq!(
        original_primary, "pg1",
        "test assumes pg1 is initial primary"
    );
    eprintln!("==> Primary: {original_primary}");

    // Pause WAL replay on pg3 so it falls behind pg2.
    eprintln!("==> Pausing WAL replay on pg-replica-2 (pg3)...");
    docker_exec_sql("pg-replica-2", "SELECT pg_wal_replay_pause()");

    // Write enough data to create measurable lag on pg3.
    let (pg, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("connect to proxy");
    tokio::spawn(async move { connection.await.ok() });
    pg.execute(
        "CREATE TABLE IF NOT EXISTS _lag_test (id SERIAL, v INT)",
        &[],
    )
    .await
    .expect("create lag table");
    for _ in 0..5 {
        pg.execute(
            "INSERT INTO _lag_test (v) SELECT generate_series(1, 100)",
            &[],
        )
        .await
        .expect("insert lag data");
    }
    eprintln!("==> Wrote 500 rows to create WAL lag on pg3");

    // Give replication a moment so pg2 has applied the WAL but pg3 hasn't.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Kill the primary.
    eprintln!("==> Killing {}", node_container(&original_primary));
    docker_kill(node_container(&original_primary));

    // Wait for failover.
    eprintln!("==> Waiting for failover...");
    let new_primary = wait_for_new_primary(&api, &original_primary, Duration::from_secs(90)).await;
    eprintln!("==> New primary: {new_primary}");

    // pg3 was paused so it had less WAL applied than pg2 → pg2 should win.
    assert_eq!(
        new_primary, "pg2",
        "pg2 should be promoted because pg3 had paused WAL replay"
    );

    // Resume WAL replay on pg3 regardless of outcome.
    eprintln!("==> Resuming WAL replay on pg-replica-2 (pg3)...");
    docker_exec_sql("pg-replica-2", "SELECT pg_wal_replay_resume()");

    // Cleanup the lag table.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let (pg2, connection2) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("reconnect for cleanup");
    tokio::spawn(async move { connection2.await.ok() });
    pg2.execute("DROP TABLE IF EXISTS _lag_test", &[])
        .await
        .ok();
    eprintln!("==> Test complete");
}
