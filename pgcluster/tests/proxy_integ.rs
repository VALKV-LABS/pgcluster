/// Proxy protocol unit tests — no Docker required.
/// Full integration tests (connecting to real Postgres) are marked #[ignore].
use pgcluster::proxy::protocol::{classify_statement, StatementIntent};
use pgcluster::proxy::session::{SessionState, TxnState};

// ── classify_statement ────────────────────────────────────────────────────────

#[test]
fn classify_select_is_read() {
    assert_eq!(
        classify_statement("SELECT * FROM users"),
        StatementIntent::Read
    );
}

#[test]
fn classify_insert_is_write() {
    assert_eq!(
        classify_statement("INSERT INTO t VALUES (1)"),
        StatementIntent::Write
    );
}

#[test]
fn classify_begin_read_only_is_read() {
    assert_eq!(classify_statement("BEGIN READ ONLY"), StatementIntent::Read);
    assert_eq!(classify_statement("begin read only"), StatementIntent::Read);
}

#[test]
fn classify_begin_is_write() {
    assert_eq!(classify_statement("BEGIN"), StatementIntent::Write);
}

#[test]
fn classify_update_is_write() {
    assert_eq!(
        classify_statement("UPDATE t SET x = 1"),
        StatementIntent::Write
    );
}

#[test]
fn classify_ddl_is_write() {
    assert_eq!(
        classify_statement("CREATE TABLE foo (id INT)"),
        StatementIntent::Write
    );
    assert_eq!(classify_statement("DROP TABLE foo"), StatementIntent::Write);
    assert_eq!(
        classify_statement("ALTER TABLE foo ADD COLUMN bar TEXT"),
        StatementIntent::Write
    );
}

#[test]
fn classify_set_is_set_local() {
    assert_eq!(
        classify_statement("SET search_path TO myschema"),
        StatementIntent::SetLocal
    );
}

#[test]
fn classify_with_leading_whitespace() {
    assert_eq!(classify_statement("  \t\nSELECT 1"), StatementIntent::Read);
}

// ── SessionState ──────────────────────────────────────────────────────────────

#[test]
fn txn_state_transitions_commit() {
    let mut s = SessionState::default();
    s.update_from_ready_for_query(b'T');
    assert_eq!(s.txn_state, TxnState::InTransaction);
    s.update_from_ready_for_query(b'I');
    assert_eq!(s.txn_state, TxnState::Idle);
}

#[test]
fn txn_state_transitions_rollback() {
    let mut s = SessionState::default();
    s.update_from_ready_for_query(b'T');
    s.update_from_ready_for_query(b'E');
    s.update_from_ready_for_query(b'I');
    assert_eq!(s.txn_state, TxnState::Idle);
}

#[test]
fn txn_state_stays_in_failed() {
    let mut s = SessionState::default();
    s.update_from_ready_for_query(b'E');
    assert_eq!(s.txn_state, TxnState::Failed);
}

#[test]
fn sticky_routing_while_in_txn() {
    use pgcluster::proxy::protocol::StatementIntent;
    use pgcluster::proxy::session::RouteTarget;
    let mut s = SessionState::default();
    let target = s.route_intent(&StatementIntent::Read);
    assert_eq!(target, RouteTarget::ReplicaOrPrimary);
    s.update_from_ready_for_query(b'T');
    let target = s.route_intent(&StatementIntent::Read);
    assert_eq!(target, RouteTarget::Primary);
}

// ── Full proxy integration tests (require Docker + E2E_STACK_ADDR) ───────────
// Skipped automatically when E2E_STACK_ADDR is not set.

fn e2e_proxy_addr() -> Option<String> {
    std::env::var("E2E_STACK_ADDR").ok()
}

fn e2e_pg_dsn() -> Option<String> {
    e2e_proxy_addr().map(|addr| {
        let host = addr.split(':').next().unwrap_or("127.0.0.1");
        format!("host={host} port=5432 user=postgres password=test dbname=postgres")
    })
}

#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn psql_select_through_proxy_returns_result() {
    let Some(dsn) = e2e_pg_dsn() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("connect to proxy");
    tokio::spawn(async move { connection.await.ok() });
    let rows = client.query("SELECT 1 AS v", &[]).await.expect("query");
    assert_eq!(rows.len(), 1);
    let v: i32 = rows[0].get("v");
    assert_eq!(v, 1);
}

#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn write_routes_to_primary() {
    let Some(dsn) = e2e_pg_dsn() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move { connection.await.ok() });
    client
        .execute(
            "CREATE TABLE IF NOT EXISTS proxy_test (id SERIAL, v TEXT)",
            &[],
        )
        .await
        .expect("create table");
    let rows_inserted = client
        .execute("INSERT INTO proxy_test (v) VALUES ('hello')", &[])
        .await
        .expect("insert");
    assert_eq!(rows_inserted, 1);
}

#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn read_only_txn_routes_to_replica() {
    let Some(dsn) = e2e_pg_dsn() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move { connection.await.ok() });
    client.execute("BEGIN READ ONLY", &[]).await.expect("begin");
    let rows = client
        .query("SELECT pg_is_in_recovery() AS r", &[])
        .await
        .expect("query");
    client.execute("COMMIT", &[]).await.expect("commit");
    let in_recovery: bool = rows[0].get("r");
    assert!(in_recovery, "read-only txn should be served by a replica");
}

/// Kill the primary Postgres node, wait for failover, then verify a fresh
/// client connection through the proxy is still functional.
///
/// The in-flight connection at kill time is expected to drop — the test
/// verifies that *new* connections succeed after the cluster recovers.
#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn client_reconnects_after_primary_failure() {
    let Some(addr) = e2e_proxy_addr() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    let host = addr.split(':').next().unwrap_or("127.0.0.1");
    let api = format!("http://{}:8009", host);
    let dsn = format!("host={host} port=5432 user=postgres password=test dbname=postgres");

    // Verify proxy is working before we start.
    let (pg, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("initial proxy connection");
    tokio::spawn(async move { connection.await.ok() });
    pg.query("SELECT 1", &[]).await.expect("initial SELECT");
    let status_body: serde_json::Value = reqwest::Client::new()
        .get(format!("{api}/api/status"))
        .send()
        .await
        .expect("GET /api/status")
        .json()
        .await
        .expect("parse status");
    let original_primary = status_body["primary_node_id"]
        .as_str()
        .expect("primary_node_id")
        .to_string();
    eprintln!("==> Current primary: {original_primary}");

    // Determine the container name for the primary's Postgres node.
    let pg_container = match original_primary.as_str() {
        "pg1" => "pg-primary",
        "pg2" => "pg-replica-1",
        "pg3" => "pg-replica-2",
        other => panic!("unexpected node id: {other}"),
    };
    eprintln!("==> Killing {pg_container}...");
    std::process::Command::new("docker")
        .args(["kill", pg_container])
        .status()
        .unwrap_or_else(|e| panic!("docker kill {pg_container}: {e}"));

    // Wait for the cluster to elect a new primary.
    eprintln!("==> Waiting for failover...");
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(90);
    let new_primary = loop {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        if let Ok(resp) = http.get(format!("{api}/api/status")).send().await {
            if let Ok(body) = resp.json::<serde_json::Value>().await {
                let p = body["primary_node_id"].as_str().unwrap_or("").to_string();
                if !p.is_empty() && p != original_primary {
                    break p;
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            panic!("primary did not change within 90s");
        }
    };
    eprintln!("==> New primary: {new_primary}");

    // Allow the proxy routing table to settle.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // A *new* client connection through the proxy must succeed.
    let (pg2, connection2) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("reconnect through proxy after failover");
    tokio::spawn(async move { connection2.await.ok() });
    let rows = pg2
        .query("SELECT 1 AS v", &[])
        .await
        .expect("SELECT through proxy after failover");
    let v: i32 = rows[0].get("v");
    assert_eq!(v, 1, "proxy must forward queries to the new primary");
    eprintln!("==> Proxy functional after failover ✓");
}

#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn health_endpoint_returns_200_when_primary_alive() {
    let Some(addr) = e2e_proxy_addr() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    let host = addr.split(':').next().unwrap_or("127.0.0.1");
    let resp = reqwest::get(format!("http://{host}:8008/health"))
        .await
        .expect("request");
    assert_eq!(resp.status(), 200);
}

/// Pause all three Postgres containers so no primary is reachable, then verify
/// the health endpoint returns 503.  Uses `docker pause`/`docker unpause` to
/// avoid container restarts — the stack is left in a clean state.
///
/// Steps:
///   1. Verify health is 200 before pausing.
///   2. `docker pause` pg-primary, pg-replica-1, pg-replica-2.
///   3. Wait for failure detection (30s covers the consecutive-failure window).
///   4. Check health returns 503.
///   5. `docker unpause` all three containers to restore the stack.
#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn health_endpoint_returns_503_when_no_primary() {
    let Some(addr) = e2e_proxy_addr() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    let host = addr.split(':').next().unwrap_or("127.0.0.1");
    let health_url = format!("http://{host}:8008/health");

    // Baseline: health must be 200 before we start.
    let pre = reqwest::get(&health_url)
        .await
        .expect("pre-test health check");
    assert_eq!(pre.status().as_u16(), 200, "health must be 200 before test");

    let pg_containers = ["pg-primary", "pg-replica-1", "pg-replica-2"];

    // Pause all postgres containers.
    eprintln!("==> Pausing all Postgres containers...");
    for c in &pg_containers {
        std::process::Command::new("docker")
            .args(["pause", c])
            .status()
            .unwrap_or_else(|e| panic!("docker pause {c}: {e}"));
    }

    // Wait for the node monitor to accumulate enough consecutive failures.
    // Default: 3 failures × ~5s interval = ~15s; we wait 30s to be safe.
    eprintln!("==> Waiting 30s for failure detection...");
    tokio::time::sleep(std::time::Duration::from_secs(30)).await;

    // Health endpoint must return 503 when no Postgres node is reachable.
    let resp = reqwest::get(&health_url).await.expect("health check");
    let status = resp.status().as_u16();

    // Unpause before any assert so the stack is always restored.
    eprintln!("==> Unpausing all Postgres containers...");
    for c in &pg_containers {
        std::process::Command::new("docker")
            .args(["unpause", c])
            .status()
            .unwrap_or_else(|e| panic!("docker unpause {c}: {e}"));
    }

    assert_eq!(
        status, 503,
        "health endpoint must return 503 when no primary is reachable"
    );
    eprintln!("==> Health returned 503 as expected ✓");
}
