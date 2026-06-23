/// Proxy protocol unit tests — no Docker required.
/// Full integration tests (connecting to real Postgres) are marked #[ignore].
use pgcluster::proxy::protocol::{classify_statement, StatementIntent};
use pgcluster::proxy::session::{SessionState, TxnState};

// ── classify_statement ────────────────────────────────────────────────────────

#[test]
fn classify_select_is_read() {
    assert_eq!(classify_statement("SELECT * FROM users"), StatementIntent::Read);
}

#[test]
fn classify_insert_is_write() {
    assert_eq!(classify_statement("INSERT INTO t VALUES (1)"), StatementIntent::Write);
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
    assert_eq!(classify_statement("UPDATE t SET x = 1"), StatementIntent::Write);
}

#[test]
fn classify_ddl_is_write() {
    assert_eq!(classify_statement("CREATE TABLE foo (id INT)"), StatementIntent::Write);
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
        .execute("CREATE TABLE IF NOT EXISTS proxy_test (id SERIAL, v TEXT)", &[])
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
    let rows = client.query("SELECT pg_is_in_recovery() AS r", &[]).await.expect("query");
    client.execute("COMMIT", &[]).await.expect("commit");
    let in_recovery: bool = rows[0].get("r");
    assert!(in_recovery, "read-only txn should be served by a replica");
}

#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn client_reconnects_after_primary_failure() {
    let Some(_) = e2e_proxy_addr() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    // TODO: implement with docker kill pg-primary
    eprintln!("client_reconnects_after_primary_failure: not yet implemented");
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

#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn health_endpoint_returns_503_when_no_primary() {
    let Some(_) = e2e_proxy_addr() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    // TODO: implement with docker kill pg-primary then immediate health check
    eprintln!("health_endpoint_returns_503_when_no_primary: not yet implemented");
}
