use std::time::Duration;
/// Integration tests for vk-agent.
/// Tests marked #[ignore] require a running Postgres instance.
use vk_agent::config::AgentConfig;
use vk_agent::heartbeat::HeartbeatTracker;

#[test]
fn agent_config_defaults() {
    let toml = r#"
data_dir     = "/pgdata"
listen_addr  = "0.0.0.0:7001"
"#;
    let cfg: AgentConfig = toml::from_str(toml).unwrap();
    assert_eq!(cfg.heartbeat_timeout_seconds, 10);
    assert_eq!(cfg.postgres_user, "postgres");
    assert_eq!(cfg.postgres_port, 5432);
}

#[test]
fn postgres_url_tcp() {
    let cfg = AgentConfig {
        listen_addr: "0.0.0.0:7001".into(),
        data_dir: "/pgdata".into(),
        postgres_host: "127.0.0.1".into(),
        postgres_port: 5432,
        postgres_user: "postgres".into(),
        postgres_password: None,
        postgres_dbname: "postgres".into(),
        heartbeat_timeout_seconds: 10,
        tls: Default::default(),
        pg_ctl_path: "pg_ctl".into(),
    };
    assert_eq!(
        cfg.postgres_url(),
        "postgresql://postgres@127.0.0.1:5432/postgres"
    );
}

#[test]
fn postgres_url_with_password() {
    let cfg = AgentConfig {
        listen_addr: "0.0.0.0:7001".into(),
        data_dir: "/pgdata".into(),
        postgres_host: "127.0.0.1".into(),
        postgres_port: 5432,
        postgres_user: "postgres".into(),
        postgres_password: Some("s3cr3t".into()),
        postgres_dbname: "postgres".into(),
        heartbeat_timeout_seconds: 10,
        tls: Default::default(),
        pg_ctl_path: "pg_ctl".into(),
    };
    assert_eq!(
        cfg.postgres_url(),
        "postgresql://postgres:s3cr3t@127.0.0.1:5432/postgres"
    );
}

#[tokio::test]
async fn heartbeat_tracker_safe_mode_lifecycle() {
    let tracker = HeartbeatTracker::new(Duration::from_secs(10));
    // Should start not in safe mode
    assert!(!tracker.is_safe_mode());
    // touch() should keep it not in safe mode and not panic
    tracker.touch();
    assert!(!tracker.is_safe_mode());
}

fn pg_test_url() -> Option<String> {
    std::env::var("PG_TEST_URL").ok()
}

#[tokio::test]
#[ignore = "requires a running Postgres instance; set PG_TEST_URL"]
async fn local_postgres_connect() {
    let Some(url) = pg_test_url() else {
        eprintln!("skipping: PG_TEST_URL not set");
        return;
    };
    let pg = vk_agent::postgres::LocalPostgres::connect(&url)
        .await
        .expect("connect to local postgres");
    assert!(pg.is_running().await, "postgres should be running");
}

#[tokio::test]
#[ignore = "requires a running Postgres instance; set PG_TEST_URL"]
async fn get_status_returns_reasonable_values() {
    let Some(url) = pg_test_url() else {
        eprintln!("skipping: PG_TEST_URL not set");
        return;
    };
    let pg = vk_agent::postgres::LocalPostgres::connect(&url).await.unwrap();
    let version = pg.get_postgres_version().await.unwrap();
    assert!(
        version.starts_with("PostgreSQL"),
        "unexpected version: {version}"
    );
    let timeline = pg.get_timeline().await.unwrap();
    assert!(timeline >= 1);
}

#[tokio::test]
#[ignore = "requires PGDATA directory and pg_ctl in PATH; set PGDATA env var"]
async fn pg_ctl_is_running() {
    let Some(pgdata) = std::env::var("PGDATA").ok() else {
        eprintln!("skipping: PGDATA not set");
        return;
    };
    let ctl = vk_agent::process::PgCtl::new("pg_ctl", &pgdata);
    let running = ctl.is_running().await.unwrap();
    println!("postgres is_running: {running}");
}
