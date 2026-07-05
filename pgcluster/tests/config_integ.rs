/// Integration tests for config loading and validation.
/// Tests that parse config or validate struct fields run without Docker.
/// Tests that spawn the binary are #[ignore] (require a built binary).

#[test]
fn valid_minimal_config_file_loads() {
    let toml = r#"
[cluster]
name     = "test-cluster"
data_dir = "/tmp/pgcluster-test"

[raft]
node_id   = 1
bootstrap = true

[[raft.peers]]
id   = 1
addr = "127.0.0.1:7000"

[[raft.peers]]
id   = 2
addr = "127.0.0.1:7001"

[[raft.peers]]
id   = 3
addr = "127.0.0.1:7002"

[[nodes.node]]
id            = "pg1"
agent_addr    = "127.0.0.1:7010"
postgres_addr = "127.0.0.1:5432"
priority      = 100

[replication]
replication_user         = "replicator"
replication_password_env = "PG_REPLICATION_PASSWORD"

[proxy]
listen_addr        = "0.0.0.0:5432"
admin_listen_addr  = "0.0.0.0:5433"
health_listen_addr = "0.0.0.0:8008"

[metrics]
listen_addr = "0.0.0.0:9090"

[api]
listen_addr = "0.0.0.0:8080"
"#;
    let cfg: pgcluster::config::PgClusterConfig = toml::from_str(toml).unwrap();
    pgcluster::config::validate::validate(&cfg).unwrap();
    assert_eq!(cfg.cluster.name, "test-cluster");
    assert_eq!(cfg.raft.node_id, 1);
    assert_eq!(cfg.raft.peers.len(), 3);
}

#[test]
fn config_rejects_empty_cluster_name() {
    let toml = r#"
[cluster]
name     = ""
data_dir = "/tmp"

[raft]
node_id = 1

[[raft.peers]]
id   = 1
addr = "127.0.0.1:7000"

[[nodes.node]]
id            = "pg1"
agent_addr    = "127.0.0.1:7001"
postgres_addr = "127.0.0.1:5432"
priority      = 100

[replication]
replication_user = "replicator"
replication_password_env = "PG_REPLICATION_PASSWORD"

[proxy]
listen_addr        = "0.0.0.0:5432"
admin_listen_addr  = "0.0.0.0:5433"
health_listen_addr = "0.0.0.0:8008"

[metrics]
listen_addr = "0.0.0.0:9090"

[api]
listen_addr = "0.0.0.0:8080"
"#;
    let cfg: pgcluster::config::PgClusterConfig = toml::from_str(toml).unwrap();
    assert!(pgcluster::config::validate::validate(&cfg).is_err());
}

#[test]
fn config_rejects_node_id_not_in_peers() {
    let toml = r#"
[cluster]
name     = "test"
data_dir = "/tmp"

[raft]
node_id = 99

[[raft.peers]]
id   = 1
addr = "127.0.0.1:7000"

[[nodes.node]]
id            = "pg1"
agent_addr    = "127.0.0.1:7001"
postgres_addr = "127.0.0.1:5432"
priority      = 100

[replication]
replication_user = "replicator"
replication_password_env = "PG_REPLICATION_PASSWORD"

[proxy]
listen_addr        = "0.0.0.0:5432"
admin_listen_addr  = "0.0.0.0:5433"
health_listen_addr = "0.0.0.0:8008"

[metrics]
listen_addr = "0.0.0.0:9090"

[api]
listen_addr = "0.0.0.0:8080"
"#;
    let cfg: pgcluster::config::PgClusterConfig = toml::from_str(toml).unwrap();
    assert!(pgcluster::config::validate::validate(&cfg).is_err());
}

#[test]
fn config_defaults_are_reasonable() {
    let toml = r#"
[cluster]
name     = "test"
data_dir = "/tmp"

[raft]
node_id = 1

[[raft.peers]]
id   = 1
addr = "127.0.0.1:7000"

[[nodes.node]]
id            = "pg1"
agent_addr    = "127.0.0.1:7001"
postgres_addr = "127.0.0.1:5432"
priority      = 100

[replication]
replication_user = "replicator"
replication_password_env = "PG_REPLICATION_PASSWORD"

[proxy]
listen_addr        = "0.0.0.0:5432"
admin_listen_addr  = "0.0.0.0:5433"
health_listen_addr = "0.0.0.0:8008"

[metrics]
listen_addr = "0.0.0.0:9090"

[api]
listen_addr = "0.0.0.0:8080"
"#;
    let cfg: pgcluster::config::PgClusterConfig = toml::from_str(toml).unwrap();
    // Defaults should be sane values
    assert!(cfg.failover.health_check_interval_ms > 0);
    assert!(cfg.failover.health_check_failures_before_failover > 0);
    assert!(cfg.proxy.pool.max_connections_per_db_user > 0);
}

// ── Backup config tests ───────────────────────────────────────────────────────

/// Base minimal TOML without a backup section (reused across backup tests).
fn base_config_toml() -> &'static str {
    r#"
[cluster]
name     = "test"
data_dir = "/tmp"

[raft]
node_id = 1

[[raft.peers]]
id   = 1
addr = "127.0.0.1:7000"

[[raft.peers]]
id   = 2
addr = "127.0.0.1:7001"

[[raft.peers]]
id   = 3
addr = "127.0.0.1:7002"

[[nodes.node]]
id            = "pg1"
agent_addr    = "127.0.0.1:7010"
postgres_addr = "127.0.0.1:5432"
priority      = 100

[replication]
replication_user         = "replicator"
replication_password_env = "PG_REPLICATION_PASSWORD"

[proxy]
listen_addr        = "0.0.0.0:5432"
admin_listen_addr  = "0.0.0.0:5433"
health_listen_addr = "0.0.0.0:8008"

[metrics]
listen_addr = "0.0.0.0:9090"

[api]
listen_addr = "0.0.0.0:8080"
"#
}

#[test]
fn backup_config_valid_passes_validation() {
    let toml = format!(
        "{}\n{}",
        base_config_toml(),
        r#"
[backup]
enabled        = true
prefer_replica = true

[backup.s3]
bucket = "my-backups"
region = "us-east-1"

[[backup.schedule]]
frequency = "daily"
retain    = 7

[[backup.schedule]]
frequency = "weekly"
retain    = 4
"#
    );
    let cfg: pgcluster::config::PgClusterConfig = toml::from_str(&toml).unwrap();
    assert!(
        pgcluster::config::validate::validate(&cfg).is_ok(),
        "valid backup config should pass validation"
    );
    let backup = cfg.backup.unwrap();
    assert_eq!(backup.s3.bucket, "my-backups");
    assert_eq!(backup.schedule.len(), 2);
}

#[test]
fn backup_config_without_s3_section_fails_toml_parse() {
    let toml = format!(
        "{}\n{}",
        base_config_toml(),
        r#"
[backup]
enabled = true

[[backup.schedule]]
frequency = "daily"
retain    = 7
"#
    );
    // `s3` is a required (non-Option) field, so parsing must fail.
    let result: Result<pgcluster::config::PgClusterConfig, _> = toml::from_str(&toml);
    assert!(
        result.is_err(),
        "[backup] without [backup.s3] must fail TOML parsing"
    );
}

#[test]
fn backup_config_empty_bucket_fails_validation() {
    let toml = format!(
        "{}\n{}",
        base_config_toml(),
        r#"
[backup]
enabled = true

[backup.s3]
bucket = ""

[[backup.schedule]]
frequency = "daily"
retain    = 7
"#
    );
    let cfg: pgcluster::config::PgClusterConfig = toml::from_str(&toml).unwrap();
    let err = pgcluster::config::validate::validate(&cfg).unwrap_err();
    assert!(
        err.to_string().contains("bucket"),
        "error should mention 'bucket': {err}"
    );
}

#[test]
fn backup_config_empty_schedule_fails_validation() {
    // `schedule = []` parses successfully (empty vec) but must fail validation.
    let toml = format!(
        "{}\n{}",
        base_config_toml(),
        r#"
[backup]
enabled   = true
schedule  = []

[backup.s3]
bucket = "my-backups"
"#
    );
    let cfg: pgcluster::config::PgClusterConfig = toml::from_str(&toml).unwrap();
    let err = pgcluster::config::validate::validate(&cfg).unwrap_err();
    assert!(
        err.to_string().contains("schedule"),
        "error should mention 'schedule': {err}"
    );
}

#[test]
fn backup_config_retain_zero_fails_validation() {
    let toml = format!(
        "{}\n{}",
        base_config_toml(),
        r#"
[backup]
enabled = true

[backup.s3]
bucket = "my-backups"

[[backup.schedule]]
frequency = "daily"
retain    = 0
"#
    );
    let cfg: pgcluster::config::PgClusterConfig = toml::from_str(&toml).unwrap();
    let err = pgcluster::config::validate::validate(&cfg).unwrap_err();
    assert!(
        err.to_string().contains("retain"),
        "error should mention 'retain': {err}"
    );
}

#[test]
fn no_backup_section_is_valid() {
    // Omitting [backup] entirely must not affect validation.
    let cfg: pgcluster::config::PgClusterConfig = toml::from_str(base_config_toml()).unwrap();
    assert!(cfg.backup.is_none());
    assert!(pgcluster::config::validate::validate(&cfg).is_ok());
}

/// BM-1: Spawn the server binary with a valid config, let it start, then kill it.
/// Requires the binary to be built and examples/cluster.toml to exist.
#[tokio::test]
#[ignore = "requires built binary and a 3-node cluster environment (run via make test-integ)"]
async fn server_starts_with_valid_config_and_exits_cleanly() {
    use std::time::Duration;
    use tokio::time::sleep;

    let binary = env!("CARGO_BIN_EXE_pgcluster");
    let config = concat!(env!("CARGO_MANIFEST_DIR"), "/../examples/cluster.toml");

    let mut child = tokio::process::Command::new(binary)
        .args(["server", "--config", config])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("failed to spawn pgcluster");

    // Give it 500ms to start up
    sleep(Duration::from_millis(500)).await;

    // Ask the process to terminate (SIGKILL is fine for this test)
    let _ = child.kill().await;

    let out = child.wait_with_output().await.expect("wait");
    let stderr = String::from_utf8_lossy(&out.stderr);
    // Server should either exit 0 (clean SIGTERM) or have logged "pgcluster starting"
    assert!(
        stderr.contains("pgcluster") || out.status.success(),
        "expected server startup output in stderr, got: {stderr}"
    );
}
