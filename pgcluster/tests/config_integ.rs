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

[tls]
insecure_plaintext = true

[api]
listen_addr = "0.0.0.0:8080"
api_keys    = ["test-key"]
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

[tls]
insecure_plaintext = true

[api]
listen_addr = "0.0.0.0:8080"
api_keys    = ["test-key"]
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

[tls]
insecure_plaintext = true

[api]
listen_addr = "0.0.0.0:8080"
api_keys    = ["test-key"]
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

[tls]
insecure_plaintext = true

[api]
listen_addr = "0.0.0.0:8080"
api_keys    = ["test-key"]
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

[tls]
insecure_plaintext = true

[api]
listen_addr = "0.0.0.0:8080"
api_keys    = ["test-key"]
"#
}

fn with_backup(section: &str) -> anyhow::Result<()> {
    let toml = format!("{}\n{section}", base_config_toml());
    let cfg: pgcluster::config::PgClusterConfig = toml::from_str(&toml)?;
    pgcluster::config::validate::validate(&cfg)
}

#[test]
fn backup_config_with_s3_passes() {
    assert!(with_backup("[backup]\n[backup.s3]\nbucket = \"b\"").is_ok());
}

#[test]
fn backup_config_with_posix_passes() {
    assert!(with_backup("[backup]\n[backup.posix]\npath = \"/backups\"").is_ok());
}

#[test]
fn backup_config_needs_exactly_one_store() {
    assert!(with_backup("[backup]").is_err());
    let both = "[backup]\n[backup.s3]\nbucket = \"b\"\n[backup.posix]\npath = \"/x\"";
    assert!(with_backup(both).is_err());
}

#[test]
fn backup_config_empty_bucket_fails() {
    assert!(with_backup("[backup]\n[backup.s3]\nbucket = \"\"").is_err());
}

#[test]
fn backup_config_relative_path_fails() {
    assert!(with_backup("[backup]\n[backup.posix]\npath = \"backups\"").is_err());
}

#[test]
fn backup_config_zero_retention_fails() {
    let section = "[backup]\nretain_full = 0\n[backup.posix]\npath = \"/x\"";
    assert!(with_backup(section).is_err());
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

#[test]
fn server_refuses_to_start_without_api_keys() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    let data_dir = dir.path().join("data");
    let toml = format!(
        r#"
[cluster]
name     = "test-cluster"
data_dir = "{}"

[raft]
node_id   = 1
bootstrap = true

[[raft.peers]]
id   = 1
addr = "127.0.0.1:7000"

[replication]
replication_user = "replicator"

[proxy]
listen_addr = "127.0.0.1:0"

[tls]
insecure_plaintext = true

[api]
listen_addr = "127.0.0.1:0"
"#,
        data_dir.display()
    );
    std::fs::write(&path, toml).unwrap();

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_pgcluster"))
        .args(["server", "--config", path.to_str().unwrap()])
        .output()
        .expect("failed to run pgcluster");

    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("api_keys"), "stderr: {stderr}");
    assert!(!data_dir.exists(), "must refuse before touching data_dir");
}

#[test]
fn server_refuses_to_start_without_certificates() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    let toml = format!(
        r#"
[cluster]
name     = "test-cluster"
data_dir = "{}"

[raft]
node_id   = 1
bootstrap = true

[[raft.peers]]
id   = 1
addr = "127.0.0.1:7000"

[replication]
replication_user = "replicator"

[proxy]
listen_addr = "127.0.0.1:0"

[api]
listen_addr = "127.0.0.1:0"
api_keys    = ["test-key"]
"#,
        dir.path().join("data").display()
    );
    std::fs::write(&path, toml).unwrap();

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_pgcluster"))
        .args(["server", "--config", path.to_str().unwrap()])
        .output()
        .expect("failed to run pgcluster");

    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("[tls]"), "stderr: {stderr}");
}
