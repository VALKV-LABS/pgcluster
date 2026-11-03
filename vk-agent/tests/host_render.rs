//! Rendered configs must pass the same validation the real processes run.

use vk_agent::config::AgentConfig;
use vk_agent::host::model::{
    BackupPolicy, BackupTarget, Instance, InstanceSecrets, Owner, Resources,
};
use vk_agent::host::render::{self, Layout};

fn instance(allowed_cidrs: &[&str], extensions: &[&str]) -> Instance {
    Instance {
        database_id: "0f3f6f3e-8f3e-4c32-b6a0-2f1d7e2f6a11".into(),
        database_name: "orders".into(),
        node: "n1".into(),
        generation: 3,
        operation_id: None,
        postgres_version: "18".into(),
        resources: Resources {
            vcpu: 2.0,
            memory_gb: 8.0,
            storage_gb: 100.0,
        },
        port_base: 20010,
        address: "10.0.0.1".into(),
        peers: vec![],
        bootstrap: "initdb".into(),
        restore: None,
        backup: BackupPolicy {
            target: BackupTarget {
                kind: "posix".into(),
                path: Some("/backups/db".into()),
                encryption_key: Some("a2V5".into()),
                ..Default::default()
            },
            retain_full: 2,
        },
        tasks: vec![],
        replication_mode: "async".into(),
        allowed_cidrs: allowed_cidrs.iter().map(|c| c.to_string()).collect(),
        extensions: extensions.iter().map(|e| e.to_string()).collect(),
        owner: Owner {
            username: "app_owner".into(),
            scram_verifier: "SCRAM-SHA-256$4096:c2FsdA==$a:b".into(),
        },
        secrets: InstanceSecrets {
            ca_cert_pem: "ca".into(),
            cert_pem: "cert".into(),
            key_pem: "key".into(),
            superuser_password: "s3cret'\"\\".into(),
            replication_password: "repl".into(),
            auth_password: "auth".into(),
            api_key: "api-key".into(),
            next: None,
        },
    }
}

fn check(instance: &Instance) -> pgcluster::config::PgClusterConfig {
    let dir = tempfile::tempdir().unwrap();
    let layout = Layout::new(dir.path().to_str().unwrap(), &instance.id());
    std::fs::create_dir_all(&layout.dir).unwrap();

    let pgcluster_path = layout.pgcluster_config();
    std::fs::write(&pgcluster_path, render::pgcluster_toml(instance, &layout)).unwrap();
    let parsed = pgcluster::config::PgClusterConfig::from_file(pgcluster_path.to_str().unwrap())
        .expect("rendered pgcluster config must load");
    pgcluster::config::validate::validate(&parsed).expect("and pass validation");

    let agent_path = layout.agent_config();
    let toml = render::agent_toml(instance, &layout, "/usr/lib/postgresql/18/bin");
    std::fs::write(&agent_path, toml).unwrap();
    let agent = AgentConfig::from_file(agent_path.to_str().unwrap())
        .expect("rendered vk-agent config must load");
    assert_eq!(
        agent.postgres_password.as_deref(),
        Some(instance.secrets.superuser_password.as_str()),
        "a password with quotes must survive rendering"
    );
    assert_eq!(agent.pg_ctl_path, "/usr/lib/postgresql/18/bin/pg_ctl");
    parsed
}

#[test]
fn a_rendered_instance_passes_validation() {
    let parsed = check(&instance(&["198.51.100.0/24"], &["vector"]));
    assert!(parsed.proxy.require_tls);
    assert_eq!(parsed.proxy.listen_addr, "0.0.0.0:20010");
    assert_eq!(parsed.nodes.node[0].postgres_addr, "127.0.0.1:20012");
    assert_eq!(
        parsed.proxy.allowed_cidrs,
        Some(vec!["198.51.100.0/24".to_string()])
    );
}

#[test]
fn an_empty_allowlist_stays_a_list_and_refuses_everyone() {
    let parsed = check(&instance(&[], &[]));
    assert_eq!(parsed.proxy.allowed_cidrs, Some(vec![]));
}

#[test]
fn every_extension_renders() {
    check(&instance(
        &["0.0.0.0/0", "::/0"],
        &[
            "uuid-ossp",
            "pgcrypto",
            "pg_trgm",
            "unaccent",
            "vector",
            "pg_stat_statements",
            "pg_tle",
            "postgis",
            "pg_cron",
        ],
    ));
}
