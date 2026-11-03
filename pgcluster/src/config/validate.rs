use super::PgClusterConfig;
use crate::raft::topology::ReplicationMode;
use std::collections::HashSet;
use std::net::SocketAddr;

/// Below this, a single slow probe round would stop the proxy.
const MIN_QUORUM_LEASE_MS: u64 = 200;

/// Validate the parsed config and return a descriptive error if anything is wrong.
pub fn validate(cfg: &PgClusterConfig) -> anyhow::Result<()> {
    validate_cluster(cfg)?;
    validate_raft(cfg)?;
    validate_nodes(cfg)?;
    validate_addresses(cfg)?;
    validate_replication(cfg)?;
    validate_timing(cfg)?;
    validate_tls(cfg)?;
    validate_api(cfg)?;
    validate_backup(cfg)?;
    Ok(())
}

fn validate_cluster(cfg: &PgClusterConfig) -> anyhow::Result<()> {
    if cfg.cluster.name.is_empty() {
        anyhow::bail!("[cluster] name must not be empty");
    }
    if cfg.cluster.data_dir.is_empty() {
        anyhow::bail!("[cluster] data_dir must not be empty");
    }
    Ok(())
}

fn validate_raft(cfg: &PgClusterConfig) -> anyhow::Result<()> {
    if cfg.raft.peers.is_empty() {
        anyhow::bail!("[raft] peers must contain at least one entry");
    }
    // node_id must appear in peers
    let peer_ids: HashSet<u64> = cfg.raft.peers.iter().map(|p| p.id).collect();
    if !peer_ids.contains(&cfg.raft.node_id) {
        anyhow::bail!(
            "[raft] node_id {} does not appear in peers list",
            cfg.raft.node_id
        );
    }
    // Peer IDs must be unique
    if peer_ids.len() != cfg.raft.peers.len() {
        anyhow::bail!("[raft] duplicate peer IDs in peers list");
    }
    // Peer addresses must be valid host:port (hostnames allowed for Docker/k8s)
    for peer in &cfg.raft.peers {
        parse_host_port(&peer.addr).map_err(|e| {
            anyhow::anyhow!(
                "[raft] peer {} has invalid addr {:?}: {}",
                peer.id,
                peer.addr,
                e
            )
        })?;
    }
    // election_timeout_ms must be at least 2× heartbeat_interval_ms.
    // A tighter ratio means followers expire before seeing even two heartbeats,
    // causing spurious elections under normal network jitter.
    let min_election = cfg.raft.heartbeat_interval_ms * 2;
    if cfg.raft.election_timeout_ms < min_election {
        anyhow::bail!(
            "[raft] election_timeout_ms ({}) must be >= 2 × heartbeat_interval_ms ({} × 2 = {})",
            cfg.raft.election_timeout_ms,
            cfg.raft.heartbeat_interval_ms,
            min_election,
        );
    }
    Ok(())
}

fn validate_nodes(cfg: &PgClusterConfig) -> anyhow::Result<()> {
    let mut seen: HashSet<&str> = HashSet::new();
    for node in &cfg.nodes.node {
        if node.id.is_empty() {
            anyhow::bail!("[[nodes.node]] id must not be empty");
        }
        if !seen.insert(node.id.as_str()) {
            anyhow::bail!("[[nodes.node]] duplicate node id {:?}", node.id);
        }
    }
    Ok(())
}

fn validate_addresses(cfg: &PgClusterConfig) -> anyhow::Result<()> {
    // Bind addresses must be IP:port (the OS rejects hostnames on bind)
    let bind_addrs = [
        ("proxy.listen_addr", &cfg.proxy.listen_addr),
        ("proxy.admin_listen_addr", &cfg.proxy.admin_listen_addr),
        ("proxy.health_listen_addr", &cfg.proxy.health_listen_addr),
        ("metrics.listen_addr", &cfg.metrics.listen_addr),
        ("api.listen_addr", &cfg.api.listen_addr),
    ];
    for (name, addr) in &bind_addrs {
        addr.parse::<SocketAddr>()
            .map_err(|e| anyhow::anyhow!("[{}] invalid bind address {:?}: {}", name, addr, e))?;
    }

    // Connect addresses accept hostnames (Docker service names, DNS, etc.)
    for node in &cfg.nodes.node {
        parse_host_port(&node.postgres_addr).map_err(|e| {
            anyhow::anyhow!(
                "node {:?} postgres_addr {:?}: {}",
                node.id,
                node.postgres_addr,
                e
            )
        })?;
        parse_host_port(&node.agent_addr).map_err(|e| {
            anyhow::anyhow!("node {:?} agent_addr {:?}: {}", node.id, node.agent_addr, e)
        })?;
    }
    Ok(())
}

fn validate_replication(cfg: &PgClusterConfig) -> anyhow::Result<()> {
    let replication = &cfg.replication;
    if replication.replication_mode != ReplicationMode::Async && cfg.nodes.node.len() < 2 {
        anyhow::bail!("[replication] replication_mode sync and sync_strict need a standby");
    }
    if replication.sync_degrade_timeout_seconds == 0 {
        anyhow::bail!("[replication] sync_degrade_timeout_seconds must be > 0");
    }
    if replication.max_slot_wal_keep_size_mb == 0 {
        anyhow::bail!("[replication] max_slot_wal_keep_size_mb must be > 0");
    }
    Ok(())
}

/// Partition safety rests on these orderings, so a config that breaks one is
/// refused rather than left to fail during an outage.
fn validate_timing(cfg: &PgClusterConfig) -> anyhow::Result<()> {
    let failover = &cfg.failover;
    let lease = cfg.proxy.quorum_lease_ms;
    let detection = failover.health_check_interval_ms
        * u64::from(failover.health_check_failures_before_failover);

    if cfg.proxy.auth_user.is_empty() {
        anyhow::bail!("[proxy] auth_user must not be empty");
    }
    crate::proxy::allowlist::Allowlist::parse(cfg.proxy.allowed_cidrs.as_deref())
        .map_err(|e| anyhow::anyhow!("[proxy] allowed_cidrs: {e}"))?;
    if cfg.proxy.require_tls && cfg.tls.insecure_plaintext {
        anyhow::bail!("[proxy] require_tls needs certificates, not insecure_plaintext");
    }
    if lease < MIN_QUORUM_LEASE_MS {
        anyhow::bail!("[proxy] quorum_lease_ms ({lease}) must be >= {MIN_QUORUM_LEASE_MS}");
    }
    // An isolated proxy must stop before the majority can start a failover.
    if lease >= detection {
        anyhow::bail!(
            "[proxy] quorum_lease_ms ({lease}) must be < failover detection time \
             (health_check_interval_ms × health_check_failures_before_failover = {detection})"
        );
    }
    // An election, plus one heartbeat period, must not read as lost contact,
    // or a routine leader change would fence healthy nodes.
    let needed = cfg.raft.election_timeout_ms * 2 + failover.health_check_interval_ms;
    let heartbeat_timeout = failover.agent_heartbeat_timeout_seconds * 1000;
    if heartbeat_timeout <= needed {
        anyhow::bail!(
            "[failover] agent_heartbeat_timeout_seconds ({}) must be longer than \
             2 × election_timeout_ms + health_check_interval_ms ({needed} ms)",
            failover.agent_heartbeat_timeout_seconds
        );
    }
    Ok(())
}

fn validate_tls(cfg: &PgClusterConfig) -> anyhow::Result<()> {
    let tls = &cfg.tls;
    let files = [&tls.ca_cert, &tls.cert, &tls.key];
    if tls.insecure_plaintext {
        if files.iter().any(|f| f.is_some()) {
            anyhow::bail!("[tls] set either certificate files or insecure_plaintext, not both");
        }
    } else if files.iter().any(|f| f.is_none()) {
        anyhow::bail!(
            "[tls] ca_cert, cert and key are all required. \
             For local development only, set insecure_plaintext = true"
        );
    }
    Ok(())
}

fn validate_api(cfg: &PgClusterConfig) -> anyhow::Result<()> {
    if cfg.api.api_keys.iter().any(|k| k.trim().is_empty()) {
        anyhow::bail!("[api] api_keys must not contain an empty key");
    }
    if cfg.api.api_keys.is_empty() && !cfg.api.insecure_no_auth {
        anyhow::bail!(
            "[api] api_keys must contain at least one key. \
             For local development only, set insecure_no_auth = true"
        );
    }
    if !cfg.api.api_keys.is_empty() && cfg.api.insecure_no_auth {
        anyhow::bail!("[api] set either api_keys or insecure_no_auth, not both");
    }
    Ok(())
}

fn validate_backup(cfg: &PgClusterConfig) -> anyhow::Result<()> {
    let Some(backup) = &cfg.backup else {
        return Ok(());
    };
    match (&backup.s3, &backup.posix) {
        (Some(s3), None) if s3.bucket.is_empty() => {
            anyhow::bail!("[backup.s3] bucket must not be empty");
        }
        (None, Some(posix)) if !posix.path.starts_with('/') => {
            anyhow::bail!("[backup.posix] path must be absolute");
        }
        (Some(_), None) | (None, Some(_)) => {}
        _ => anyhow::bail!("[backup] set exactly one of [backup.s3] and [backup.posix]"),
    }
    if backup.interval_seconds == 0 {
        anyhow::bail!("[backup] interval_seconds must be > 0");
    }
    if backup.retain_full == 0 {
        anyhow::bail!("[backup] retain_full must be >= 1");
    }
    Ok(())
}

/// Accept any "host:port" where port is a valid u16.
/// Allows IP addresses, DNS names, and Docker service names.
fn parse_host_port(s: &str) -> Result<(), String> {
    match s.rfind(':') {
        None => Err(format!("missing port in {s:?}")),
        Some(colon) => {
            let port_str = &s[colon + 1..];
            let host = &s[..colon];
            if host.is_empty() {
                return Err(format!("missing host in {s:?}"));
            }
            port_str
                .parse::<u16>()
                .map_err(|_| format!("invalid port {port_str:?} in {s:?}"))?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;

    fn default_quorum_lease() -> u64 {
        toml::from_str::<ProxyConfig>("").unwrap().quorum_lease_ms
    }

    fn minimal() -> PgClusterConfig {
        PgClusterConfig {
            cluster: ClusterConfig {
                name: "test".into(),
                data_dir: "/tmp".into(),
                secrets_file: None,
                mode: ClusterMode::Cluster,
            },
            raft: RaftConfig {
                node_id: 1,
                peers: vec![
                    RaftPeer {
                        id: 1,
                        addr: "127.0.0.1:7000".into(),
                    },
                    RaftPeer {
                        id: 2,
                        addr: "127.0.0.1:7001".into(),
                    },
                    RaftPeer {
                        id: 3,
                        addr: "127.0.0.1:7002".into(),
                    },
                ],
                heartbeat_interval_ms: 150,
                election_timeout_ms: 500,
                bootstrap: true,
            },
            nodes: NodesConfig {
                node: vec![NodeConfig {
                    id: "pg1".into(),
                    agent_addr: "127.0.0.1:7010".into(),
                    postgres_addr: "127.0.0.1:5432".into(),
                    priority: 100,
                    tags: Default::default(),
                }],
            },
            replication: ReplicationConfig {
                replication_user: "replicator".into(),
                replication_password_env: "PG_REPLICATION_PASSWORD".into(),
                slot_prefix: "pgcluster_".into(),
                replication_mode: Default::default(),
                sync_degrade_timeout_seconds: 30,
                max_slot_wal_keep_size_mb: 1024,
            },
            proxy: ProxyConfig {
                listen_addr: "0.0.0.0:5432".into(),
                admin_listen_addr: "0.0.0.0:5433".into(),
                health_listen_addr: "0.0.0.0:8008".into(),
                pool: Default::default(),
                read_routing: Default::default(),
                auth_user: "pgcluster_auth".into(),
                auth_password_env: "PGCLUSTER_AUTH_PASSWORD".into(),
                quorum_lease_ms: 1000,
                allowed_cidrs: None,
                require_tls: false,
            },
            failover: FailoverConfig::default(),
            tls: TlsConfig {
                ca_cert: Some("/certs/ca.crt".into()),
                cert: Some("/certs/node.crt".into()),
                key: Some("/certs/node.key".into()),
                insecure_plaintext: false,
            },
            metrics: MetricsConfig::default(),
            api: ApiConfig {
                api_keys: vec!["test-key".into()],
                ..Default::default()
            },
            backup: None,
        }
    }

    #[test]
    fn valid_config_passes() {
        assert!(validate(&minimal()).is_ok());
    }

    #[test]
    fn docker_hostnames_pass_validation() {
        let mut cfg = minimal();
        cfg.nodes.node[0].postgres_addr = "postgres:5432".into();
        cfg.nodes.node[0].agent_addr = "vk-agent:7001".into();
        cfg.raft.peers[0].addr = "pgcluster-1:7000".into();
        cfg.raft.peers[1].addr = "pgcluster-2:7000".into();
        cfg.raft.peers[2].addr = "pgcluster-3:7000".into();
        assert!(
            validate(&cfg).is_ok(),
            "Docker hostnames should be valid connect addresses"
        );
    }

    #[test]
    fn empty_cluster_name_fails() {
        let mut cfg = minimal();
        cfg.cluster.name = "".into();
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn node_id_not_in_peers_fails() {
        let mut cfg = minimal();
        cfg.raft.node_id = 99;
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn duplicate_node_ids_fail() {
        let mut cfg = minimal();
        cfg.nodes.node.push(NodeConfig {
            id: "pg1".into(),
            agent_addr: "127.0.0.1:7011".into(),
            postgres_addr: "127.0.0.1:5433".into(),
            priority: 90,
            tags: Default::default(),
        });
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn invalid_proxy_addr_fails() {
        let mut cfg = minimal();
        cfg.proxy.listen_addr = "not-an-addr".into();
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn node_addr_missing_port_fails() {
        let mut cfg = minimal();
        cfg.nodes.node[0].postgres_addr = "postgres".into();
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn node_addr_invalid_port_fails() {
        let mut cfg = minimal();
        cfg.nodes.node[0].postgres_addr = "postgres:notaport".into();
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn replication_mode_parses_each_value() {
        for (value, mode) in [
            ("async", ReplicationMode::Async),
            ("sync", ReplicationMode::Sync),
            ("sync_strict", ReplicationMode::SyncStrict),
        ] {
            let toml = format!("replication_user = \"r\"\nreplication_mode = \"{value}\"");
            let parsed: ReplicationConfig = toml::from_str(&toml).unwrap();
            assert_eq!(parsed.replication_mode, mode);
        }
        let bad = "replication_user = \"r\"\nreplication_mode = \"quorum\"";
        assert!(toml::from_str::<ReplicationConfig>(bad).is_err());
    }

    #[test]
    fn synchronous_modes_need_a_standby() {
        let mut cfg = minimal();
        cfg.replication.replication_mode = ReplicationMode::Sync;
        assert!(validate(&cfg).is_err());

        cfg.nodes.node.push(NodeConfig {
            id: "pg2".into(),
            agent_addr: "127.0.0.1:7011".into(),
            postgres_addr: "127.0.0.1:5433".into(),
            priority: 90,
            tags: Default::default(),
        });
        assert!(validate(&cfg).is_ok());
    }

    #[test]
    fn zero_replication_settings_fail() {
        let mut cfg = minimal();
        cfg.replication.sync_degrade_timeout_seconds = 0;
        assert!(validate(&cfg).is_err());

        let mut cfg = minimal();
        cfg.replication.max_slot_wal_keep_size_mb = 0;
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn quorum_lease_must_expire_before_failover_detection() {
        let mut cfg = minimal();
        cfg.failover.health_check_interval_ms = 500;
        cfg.failover.health_check_failures_before_failover = 3;
        cfg.proxy.quorum_lease_ms = 1500;
        let err = validate(&cfg).unwrap_err();
        assert!(err.to_string().contains("quorum_lease_ms"), "{err}");

        cfg.proxy.quorum_lease_ms = 1499;
        assert!(validate(&cfg).is_ok());
    }

    #[test]
    fn quorum_lease_must_not_be_too_short() {
        let mut cfg = minimal();
        cfg.proxy.quorum_lease_ms = 50;
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn agent_heartbeat_timeout_must_outlast_an_election() {
        let mut cfg = minimal();
        cfg.raft.election_timeout_ms = 5000;
        cfg.failover.agent_heartbeat_timeout_seconds = 10;
        let err = validate(&cfg).unwrap_err();
        assert!(
            err.to_string().contains("agent_heartbeat_timeout_seconds"),
            "{err}"
        );
    }

    #[test]
    fn default_timing_is_valid() {
        let cfg = minimal();
        assert_eq!(cfg.proxy.quorum_lease_ms, default_quorum_lease());
        assert!(validate(&cfg).is_ok());
    }

    #[test]
    fn missing_certificates_fail() {
        for clear in [
            |t: &mut TlsConfig| t.ca_cert = None,
            |t: &mut TlsConfig| t.cert = None,
            |t: &mut TlsConfig| t.key = None,
        ] {
            let mut cfg = minimal();
            clear(&mut cfg.tls);
            let err = validate(&cfg).unwrap_err();
            assert!(err.to_string().contains("[tls]"), "{err}");
        }
    }

    #[test]
    fn development_flag_allows_missing_certificates() {
        let mut cfg = minimal();
        cfg.tls = TlsConfig {
            insecure_plaintext: true,
            ..Default::default()
        };
        assert!(validate(&cfg).is_ok());
    }

    #[test]
    fn development_flag_with_certificates_fails() {
        let mut cfg = minimal();
        cfg.tls.insecure_plaintext = true;
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn missing_api_keys_fail() {
        let mut cfg = minimal();
        cfg.api.api_keys.clear();
        let err = validate(&cfg).unwrap_err();
        assert!(err.to_string().contains("api_keys"), "{err}");
    }

    #[test]
    fn empty_api_key_fails() {
        let mut cfg = minimal();
        cfg.api.api_keys = vec!["  ".into()];
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn development_flag_allows_missing_api_keys() {
        let mut cfg = minimal();
        cfg.api.api_keys.clear();
        cfg.api.insecure_no_auth = true;
        assert!(validate(&cfg).is_ok());
    }

    #[test]
    fn development_flag_with_api_keys_fails() {
        let mut cfg = minimal();
        cfg.api.insecure_no_auth = true;
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn election_timeout_too_small_fails() {
        let mut cfg = minimal();
        cfg.raft.heartbeat_interval_ms = 150;
        cfg.raft.election_timeout_ms = 200; // < 2×150 = 300
        let err = validate(&cfg).unwrap_err();
        assert!(
            err.to_string().contains("election_timeout_ms"),
            "error should mention election_timeout_ms: {err}"
        );
    }

    #[test]
    fn election_timeout_exactly_2x_passes() {
        let mut cfg = minimal();
        cfg.raft.heartbeat_interval_ms = 150;
        cfg.raft.election_timeout_ms = 300; // exactly 2×150
        assert!(validate(&cfg).is_ok(), "2× ratio should pass validation");
    }

    #[test]
    fn election_timeout_well_above_2x_passes() {
        let mut cfg = minimal();
        cfg.raft.heartbeat_interval_ms = 150;
        cfg.raft.election_timeout_ms = 750; // 5× — well above minimum
        assert!(validate(&cfg).is_ok());
    }
}
