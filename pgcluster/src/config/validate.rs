use super::PgClusterConfig;
use std::collections::HashSet;
use std::net::SocketAddr;

/// Validate the parsed config and return a descriptive error if anything is wrong.
pub fn validate(cfg: &PgClusterConfig) -> anyhow::Result<()> {
    validate_cluster(cfg)?;
    validate_raft(cfg)?;
    validate_nodes(cfg)?;
    validate_addresses(cfg)?;
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

fn validate_backup(cfg: &PgClusterConfig) -> anyhow::Result<()> {
    let Some(backup) = &cfg.backup else {
        return Ok(());
    };
    if backup.s3.bucket.is_empty() {
        anyhow::bail!("[backup.s3] bucket must not be empty");
    }
    if backup.schedule.is_empty() {
        anyhow::bail!("[backup] schedule must have at least one [[backup.schedule]] entry");
    }
    for entry in &backup.schedule {
        if entry.retain == 0 {
            anyhow::bail!(
                "[backup] schedule retain must be >= 1 (frequency: {})",
                entry.frequency
            );
        }
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

    fn minimal() -> PgClusterConfig {
        PgClusterConfig {
            cluster: ClusterConfig {
                name: "test".into(),
                data_dir: "/tmp".into(),
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
                synchronous_standby_names: "".into(),
            },
            proxy: ProxyConfig {
                listen_addr: "0.0.0.0:5432".into(),
                admin_listen_addr: "0.0.0.0:5433".into(),
                health_listen_addr: "0.0.0.0:8008".into(),
                pool: Default::default(),
                read_routing: Default::default(),
            },
            failover: FailoverConfig::default(),
            tls: TlsConfig::default(),
            metrics: MetricsConfig::default(),
            api: ApiConfig::default(),
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
