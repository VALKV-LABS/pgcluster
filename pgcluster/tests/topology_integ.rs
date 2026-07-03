/// Unit-level integration tests for ClusterTopology logic.
/// No Docker required.
use pgcluster::raft::topology::{ClusterTopology, NodeConfig, NodeRole};

fn make_node(id: &str, priority: u32) -> NodeConfig {
    NodeConfig {
        node_id: id.into(),
        agent_addr: format!("127.0.0.1:700{}", id.chars().last().unwrap_or('0')),
        postgres_addr: format!("127.0.0.1:543{}", id.chars().last().unwrap_or('0')),
        priority,
        tags: Default::default(),
    }
}

#[test]
fn topology_default_is_empty() {
    let t = ClusterTopology::default();
    assert!(t.primary_node_id.is_empty());
    assert!(t.node_roles.is_empty());
    assert_eq!(t.version, 0);
}

#[test]
fn best_failover_candidate_prefers_highest_lsn() {
    let mut t = ClusterTopology::default();
    for id in ["pg1", "pg2", "pg3"] {
        t.node_configs.insert(id.into(), make_node(id, 100));
    }
    t.node_roles.insert("pg1".into(), NodeRole::Primary);
    t.node_roles.insert("pg2".into(), NodeRole::Replica);
    t.node_roles.insert("pg3".into(), NodeRole::Replica);
    t.last_flush_lsns.insert("pg2".into(), 100);
    t.last_flush_lsns.insert("pg3".into(), 500); // higher

    assert_eq!(t.best_failover_candidate("pg1").as_deref(), Some("pg3"));
}

#[test]
fn best_failover_candidate_priority_breaks_tie() {
    let mut t = ClusterTopology::default();
    t.node_configs.insert("pg1".into(), make_node("pg1", 100));
    t.node_configs.insert("pg2".into(), make_node("pg2", 80));
    t.node_configs.insert("pg3".into(), make_node("pg3", 120));
    t.node_roles.insert("pg1".into(), NodeRole::Primary);
    t.node_roles.insert("pg2".into(), NodeRole::Replica);
    t.node_roles.insert("pg3".into(), NodeRole::Replica);
    t.last_flush_lsns.insert("pg2".into(), 100);
    t.last_flush_lsns.insert("pg3".into(), 100); // tied
                                                 // pg3 has priority 120 > pg2's 80

    assert_eq!(t.best_failover_candidate("pg1").as_deref(), Some("pg3"));
}

#[test]
fn replica_addrs_within_lag_excludes_lagged() {
    let mut t = ClusterTopology {
        primary_node_id: "pg1".into(),
        ..Default::default()
    };
    t.node_configs.insert("pg1".into(), make_node("pg1", 100));
    t.node_configs.insert("pg2".into(), make_node("pg2", 90));
    t.node_configs.insert("pg3".into(), make_node("pg3", 80));
    t.node_roles.insert("pg1".into(), NodeRole::Primary);
    t.node_roles.insert("pg2".into(), NodeRole::Replica);
    t.node_roles.insert("pg3".into(), NodeRole::Replica);
    t.last_flush_lsns.insert("pg1".into(), 10_000_000);
    t.last_flush_lsns.insert("pg2".into(), 9_999_990); // 10 bytes lag
    t.last_flush_lsns.insert("pg3".into(), 0); // way behind

    let max_lag: u64 = 1024 * 1024; // 1 MiB
    let addrs = t.replica_addrs_within_lag(max_lag);
    assert_eq!(addrs.len(), 1);
    assert!(addrs[0].contains("543")); // pg2's postgres_addr
}

#[test]
fn lsn_parse_format_roundtrip() {
    use pgcluster::raft::topology::{format_lsn, parse_lsn};
    // Canonical Postgres pg_lsn format is %X/%X — no zero-padding on the lo segment.
    // "1/00000001" parses fine but formats back as "1/1".
    let cases = ["0/0", "0/1A2B3C", "A/DEADBEEF", "1/1"];
    for s in cases {
        let v = parse_lsn(s).expect(s);
        assert_eq!(format_lsn(v), s, "roundtrip failed for {s}");
    }
}

#[test]
fn failover_history_capped_at_10() {
    use pgcluster::raft::topology::FailoverEvent;
    let mut t = ClusterTopology::default();
    for i in 0u32..15 {
        t.record_failover(FailoverEvent {
            old_primary: "pg1".into(),
            new_primary: format!("pg{}", i + 2),
            triggered_at: i as i64,
            duration_ms: 100,
            reason: "test".into(),
        });
    }
    assert_eq!(t.failover_history.len(), 10);
    // Most recent should be the last inserted
    assert_eq!(t.failover_history.back().unwrap().triggered_at, 14);
}
