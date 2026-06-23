use crate::raft::topology::ClusterTopology;

/// Pick the best failover candidate from the current topology.
///
/// Returns the node ID of the healthy replica with the highest flush LSN
/// (breaking ties by node priority), or `None` if no eligible replica exists.
pub fn pick_candidate(topology: &ClusterTopology, failed_node_id: &str) -> Option<String> {
    topology.best_failover_candidate(failed_node_id)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::topology::{ClusterTopology, NodeConfig, NodeRole};

    fn make_config(id: &str) -> NodeConfig {
        NodeConfig {
            node_id: id.into(),
            agent_addr: format!("127.0.0.1:700{}", id.chars().last().unwrap_or('0')),
            postgres_addr: format!("127.0.0.1:543{}", id.chars().last().unwrap_or('0')),
            priority: 100,
            tags: Default::default(),
        }
    }

    #[test]
    fn returns_none_when_no_replicas() {
        let mut t = ClusterTopology::default();
        t.node_roles.insert("pg1".into(), NodeRole::Primary);
        t.node_configs.insert("pg1".into(), make_config("pg1"));
        assert_eq!(pick_candidate(&t, "pg1"), None);
    }

    #[test]
    fn returns_highest_lsn_replica() {
        let mut t = ClusterTopology::default();
        t.node_roles.insert("pg1".into(), NodeRole::Primary);
        t.node_roles.insert("pg2".into(), NodeRole::Replica);
        t.node_roles.insert("pg3".into(), NodeRole::Replica);
        t.last_flush_lsns.insert("pg2".into(), 100);
        t.last_flush_lsns.insert("pg3".into(), 200);
        for id in ["pg1", "pg2", "pg3"] {
            t.node_configs.insert(id.into(), make_config(id));
        }
        assert_eq!(pick_candidate(&t, "pg1").as_deref(), Some("pg3"));
    }

    #[test]
    fn excludes_failed_node_from_candidates() {
        let mut t = ClusterTopology::default();
        t.node_roles.insert("pg1".into(), NodeRole::Primary);
        t.node_roles.insert("pg2".into(), NodeRole::Replica);
        t.last_flush_lsns.insert("pg2".into(), 500);
        for id in ["pg1", "pg2"] {
            t.node_configs.insert(id.into(), make_config(id));
        }
        let candidate = pick_candidate(&t, "pg1");
        assert_eq!(candidate.as_deref(), Some("pg2"));
    }

    #[test]
    fn highest_lsn_wins() {
        let mut t = ClusterTopology::default();
        for id in ["pg1", "pg2", "pg3"] {
            t.node_configs.insert(id.into(), make_config(id));
        }
        t.node_roles.insert("pg1".into(), NodeRole::Primary);
        t.node_roles.insert("pg2".into(), NodeRole::Replica);
        t.node_roles.insert("pg3".into(), NodeRole::Replica);
        t.last_flush_lsns.insert("pg2".into(), 50);
        t.last_flush_lsns.insert("pg3".into(), 500);
        assert_eq!(pick_candidate(&t, "pg1").as_deref(), Some("pg3"));
    }

    #[test]
    fn tied_lsn_priority_wins() {
        let mut t = ClusterTopology::default();
        t.node_roles.insert("pg1".into(), NodeRole::Primary);
        t.node_roles.insert("pg2".into(), NodeRole::Replica);
        t.node_roles.insert("pg3".into(), NodeRole::Replica);
        t.last_flush_lsns.insert("pg2".into(), 100);
        t.last_flush_lsns.insert("pg3".into(), 100); // tied
        t.node_configs.insert("pg1".into(), NodeConfig {
            node_id: "pg1".into(), agent_addr: "x".into(), postgres_addr: "y".into(),
            priority: 100, tags: Default::default(),
        });
        t.node_configs.insert("pg2".into(), NodeConfig {
            node_id: "pg2".into(), agent_addr: "x".into(), postgres_addr: "y".into(),
            priority: 80, tags: Default::default(),
        });
        t.node_configs.insert("pg3".into(), NodeConfig {
            node_id: "pg3".into(), agent_addr: "x".into(), postgres_addr: "y".into(),
            priority: 120, tags: Default::default(), // higher priority wins tie
        });
        assert_eq!(pick_candidate(&t, "pg1").as_deref(), Some("pg3"));
    }

    #[test]
    fn unhealthy_replica_excluded() {
        let mut t = ClusterTopology::default();
        for id in ["pg1", "pg2", "pg3"] {
            t.node_configs.insert(id.into(), make_config(id));
        }
        t.node_roles.insert("pg1".into(), NodeRole::Primary);
        t.node_roles.insert("pg2".into(), NodeRole::Offline); // unhealthy
        t.node_roles.insert("pg3".into(), NodeRole::Replica);
        t.last_flush_lsns.insert("pg3".into(), 100);
        // pg2 is offline so only pg3 is eligible
        assert_eq!(pick_candidate(&t, "pg1").as_deref(), Some("pg3"));
    }

    #[test]
    fn single_replica_is_selected() {
        let mut t = ClusterTopology::default();
        t.node_configs.insert("pg1".into(), make_config("pg1"));
        t.node_configs.insert("pg2".into(), make_config("pg2"));
        t.node_roles.insert("pg1".into(), NodeRole::Primary);
        t.node_roles.insert("pg2".into(), NodeRole::Replica);
        t.last_flush_lsns.insert("pg2".into(), 1);
        assert_eq!(pick_candidate(&t, "pg1").as_deref(), Some("pg2"));
    }

    #[test]
    fn no_candidates_returns_empty() {
        let mut t = ClusterTopology::default();
        t.node_configs.insert("pg1".into(), make_config("pg1"));
        t.node_roles.insert("pg1".into(), NodeRole::Primary);
        assert_eq!(pick_candidate(&t, "pg1"), None);
    }
}
