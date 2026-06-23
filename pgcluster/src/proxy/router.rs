//! Routing decisions: which backend should a statement go to?
//!
//! The [`Router`] reads the current [`ClusterTopology`] from a local
//! [`TopologyWatch`] — no network hop.

use crate::config::ReadRoutingConfig;
use crate::raft::{NodeRole, TopologyWatch};

// ── Router ────────────────────────────────────────────────────────────────────

/// Decides which backend address to use for a given routing intent.
pub struct Router {
    topology: TopologyWatch,
    config: ReadRoutingConfig,
}

impl Router {
    pub fn new(watch: TopologyWatch, config: ReadRoutingConfig) -> Self {
        Self {
            topology: watch,
            config,
        }
    }

    /// Returns the Postgres address of the current primary, if known.
    pub fn primary_addr(&self) -> Option<String> {
        let topo = self.topology.current();
        topo.node_configs
            .get(&topo.primary_node_id)
            .map(|c| c.postgres_addr.clone())
    }

    /// Returns the `(node_id, postgres_addr)` of the least-lagged replica that
    /// is within the configured `max_replica_lag_bytes` threshold.
    ///
    /// Tie-breaking: replicas are sorted by `replica_lag_bytes` ascending.
    /// If lag info is missing for a replica, it is treated as zero lag.
    ///
    /// Returns `None` if no eligible replica exists.
    pub fn best_replica_addr(&self) -> Option<(String, String)> {
        let topo = self.topology.current();
        let max_lag = self.config.max_replica_lag_bytes;

        let mut candidates: Vec<(&String, &crate::raft::topology::NodeConfig, u64)> = topo
            .node_roles
            .iter()
            .filter(|(_, role)| **role == NodeRole::Replica)
            .filter_map(|(node_id, _)| {
                topo.node_configs.get(node_id).map(|cfg| {
                    let lag = topo.replica_lag_bytes.get(node_id).copied().unwrap_or(0);
                    (node_id, cfg, lag)
                })
            })
            .filter(|(_, _, lag)| *lag <= max_lag)
            .collect();

        if candidates.is_empty() {
            return None;
        }

        // Pick the replica with the least lag (smallest lag = most caught up).
        candidates.sort_by_key(|(_, _, lag)| *lag);

        let (node_id, cfg, _) = candidates.into_iter().next()?;
        Some((node_id.clone(), cfg.postgres_addr.clone()))
    }

    /// Returns the node ID of the current primary, if known.
    pub fn primary_node_id(&self) -> Option<String> {
        let topo = self.topology.current();
        if topo.primary_node_id.is_empty() {
            None
        } else {
            Some(topo.primary_node_id.clone())
        }
    }

    /// Return the best backend for a read statement.
    ///
    /// Prefers a replica; falls back to primary if no replica is eligible.
    pub fn read_backend(&self) -> Option<(String, String)> {
        if self.config.enabled {
            if let Some(r) = self.best_replica_addr() {
                return Some(r);
            }
        }
        // Fall back to primary
        let topo = self.topology.current();
        topo.node_configs
            .get(&topo.primary_node_id)
            .map(|c| (topo.primary_node_id.clone(), c.postgres_addr.clone()))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::raft::{
        topology::{ClusterTopology, NodeConfig},
        TopologyWatch,
    };

    fn make_topology(
        primary: &str,
        replicas: &[(&str, u64)], // (node_id, lag_bytes)
    ) -> ClusterTopology {
        let mut topo = ClusterTopology {
            primary_node_id: primary.to_owned(),
            ..Default::default()
        };

        // Primary
        topo.node_roles
            .insert(primary.to_owned(), NodeRole::Primary);
        topo.node_configs.insert(
            primary.to_owned(),
            NodeConfig {
                node_id: primary.to_owned(),
                agent_addr: "127.0.0.1:7000".to_string(),
                postgres_addr: "127.0.0.1:5432".to_string(),
                priority: 100,
                tags: HashMap::new(),
            },
        );

        // Replicas
        for (i, (rid, lag)) in replicas.iter().enumerate() {
            topo.node_roles.insert(rid.to_string(), NodeRole::Replica);
            topo.replica_lag_bytes.insert(rid.to_string(), *lag);
            topo.node_configs.insert(
                rid.to_string(),
                NodeConfig {
                    node_id: rid.to_string(),
                    agent_addr: format!("127.0.0.1:701{i}"),
                    postgres_addr: format!("127.0.0.1:543{}", i + 2),
                    priority: 90,
                    tags: HashMap::new(),
                },
            );
        }

        topo
    }

    fn make_router(topo: ClusterTopology, max_lag: u64) -> Router {
        let (sender, watch) = TopologyWatch::new(topo);
        let _ = sender; // keep alive
        Router::new(
            watch,
            ReadRoutingConfig {
                enabled: true,
                max_replica_lag_bytes: max_lag,
            },
        )
    }

    #[test]
    fn primary_addr_returns_current_primary() {
        let topo = make_topology("pg1", &[("pg2", 0)]);
        let router = make_router(topo, 10_000_000);

        assert_eq!(router.primary_addr().as_deref(), Some("127.0.0.1:5432"));
    }

    #[test]
    fn best_replica_excludes_lagged() {
        // pg2 has 5 MB lag (within threshold), pg3 has 20 MB lag (over threshold)
        let topo = make_topology("pg1", &[("pg2", 5_000_000), ("pg3", 20_000_000)]);
        let router = make_router(topo, 10_000_000); // 10 MB max

        let result = router.best_replica_addr();
        assert!(result.is_some());
        // Only pg2 is within threshold
        let (node_id, _) = result.unwrap();
        assert_eq!(node_id, "pg2");
    }

    #[test]
    fn best_replica_returns_none_when_all_over_lag() {
        let topo = make_topology("pg1", &[("pg2", 50_000_000), ("pg3", 60_000_000)]);
        let router = make_router(topo, 10_000_000); // 10 MB max — both over

        assert!(router.best_replica_addr().is_none());
    }

    #[test]
    fn best_replica_picks_least_lagged() {
        // pg2 = 2 MB, pg3 = 1 MB — pg3 should win
        let topo = make_topology("pg1", &[("pg2", 2_000_000), ("pg3", 1_000_000)]);
        let router = make_router(topo, 10_000_000);

        let (node_id, _) = router.best_replica_addr().unwrap();
        assert_eq!(node_id, "pg3");
    }

    #[test]
    fn read_backend_falls_back_to_primary_when_no_replicas() {
        let topo = make_topology("pg1", &[]);
        let router = make_router(topo, 10_000_000);

        let (node_id, addr) = router.read_backend().unwrap();
        assert_eq!(node_id, "pg1");
        assert_eq!(addr, "127.0.0.1:5432");
    }
}
