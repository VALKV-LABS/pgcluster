pub mod failure_state;
pub mod health_check;

use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

use crate::agent_clients::AgentClientPool;
use crate::config::FailoverConfig;
use crate::metrics_registry::Metrics;
use crate::raft::commands::TopologyCommand;
use crate::raft::topology::{ClusterTopology, NodeRole};
use crate::raft::{RaftNode, TopologyWatch};
use failure_state::FailureTracker;
use health_check::check_node;

// ── NodeMonitor ───────────────────────────────────────────────────────────────

/// Polls every vk-agent on the Raft leader and triggers failover when the
/// primary misses enough consecutive health checks.
pub struct NodeMonitor {
    raft: Arc<RaftNode>,
    topology_rx: TopologyWatch,
    pool: Arc<AgentClientPool>,
    config: FailoverConfig,
    metrics: Arc<Metrics>,
}

impl NodeMonitor {
    pub fn new(
        raft: Arc<RaftNode>,
        topology_rx: TopologyWatch,
        pool: Arc<AgentClientPool>,
        config: FailoverConfig,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            raft,
            topology_rx,
            pool,
            config,
            metrics,
        }
    }

    /// Run the monitor loop.  Spawn this as a background task.
    ///
    /// The loop:
    /// 1. Skip if this pgcluster instance is not the Raft leader.
    /// 2. Poll every agent in the cluster.
    /// 3. Propose `UpdateFlushLsn` for nodes that answer OK.
    /// 4. If the primary misses `failures_before_failover` consecutive checks,
    ///    mark it offline and call `failover::trigger_failover`.
    pub async fn run(&mut self) {
        let mut tracker = FailureTracker::default();
        let interval = Duration::from_millis(self.config.health_check_interval_ms);
        let mut tick = time::interval(interval);

        info!("node monitor started");

        loop {
            tick.tick().await;

            // Only the Raft leader should drive health checks.
            let leader_opt = self.raft.raft.current_leader().await;
            let Some(leader_id) = leader_opt else {
                debug!("no Raft leader elected yet, skipping health check");
                continue;
            };

            let my_id = self.raft.raft.metrics().borrow().id;
            let is_leader = leader_id == my_id;
            self.metrics.raft_leader.set(if is_leader { 1 } else { 0 });
            if !is_leader {
                continue;
            }

            let topology = self.topology_rx.current();
            self.poll_all_nodes(&*topology, &mut tracker).await;

            // Update topology_version metric
            self.metrics.topology_version.set(topology.version as i64);
        }
    }

    async fn poll_all_nodes(&self, topology: &ClusterTopology, tracker: &mut FailureTracker) {
        let primary_lsn = topology
            .last_flush_lsns
            .get(&topology.primary_node_id)
            .copied()
            .unwrap_or(0);

        for (node_id, cfg) in &topology.node_configs {
            let result = check_node(node_id, &cfg.agent_addr, &self.pool).await;

            if result.reachable && result.pg_running {
                tracker.record_success(node_id);

                // Update replication lag metric for replicas
                if node_id != &topology.primary_node_id {
                    let lag = primary_lsn.saturating_sub(result.flush_lsn);
                    self.metrics
                        .replica_lag_bytes
                        .with_label_values(&[node_id])
                        .set(lag as f64);
                }

                // Propose an LSN update so the topology stays current.
                let cmd = TopologyCommand::UpdateFlushLsn {
                    node_id: node_id.clone(),
                    flush_lsn: result.flush_lsn,
                    replay_lsn: result.replay_lsn,
                };
                if let Err(e) = self.raft.raft.client_write(cmd).await {
                    warn!(node_id, err = %e, "failed to propose LSN update");
                }
            } else {
                tracker.record_failure(node_id);
                self.metrics
                    .health_check_failures
                    .with_label_values(&[node_id])
                    .inc();

                let threshold = self.config.health_check_failures_before_failover;
                if tracker.is_failed(node_id, threshold) {
                    let is_primary = topology.node_roles.get(node_id) == Some(&NodeRole::Primary);

                    // Mark the node offline in Raft regardless of role.
                    let _ = self
                        .raft
                        .raft
                        .client_write(TopologyCommand::MarkOffline {
                            node_id: node_id.clone(),
                        })
                        .await;

                    if is_primary {
                        warn!(
                            node_id,
                            threshold, "primary has failed health checks, triggering failover"
                        );
                        crate::failover::trigger_failover(
                            &self.raft,
                            topology,
                            node_id,
                            &self.pool,
                            &self.metrics,
                        )
                        .await;
                        // Reset tracker so we don't re-trigger on the same node.
                        tracker.record_success(node_id);
                    } else {
                        error!(
                            node_id,
                            threshold, "replica has failed health checks, marked offline"
                        );
                    }
                }
            }
        }
    }
}
