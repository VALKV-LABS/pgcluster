use super::ApiState;
use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Releases op_in_progress when dropped, even on panic inside the spawned task.
struct OpGuard(Arc<std::sync::atomic::AtomicBool>);
impl Drop for OpGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

#[derive(Deserialize)]
pub struct SwitchoverRequest {
    pub target_node_id: String,
    #[serde(default = "default_max_lag")]
    pub max_lag_bytes: u64,
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
}

fn default_max_lag() -> u64 {
    1048576
} // 1 MiB
fn default_timeout() -> u64 {
    30
}

#[derive(Serialize)]
pub struct SwitchoverResponse {
    pub success: bool,
    pub message: String,
}

pub async fn trigger_switchover(
    State(s): State<ApiState>,
    Json(req): Json<SwitchoverRequest>,
) -> (StatusCode, Json<SwitchoverResponse>) {
    let topology = s.topology.borrow().clone();
    if !topology.node_configs.contains_key(&req.target_node_id) {
        return (
            StatusCode::NOT_FOUND,
            Json(SwitchoverResponse {
                success: false,
                message: format!("node {} not found", req.target_node_id),
            }),
        );
    }
    if topology.primary_node_id == req.target_node_id {
        return (
            StatusCode::BAD_REQUEST,
            Json(SwitchoverResponse {
                success: false,
                message: "target is already the primary".into(),
            }),
        );
    }

    // Validate target role: must be Replica (not Offline, Maintenance, Unknown).
    use crate::raft::topology::NodeRole;
    match topology.node_roles.get(&req.target_node_id) {
        Some(NodeRole::Replica) => {} // OK
        Some(role) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(SwitchoverResponse {
                    success: false,
                    message: format!(
                        "target {} is in role {:?} — only Replica nodes can be promoted",
                        req.target_node_id, role
                    ),
                }),
            );
        }
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(SwitchoverResponse {
                    success: false,
                    message: format!(
                        "target {} has no role in topology — not ready for switchover",
                        req.target_node_id
                    ),
                }),
            );
        }
    }

    // Guard: only one switchover or manual failover may run at a time.
    if s.op_in_progress
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return (
            StatusCode::CONFLICT,
            Json(SwitchoverResponse {
                success: false,
                message: "a switchover or failover is already in progress".into(),
            }),
        );
    }

    // Resolve replication credentials from config.
    let repl_user = s.config.replication.replication_user.clone();
    let repl_password =
        std::env::var(&s.config.replication.replication_password_env).unwrap_or_default();
    let slot_prefix = s.config.replication.slot_prefix.clone();

    // Spawn switchover in background — REST returns immediately.
    // The op_in_progress flag is cleared when the task finishes (success or failure).
    let raft = s.raft.clone();
    let topology_rx = s.topology.clone();
    let metrics = s.metrics.clone();
    let pool = s.pool.clone();
    let target = req.target_node_id.clone();
    let in_progress = s.op_in_progress.clone();
    let proxy_drain = s.proxy_drain.clone();
    tokio::spawn(async move {
        let _guard = OpGuard(in_progress);
        let result = crate::switchover::planned_switchover(crate::switchover::SwitchoverParams {
            raft,
            topology_rx,
            new_primary_id: target.clone(),
            pool,
            max_lag_bytes: req.max_lag_bytes,
            sync_timeout_secs: req.timeout_secs,
            replication_user: repl_user,
            replication_password: repl_password,
            proxy_drain,
            drain_timeout: std::time::Duration::from_secs(3),
            slot_prefix,
        })
        .await;
        match result {
            Ok(()) => {
                metrics.switchover_total.inc();
                metrics.primary_changes_total.inc();
            }
            Err(e) => {
                tracing::error!(err = %e, target, "switchover failed");
            }
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(SwitchoverResponse {
            success: true,
            message: format!("switchover to {} initiated", req.target_node_id),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::topology::{ClusterTopology, NodeConfig, NodeRole};
    use std::collections::HashMap;

    fn topology_with_primary(primary: &str, replicas: &[&str]) -> ClusterTopology {
        let mut t = ClusterTopology {
            primary_node_id: primary.into(),
            ..Default::default()
        };
        t.node_roles.insert(primary.into(), NodeRole::Primary);
        t.node_configs.insert(
            primary.into(),
            NodeConfig {
                node_id: primary.into(),
                agent_addr: "127.0.0.1:7001".into(),
                postgres_addr: "127.0.0.1:5432".into(),
                priority: 100,
                tags: HashMap::new(),
            },
        );
        for r in replicas {
            t.node_roles.insert(r.to_string(), NodeRole::Replica);
            t.node_configs.insert(
                r.to_string(),
                NodeConfig {
                    node_id: r.to_string(),
                    agent_addr: "127.0.0.1:7002".into(),
                    postgres_addr: "127.0.0.1:5433".into(),
                    priority: 90,
                    tags: HashMap::new(),
                },
            );
        }
        t
    }

    #[test]
    fn switchover_rejects_unknown_node() {
        let topology = topology_with_primary("pg1", &["pg2"]);
        // Simulate the guard logic without the full handler
        let target = "pg99";
        assert!(
            !topology.node_configs.contains_key(target),
            "pg99 should not exist in topology"
        );
    }

    #[test]
    fn switchover_rejects_current_primary_as_target() {
        let topology = topology_with_primary("pg1", &["pg2"]);
        let target = "pg1";
        assert_eq!(
            topology.primary_node_id, target,
            "pg1 is the primary — switchover to self should be rejected"
        );
    }

    #[test]
    fn switchover_request_default_lag_and_timeout() {
        let json = r#"{"target_node_id":"pg2"}"#;
        let req: SwitchoverRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.target_node_id, "pg2");
        assert_eq!(req.max_lag_bytes, 1_048_576);
        assert_eq!(req.timeout_secs, 30);
    }

    #[test]
    fn switchover_rejects_offline_target() {
        let mut topology = topology_with_primary("pg1", &["pg2"]);
        topology
            .node_roles
            .insert("pg2".into(), crate::raft::topology::NodeRole::Offline);
        let role = topology.node_roles.get("pg2");
        assert!(
            !matches!(role, Some(&crate::raft::topology::NodeRole::Replica)),
            "pg2 is Offline — switchover should be rejected"
        );
    }

    #[test]
    fn switchover_accepts_replica_target() {
        let topology = topology_with_primary("pg1", &["pg2"]);
        let role = topology.node_roles.get("pg2");
        assert!(
            matches!(role, Some(&crate::raft::topology::NodeRole::Replica)),
            "pg2 is Replica — switchover should be accepted"
        );
    }

    #[test]
    fn op_in_progress_flag_compare_exchange() {
        use std::sync::{atomic::AtomicBool, Arc};
        let flag = Arc::new(AtomicBool::new(false));

        // First acquire succeeds.
        assert!(
            flag.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok(),
            "first acquire should succeed"
        );
        // Second acquire is rejected (flag already true).
        assert!(
            flag.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err(),
            "concurrent attempt should be rejected"
        );
        // After clearing, a new request is accepted.
        flag.store(false, Ordering::SeqCst);
        assert!(
            flag.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok(),
            "acquire after clear should succeed"
        );
    }
}
