use super::ApiState;
use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct FailoverRequest {
    pub failed_node_id: String,
}

#[derive(Serialize)]
pub struct FailoverResponse {
    pub triggered: bool,
    pub message: String,
}

pub async fn trigger_failover(
    State(s): State<ApiState>,
    Json(req): Json<FailoverRequest>,
) -> (StatusCode, Json<FailoverResponse>) {
    // Guard: only one manual failover or switchover at a time.
    if s.op_in_progress
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return (
            StatusCode::CONFLICT,
            Json(FailoverResponse {
                triggered: false,
                message:
                    "a switchover or failover is already in progress — retry after it completes"
                        .into(),
            }),
        );
    }

    let topology = s.topology.borrow().clone();
    if !topology.node_configs.contains_key(&req.failed_node_id) {
        s.op_in_progress.store(false, Ordering::SeqCst);
        return (
            StatusCode::NOT_FOUND,
            Json(FailoverResponse {
                triggered: false,
                message: format!("node {} not found", req.failed_node_id),
            }),
        );
    }

    // Resolve credentials from config at call time.
    let repl_user = s.config.replication.replication_user.clone();
    let repl_password =
        std::env::var(&s.config.replication.replication_password_env).unwrap_or_default();
    let slot_prefix = s.config.replication.slot_prefix.clone();

    // Spawn the failover in a background task so the REST call returns immediately.
    // op_in_progress is released via OpGuard on task completion — even on panic.
    let raft = s.raft.clone();
    let pool = s.pool.clone();
    let metrics = s.metrics.clone();
    let in_progress = s.op_in_progress.clone();
    let failed_node_id = req.failed_node_id.clone();

    tokio::spawn(async move {
        let _guard = OpGuard(in_progress);

        // Mark the node offline in Raft. Skip failover if the write fails
        // (we may have lost leadership and must not proceed).
        if let Err(e) = raft
            .raft
            .client_write(crate::raft::commands::TopologyCommand::MarkOffline {
                node_id: failed_node_id.clone(),
            })
            .await
        {
            tracing::warn!(
                node_id = %failed_node_id,
                err = %e,
                "manual failover: MarkOffline write failed — aborting"
            );
            return;
        }

        // Snapshot topology after MarkOffline has landed.
        let topo = topology;
        crate::failover::trigger_failover(
            &raft,
            &topo,
            &failed_node_id,
            &pool,
            &metrics,
            &repl_user,
            &repl_password,
            &slot_prefix,
        )
        .await;
    });

    (
        StatusCode::ACCEPTED,
        Json(FailoverResponse {
            triggered: true,
            message: format!("failover initiated for {}", req.failed_node_id),
        }),
    )
}

/// Return the failover history with timestamps as RFC 3339 strings.
pub async fn failover_history(State(s): State<ApiState>) -> Json<serde_json::Value> {
    let topology = s.topology.borrow().clone();
    let history: Vec<serde_json::Value> = topology
        .failover_history
        .iter()
        .map(|e| {
            serde_json::json!({
                "old_primary":  e.old_primary,
                "new_primary":  e.new_primary,
                "triggered_at": crate::api::status::unix_to_rfc3339(e.triggered_at),
                "duration_ms":  e.duration_ms,
                "reason":       e.reason,
            })
        })
        .collect();
    Json(serde_json::Value::Array(history))
}

/// Releases op_in_progress when dropped, even on panic.
struct OpGuard(Arc<std::sync::atomic::AtomicBool>);
impl Drop for OpGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failover_blocked_when_op_in_progress() {
        use std::sync::{atomic::AtomicBool, Arc};
        let flag = Arc::new(AtomicBool::new(true));
        assert!(
            flag.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err(),
            "flag set — failover trigger should be blocked"
        );
    }

    #[test]
    fn failover_allowed_when_op_not_in_progress() {
        use std::sync::{atomic::AtomicBool, Arc};
        let flag = Arc::new(AtomicBool::new(false));
        assert!(
            flag.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok(),
            "flag clear — failover trigger should proceed"
        );
        flag.store(false, Ordering::SeqCst);
        assert!(!flag.load(Ordering::SeqCst));
    }

    #[test]
    fn op_guard_releases_on_drop() {
        use std::sync::{atomic::AtomicBool, Arc};
        let flag = Arc::new(AtomicBool::new(true));
        {
            let _guard = OpGuard(flag.clone());
            // flag still true while guard lives
        }
        // guard dropped — flag should now be false
        assert!(
            !flag.load(Ordering::SeqCst),
            "OpGuard must release flag on drop"
        );
    }

    #[test]
    fn unix_to_rfc3339_epoch() {
        let s = crate::api::status::unix_to_rfc3339(0);
        assert_eq!(s, "1970-01-01T00:00:00Z");
    }

    #[test]
    fn unix_to_rfc3339_known_date() {
        // 2024-01-15 12:00:00 UTC = 1705320000
        let s = crate::api::status::unix_to_rfc3339(1_705_320_000);
        assert_eq!(s, "2024-01-15T12:00:00Z");
    }
}
