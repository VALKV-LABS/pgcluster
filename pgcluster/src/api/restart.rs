use super::ApiState;
use crate::switchover::{rolling::rolling_restart, SwitchoverParams};
use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// The most recent rolling restart run from this instance.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RestartStatus {
    pub running: bool,
    /// Set if the last run stopped early.
    pub error: Option<String>,
}

/// Releases op_in_progress when dropped, even on panic.
struct OpGuard(Arc<AtomicBool>);
impl Drop for OpGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

pub async fn get_restart(State(s): State<ApiState>) -> Json<RestartStatus> {
    Json(s.restart_status.lock().unwrap().clone())
}

/// Start a rolling restart on this instance, which must be the Raft leader.
/// `GET /api/restart` on the same instance reports how it went.
pub async fn start_restart(State(s): State<ApiState>) -> Result<StatusCode, (StatusCode, String)> {
    let metrics = s.raft.raft.metrics().borrow().clone();
    if metrics.current_leader != Some(metrics.id) {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "this instance is not the Raft leader".to_string(),
        ));
    }
    if s.op_in_progress
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return Err((
            StatusCode::CONFLICT,
            "a switchover, failover or restart is already in progress".to_string(),
        ));
    }
    *s.restart_status.lock().unwrap() = RestartStatus {
        running: true,
        error: None,
    };

    let replication = &s.config.replication;
    let params = SwitchoverParams {
        raft: s.raft.clone(),
        topology_rx: s.topology.clone(),
        new_primary_id: String::new(),
        pool: s.pool.clone(),
        max_lag_bytes: 1024 * 1024,
        sync_timeout_secs: 60,
        replication_user: replication.replication_user.clone(),
        replication_password: std::env::var(&replication.replication_password_env)
            .unwrap_or_default(),
        proxy_drain: s.proxy_drain.clone(),
        drain_timeout: Duration::from_secs(3),
        slot_prefix: replication.slot_prefix.clone(),
    };
    let in_progress = s.op_in_progress.clone();
    let status = s.restart_status.clone();
    tokio::spawn(async move {
        let _guard = OpGuard(in_progress);
        let error = rolling_restart(params)
            .await
            .err()
            .map(|e| format!("{e:#}"));
        if let Some(error) = &error {
            tracing::error!(err = %error, "rolling restart stopped");
        }
        *status.lock().unwrap() = RestartStatus {
            running: false,
            error,
        };
    });
    Ok(StatusCode::ACCEPTED)
}
