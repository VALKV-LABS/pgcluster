use super::ApiState;
use crate::raft::commands::TopologyCommand;
use crate::raft::topology::ReplicationMode;
use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicationStatus {
    pub mode: ReplicationMode,
    /// True when `sync` has fallen back to async.
    pub degraded: bool,
    /// The value the leader keeps on the primary.
    pub synchronous_standby_names: String,
    /// Standbys past `max_slot_wal_keep_size`, which need seeding again.
    pub needs_reseed: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SetModeRequest {
    pub mode: ReplicationMode,
}

pub async fn get_replication(State(s): State<ApiState>) -> Json<ReplicationStatus> {
    let t = s.topology.borrow();
    Json(ReplicationStatus {
        mode: t.replication_mode,
        degraded: t.sync_degraded,
        synchronous_standby_names: t.synchronous_standby_names(),
        needs_reseed: t.needs_reseed.iter().cloned().collect(),
    })
}

/// Change the mode online. The leader applies it to Postgres on its next tick.
pub async fn set_mode(
    State(s): State<ApiState>,
    Json(req): Json<SetModeRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    if req.mode != ReplicationMode::Async && s.topology.borrow().node_configs.len() < 2 {
        return Err((
            StatusCode::BAD_REQUEST,
            "sync and sync_strict need a standby".to_string(),
        ));
    }
    s.raft
        .raft
        .client_write(TopologyCommand::SetReplicationMode { mode: req.mode })
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_uses_the_config_spelling() {
        let req: SetModeRequest = serde_json::from_str(r#"{"mode":"sync_strict"}"#).unwrap();
        assert_eq!(req.mode, ReplicationMode::SyncStrict);
        assert!(serde_json::from_str::<SetModeRequest>(r#"{"mode":"nope"}"#).is_err());
    }
}
