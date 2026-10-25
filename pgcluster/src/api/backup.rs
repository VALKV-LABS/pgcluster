use super::ApiState;
use crate::backup::{build_s3_store, delete_from_s3, execute_backup, select_source};
use crate::raft::commands::TopologyCommand;
use crate::raft::topology::BackupManifest;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};

// ── List ───────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct ListQuery {
    pub frequency: Option<String>,
}

#[derive(Serialize)]
pub struct BackupListResponse {
    pub backups: Vec<BackupManifest>,
}

pub async fn list_backups(
    State(s): State<ApiState>,
    Query(q): Query<ListQuery>,
) -> Json<BackupListResponse> {
    let topology = s.topology.current();
    let mut backups: Vec<BackupManifest> = topology
        .backups
        .iter()
        .filter(|b| q.frequency.as_deref().is_none_or(|f| b.frequency == f))
        .cloned()
        .collect();
    backups.sort_by_key(|m| std::cmp::Reverse(m.completed_at));
    Json(BackupListResponse { backups })
}

// ── Trigger (manual) ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct TriggerBackupRequest {
    pub label: Option<String>,
    pub frequency: Option<String>,
}

#[derive(Serialize)]
pub struct TriggerBackupResponse {
    pub backup_id: String,
    pub message: String,
}

pub async fn trigger_backup(
    State(s): State<ApiState>,
    Json(req): Json<TriggerBackupRequest>,
) -> Result<(StatusCode, Json<TriggerBackupResponse>), (StatusCode, String)> {
    let backup_cfg = s
        .config
        .backup
        .as_ref()
        .ok_or_else(|| (StatusCode::NOT_FOUND, "backup not configured".into()))?
        .clone();

    if !backup_cfg.enabled {
        return Err((StatusCode::SERVICE_UNAVAILABLE, "backup is disabled".into()));
    }

    let topology = s.topology.current();
    let Some((source_node, source_addr)) = select_source(&topology, backup_cfg.prefer_replica)
    else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "no suitable backup source node available".into(),
        ));
    };

    let backup_id = uuid::Uuid::new_v4().to_string();
    let frequency = req.frequency.unwrap_or_else(|| "manual".to_string());
    let label = req
        .label
        .unwrap_or_else(|| format!("manual-{}", &backup_id[..8]));

    let store = build_s3_store(&backup_cfg.s3)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let raft = s.raft.clone();
    let repl_user = s.config.replication.replication_user.clone();
    let repl_password =
        std::env::var(&s.config.replication.replication_password_env).unwrap_or_default();
    let bid = backup_id.clone();
    let freq = frequency.clone();
    let lbl = label.clone();
    let source_node_msg = source_node.clone();

    tokio::spawn(async move {
        match execute_backup(
            &source_node,
            &source_addr,
            &repl_user,
            &repl_password,
            &freq,
            &lbl,
            &backup_cfg,
            &store,
        )
        .await
        {
            Ok(manifest) => {
                if let Err(e) = raft
                    .raft
                    .client_write(TopologyCommand::AddBackupManifest(manifest))
                    .await
                {
                    tracing::error!(backup_id = bid, err = %e, "failed to record manual backup manifest");
                } else {
                    tracing::info!(backup_id = bid, "manual backup complete");
                }
            }
            Err(e) => {
                tracing::error!(backup_id = bid, err = %e, "manual backup failed");
            }
        }
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(TriggerBackupResponse {
            backup_id,
            message: format!(
                "backup started: frequency={}, label={}, source={}",
                frequency, label, source_node_msg
            ),
        }),
    ))
}

// ── Delete ─────────────────────────────────────────────────────────────────────

pub async fn delete_backup(
    State(s): State<ApiState>,
    Path(backup_id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let backup_cfg = s
        .config
        .backup
        .as_ref()
        .ok_or_else(|| (StatusCode::NOT_FOUND, "backup not configured".into()))?
        .clone();

    let topology = s.topology.current();
    let manifest = topology
        .backups
        .iter()
        .find(|b| b.backup_id == backup_id)
        .cloned()
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!("backup {} not found", backup_id),
            )
        })?;

    let store = build_s3_store(&backup_cfg.s3)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Delete S3 objects first, then remove from Raft.
    delete_from_s3(&store, &backup_cfg.s3.bucket, &manifest.s3_uri).await;

    s.raft
        .raft
        .client_write(TopologyCommand::RemoveBackupManifest { backup_id })
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(StatusCode::NO_CONTENT)
}
