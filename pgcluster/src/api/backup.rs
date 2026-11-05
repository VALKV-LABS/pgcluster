use super::restart::RestartStatus;
use super::ApiState;
use crate::agent_clients::agent_proto::RestoreRequest as AgentRestoreRequest;
use crate::backup::{run_backup, walg};
use crate::config::BackupConfig;
use crate::raft::commands::TopologyCommand;
use crate::raft::topology::BackupManifest;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};

type ApiError = (StatusCode, String);

fn enabled(s: &ApiState) -> Result<BackupConfig, ApiError> {
    s.runtime
        .backup()
        .ok_or_else(|| (StatusCode::NOT_FOUND, "backup not configured".to_string()))
}

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
    pub message: String,
}

/// Start a backup. It appears in the list when it completes.
pub async fn trigger_backup(
    State(s): State<ApiState>,
    Json(req): Json<TriggerBackupRequest>,
) -> Result<(StatusCode, Json<TriggerBackupResponse>), ApiError> {
    let config = enabled(&s)?;
    let frequency = req.frequency.unwrap_or_else(|| "manual".to_string());
    let label = req
        .label
        .unwrap_or_else(|| format!("manual-{}", chrono::Utc::now().format("%Y-%m-%dT%H%M%S")));
    let message = format!("backup started: label={label}");

    let raft = s.raft.clone();
    let topology = s.topology.current();
    let pool = s.pool.clone();
    let runtime = s.runtime.clone();
    tokio::spawn(async move {
        match run_backup(&raft, &topology, &pool, &config, &frequency, &label).await {
            Ok(manifest) => {
                runtime.record_backup(None);
                tracing::info!(backup_id = manifest.backup_id, "manual backup complete")
            }
            Err(e) => {
                tracing::error!(err = %e, "manual backup failed");
                runtime.record_backup(Some((label, format!("{e:#}"))));
            }
        }
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(TriggerBackupResponse { message }),
    ))
}

/// How the last backup this instance ran went.
#[derive(Serialize)]
pub struct BackupStatusResponse {
    /// The label of the backup that failed, if the last one did.
    pub failed_label: Option<String>,
    pub error: Option<String>,
}

pub async fn backup_status(State(s): State<ApiState>) -> Json<BackupStatusResponse> {
    let (failed_label, error) = s.runtime.last_backup_failure().unzip();
    Json(BackupStatusResponse {
        failed_label,
        error,
    })
}

// ── Delete ─────────────────────────────────────────────────────────────────────

pub async fn delete_backup(
    State(s): State<ApiState>,
    Path(backup_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    enabled(&s)?;
    let topology = s.topology.current();
    if !topology.backups.iter().any(|b| b.backup_id == backup_id) {
        return Err((
            StatusCode::NOT_FOUND,
            format!("backup {backup_id} not found"),
        ));
    }

    let internal = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"));
    let (node, addr) = primary_agent(&s)?;
    let mut client = s
        .pool
        .get_or_connect(&node, &addr)
        .await
        .map_err(internal)?;
    let response = client
        .backup_delete(0, &backup_id)
        .await
        .map_err(internal)?;
    if !response.success {
        return Err((StatusCode::INTERNAL_SERVER_ERROR, response.error));
    }

    s.raft
        .raft
        .client_write(TopologyCommand::RemoveBackupManifest { backup_id })
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(StatusCode::NO_CONTENT)
}

fn primary_agent(s: &ApiState) -> Result<(String, String), ApiError> {
    let topology = s.topology.current();
    let cfg = topology
        .node_configs
        .get(&topology.primary_node_id)
        .ok_or_else(|| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "the cluster has no primary".to_string(),
            )
        })?;
    Ok((topology.primary_node_id.clone(), cfg.agent_addr.clone()))
}

// ── Verify ─────────────────────────────────────────────────────────────────────

/// Check the WAL archive for gaps, from the primary. Returns the tool's report.
pub async fn verify_backups(
    State(s): State<ApiState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    enabled(&s)?;
    let internal = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"));
    let (node, addr) = primary_agent(&s)?;
    let mut client = s
        .pool
        .get_or_connect(&node, &addr)
        .await
        .map_err(internal)?;
    let response = client.backup_verify().await.map_err(internal)?;
    if !response.success {
        return Err((StatusCode::INTERNAL_SERVER_ERROR, response.error));
    }
    let report = serde_json::from_str(&response.report)
        .unwrap_or(serde_json::Value::String(response.report));
    Ok(Json(report))
}

// ── Restore ────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct RestoreRequest {
    /// The agent to restore onto, which must not be a node of this cluster: the
    /// result is an independent database.
    pub agent_addr: String,
    /// Empty means the latest backup.
    #[serde(default)]
    pub backup_name: String,
    /// Recover to this time, such as `2026-11-01 12:00:00+00`. Empty means the
    /// end of the WAL archive.
    #[serde(default)]
    pub target_time: String,
    /// Discard data already on the target.
    #[serde(default)]
    pub force: bool,
}

/// Start a restore. `GET /api/backups/restore` reports how it went.
pub async fn start_restore(
    State(s): State<ApiState>,
    Json(req): Json<RestoreRequest>,
) -> Result<StatusCode, ApiError> {
    let config = enabled(&s)?;
    let topology = s.topology.current();
    if topology
        .node_configs
        .values()
        .any(|n| n.agent_addr == req.agent_addr)
    {
        return Err((
            StatusCode::CONFLICT,
            "restore targets must not be nodes of this cluster".to_string(),
        ));
    }
    {
        let mut status = s.restore_status.lock().unwrap();
        if status.running {
            return Err((
                StatusCode::CONFLICT,
                "a restore is already running".to_string(),
            ));
        }
        *status = RestartStatus {
            running: true,
            error: None,
        };
    }

    let pool = s.pool.clone();
    let status = s.restore_status.clone();
    tokio::spawn(async move {
        let request = AgentRestoreRequest {
            backup_name: req.backup_name,
            target_time: req.target_time,
            force: req.force,
            standby: None,
            settings: walg::settings(&config),
        };
        let result = async {
            let mut client = pool
                .get_or_connect(&req.agent_addr, &req.agent_addr)
                .await?;
            let response = client.restore(request).await?;
            if !response.success {
                anyhow::bail!("{}", response.error);
            }
            Ok(())
        }
        .await;
        pool.remove(&req.agent_addr);

        let error = result.err().map(|e: anyhow::Error| format!("{e:#}"));
        match &error {
            Some(e) => tracing::error!(err = %e, "restore failed"),
            None => tracing::info!("restore complete"),
        }
        *status.lock().unwrap() = RestartStatus {
            running: false,
            error,
        };
    });
    Ok(StatusCode::ACCEPTED)
}

pub async fn get_restore(State(s): State<ApiState>) -> Json<RestartStatus> {
    Json(s.restore_status.lock().unwrap().clone())
}
