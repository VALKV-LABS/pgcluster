//! Backup repositories, a database's backups, and restore.

use std::time::{Duration, Instant};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::FromRow;

use super::databases::{lock, spec_of};
use super::{JsonBody, List, Page};
use crate::error::ApiError;
use crate::model::{
    check_database_name, check_hosts, CreateRepository, DatabaseSpec, RepositoryConfig,
    RestoreSource,
};
use crate::operations::{self, Kind, Operation};
use crate::reconciler::{bump_host, ONLINE_WINDOW_SECONDS};
use crate::state::AdminState;

/// A repository is checked before it is accepted, by a host, so this is how long
/// the request waits for one to answer.
const CHECK_TIMEOUT: Duration = Duration::from_secs(30);
const CHECK_POLL: Duration = Duration::from_millis(250);

#[derive(Debug, Serialize, FromRow)]
pub struct Repository {
    id: String,
    name: String,
    #[serde(rename = "type")]
    #[sqlx(rename = "type")]
    kind: String,
    config: sqlx::types::Json<RepositoryConfig>,
    status: String,
    created_at: DateTime<Utc>,
}

const REPOSITORY_COLUMNS: &str = "id, name, type, config, status, created_at";

pub async fn create_repository(
    State(state): State<AdminState>,
    JsonBody(request): JsonBody<CreateRepository>,
) -> Result<(StatusCode, Json<Repository>), ApiError> {
    let (name, kind, config, credentials) = request.into_parts()?;
    let host: Option<String> = sqlx::query_scalar(
        "SELECT id FROM hosts WHERE last_seen_at > now() - make_interval(secs => $1) \
         ORDER BY last_seen_at DESC LIMIT 1",
    )
    .bind(ONLINE_WINDOW_SECONDS as f64)
    .fetch_optional(&state.pool)
    .await?;
    let Some(host) = host else {
        return Err(ApiError::conflict(
            "no online host can check the repository",
        ));
    };

    let id = uuid::Uuid::new_v4().to_string();
    let sealed = match credentials.access_key_id {
        Some(_) => Some(
            state
                .secrets
                .seal(&format!("repository/{id}"), &credentials)?,
        ),
        None => None,
    };
    let mut tx = state.pool.begin().await?;
    let inserted = sqlx::query(
        "INSERT INTO backup_repositories (id, name, type, config, credentials, check_host_id) \
         VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (name) DO NOTHING",
    )
    .bind(&id)
    .bind(&name)
    .bind(&kind)
    .bind(sqlx::types::Json(&config))
    .bind(&sealed)
    .bind(&host)
    .execute(&mut *tx)
    .await?;
    if inserted.rows_affected() == 0 {
        return Err(ApiError::conflict(format!(
            "a repository named {name:?} exists"
        )));
    }
    bump_host(&mut tx, &host).await?;
    tx.commit().await?;

    let deadline = Instant::now() + CHECK_TIMEOUT;
    loop {
        let (status, error): (String, Option<String>) =
            sqlx::query_as("SELECT status, error FROM backup_repositories WHERE id = $1")
                .bind(&id)
                .fetch_one(&state.pool)
                .await?;
        match status.as_str() {
            "verified" => break,
            "failed" => {
                remove_unverified(&state, &id).await?;
                return Err(ApiError::invalid(format!(
                    "the repository failed its check: {}",
                    error.unwrap_or_default()
                )));
            }
            _ if Instant::now() >= deadline => {
                remove_unverified(&state, &id).await?;
                return Err(ApiError::conflict("no host answered the repository check"));
            }
            _ => tokio::time::sleep(CHECK_POLL).await,
        }
    }
    let repository = sqlx::query_as::<_, Repository>(&format!(
        "SELECT {REPOSITORY_COLUMNS} FROM backup_repositories WHERE id = $1"
    ))
    .bind(&id)
    .fetch_one(&state.pool)
    .await?;
    Ok((StatusCode::CREATED, Json(repository)))
}

async fn remove_unverified(state: &AdminState, id: &str) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM backup_repositories WHERE id = $1")
        .bind(id)
        .execute(&state.pool)
        .await?;
    Ok(())
}

pub async fn list_repositories(
    State(state): State<AdminState>,
    page: Page,
) -> Result<Json<List<Repository>>, ApiError> {
    let rows = sqlx::query_as::<_, Repository>(&format!(
        "SELECT {REPOSITORY_COLUMNS} FROM backup_repositories \
         WHERE id > $1 ORDER BY id LIMIT $2"
    ))
    .bind(page.after())
    .bind(page.fetch_limit())
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(List::from_rows(rows, &page, |r: &Repository| {
        r.id.clone()
    })))
}

pub async fn delete_repository(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let mut tx = state.pool.begin().await?;
    let found: Option<String> =
        sqlx::query_scalar("SELECT id FROM backup_repositories WHERE id = $1 FOR UPDATE")
            .bind(&id)
            .fetch_optional(&mut *tx)
            .await?;
    if found.is_none() {
        return Err(ApiError::not_found("repository"));
    }
    let users: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM databases \
         WHERE spec->'backup'->>'repository_id' = $1 AND status <> 'deleted'",
    )
    .bind(&id)
    .fetch_one(&mut *tx)
    .await?;
    if users > 0 {
        return Err(ApiError::conflict(format!(
            "{users} database(s) still use this repository"
        )));
    }
    sqlx::query("DELETE FROM backup_repositories WHERE id = $1")
        .bind(&id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Check that a repository exists and has been verified.
pub async fn check_repository(state: &AdminState, id: &str) -> Result<(), ApiError> {
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM backup_repositories WHERE id = $1")
            .bind(id)
            .fetch_optional(&state.pool)
            .await?;
    if status.as_deref() != Some("verified") {
        return Err(ApiError::invalid(format!(
            "backup repository {id:?} does not exist"
        )));
    }
    Ok(())
}

pub async fn backup_now(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Operation>), ApiError> {
    let label = format!("api-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let mut tx = state.pool.begin().await?;
    let (status, spec) = lock(&mut tx, &id).await?;
    super::databases::require_serving(&status)?;
    super::databases::not_a_replica(&spec)?;
    if spec.backup.is_none() {
        return Err(no_backups());
    }
    let generation: i64 = sqlx::query_scalar("SELECT generation FROM databases WHERE id = $1")
        .bind(&id)
        .fetch_one(&mut *tx)
        .await?;
    let operation = operations::create_with(
        &mut tx,
        &id,
        Kind::Backup,
        generation,
        json!({ "label": label }),
    )
    .await?;
    tx.commit().await?;
    Ok((StatusCode::ACCEPTED, Json(operation)))
}

#[derive(Debug, Serialize, FromRow)]
pub struct Backup {
    id: String,
    label: String,
    #[sqlx(rename = "completed_at")]
    finished_at: DateTime<Utc>,
    size_bytes: i64,
}

pub async fn list_backups(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    page: Page,
) -> Result<Json<List<Backup>>, ApiError> {
    spec_of(&state, &id).await?;
    let rows = sqlx::query_as::<_, Backup>(
        "SELECT id, label, completed_at, size_bytes FROM backups \
         WHERE database_id = $1 AND id > $2 ORDER BY id LIMIT $3",
    )
    .bind(&id)
    .bind(page.after())
    .bind(page.fetch_limit())
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(List::from_rows(rows, &page, |b: &Backup| {
        b.id.clone()
    })))
}

#[derive(Debug, Serialize)]
pub struct RestoreWindow {
    earliest: DateTime<Utc>,
    latest: DateTime<Utc>,
}

/// From the end of the oldest backup, because recovery only replays forward, to
/// the last moment the primary knows has reached the WAL archive.
async fn window(state: &AdminState, id: &str) -> Result<Option<RestoreWindow>, ApiError> {
    let row: (Option<DateTime<Utc>>, Option<DateTime<Utc>>) = sqlx::query_as(
        "SELECT (SELECT min(completed_at) FROM backups WHERE database_id = $1), \
                (SELECT max(archived_until) FROM nodes WHERE database_id = $1)",
    )
    .bind(id)
    .fetch_one(&state.pool)
    .await?;
    Ok(match row {
        (Some(earliest), Some(latest)) if earliest <= latest => {
            Some(RestoreWindow { earliest, latest })
        }
        _ => None,
    })
}

/// What a database created without backups answers to a backup or restore.
fn no_backups() -> ApiError {
    ApiError::conflict("the database has no backups, so there is nothing to restore")
}

pub async fn restore_window(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Json<RestoreWindow>, ApiError> {
    let (_, spec) = spec_of(&state, &id).await?;
    if spec.backup.is_none() {
        return Err(no_backups());
    }
    window(&state, &id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::conflict("the database has nothing to restore yet"))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreRequest {
    name: String,
    target_time: DateTime<Utc>,
    #[serde(default)]
    hosts: Vec<String>,
}

/// Restore into a new database. The source stays as it is, so a failed restore
/// harms nothing.
pub async fn restore(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    JsonBody(request): JsonBody<RestoreRequest>,
) -> Result<(StatusCode, Json<Operation>), ApiError> {
    check_database_name(&request.name)?;
    let (source_name, source): (String, DatabaseSpec) = spec_of(&state, &id).await?;
    if source.backup.is_none() {
        return Err(no_backups());
    }
    check_hosts(&request.hosts, source.ha)?;
    let Some(window) = window(&state, &id).await? else {
        return Err(ApiError::conflict(
            "the database has nothing to restore yet",
        ));
    };
    if request.target_time < window.earliest || request.target_time > window.latest {
        return Err(ApiError::invalid(format!(
            "target_time must be between {} and {}",
            window.earliest, window.latest
        )));
    }
    let mut spec = source;
    spec.hosts = request.hosts;
    spec.replica_of = None;
    spec.data_name = None;
    spec.restore = Some(RestoreSource {
        database_id: id.clone(),
        database_name: source_name,
        target_time: request.target_time.to_rfc3339(),
    });

    let new_id = uuid::Uuid::new_v4().to_string();
    let mut tx = state.pool.begin().await?;
    let inserted = sqlx::query(
        "INSERT INTO databases (id, name, status, spec) VALUES ($1, $2, 'provisioning', $3) \
         ON CONFLICT (name) WHERE status <> 'deleted' DO NOTHING",
    )
    .bind(&new_id)
    .bind(&request.name)
    .bind(sqlx::types::Json(&spec))
    .execute(&mut *tx)
    .await?;
    if inserted.rows_affected() == 0 {
        return Err(ApiError::conflict(format!(
            "a database named {:?} exists",
            request.name
        )));
    }
    let operation = operations::create(&mut tx, &new_id, Kind::Create, 1).await?;
    tx.commit().await?;
    Ok((StatusCode::ACCEPTED, Json(operation)))
}
