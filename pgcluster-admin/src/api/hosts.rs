use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::FromRow;

use super::{JsonBody, List, Page};
use crate::agent::token_hash;
use crate::error::ApiError;
use crate::model::{check_host_name, Resources};
use crate::operations::event;
use crate::reconciler::ONLINE_WINDOW_SECONDS;
use crate::scheduler::{HostLoad, SchedulerConfig};
use crate::secrets::random_token;
use crate::state::AdminState;

const ENROLLMENT_TOKEN_MINUTES: i32 = 15;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateHost {
    name: String,
    #[serde(default)]
    failure_domain: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct Host {
    id: String,
    name: String,
    failure_domain: Option<String>,
    status: &'static str,
    address: Option<String>,
    version: Option<String>,
    capacity: Option<Resources>,
    /// What the scheduler can still hand out: capacity, less the memory and
    /// disk it keeps back, less every instance on the host.
    free_capacity: Option<Resources>,
    instances: i64,
    postgres_versions: serde_json::Value,
    draining: bool,
    drain_error: Option<String>,
    drained_at: Option<DateTime<Utc>>,
    last_seen_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct HostEnrollment {
    host: Host,
    enrollment_token: String,
    expires_at: DateTime<Utc>,
}

#[derive(FromRow)]
struct HostRow {
    #[sqlx(flatten)]
    load: HostLoad,
    version: Option<String>,
    enrolled_at: Option<DateTime<Utc>>,
    last_seen_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    drain_error: Option<String>,
    drained_at: Option<DateTime<Utc>>,
    postgres_versions: sqlx::types::Json<serde_json::Value>,
}

const HOST_QUERY: &str = "SELECT l.*, h.version, h.enrolled_at, h.last_seen_at, h.created_at, \
        h.drain_error, h.drained_at, h.postgres_versions \
    FROM hosts h JOIN LATERAL ( \
        SELECT h2.id, h2.name, h2.address, h2.failure_domain, h2.capacity, \
            coalesce(sum((d.spec->'resources'->>'vcpu')::float8), 0) AS used_vcpu, \
            coalesce(sum((d.spec->'resources'->>'memory_gb')::float8), 0) AS used_memory_gb, \
            coalesce(sum((d.spec->'resources'->>'storage_gb')::float8), 0) AS used_storage_gb, \
            count(n.node) AS instances, \
            coalesce(h2.last_seen_at > now() - make_interval(secs => $1), false) AS online, \
            h2.draining \
        FROM hosts h2 LEFT JOIN nodes n ON n.host_id = h2.id \
        LEFT JOIN databases d ON d.id = n.database_id \
        WHERE h2.id = h.id GROUP BY h2.id) l ON true";

fn host_from(row: HostRow, config: &SchedulerConfig) -> Host {
    let load = row.load;
    let status = match (row.enrolled_at, load.online, load.draining) {
        (None, _, _) => "pending",
        (Some(_), false, _) => "offline",
        (Some(_), true, true) if load.instances == 0 => "drained",
        (Some(_), true, true) => "draining",
        (Some(_), true, false) => "online",
    };
    Host {
        free_capacity: load.free(config),
        capacity: load.capacity.as_ref().map(|c| c.0.clone()),
        id: load.id,
        name: load.name,
        failure_domain: load.failure_domain,
        status,
        address: load.address,
        version: row.version,
        instances: load.instances,
        postgres_versions: row.postgres_versions.0,
        draining: load.draining,
        drain_error: row.drain_error,
        drained_at: row.drained_at,
        last_seen_at: row.last_seen_at,
        created_at: row.created_at,
    }
}

async fn fetch(state: &AdminState, id: &str) -> Result<Host, ApiError> {
    sqlx::query_as::<_, HostRow>(&format!("{HOST_QUERY} WHERE h.id = $2"))
        .bind(ONLINE_WINDOW_SECONDS as f64)
        .bind(id)
        .fetch_optional(&state.pool)
        .await?
        .map(|row| host_from(row, &state.scheduler))
        .ok_or_else(|| ApiError::not_found("host"))
}

pub async fn create(
    State(state): State<AdminState>,
    JsonBody(request): JsonBody<CreateHost>,
) -> Result<(StatusCode, Json<HostEnrollment>), ApiError> {
    check_host_name(&request.name)?;
    let id = uuid::Uuid::new_v4().to_string();
    let secret = random_token();

    let mut tx = state.pool.begin().await?;
    let inserted = sqlx::query(
        "INSERT INTO hosts (id, name, failure_domain) VALUES ($1, $2, $3) \
         ON CONFLICT (name) DO NOTHING",
    )
    .bind(&id)
    .bind(&request.name)
    .bind(&request.failure_domain)
    .execute(&mut *tx)
    .await?;
    if inserted.rows_affected() == 0 {
        return Err(ApiError::conflict(format!(
            "a host named {:?} exists",
            request.name
        )));
    }
    let expires_at: DateTime<Utc> = sqlx::query_scalar(
        "INSERT INTO enrollment_tokens (token_hash, host_id, expires_at) \
         VALUES ($1, $2, now() + make_interval(mins => $3)) RETURNING expires_at",
    )
    .bind(token_hash(&secret))
    .bind(&id)
    .bind(ENROLLMENT_TOKEN_MINUTES)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;

    Ok((
        StatusCode::CREATED,
        Json(HostEnrollment {
            host: fetch(&state, &id).await?,
            // The token carries the CA's hash, so a new host can trust the
            // server with nothing else copied to it.
            enrollment_token: format!("{secret}.{}", state.ca().fingerprint()),
            expires_at,
        }),
    ))
}

pub async fn list(
    State(state): State<AdminState>,
    page: Page,
) -> Result<Json<List<Host>>, ApiError> {
    let rows = sqlx::query_as::<_, HostRow>(&format!(
        "{HOST_QUERY} WHERE h.id > $2 ORDER BY h.id LIMIT $3"
    ))
    .bind(ONLINE_WINDOW_SECONDS as f64)
    .bind(page.after())
    .bind(page.fetch_limit())
    .fetch_all(&state.pool)
    .await?;
    let hosts = rows
        .into_iter()
        .map(|row| host_from(row, &state.scheduler))
        .collect();
    Ok(Json(List::from_rows(hosts, &page, |h: &Host| h.id.clone())))
}

pub async fn get(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Json<Host>, ApiError> {
    Ok(Json(fetch(&state, &id).await?))
}

pub async fn delete(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let mut tx = state.pool.begin().await?;
    let found: Option<String> = sqlx::query_scalar("SELECT id FROM hosts WHERE id = $1 FOR UPDATE")
        .bind(&id)
        .fetch_optional(&mut *tx)
        .await?;
    if found.is_none() {
        return Err(ApiError::not_found("host"));
    }
    let nodes: i64 = sqlx::query_scalar("SELECT count(*) FROM nodes WHERE host_id = $1")
        .bind(&id)
        .fetch_one(&mut *tx)
        .await?;
    if nodes > 0 {
        return Err(ApiError::conflict(format!(
            "the host still runs {nodes} instance(s)"
        )));
    }
    sqlx::query("DELETE FROM hosts WHERE id = $1")
        .bind(&id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Stop placing nodes on a host, and move every node it has to other hosts.
/// A one-node database cannot move without downtime, so a host with one is
/// refused: restore it to a new database elsewhere, or delete it, first.
pub async fn drain(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Host>), ApiError> {
    let mut tx = state.pool.begin().await?;
    let found: Option<bool> =
        sqlx::query_scalar("SELECT draining FROM hosts WHERE id = $1 FOR UPDATE")
            .bind(&id)
            .fetch_optional(&mut *tx)
            .await?;
    let Some(draining) = found else {
        return Err(ApiError::not_found("host"));
    };
    let single: Vec<String> = sqlx::query_scalar(
        "SELECT d.name FROM nodes n JOIN databases d ON d.id = n.database_id \
         WHERE n.host_id = $1 AND NOT n.retracted AND NOT (d.spec->>'ha')::boolean \
         ORDER BY d.name",
    )
    .bind(&id)
    .fetch_all(&mut *tx)
    .await?;
    if !single.is_empty() {
        return Err(ApiError::conflict(format!(
            "one-node databases cannot move without downtime: {}. Restore each to a new \
             database on another host, or delete it, then drain",
            single.join(", ")
        )));
    }
    if !draining {
        sqlx::query(
            "UPDATE hosts SET draining = true, drain_error = NULL, drained_at = NULL \
             WHERE id = $1",
        )
        .bind(&id)
        .execute(&mut *tx)
        .await?;
        event(
            &mut tx,
            "host_draining",
            None,
            None,
            json!({ "host_id": id }),
        )
        .await?;
    }
    tx.commit().await?;
    Ok((StatusCode::ACCEPTED, Json(fetch(&state, &id).await?)))
}

/// Let nodes land on a host again. Moves already started finish.
pub async fn undrain(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Json<Host>, ApiError> {
    let updated = sqlx::query(
        "UPDATE hosts SET draining = false, drain_error = NULL, drained_at = NULL WHERE id = $1",
    )
    .bind(&id)
    .execute(&state.pool)
    .await?;
    if updated.rows_affected() == 0 {
        return Err(ApiError::not_found("host"));
    }
    Ok(Json(fetch(&state, &id).await?))
}
