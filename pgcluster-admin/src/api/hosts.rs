use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

use super::{JsonBody, List, Page};
use crate::agent::token_hash;
use crate::error::ApiError;
use crate::model::{check_host_name, Resources};
use crate::reconciler::ONLINE_WINDOW_SECONDS;
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
    free_capacity: Option<Resources>,
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
    id: String,
    name: String,
    failure_domain: Option<String>,
    address: Option<String>,
    version: Option<String>,
    capacity: Option<sqlx::types::Json<Resources>>,
    enrolled_at: Option<DateTime<Utc>>,
    last_seen_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    used_vcpu: f64,
    used_memory_gb: f64,
    used_storage_gb: f64,
}

const HOST_COLUMNS: &str = "h.id, h.name, h.failure_domain, h.address, h.version, h.capacity, \
    h.enrolled_at, h.last_seen_at, h.created_at, \
    coalesce(u.vcpu, 0) AS used_vcpu, coalesce(u.memory_gb, 0) AS used_memory_gb, \
    coalesce(u.storage_gb, 0) AS used_storage_gb";

const HOST_FROM: &str = "hosts h LEFT JOIN LATERAL ( \
        SELECT sum((d.spec->'resources'->>'vcpu')::float8) AS vcpu, \
               sum((d.spec->'resources'->>'memory_gb')::float8) AS memory_gb, \
               sum((d.spec->'resources'->>'storage_gb')::float8) AS storage_gb \
        FROM nodes n JOIN databases d ON d.id = n.database_id \
        WHERE n.host_id = h.id) u ON true";

impl From<HostRow> for Host {
    fn from(row: HostRow) -> Self {
        let online = row
            .last_seen_at
            .is_some_and(|seen| (Utc::now() - seen).num_seconds() < ONLINE_WINDOW_SECONDS);
        let status = match (row.enrolled_at, online) {
            (None, _) => "pending",
            (Some(_), true) => "online",
            (Some(_), false) => "offline",
        };
        let capacity = row.capacity.map(|c| c.0);
        let free_capacity = capacity.as_ref().map(|c| Resources {
            vcpu: c.vcpu - row.used_vcpu,
            memory_gb: c.memory_gb - row.used_memory_gb,
            storage_gb: c.storage_gb - row.used_storage_gb,
        });
        Self {
            id: row.id,
            name: row.name,
            failure_domain: row.failure_domain,
            status,
            address: row.address,
            version: row.version,
            capacity,
            free_capacity,
            last_seen_at: row.last_seen_at,
            created_at: row.created_at,
        }
    }
}

async fn fetch(state: &AdminState, id: &str) -> Result<Host, ApiError> {
    sqlx::query_as::<_, HostRow>(&format!(
        "SELECT {HOST_COLUMNS} FROM {HOST_FROM} WHERE h.id = $1"
    ))
    .bind(id)
    .fetch_optional(&state.pool)
    .await?
    .map(Host::from)
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
        "SELECT {HOST_COLUMNS} FROM {HOST_FROM} WHERE h.id > $1 ORDER BY h.id LIMIT $2"
    ))
    .bind(page.after())
    .bind(page.fetch_limit())
    .fetch_all(&state.pool)
    .await?;
    let hosts = rows.into_iter().map(Host::from).collect();
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
