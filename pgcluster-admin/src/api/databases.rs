use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Postgres, Transaction};

use super::{JsonBody, List, Page};
use crate::error::ApiError;
use crate::model::{
    check_cidrs, check_extensions, check_verifier, CreateDatabase, DatabaseSpec, Resources,
};
use crate::operations::{self, Kind, Operation, OPERATION_COLUMNS};
use crate::reconciler::bump_host;
use crate::state::AdminState;

#[derive(Debug, Serialize)]
pub struct Endpoint {
    host: String,
    port: i32,
    database: String,
    ca_cert_pem: String,
}

#[derive(Debug, Serialize)]
pub struct Database {
    id: String,
    name: String,
    workspace: Option<String>,
    version: String,
    ha: bool,
    status: String,
    status_reason: Option<String>,
    generation: i64,
    resources: Resources,
    replication_mode: String,
    allowed_cidrs: Vec<String>,
    extensions: Vec<String>,
    owner_username: String,
    endpoint: Option<Endpoint>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(FromRow)]
struct DatabaseRow {
    id: String,
    name: String,
    workspace: Option<String>,
    status: String,
    status_reason: Option<String>,
    generation: i64,
    spec: sqlx::types::Json<DatabaseSpec>,
    ca_cert_pem: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    address: Option<String>,
    port_base: Option<i32>,
}

/// With HA, `n1` may no longer be the primary, but every proxy routes writes.
const DATABASE_QUERY: &str = "SELECT d.id, d.name, d.workspace, d.status, d.status_reason, \
        d.generation, d.spec, d.ca_cert_pem, d.created_at, d.updated_at, \
        h.address, n.port_base \
    FROM databases d \
    LEFT JOIN nodes n ON n.database_id = d.id AND n.node = 'n1' \
    LEFT JOIN hosts h ON h.id = n.host_id";

impl From<DatabaseRow> for Database {
    fn from(row: DatabaseRow) -> Self {
        let endpoint = match (row.address, row.port_base, row.ca_cert_pem) {
            (Some(host), Some(port), Some(ca_cert_pem)) => Some(Endpoint {
                host,
                port,
                database: row.name.clone(),
                ca_cert_pem,
            }),
            _ => None,
        };
        let spec = row.spec.0;
        Self {
            id: row.id,
            name: row.name,
            workspace: row.workspace,
            version: spec.version,
            ha: spec.ha,
            status: row.status,
            status_reason: row.status_reason,
            generation: row.generation,
            resources: spec.resources,
            replication_mode: spec.replication_mode,
            allowed_cidrs: spec.allowed_cidrs,
            extensions: spec.extensions,
            owner_username: spec.owner.username,
            endpoint,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

async fn fetch(state: &AdminState, id: &str) -> Result<Database, ApiError> {
    sqlx::query_as::<_, DatabaseRow>(&format!("{DATABASE_QUERY} WHERE d.id = $1"))
        .bind(id)
        .fetch_optional(&state.pool)
        .await?
        .map(Database::from)
        .filter(|d| d.status != "deleted")
        .ok_or_else(|| ApiError::not_found("database"))
}

pub async fn create(
    State(state): State<AdminState>,
    JsonBody(request): JsonBody<CreateDatabase>,
) -> Result<(StatusCode, Json<Operation>), ApiError> {
    let (name, workspace, spec) = request.into_spec()?;
    if let Some(wanted) = spec.hosts.first() {
        let found: Option<String> =
            sqlx::query_scalar("SELECT id FROM hosts WHERE id = $1 OR name = $1")
                .bind(wanted)
                .fetch_optional(&state.pool)
                .await?;
        if found.is_none() {
            return Err(ApiError::invalid(format!("host {wanted:?} does not exist")));
        }
    }

    let id = uuid::Uuid::new_v4().to_string();
    let mut tx = state.pool.begin().await?;
    let inserted = sqlx::query(
        "INSERT INTO databases (id, name, workspace, status, spec) \
         VALUES ($1, $2, $3, 'provisioning', $4) \
         ON CONFLICT (name) WHERE status <> 'deleted' DO NOTHING",
    )
    .bind(&id)
    .bind(&name)
    .bind(&workspace)
    .bind(sqlx::types::Json(&spec))
    .execute(&mut *tx)
    .await?;
    if inserted.rows_affected() == 0 {
        return Err(ApiError::conflict(format!(
            "a database named {name:?} exists"
        )));
    }
    let operation = operations::create(&mut tx, &id, Kind::Create, 1).await?;
    tx.commit().await?;
    Ok((StatusCode::ACCEPTED, Json(operation)))
}

pub async fn list(
    State(state): State<AdminState>,
    page: Page,
) -> Result<Json<List<Database>>, ApiError> {
    let rows = sqlx::query_as::<_, DatabaseRow>(&format!(
        "{DATABASE_QUERY} WHERE d.status <> 'deleted' AND d.id > $1 ORDER BY d.id LIMIT $2"
    ))
    .bind(page.after())
    .bind(page.fetch_limit())
    .fetch_all(&state.pool)
    .await?;
    let databases = rows.into_iter().map(Database::from).collect();
    Ok(Json(List::from_rows(databases, &page, |d: &Database| {
        d.id.clone()
    })))
}

pub async fn get(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Json<Database>, ApiError> {
    Ok(Json(fetch(&state, &id).await?))
}

/// Lock a database for a change, and read its status and spec.
async fn lock(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
) -> Result<(String, DatabaseSpec), ApiError> {
    let row: Option<(String, sqlx::types::Json<DatabaseSpec>)> =
        sqlx::query_as("SELECT status, spec FROM databases WHERE id = $1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut **tx)
            .await?;
    let (status, spec) = row
        .filter(|(status, _)| status != "deleted")
        .ok_or_else(|| ApiError::not_found("database"))?;
    Ok((status, spec.0))
}

/// Store a changed spec, publish it to the database's hosts, and record the
/// operation that waits for them.
async fn change_spec(
    state: &AdminState,
    id: &str,
    change: impl FnOnce(&mut DatabaseSpec),
) -> Result<(StatusCode, Json<Operation>), ApiError> {
    let mut tx = state.pool.begin().await?;
    let (status, mut spec) = lock(&mut tx, id).await?;
    if status == "deleting" || status == "error" {
        return Err(ApiError::conflict(format!(
            "a database in status {status} cannot change"
        )));
    }
    change(&mut spec);
    let generation: i64 = sqlx::query_scalar(
        "UPDATE databases SET spec = $2, generation = generation + 1, updated_at = now() \
         WHERE id = $1 RETURNING generation",
    )
    .bind(id)
    .bind(sqlx::types::Json(&spec))
    .fetch_one(&mut *tx)
    .await?;
    let hosts: Vec<String> =
        sqlx::query_scalar("SELECT host_id FROM nodes WHERE database_id = $1 AND NOT retracted")
            .bind(id)
            .fetch_all(&mut *tx)
            .await?;
    for host in hosts {
        bump_host(&mut tx, &host).await?;
    }
    let operation = operations::create(&mut tx, id, Kind::Update, generation).await?;
    tx.commit().await?;
    Ok((StatusCode::ACCEPTED, Json(operation)))
}

pub async fn delete(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Operation>), ApiError> {
    let mut tx = state.pool.begin().await?;
    let (status, _) = lock(&mut tx, &id).await?;
    if status == "deleting" {
        return Err(ApiError::conflict("the database is already being deleted"));
    }
    // A stuck create must not hold up its own removal.
    sqlx::query(
        "UPDATE operations SET status = 'failed', error = 'superseded by delete', \
                               finished_at = now() \
         WHERE database_id = $1 AND status IN ('pending', 'running')",
    )
    .bind(&id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE databases SET status = 'deleting', status_reason = NULL, updated_at = now() \
         WHERE id = $1",
    )
    .bind(&id)
    .execute(&mut *tx)
    .await?;
    let generation: i64 = sqlx::query_scalar("SELECT generation FROM databases WHERE id = $1")
        .bind(&id)
        .fetch_one(&mut *tx)
        .await?;
    let operation = operations::create(&mut tx, &id, Kind::Delete, generation).await?;
    tx.commit().await?;
    Ok((StatusCode::ACCEPTED, Json(operation)))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerPassword {
    scram_verifier: String,
}

pub async fn set_owner_password(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    JsonBody(request): JsonBody<OwnerPassword>,
) -> Result<(StatusCode, Json<Operation>), ApiError> {
    check_verifier(&request.scram_verifier)?;
    change_spec(&state, &id, |spec| {
        spec.owner.scram_verifier = request.scram_verifier
    })
    .await
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Allowlist {
    allowed_cidrs: Vec<String>,
}

pub async fn get_allowlist(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Json<Allowlist>, ApiError> {
    Ok(Json(Allowlist {
        allowed_cidrs: fetch(&state, &id).await?.allowed_cidrs,
    }))
}

pub async fn put_allowlist(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    JsonBody(request): JsonBody<Allowlist>,
) -> Result<(StatusCode, Json<Operation>), ApiError> {
    check_cidrs(&request.allowed_cidrs)?;
    change_spec(&state, &id, |spec| {
        spec.allowed_cidrs = request.allowed_cidrs
    })
    .await
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Extensions {
    extensions: Vec<String>,
}

pub async fn get_extensions(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Json<Extensions>, ApiError> {
    Ok(Json(Extensions {
        extensions: fetch(&state, &id).await?.extensions,
    }))
}

pub async fn put_extensions(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    JsonBody(request): JsonBody<Extensions>,
) -> Result<(StatusCode, Json<Operation>), ApiError> {
    check_extensions(&request.extensions)?;
    change_spec(&state, &id, |spec| spec.extensions = request.extensions).await
}

#[derive(Debug, Serialize, FromRow)]
pub struct Node {
    node: String,
    host_id: String,
    host_name: String,
    port_base: i32,
    observed_generation: i64,
    state: String,
    error: Option<String>,
}

pub async fn list_nodes(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    page: Page,
) -> Result<Json<List<Node>>, ApiError> {
    fetch(&state, &id).await?;
    let rows = sqlx::query_as::<_, Node>(
        "SELECT n.node, n.host_id, h.name AS host_name, n.port_base, n.observed_generation, \
                n.state, n.error \
         FROM nodes n JOIN hosts h ON h.id = n.host_id \
         WHERE n.database_id = $1 AND n.node > $2 ORDER BY n.node LIMIT $3",
    )
    .bind(&id)
    .bind(page.after())
    .bind(page.fetch_limit())
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(List::from_rows(rows, &page, |n: &Node| {
        n.node.clone()
    })))
}

pub async fn list_operations(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    page: Page,
) -> Result<Json<List<Operation>>, ApiError> {
    fetch(&state, &id).await?;
    let rows = sqlx::query_as::<_, Operation>(&format!(
        "SELECT {OPERATION_COLUMNS} FROM operations \
         WHERE database_id = $1 AND id > $2 ORDER BY id LIMIT $3"
    ))
    .bind(&id)
    .bind(page.after())
    .bind(page.fetch_limit())
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(List::from_rows(rows, &page, |o: &Operation| {
        o.id.clone()
    })))
}

#[derive(Debug, Serialize, FromRow)]
pub struct OperationStep {
    seq: i32,
    name: String,
    status: String,
    attempts: i32,
    finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct OperationLog {
    at: DateTime<Utc>,
    source: String,
    message: String,
}

#[derive(Debug, Serialize)]
pub struct OperationDetail {
    #[serde(flatten)]
    operation: Operation,
    steps: Vec<OperationStep>,
    logs: Vec<OperationLog>,
}

pub async fn get_operation(
    State(state): State<AdminState>,
    Path((id, op_id)): Path<(String, String)>,
) -> Result<Json<OperationDetail>, ApiError> {
    let operation = sqlx::query_as::<_, Operation>(&format!(
        "SELECT {OPERATION_COLUMNS} FROM operations WHERE id = $1 AND database_id = $2"
    ))
    .bind(&op_id)
    .bind(&id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or_else(|| ApiError::not_found("operation"))?;
    let steps = sqlx::query_as::<_, OperationStep>(
        "SELECT seq, name, status, attempts, finished_at FROM operation_steps \
         WHERE operation_id = $1 ORDER BY seq",
    )
    .bind(&op_id)
    .fetch_all(&state.pool)
    .await?;
    let logs = sqlx::query_as::<_, OperationLog>(
        "SELECT at, source, message FROM operation_logs WHERE operation_id = $1 ORDER BY id",
    )
    .bind(&op_id)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(OperationDetail {
        operation,
        steps,
        logs,
    }))
}
