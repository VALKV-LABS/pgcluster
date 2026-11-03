use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{FromRow, Postgres, Transaction};

use super::{JsonBody, List, Page};
use crate::ca::Ca;
use crate::error::ApiError;
use crate::model::{
    check_cidrs, check_extensions, check_replication_mode, check_retention, check_verifier,
    node_number, CreateDatabase, DatabaseSpec, Resources, UpdateDatabase, NEEDS_PRELOAD,
};
use crate::operations::{self, Kind, Operation, OPERATION_COLUMNS};
use crate::reconciler::{add_node, bump_hosts, ONLINE_WINDOW_SECONDS};
use crate::secrets::DatabaseSecrets;
use crate::state::AdminState;

// Offsets into a node's block of ports, as peers use them.
const POSTGRES_PORT: i32 = 2;
const RAFT_PORT: i32 = 3;
const AGENT_PORT: i32 = 4;

#[derive(Debug, Serialize)]
pub struct Endpoint {
    host: String,
    port: i32,
    database: String,
    ca_cert_pem: String,
}

/// One node's proxy. Each routes writes to the primary, wherever it is.
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeAddress {
    host: String,
    port: i32,
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
    /// Every node's proxy, so a client can list them all.
    endpoints: Vec<NodeAddress>,
    backup: crate::model::BackupSettings,
    primary: Option<String>,
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
    addresses: sqlx::types::Json<Vec<NodeAddress>>,
    observed: sqlx::types::Json<serde_json::Value>,
}

/// Any node's proxy routes writes, so the endpoint is the first node on a host
/// that is online.
const DATABASE_QUERY: &str = "SELECT d.id, d.name, d.workspace, d.status, d.status_reason, \
        d.generation, d.spec, d.ca_cert_pem, d.created_at, d.updated_at, d.observed, \
        e.address, e.port_base, a.addresses \
    FROM databases d \
    LEFT JOIN LATERAL (SELECT h.address, n.port_base FROM nodes n \
        JOIN hosts h ON h.id = n.host_id \
        WHERE n.database_id = d.id AND NOT n.retracted AND h.address IS NOT NULL \
        ORDER BY n.offline_since IS NOT NULL, length(n.node), n.node LIMIT 1) e ON true \
    CROSS JOIN LATERAL (SELECT coalesce(json_agg(json_build_object( \
            'host', h.address, 'port', n.port_base) ORDER BY length(n.node), n.node), '[]') \
            AS addresses \
        FROM nodes n JOIN hosts h ON h.id = n.host_id \
        WHERE n.database_id = d.id AND NOT n.retracted AND h.address IS NOT NULL) a";

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
            endpoints: row.addresses.0,
            backup: spec.backup,
            primary: row.observed.0["primary"].as_str().map(str::to_string),
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
    super::backups::check_repository(&state, &spec.backup.repository_id).await?;
    check_placement(&state, &spec.hosts).await?;

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

/// Named hosts must exist, and each be a failure domain of its own.
pub async fn check_placement(state: &AdminState, hosts: &[String]) -> Result<(), ApiError> {
    let mut domains = Vec::with_capacity(hosts.len());
    for wanted in hosts {
        let found: Option<(String, Option<String>)> =
            sqlx::query_as("SELECT id, failure_domain FROM hosts WHERE id = $1 OR name = $1")
                .bind(wanted)
                .fetch_optional(&state.pool)
                .await?;
        let Some((id, domain)) = found else {
            return Err(ApiError::invalid(format!("host {wanted:?} does not exist")));
        };
        let domain = domain.unwrap_or(id);
        if domains.contains(&domain) {
            return Err(ApiError::invalid(format!(
                "host {wanted:?} shares a failure domain with another"
            )));
        }
        domains.push(domain);
    }
    Ok(())
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

/// A database's name and spec, without locking it.
pub async fn spec_of(state: &AdminState, id: &str) -> Result<(String, DatabaseSpec), ApiError> {
    let row: Option<(String, String, sqlx::types::Json<DatabaseSpec>)> =
        sqlx::query_as("SELECT name, status, spec FROM databases WHERE id = $1")
            .bind(id)
            .fetch_optional(&state.pool)
            .await?;
    row.filter(|(_, status, _)| status != "deleted")
        .map(|(name, _, spec)| (name, spec.0))
        .ok_or_else(|| ApiError::not_found("database"))
}

pub fn require_serving(status: &str) -> Result<(), ApiError> {
    if status == "active" || status == "degraded" {
        Ok(())
    } else {
        Err(ApiError::conflict(format!(
            "a database in status {status} cannot do this"
        )))
    }
}

/// Lock a database for a change, and read its status and spec.
pub async fn lock(
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
    change_spec_as(state, id, Kind::Update, json!({}), change).await
}

async fn change_spec_as(
    state: &AdminState,
    id: &str,
    kind: Kind,
    params: serde_json::Value,
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
    bump_hosts(&mut tx, id).await?;
    let operation = operations::create_with(&mut tx, id, kind, generation, params).await?;
    tx.commit().await?;
    Ok((StatusCode::ACCEPTED, Json(operation)))
}

/// Start an operation that changes no spec.
async fn start(
    state: &AdminState,
    id: &str,
    kind: Kind,
    params: serde_json::Value,
) -> Result<(StatusCode, Json<Operation>), ApiError> {
    let mut tx = state.pool.begin().await?;
    let (status, _) = lock(&mut tx, id).await?;
    require_serving(&status)?;
    let generation: i64 = sqlx::query_scalar("SELECT generation FROM databases WHERE id = $1")
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
    let operation = operations::create_with(&mut tx, id, kind, generation, params).await?;
    tx.commit().await?;
    Ok((StatusCode::ACCEPTED, Json(operation)))
}

pub async fn update(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    JsonBody(request): JsonBody<UpdateDatabase>,
) -> Result<(StatusCode, Json<Operation>), ApiError> {
    if request.resources.is_some() {
        return Err(ApiError::not_supported("resizing is not available yet"));
    }
    let (_, spec) = spec_of(&state, &id).await?;
    if let Some(days) = request.retention_days {
        check_retention(days)?;
    }
    match request.replication_mode {
        Some(mode) => {
            check_replication_mode(&mode, spec.ha)?;
            let params = json!({ "mode": mode });
            change_spec_as(&state, &id, Kind::ReplicationMode, params, |spec| {
                spec.replication_mode = mode;
                if let Some(days) = request.retention_days {
                    spec.backup.retention_days = days;
                }
            })
            .await
        }
        None => {
            let Some(days) = request.retention_days else {
                return Err(ApiError::invalid("name something to change"));
            };
            change_spec(&state, &id, |spec| spec.backup.retention_days = days).await
        }
    }
}

pub async fn restart(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Operation>), ApiError> {
    start(&state, &id, Kind::Restart, json!({})).await
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Switchover {
    /// The node to hand over to. Left out, the most current standby.
    #[serde(default)]
    target: Option<String>,
}

pub async fn switchover(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    body: Option<JsonBody<Switchover>>,
) -> Result<(StatusCode, Json<Operation>), ApiError> {
    let (_, spec) = spec_of(&state, &id).await?;
    if !spec.ha {
        return Err(ApiError::conflict("a one-node database has no standby"));
    }
    let target = body.and_then(|JsonBody(b)| b.target);
    start(&state, &id, Kind::Switchover, json!({ "target": target })).await
}

pub async fn rotate_ca(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Operation>), ApiError> {
    start(&state, &id, Kind::RotateCa, json!({})).await
}

pub async fn rotate_passwords(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Operation>), ApiError> {
    start(&state, &id, Kind::RotatePasswords, json!({})).await
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplaceNode {
    host_id: String,
}

/// Bring up a node on `host_id` and retire `node`. The new node joins first, so
/// the cluster never runs short of voters.
pub async fn replace_node(
    State(state): State<AdminState>,
    Path((id, node)): Path<(String, String)>,
    JsonBody(request): JsonBody<ReplaceNode>,
) -> Result<(StatusCode, Json<Operation>), ApiError> {
    let mut tx = state.pool.begin().await?;
    let (status, spec) = lock(&mut tx, &id).await?;
    require_serving(&status)?;
    if !spec.ha {
        return Err(ApiError::conflict(
            "a one-node database is restored from backup, not replaced",
        ));
    }
    let busy: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM operations \
                        WHERE database_id = $1 AND status IN ('pending', 'running'))",
    )
    .bind(&id)
    .fetch_one(&mut *tx)
    .await?;
    if busy {
        return Err(ApiError::conflict("another operation is running"));
    }

    let nodes: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT n.node, h.id, h.failure_domain FROM nodes n JOIN hosts h ON h.id = n.host_id \
         WHERE n.database_id = $1 AND NOT n.retracted",
    )
    .bind(&id)
    .fetch_all(&mut *tx)
    .await?;
    if !nodes.iter().any(|(name, ..)| *name == node) {
        return Err(ApiError::not_found("node"));
    }

    let host: Option<(String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT id, address, failure_domain FROM hosts \
         WHERE (id = $1 OR name = $1) AND last_seen_at > now() - make_interval(secs => $2)",
    )
    .bind(&request.host_id)
    .bind(ONLINE_WINDOW_SECONDS as f64)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((host_id, Some(address), domain)) = host else {
        return Err(ApiError::invalid(format!(
            "host {:?} is not online",
            request.host_id
        )));
    };
    let new_domain = domain.unwrap_or_else(|| host_id.clone());
    for (name, peer_host, peer_domain) in &nodes {
        if *name == node {
            continue;
        }
        let peer_domain = peer_domain.as_ref().unwrap_or(peer_host);
        if *peer_host == host_id || *peer_domain == new_domain {
            return Err(ApiError::invalid(format!(
                "host {:?} shares a failure domain with {name}",
                request.host_id
            )));
        }
    }
    if nodes.iter().any(|(_, peer_host, _)| *peer_host == host_id) {
        return Err(ApiError::invalid(format!(
            "host {:?} already runs this node",
            request.host_id
        )));
    }

    let sealed: Option<Vec<u8>> = sqlx::query_scalar("SELECT secrets FROM databases WHERE id = $1")
        .bind(&id)
        .fetch_one(&mut *tx)
        .await?;
    let sealed = sealed.ok_or_else(|| anyhow::anyhow!("the database has no secrets"))?;
    let secrets: DatabaseSecrets = state.secrets.open(&id, &sealed)?;
    let ca = Ca::from_pem(&secrets.ca_cert_pem, &secrets.ca_key_pem)?;

    let number = nodes
        .iter()
        .map(|(n, ..)| node_number(n))
        .max()
        .unwrap_or(0)
        + 1;
    let new_node = format!("n{number}");
    let added = add_node(
        &state, &mut tx, &id, &new_node, &host_id, &address, "seed", &ca,
    )
    .await?;
    let Some(port_base) = added else {
        return Err(ApiError::conflict(format!(
            "host {:?} has no free ports",
            request.host_id
        )));
    };
    let generation: i64 = sqlx::query_scalar(
        "UPDATE databases SET generation = generation + 1, updated_at = now() \
         WHERE id = $1 RETURNING generation",
    )
    .bind(&id)
    .fetch_one(&mut *tx)
    .await?;
    bump_hosts(&mut tx, &id).await?;
    let addr = |offset: i32| format!("{address}:{}", port_base + offset);
    let operation = operations::create_with(
        &mut tx,
        &id,
        Kind::Replace,
        generation,
        json!({
            "old_node": node,
            "new_node": new_node,
            "raft_addr": addr(RAFT_PORT),
            "agent_addr": addr(AGENT_PORT),
            "postgres_addr": addr(POSTGRES_PORT),
        }),
    )
    .await?;
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
        "UPDATE tasks SET status = 'failed', error = 'superseded by delete', \
                          finished_at = now() \
         WHERE database_id = $1 AND status = 'pending'",
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
    let (_, spec) = spec_of(&state, &id).await?;
    let preloaded = |extensions: &[String]| -> Vec<&str> {
        NEEDS_PRELOAD
            .iter()
            .copied()
            .filter(|lib| extensions.iter().any(|e| e == lib))
            .collect()
    };
    // A library loads at start, so on three nodes it takes a rolling restart.
    let kind = if spec.ha && preloaded(&spec.extensions) != preloaded(&request.extensions) {
        Kind::RollingUpdate
    } else {
        Kind::Update
    };
    change_spec_as(&state, &id, kind, json!({}), |spec| {
        spec.extensions = request.extensions
    })
    .await
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
         WHERE n.database_id = $1 AND n.node > $2 AND NOT n.retracted \
         ORDER BY n.node LIMIT $3",
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

#[derive(Debug, Deserialize)]
pub struct EventQuery {
    #[serde(default)]
    after: Option<i64>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct Event {
    cursor: String,
    #[sqlx(rename = "type")]
    #[serde(rename = "type")]
    kind: String,
    database_id: Option<String>,
    node: Option<String>,
    detail: sqlx::types::Json<serde_json::Value>,
    at: DateTime<Utc>,
}

/// State changes in order, from just after a cursor. Tooling keeps the cursor of
/// the last one it handled.
pub async fn list_events(
    State(state): State<AdminState>,
    Query(query): Query<EventQuery>,
    page: Page,
) -> Result<Json<List<Event>>, ApiError> {
    let after = match page.after() {
        "" => query.after.unwrap_or(0),
        token => token
            .parse()
            .map_err(|_| ApiError::invalid("page_token is not a cursor"))?,
    };
    let rows = sqlx::query_as::<_, Event>(
        "SELECT id::text AS cursor, type, database_id, node, detail, at FROM events \
         WHERE id > $1 ORDER BY id LIMIT $2",
    )
    .bind(after)
    .bind(page.fetch_limit())
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(List::from_rows(rows, &page, |e: &Event| {
        e.cursor.clone()
    })))
}
