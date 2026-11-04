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
    node_number, CreateDatabase, DatabaseSpec, Resources, UpdateDatabase, HA_NODES, NEEDS_PRELOAD,
};
use crate::operations::{self, Kind, Operation, OPERATION_COLUMNS};
use crate::reconciler::{add_node, bump_hosts};
use crate::scheduler::{describe, fits, host_loads, place, too_large, HostLoad, Placement};
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
    /// The release every node runs, or nothing while they differ.
    server_version: Option<String>,
    /// The database this one is an external standby of.
    replica_of: Option<String>,
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
    server_version: Option<String>,
}

/// Any node's proxy routes writes, so the endpoint is the first node on a host
/// that is online.
const DATABASE_QUERY: &str = "SELECT d.id, d.name, d.workspace, d.status, d.status_reason, \
        d.generation, d.spec, d.ca_cert_pem, d.created_at, d.updated_at, d.observed, \
        e.address, e.port_base, a.addresses, v.server_version \
    FROM databases d \
    LEFT JOIN LATERAL (SELECT h.address, n.port_base FROM nodes n \
        JOIN hosts h ON h.id = n.host_id \
        WHERE n.database_id = d.id AND NOT n.retracted AND h.address IS NOT NULL \
        ORDER BY n.offline_since IS NOT NULL, length(n.node), n.node LIMIT 1) e ON true \
    CROSS JOIN LATERAL (SELECT coalesce(json_agg(json_build_object( \
            'host', h.address, 'port', n.port_base) ORDER BY length(n.node), n.node), '[]') \
            AS addresses \
        FROM nodes n JOIN hosts h ON h.id = n.host_id \
        WHERE n.database_id = d.id AND NOT n.retracted AND h.address IS NOT NULL) a \
    CROSS JOIN LATERAL (SELECT CASE WHEN count(DISTINCT n.server_version) = 1 \
            AND count(*) = count(n.server_version) THEN max(n.server_version) END \
            AS server_version \
        FROM nodes n WHERE n.database_id = d.id AND NOT n.retracted) v";

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
            server_version: row.server_version,
            replica_of: spec.replica_of.map(|r| r.database_id),
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
    check_capacity(&state, &spec).await?;

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

/// Refuse at once what the fleet cannot hold: a node larger than any host, or
/// nodes for which enough hosts exist but too few have room. Too few hosts is
/// not refused, because more may enrol, and the create waits for them.
pub async fn check_capacity(state: &AdminState, spec: &DatabaseSpec) -> Result<(), ApiError> {
    let mut conn = state.pool.acquire().await?;
    let hosts: Vec<HostLoad> = host_loads(&mut conn, false)
        .await?
        .into_iter()
        .filter(|h| !h.draining && h.capacity.is_some())
        .filter(|h| {
            spec.hosts.is_empty() || spec.hosts.contains(&h.id) || spec.hosts.contains(&h.name)
        })
        .collect();
    if let Some(message) = too_large(&state.scheduler, &hosts, &spec.resources) {
        return Err(ApiError::invalid(message));
    }
    let count = if spec.ha { HA_NODES } else { 1 };
    match place(&state.scheduler, &hosts, count, &spec.resources) {
        Placement::NoRoom(message) => Err(ApiError::invalid(message)),
        Placement::Placed(_) | Placement::TooFewHosts => Ok(()),
    }
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

/// A replica follows its source and changes only by promotion or deletion.
pub fn not_a_replica(spec: &DatabaseSpec) -> Result<(), ApiError> {
    if spec.replica_of.is_some() {
        return Err(ApiError::conflict(
            "a replica changes only by promotion or deletion",
        ));
    }
    Ok(())
}

pub async fn has_replicas(tx: &mut Transaction<'_, Postgres>, id: &str) -> Result<bool, ApiError> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM databases \
                        WHERE spec->'replica_of'->>'database_id' = $1 AND status <> 'deleted')",
    )
    .bind(id)
    .fetch_one(&mut **tx)
    .await?)
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
    not_a_replica(&spec)?;
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
    let (status, spec) = lock(&mut tx, id).await?;
    require_serving(&status)?;
    not_a_replica(&spec)?;
    // A replica shares its source's CA and passwords, which it cannot follow
    // through a rotation.
    if matches!(kind, Kind::RotateCa | Kind::RotatePasswords) && has_replicas(&mut tx, id).await? {
        return Err(ApiError::conflict(
            "promote or delete this database's replicas before rotating its secrets",
        ));
    }
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
    let (_, spec) = spec_of(&state, &id).await?;
    if let Some(days) = request.retention_days {
        check_retention(days)?;
    }
    if let Some(resources) = request.resources {
        if request.replication_mode.is_some() {
            return Err(ApiError::invalid(
                "change resources and replication_mode in separate requests",
            ));
        }
        return resize(&state, &id, resources, request.retention_days).await;
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

/// Give every node new resources. Each node's host must have room for the
/// difference. Storage only grows, because a quota cannot take back what data
/// already uses.
async fn resize(
    state: &AdminState,
    id: &str,
    resources: Resources,
    retention_days: Option<u32>,
) -> Result<(StatusCode, Json<Operation>), ApiError> {
    if !(resources.vcpu > 0.0 && resources.memory_gb > 0.0 && resources.storage_gb > 0.0) {
        return Err(ApiError::invalid("resources must all be above zero"));
    }
    let mut tx = state.pool.begin().await?;
    let (status, mut spec) = lock(&mut tx, id).await?;
    require_serving(&status)?;
    not_a_replica(&spec)?;
    let busy: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM operations \
                        WHERE database_id = $1 AND status IN ('pending', 'running'))",
    )
    .bind(id)
    .fetch_one(&mut *tx)
    .await?;
    if busy {
        return Err(ApiError::conflict("another operation is running"));
    }
    let old = spec.resources.clone();
    if resources.storage_gb < old.storage_gb {
        return Err(ApiError::invalid(format!(
            "storage cannot shrink, from {} GB to {} GB",
            old.storage_gb, resources.storage_gb
        )));
    }
    let hosts: Vec<String> =
        sqlx::query_scalar("SELECT host_id FROM nodes WHERE database_id = $1 AND NOT retracted")
            .bind(id)
            .fetch_all(&mut *tx)
            .await?;
    // This database already counts in what each host uses.
    let growth = Resources {
        vcpu: (resources.vcpu - old.vcpu).max(0.0),
        memory_gb: (resources.memory_gb - old.memory_gb).max(0.0),
        storage_gb: resources.storage_gb - old.storage_gb,
    };
    for host in host_loads(&mut tx, true).await? {
        if !hosts.contains(&host.id) {
            continue;
        }
        let free = host.free(&state.scheduler);
        if !free.as_ref().is_some_and(|free| fits(free, &growth)) {
            let free = free
                .map(|f| describe(&f))
                .unwrap_or_else(|| "nothing".into());
            return Err(ApiError::invalid(format!(
                "host {} has no room to grow by {}: it has {free} free",
                host.name,
                describe(&growth)
            )));
        }
    }

    // On three nodes, a new memory size takes a rolling restart. One node
    // restarts itself when its host applies the change.
    let restart = spec.ha && resources.memory_gb != old.memory_gb;
    spec.resources = resources.clone();
    if let Some(days) = retention_days {
        spec.backup.retention_days = days;
    }
    let generation: i64 = sqlx::query_scalar(
        "UPDATE databases SET spec = $2, generation = generation + 1, updated_at = now() \
         WHERE id = $1 RETURNING generation",
    )
    .bind(id)
    .bind(sqlx::types::Json(&spec))
    .fetch_one(&mut *tx)
    .await?;
    bump_hosts(&mut tx, id).await?;
    let params = json!({ "from": old, "to": resources, "restart": restart });
    let operation = operations::create_with(&mut tx, id, Kind::Resize, generation, params).await?;
    tx.commit().await?;
    Ok((StatusCode::ACCEPTED, Json(operation)))
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Upgrade {
    /// A full version, such as `18.2`. Left out, the newest every host has.
    #[serde(default)]
    version: Option<String>,
}

/// Move to a newer minor release of the same major, which the hosts already
/// have installed: standbys restart first, then a switchover, then the old
/// primary.
pub async fn upgrade(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    body: Option<JsonBody<Upgrade>>,
) -> Result<(StatusCode, Json<Operation>), ApiError> {
    let wanted = body.and_then(|JsonBody(b)| b.version);
    let (_, spec) = spec_of(&state, &id).await?;
    let nodes: Vec<(String, Option<String>, sqlx::types::Json<serde_json::Value>)> =
        sqlx::query_as(
            "SELECT h.name, n.server_version, h.postgres_versions \
             FROM nodes n JOIN hosts h ON h.id = n.host_id \
             WHERE n.database_id = $1 AND NOT n.retracted",
        )
        .bind(&id)
        .fetch_all(&state.pool)
        .await?;
    let mut installed = Vec::new();
    for (host, _, versions) in &nodes {
        let Some(version) = versions.0[&spec.version].as_str() else {
            return Err(ApiError::conflict(format!(
                "host {host} has not reported its Postgres {} version",
                spec.version
            )));
        };
        installed.push(version.to_string());
    }
    let newest_everywhere = installed
        .iter()
        .min_by(|a, b| compare_versions(a, b))
        .cloned()
        .ok_or_else(|| ApiError::conflict("the database has no nodes"))?;
    let target = match wanted {
        Some(version) => {
            if version.split('.').next() != Some(spec.version.as_str()) {
                return Err(ApiError::invalid(format!(
                    "{version} is not a release of Postgres {}. Major upgrades \
                     restore into a new database",
                    spec.version
                )));
            }
            if let Some(((host, ..), _)) = nodes
                .iter()
                .zip(&installed)
                .find(|(_, have)| **have != version)
            {
                return Err(ApiError::conflict(format!(
                    "host {host} does not have Postgres {version} installed"
                )));
            }
            version
        }
        None => newest_everywhere,
    };
    if nodes
        .iter()
        .all(|(_, running, _)| running.as_deref() == Some(target.as_str()))
    {
        return Err(ApiError::conflict(format!(
            "every node already runs {target}. Install a newer release on its hosts first"
        )));
    }
    if let Some((_, Some(running), _)) = nodes.iter().find(|(_, running, _)| {
        running
            .as_deref()
            .is_some_and(|r| compare_versions(r, &target).is_gt())
    }) {
        return Err(ApiError::conflict(format!(
            "a node runs {running}, newer than {target}: a minor release cannot go back"
        )));
    }
    start(
        &state,
        &id,
        Kind::Upgrade,
        json!({ "target_version": target }),
    )
    .await
}

/// Orders `18.2` before `18.10`.
fn compare_versions(a: &str, b: &str) -> std::cmp::Ordering {
    let parts = |v: &str| -> Vec<u64> {
        v.split(|c: char| !c.is_ascii_digit())
            .filter(|p| !p.is_empty())
            .map(|p| p.parse().unwrap_or(0))
            .collect()
    };
    parts(a).cmp(&parts(b))
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
    let operation = start_replacement(&state, &mut tx, &id, &node, &request.host_id).await?;
    tx.commit().await?;
    Ok((StatusCode::ACCEPTED, Json(operation)))
}

/// Start replacing `node` with a new node on `wanted_host`, by ID or name. A
/// host drain uses this too.
pub async fn start_replacement(
    state: &AdminState,
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
    node: &str,
    wanted_host: &str,
) -> Result<Operation, ApiError> {
    let (status, spec) = lock(tx, id).await?;
    require_serving(&status)?;
    not_a_replica(&spec)?;
    if !spec.ha {
        return Err(ApiError::conflict(
            "a one-node database is restored from backup, not replaced",
        ));
    }
    let busy: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM operations \
                        WHERE database_id = $1 AND status IN ('pending', 'running'))",
    )
    .bind(id)
    .fetch_one(&mut **tx)
    .await?;
    if busy {
        return Err(ApiError::conflict("another operation is running"));
    }

    let nodes: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT n.node, h.id, h.failure_domain FROM nodes n JOIN hosts h ON h.id = n.host_id \
         WHERE n.database_id = $1 AND NOT n.retracted",
    )
    .bind(id)
    .fetch_all(&mut **tx)
    .await?;
    if !nodes.iter().any(|(name, ..)| name == node) {
        return Err(ApiError::not_found("node"));
    }

    let hosts = host_loads(tx, true).await?;
    let host = hosts
        .iter()
        .find(|h| (h.id == wanted_host || h.name == wanted_host) && h.online);
    let Some((host, Some(address))) = host.map(|h| (h, h.address.clone())) else {
        return Err(ApiError::invalid(format!(
            "host {wanted_host:?} is not online"
        )));
    };
    if host.draining {
        return Err(ApiError::invalid(format!(
            "host {wanted_host:?} is draining"
        )));
    }
    let host_id = host.id.clone();
    let new_domain = host.domain();
    for (name, peer_host, peer_domain) in &nodes {
        if name == node {
            continue;
        }
        let peer_domain = peer_domain.as_ref().unwrap_or(peer_host);
        if *peer_host == host_id || *peer_domain == new_domain {
            return Err(ApiError::invalid(format!(
                "host {wanted_host:?} shares a failure domain with {name}"
            )));
        }
    }
    if nodes.iter().any(|(_, peer_host, _)| *peer_host == host_id) {
        return Err(ApiError::invalid(format!(
            "host {wanted_host:?} already runs this node"
        )));
    }
    if !host.has_room(&state.scheduler, &spec.resources) {
        let free = host
            .free(&state.scheduler)
            .map(|free| format!(": it has {} free", describe(&free)))
            .unwrap_or_default();
        return Err(ApiError::invalid(format!(
            "host {wanted_host:?} has no room for {}{free}",
            describe(&spec.resources)
        )));
    }

    let sealed: Option<Vec<u8>> = sqlx::query_scalar("SELECT secrets FROM databases WHERE id = $1")
        .bind(id)
        .fetch_one(&mut **tx)
        .await?;
    let sealed = sealed.ok_or_else(|| anyhow::anyhow!("the database has no secrets"))?;
    let secrets: DatabaseSecrets = state.secrets.open(id, &sealed)?;
    let ca = Ca::from_pem(&secrets.ca_cert_pem, &secrets.ca_key_pem)?;

    let number = nodes
        .iter()
        .map(|(n, ..)| node_number(n))
        .max()
        .unwrap_or(0)
        + 1;
    let new_node = format!("n{number}");
    let added = add_node(state, tx, id, &new_node, &host_id, &address, "seed", &ca).await?;
    let Some(port_base) = added else {
        return Err(ApiError::conflict(format!(
            "host {wanted_host:?} has no free ports"
        )));
    };
    let generation: i64 = sqlx::query_scalar(
        "UPDATE databases SET generation = generation + 1, updated_at = now() \
         WHERE id = $1 RETURNING generation",
    )
    .bind(id)
    .fetch_one(&mut **tx)
    .await?;
    bump_hosts(tx, id).await?;
    let addr = |offset: i32| format!("{address}:{}", port_base + offset);
    let operation = operations::create_with(
        tx,
        id,
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
    Ok(operation)
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
    if has_replicas(&mut tx, &id).await? {
        return Err(ApiError::conflict(
            "promote or delete this database's replicas first",
        ));
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
    server_version: Option<String>,
}

pub async fn list_nodes(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    page: Page,
) -> Result<Json<List<Node>>, ApiError> {
    fetch(&state, &id).await?;
    let rows = sqlx::query_as::<_, Node>(
        "SELECT n.node, n.host_id, h.name AS host_name, n.port_base, n.observed_generation, \
                n.state, n.error, n.server_version \
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

#[cfg(test)]
mod tests {
    use super::compare_versions;

    #[test]
    fn versions_compare_by_number() {
        assert!(compare_versions("18.2", "18.10").is_lt());
        assert!(compare_versions("18.1", "18.1").is_eq());
        assert!(compare_versions("17.9", "18.0").is_lt());
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Certificate {
    certificate_pem: String,
    private_key_pem: String,
}

/// Have every node's proxy present this certificate, such as one from a public
/// CA for the database's own name. Proxies load it with no restart, and Raft,
/// agents and Postgres keep the database's own CA.
pub async fn put_certificate(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    JsonBody(request): JsonBody<Certificate>,
) -> Result<(StatusCode, Json<Operation>), ApiError> {
    crate::tls::check_certificate(&request.certificate_pem, &request.private_key_pem)
        .map_err(|e| ApiError::invalid(format!("{e:#}")))?;
    let mut tx = state.pool.begin().await?;
    let (status, spec) = lock(&mut tx, &id).await?;
    require_serving(&status)?;
    not_a_replica(&spec)?;
    let sealed: Option<Vec<u8>> = sqlx::query_scalar("SELECT secrets FROM databases WHERE id = $1")
        .bind(&id)
        .fetch_one(&mut *tx)
        .await?;
    let sealed = sealed.ok_or_else(|| anyhow::anyhow!("the database has no secrets"))?;
    let mut secrets: DatabaseSecrets = state.secrets.open(&id, &sealed)?;
    secrets.proxy_certificate = Some(crate::secrets::ProxyCertificate {
        cert_pem: request.certificate_pem,
        key_pem: request.private_key_pem,
    });
    let generation: i64 = sqlx::query_scalar(
        "UPDATE databases SET secrets = $2, generation = generation + 1, updated_at = now() \
         WHERE id = $1 RETURNING generation",
    )
    .bind(&id)
    .bind(state.secrets.seal(&id, &secrets)?)
    .fetch_one(&mut *tx)
    .await?;
    bump_hosts(&mut tx, &id).await?;
    let operation = operations::create(&mut tx, &id, Kind::Update, generation).await?;
    tx.commit().await?;
    Ok((StatusCode::ACCEPTED, Json(operation)))
}
