//! The agent API. `vk-agent host` calls it, and a host is identified only by
//! its client certificate.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use axum::async_trait;
use axum::extract::rejection::QueryRejection;
use axum::extract::{FromRequest, FromRequestParts, Path, Query, State};
use axum::http::request::Parts;
use axum::http::{header, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::FromRow;

use crate::error::ApiError;
use crate::model::{
    BackupTarget, ClusterReport, DatabaseSpec, DesiredState, Instance, InstanceBackup,
    InstanceRestore, InstanceSecrets, Peer, Report, RepositoryCheck, RepositoryCheckResult,
    RepositoryConfig, Task, TaskResult,
};
use crate::operations::{self, event};
use crate::reconciler::bump_hosts;
use crate::secrets::{DatabaseSecrets, NodeSecrets, RepositoryCredentials};
use crate::state::AdminState;
use crate::tls::PeerHost;

const DEFAULT_WAIT_SECONDS: u64 = 25;
const MAX_WAIT_SECONDS: u64 = 60;
const POLL_EVERY: Duration = Duration::from_millis(500);
/// A log line is step output, not a place to store files.
const MAX_LOG_BYTES: usize = 8192;

pub fn router(state: AdminState) -> Router {
    Router::new()
        .route("/agent/v1/ca", get(get_ca))
        .route(
            "/agent/v1/enroll",
            post(enroll).route_layer(middleware::from_fn_with_state(
                state.clone(),
                limit_open_route,
            )),
        )
        .route("/agent/v1/certificate", post(rotate_certificate))
        .route("/agent/v1/desired-state", get(desired_state))
        .route("/agent/v1/report", post(report))
        .route("/agent/v1/operations/:id/log", post(append_log))
        .route("/agent/v1/tasks/:id/result", post(task_result))
        .route("/agent/v1/repository-checks/:id", post(repository_check))
        .route("/agent/v1/cluster", post(cluster_report))
        .with_state(state)
}

#[derive(FromRequest)]
#[from_request(via(axum::Json), rejection(ApiError))]
struct Body<T>(T);

/// Anyone can reach a route that takes no credentials, so these routes share a
/// few slots. A flood fails fast here instead of queuing for the database, and
/// enrolled hosts and operators keep their connections.
pub async fn limit_open_route(
    State(state): State<AdminState>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    match state.open_routes.clone().try_acquire_owned() {
        Ok(_permit) => next.run(request).await,
        Err(_) => (
            [(header::RETRY_AFTER, "1")],
            ApiError::new(
                StatusCode::TOO_MANY_REQUESTS,
                "too_many_requests",
                "too many requests, try again shortly",
            ),
        )
            .into_response(),
    }
}

/// The enrolled host making this call. A deleted host fails here, which is what
/// revokes it, even though its certificate has not expired.
struct Host(String);

#[async_trait]
impl FromRequestParts<AdminState> for Host {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AdminState,
    ) -> Result<Self, Self::Rejection> {
        let claimed = parts
            .extensions
            .get::<PeerHost>()
            .and_then(|peer| peer.0.clone())
            .ok_or_else(|| ApiError::unauthorized("a client certificate is required"))?;
        let known: Option<String> =
            sqlx::query_scalar("SELECT id FROM hosts WHERE id = $1 AND enrolled_at IS NOT NULL")
                .bind(&claimed)
                .fetch_optional(&state.pool)
                .await?;
        known
            .map(Host)
            .ok_or_else(|| ApiError::unauthorized("this host is not enrolled"))
    }
}

async fn get_ca(State(state): State<AdminState>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/x-pem-file")],
        state.ca().cert_pem().to_string(),
    )
}

#[derive(Deserialize)]
struct EnrollRequest {
    token: String,
    csr_pem: String,
}

#[derive(Serialize)]
struct EnrollResponse {
    host_id: String,
    certificate_pem: String,
}

pub fn token_hash(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

async fn enroll(
    State(state): State<AdminState>,
    Body(request): Body<EnrollRequest>,
) -> Result<Json<EnrollResponse>, ApiError> {
    let mut tx = state.pool.begin().await?;
    // One statement, so two callers with one token cannot both win.
    let host_id: Option<String> = sqlx::query_scalar(
        "UPDATE enrollment_tokens SET used_at = now() \
         WHERE token_hash = $1 AND used_at IS NULL AND expires_at > now() \
         RETURNING host_id",
    )
    .bind(token_hash(&request.token))
    .fetch_optional(&mut *tx)
    .await?;
    let Some(host_id) = host_id else {
        return Err(ApiError::unauthorized(
            "the token is invalid, expired or already used",
        ));
    };
    // A bad request rolls back, so a typo does not burn the token.
    let ca = state.ca();
    let certificate_pem = ca
        .sign_host_csr(&request.csr_pem, &host_id)
        .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, "invalid_csr", e.to_string()))?;
    sqlx::query("UPDATE hosts SET enrolled_at = now(), certificate_ca = $2 WHERE id = $1")
        .bind(&host_id)
        .bind(ca.fingerprint())
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    tracing::info!(host_id, "host enrolled");
    Ok(Json(EnrollResponse {
        host_id,
        certificate_pem,
    }))
}

#[derive(Deserialize)]
struct RotateRequest {
    csr_pem: String,
}

#[derive(Serialize)]
struct RotateResponse {
    certificate_pem: String,
}

async fn rotate_certificate(
    State(state): State<AdminState>,
    Host(host_id): Host,
    Body(request): Body<RotateRequest>,
) -> Result<Json<RotateResponse>, ApiError> {
    let ca = state.ca();
    let certificate_pem = ca
        .sign_host_csr(&request.csr_pem, &host_id)
        .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, "invalid_csr", e.to_string()))?;
    sqlx::query("UPDATE hosts SET certificate_ca = $2 WHERE id = $1")
        .bind(&host_id)
        .bind(ca.fingerprint())
        .execute(&state.pool)
        .await?;
    Ok(Json(RotateResponse { certificate_pem }))
}

#[derive(Deserialize)]
struct DesiredStateQuery {
    #[serde(default)]
    generation: i64,
    #[serde(default)]
    wait_seconds: Option<u64>,
}

struct Query2<T>(T);

#[async_trait]
impl<T: serde::de::DeserializeOwned + Send> FromRequestParts<AdminState> for Query2<T> {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AdminState,
    ) -> Result<Self, Self::Rejection> {
        let Query(value) = Query::<T>::from_request_parts(parts, state)
            .await
            .map_err(|e: QueryRejection| ApiError::from(e))?;
        Ok(Self(value))
    }
}

async fn desired_state(
    State(state): State<AdminState>,
    Host(host_id): Host,
    Query2(query): Query2<DesiredStateQuery>,
) -> Result<Response, ApiError> {
    let wait = query
        .wait_seconds
        .unwrap_or(DEFAULT_WAIT_SECONDS)
        .min(MAX_WAIT_SECONDS);
    let deadline = Instant::now() + Duration::from_secs(wait);
    loop {
        let generation: i64 = sqlx::query_scalar("SELECT generation FROM hosts WHERE id = $1")
            .bind(&host_id)
            .fetch_one(&state.pool)
            .await?;
        if generation != query.generation {
            return Ok(Json(build_desired_state(&state, &host_id).await?).into_response());
        }
        if Instant::now() >= deadline {
            return Ok(StatusCode::NOT_MODIFIED.into_response());
        }
        tokio::time::sleep(POLL_EVERY).await;
    }
}

#[derive(FromRow)]
struct InstanceRow {
    database_id: String,
    database_name: String,
    node: String,
    port_base: i32,
    node_secrets: Vec<u8>,
    generation: i64,
    spec: sqlx::types::Json<DatabaseSpec>,
    database_secrets: Option<Vec<u8>>,
    operation_id: Option<String>,
    bootstrap: String,
    address: Option<String>,
    access_open: bool,
}

/// What a database's nodes need to know about its repository.
async fn repository(
    state: &AdminState,
    repository_id: &str,
) -> Result<(String, RepositoryConfig, RepositoryCredentials), ApiError> {
    let row: Option<(String, sqlx::types::Json<RepositoryConfig>, Option<Vec<u8>>)> =
        sqlx::query_as("SELECT type, config, credentials FROM backup_repositories WHERE id = $1")
            .bind(repository_id)
            .fetch_optional(&state.pool)
            .await?;
    let (kind, config, sealed) = row.ok_or_else(|| anyhow::anyhow!("no repository"))?;
    let credentials = match sealed {
        Some(sealed) => state
            .secrets
            .open(&format!("repository/{repository_id}"), &sealed)?,
        None => RepositoryCredentials::default(),
    };
    Ok((kind, config.0, credentials))
}

async fn checks_for(state: &AdminState, host_id: &str) -> Result<Vec<RepositoryCheck>, ApiError> {
    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM backup_repositories WHERE check_host_id = $1 AND status = 'verifying'",
    )
    .bind(host_id)
    .fetch_all(&state.pool)
    .await?;
    let mut checks = Vec::with_capacity(ids.len());
    for id in ids {
        let (kind, config, credentials) = repository(state, &id).await?;
        checks.push(RepositoryCheck {
            target: BackupTarget {
                kind,
                config,
                credentials,
                encryption_key: None,
            },
            id,
        });
    }
    Ok(checks)
}

/// Read generation and instances in one statement's snapshot, so a host never
/// records a generation newer than the state it was sent.
pub async fn build_desired_state(
    state: &AdminState,
    host_id: &str,
) -> Result<DesiredState, ApiError> {
    let mut tx = state.pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    let generation: i64 = sqlx::query_scalar("SELECT generation FROM hosts WHERE id = $1")
        .bind(host_id)
        .fetch_one(&mut *tx)
        .await?;
    let rows = sqlx::query_as::<_, InstanceRow>(
        "SELECT n.database_id, d.name AS database_name, n.node, n.port_base, \
                n.secrets AS node_secrets, d.generation, d.spec, n.bootstrap, h.address, \
                d.access_open, d.secrets AS database_secrets, \
                (SELECT o.id FROM operations o \
                 WHERE o.database_id = d.id AND o.status IN ('pending', 'running') \
                 ORDER BY o.seq LIMIT 1) AS operation_id \
         FROM nodes n JOIN databases d ON d.id = n.database_id \
         JOIN hosts h ON h.id = n.host_id \
         WHERE n.host_id = $1 AND NOT n.retracted \
         ORDER BY n.database_id, n.node",
    )
    .bind(host_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    let repository_checks = checks_for(state, host_id).await?;
    let admin_ca = crate::rotation::for_hosts(state).await?;

    let mut instances = Vec::with_capacity(rows.len());
    for row in rows {
        let spec = row.spec.0;
        let sealed = row
            .database_secrets
            .ok_or_else(|| anyhow::anyhow!("database {} has no secrets", row.database_id))?;
        let database: DatabaseSecrets = state.secrets.open(&row.database_id, &sealed)?;
        let (kind, config, credentials) = repository(state, &spec.backup.repository_id).await?;
        let target = |id: &str, key: String| {
            BackupTarget::for_database(&kind, config.clone(), credentials.clone(), id, key)
        };
        let backup = InstanceBackup {
            target: target(&row.database_id, database.backup_key.clone()),
            retain_full: spec.backup.retain_full(),
        };
        let restore = spec.restore.as_ref().map(|source| InstanceRestore {
            source: target(
                &source.database_id,
                database.source_backup_key.clone().unwrap_or_default(),
            ),
            target_time: Some(source.target_time.clone()),
            source_database: source.database_name.clone(),
        });
        let peers: Vec<(String, Option<String>, i32)> = sqlx::query_as(
            "SELECT n.node, h.address, n.port_base FROM nodes n JOIN hosts h ON h.id = n.host_id \
             WHERE n.database_id = $1 AND n.node <> $2 AND NOT n.retracted ORDER BY n.node",
        )
        .bind(&row.database_id)
        .bind(&row.node)
        .fetch_all(&state.pool)
        .await?;
        let peers = peers
            .into_iter()
            .filter_map(|(node, address, port_base)| {
                address.map(|address| Peer {
                    node,
                    address,
                    port_base,
                })
            })
            .collect();
        let tasks = sqlx::query_as::<_, Task>(
            "SELECT id, kind, run_on, params FROM tasks \
             WHERE database_id = $1 AND status = 'pending' ORDER BY created_at",
        )
        .bind(&row.database_id)
        .fetch_all(&state.pool)
        .await?;
        let node: NodeSecrets = state.secrets.open(
            &format!("{}/{}", row.database_id, row.node),
            &row.node_secrets,
        )?;
        instances.push(Instance {
            database_id: row.database_id,
            database_name: row.database_name,
            node: row.node,
            generation: row.generation,
            operation_id: row.operation_id,
            postgres_version: spec.version,
            resources: spec.resources,
            port_base: row.port_base,
            address: row.address.unwrap_or_default(),
            peers,
            bootstrap: row.bootstrap,
            restore,
            backup,
            tasks,
            replication_mode: spec.replication_mode,
            // Clients get no access until a restorable backup exists.
            allowed_cidrs: if row.access_open {
                spec.allowed_cidrs
            } else {
                vec![]
            },
            extensions: spec.extensions,
            owner: spec.owner,
            secrets: InstanceSecrets::merge(database, node),
        });
    }
    Ok(DesiredState {
        generation,
        instances,
        repository_checks,
        admin_ca,
    })
}

async fn report(
    State(state): State<AdminState>,
    Host(host_id): Host,
    Body(report): Body<Report>,
) -> Result<StatusCode, ApiError> {
    crate::model::check_host_name(&report.address)
        .map_err(|_| ApiError::invalid("address is not a hostname or IP address"))?;
    let mut tx = state.pool.begin().await?;
    sqlx::query(
        "UPDATE hosts SET observed_generation = $2, version = $3, address = $4, capacity = $5, \
                          ca_fingerprint = $6, last_seen_at = now() \
         WHERE id = $1",
    )
    .bind(&host_id)
    .bind(report.observed_generation)
    .bind(&report.version)
    .bind(&report.address)
    .bind(sqlx::types::Json(&report.capacity))
    .bind(&report.ca_fingerprint)
    .execute(&mut *tx)
    .await?;

    let reported: HashMap<(&str, &str), _> = report
        .instances
        .iter()
        .map(|i| ((i.database_id.as_str(), i.node.as_str()), i))
        .collect();
    let nodes: Vec<(String, String)> =
        sqlx::query_as("SELECT database_id, node FROM nodes WHERE host_id = $1 FOR UPDATE")
            .bind(&host_id)
            .fetch_all(&mut *tx)
            .await?;
    for (database_id, node) in nodes {
        match reported.get(&(database_id.as_str(), node.as_str())) {
            Some(instance) => {
                sqlx::query(
                    "UPDATE nodes SET present = true, observed_generation = $3, state = $4, \
                                      error = $5, archived_until = $6 \
                     WHERE database_id = $1 AND node = $2",
                )
                .bind(&database_id)
                .bind(&node)
                .bind(instance.observed_generation)
                .bind(&instance.state)
                .bind(&instance.error)
                .bind(instance.archived_until)
                .execute(&mut *tx)
                .await?;
            }
            None => {
                sqlx::query(
                    "UPDATE nodes SET present = false WHERE database_id = $1 AND node = $2",
                )
                .bind(&database_id)
                .bind(&node)
                .execute(&mut *tx)
                .await?;
            }
        }
    }
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Whether this host runs a node of this database.
async fn runs_a_node(
    state: &AdminState,
    host_id: &str,
    database_id: &str,
) -> Result<bool, ApiError> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM nodes WHERE host_id = $1 AND database_id = $2)",
    )
    .bind(host_id)
    .bind(database_id)
    .fetch_one(&state.pool)
    .await?)
}

async fn task_result(
    State(state): State<AdminState>,
    Host(host_id): Host,
    Path(task_id): Path<String>,
    Body(result): Body<TaskResult>,
) -> Result<StatusCode, ApiError> {
    let database_id: Option<String> =
        sqlx::query_scalar("SELECT database_id FROM tasks WHERE id = $1")
            .bind(&task_id)
            .fetch_optional(&state.pool)
            .await?;
    let Some(database_id) = database_id else {
        return Err(ApiError::not_found("task"));
    };
    if !runs_a_node(&state, &host_id, &database_id).await? {
        return Err(ApiError::not_found("task"));
    }
    let status = if result.success {
        "succeeded"
    } else {
        "failed"
    };
    let mut tx = state.pool.begin().await?;
    // The first answer stands: a task can run twice, if its node changed hands.
    sqlx::query(
        "UPDATE tasks SET status = $2, error = $3, output = $4, finished_at = now() \
         WHERE id = $1 AND status = 'pending'",
    )
    .bind(&task_id)
    .bind(status)
    .bind(&result.error)
    .bind(sqlx::types::Json(&result.output))
    .execute(&mut *tx)
    .await?;
    bump_hosts(&mut tx, &database_id).await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn repository_check(
    State(state): State<AdminState>,
    Host(host_id): Host,
    Path(id): Path<String>,
    Body(result): Body<RepositoryCheckResult>,
) -> Result<StatusCode, ApiError> {
    let status = if result.error.is_none() {
        "verified"
    } else {
        "failed"
    };
    sqlx::query(
        "UPDATE backup_repositories SET status = $3, error = $4 \
         WHERE id = $1 AND check_host_id = $2 AND status = 'verifying'",
    )
    .bind(&id)
    .bind(&host_id)
    .bind(status)
    .bind(&result.error)
    .execute(&state.pool)
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Record what a cluster's leader says, and turn what changed into events. This
/// is a record: the cluster goes on deciding for itself.
async fn cluster_report(
    State(state): State<AdminState>,
    Host(host_id): Host,
    Body(report): Body<ClusterReport>,
) -> Result<StatusCode, ApiError> {
    if !runs_a_node(&state, &host_id, &report.database_id).await? {
        return Err(ApiError::not_found("database"));
    }
    let id = report.database_id.as_str();
    let mut tx = state.pool.begin().await?;
    let before: Option<sqlx::types::Json<serde_json::Value>> =
        sqlx::query_scalar("SELECT observed FROM databases WHERE id = $1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
    let Some(before) = before.map(|b| b.0) else {
        return Err(ApiError::not_found("database"));
    };

    let seen = before["failover_count"].as_u64().unwrap_or(0) as usize;
    for failover in report.failovers.iter().skip(seen) {
        event(
            &mut tx,
            "failover_completed",
            Some(id),
            None,
            failover.clone(),
        )
        .await?;
    }
    let was_degraded = before["sync_degraded"].as_bool().unwrap_or(false);
    if report.sync_degraded != was_degraded {
        let kind = if report.sync_degraded {
            "sync_degraded"
        } else {
            "sync_restored"
        };
        event(&mut tx, kind, Some(id), None, serde_json::json!({})).await?;
    }
    if let Some(error) = &report.backup_error {
        if before["backup_error"].as_str() != Some(error) {
            let detail = serde_json::json!({ "error": error });
            event(&mut tx, "backup_failed", Some(id), None, detail).await?;
        }
    }

    let known: Vec<String> = sqlx::query_scalar("SELECT id FROM backups WHERE database_id = $1")
        .bind(id)
        .fetch_all(&mut *tx)
        .await?;
    for backup in &report.backups {
        if known.contains(&backup.id) {
            continue;
        }
        sqlx::query(
            "INSERT INTO backups (database_id, id, label, completed_at, size_bytes) \
             VALUES ($1, $2, $3, to_timestamp($4), $5)",
        )
        .bind(id)
        .bind(&backup.id)
        .bind(&backup.label)
        .bind(backup.completed_at as f64)
        .bind(backup.size_bytes)
        .execute(&mut *tx)
        .await?;
        let detail = serde_json::json!({ "backup_id": backup.id, "label": backup.label });
        event(&mut tx, "backup_completed", Some(id), None, detail).await?;
    }
    // Retention took these from the repository, so they can no longer restore.
    let current: Vec<&str> = report.backups.iter().map(|b| b.id.as_str()).collect();
    sqlx::query("DELETE FROM backups WHERE database_id = $1 AND NOT (id = ANY($2))")
        .bind(id)
        .bind(&current)
        .execute(&mut *tx)
        .await?;

    let observed = serde_json::json!({
        "primary": report.primary,
        "sync_degraded": report.sync_degraded,
        "needs_reseed": report.needs_reseed,
        "backup_error": report.backup_error,
        "failover_count": report.failovers.len().max(seen),
    });
    sqlx::query("UPDATE databases SET observed = $2 WHERE id = $1")
        .bind(id)
        .bind(sqlx::types::Json(&observed))
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct LogEntry {
    message: String,
}

async fn append_log(
    State(state): State<AdminState>,
    Host(host_id): Host,
    Path(operation_id): Path<String>,
    Body(entry): Body<LogEntry>,
) -> Result<StatusCode, ApiError> {
    let mut tx = state.pool.begin().await?;
    let allowed: Option<String> = sqlx::query_scalar(
        "SELECT o.id FROM operations o \
         JOIN nodes n ON n.database_id = o.database_id \
         WHERE o.id = $1 AND n.host_id = $2 LIMIT 1",
    )
    .bind(&operation_id)
    .bind(&host_id)
    .fetch_optional(&mut *tx)
    .await?;
    if allowed.is_none() {
        return Err(ApiError::not_found("operation"));
    }
    let message: String = entry
        .message
        .chars()
        .scan(0usize, |bytes, c| {
            *bytes += c.len_utf8();
            (*bytes <= MAX_LOG_BYTES).then_some(c)
        })
        .collect();
    operations::log(&mut tx, &operation_id, &format!("host {host_id}"), &message).await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}
