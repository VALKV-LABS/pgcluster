//! The agent API. `vk-agent host` calls it, and a host is identified only by
//! its client certificate.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use axum::async_trait;
use axum::extract::rejection::QueryRejection;
use axum::extract::{FromRequest, FromRequestParts, Path, Query, State};
use axum::http::request::Parts;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::FromRow;

use crate::error::ApiError;
use crate::model::{DatabaseSpec, DesiredState, Instance, InstanceSecrets, Report};
use crate::operations;
use crate::secrets::{DatabaseSecrets, NodeSecrets};
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
        .route("/agent/v1/enroll", post(enroll))
        .route("/agent/v1/certificate", post(rotate_certificate))
        .route("/agent/v1/desired-state", get(desired_state))
        .route("/agent/v1/report", post(report))
        .route("/agent/v1/operations/:id/log", post(append_log))
        .with_state(state)
}

#[derive(FromRequest)]
#[from_request(via(axum::Json), rejection(ApiError))]
struct Body<T>(T);

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
        state.ca.cert_pem().to_string(),
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
    let certificate_pem = state
        .ca
        .sign_host_csr(&request.csr_pem, &host_id)
        .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, "invalid_csr", e.to_string()))?;
    sqlx::query("UPDATE hosts SET enrolled_at = now() WHERE id = $1")
        .bind(&host_id)
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
    let certificate_pem = state
        .ca
        .sign_host_csr(&request.csr_pem, &host_id)
        .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, "invalid_csr", e.to_string()))?;
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
                n.secrets AS node_secrets, d.generation, d.spec, \
                d.secrets AS database_secrets, \
                (SELECT o.id FROM operations o \
                 WHERE o.database_id = d.id AND o.status IN ('pending', 'running') \
                 ORDER BY o.seq LIMIT 1) AS operation_id \
         FROM nodes n JOIN databases d ON d.id = n.database_id \
         WHERE n.host_id = $1 AND NOT n.retracted \
         ORDER BY n.database_id, n.node",
    )
    .bind(host_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;

    let mut instances = Vec::with_capacity(rows.len());
    for row in rows {
        let spec = row.spec.0;
        let sealed = row
            .database_secrets
            .ok_or_else(|| anyhow::anyhow!("database {} has no secrets", row.database_id))?;
        let database: DatabaseSecrets = state.secrets.open(&row.database_id, &sealed)?;
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
            peers: vec![],
            bootstrap: "initdb".into(),
            replication_mode: spec.replication_mode,
            allowed_cidrs: spec.allowed_cidrs,
            extensions: spec.extensions,
            owner: spec.owner,
            secrets: InstanceSecrets::merge(database, node),
        });
    }
    Ok(DesiredState {
        generation,
        instances,
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
                          last_seen_at = now() \
         WHERE id = $1",
    )
    .bind(&host_id)
    .bind(report.observed_generation)
    .bind(&report.version)
    .bind(&report.address)
    .bind(sqlx::types::Json(&report.capacity))
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
                                      error = $5 \
                     WHERE database_id = $1 AND node = $2",
                )
                .bind(&database_id)
                .bind(&node)
                .bind(instance.observed_generation)
                .bind(&instance.state)
                .bind(&instance.error)
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
