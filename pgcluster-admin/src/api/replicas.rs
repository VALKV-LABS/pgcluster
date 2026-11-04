//! Cross-region replicas: external standbys of a database, each on a host
//! outside the source's failure domains. A replica follows the source
//! asynchronously, takes no part in its Raft group or failover, and `promote`
//! detaches it into a one-node database of its own.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::FromRow;

use super::databases::{check_capacity, lock, not_a_replica, require_serving};
use super::{JsonBody, List, Page};
use crate::error::ApiError;
use crate::model::{check_database_name, CreateReplica, ReplicaOf};
use crate::operations::{self, Kind, Operation};
use crate::state::AdminState;

#[derive(Debug, Serialize, FromRow)]
pub struct Replica {
    id: String,
    name: String,
    source_id: String,
    status: String,
    status_reason: Option<String>,
    host_id: Option<String>,
    host_name: Option<String>,
    created_at: DateTime<Utc>,
}

const REPLICA_QUERY: &str =
    "SELECT d.id, d.name, d.spec->'replica_of'->>'database_id' AS source_id, \
        d.status, d.status_reason, n.host_id, h.name AS host_name, d.created_at \
    FROM databases d \
    LEFT JOIN nodes n ON n.database_id = d.id AND NOT n.retracted \
    LEFT JOIN hosts h ON h.id = n.host_id \
    WHERE d.spec->'replica_of'->>'database_id' = $1 AND d.status <> 'deleted'";

/// Start a replica of database `id`. It needs a three-node source, whose nodes
/// are reachable from other hosts; a one-node database listens on loopback.
pub async fn create(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    JsonBody(request): JsonBody<CreateReplica>,
) -> Result<(StatusCode, Json<Operation>), ApiError> {
    check_database_name(&request.name)?;
    let mut tx = state.pool.begin().await?;
    let (status, source) = lock(&mut tx, &id).await?;
    require_serving(&status)?;
    not_a_replica(&source)?;
    if !source.ha {
        return Err(ApiError::conflict(
            "a replica follows a three-node database. Restore a one-node database into \
             another region instead",
        ));
    }
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM databases WHERE spec->'replica_of'->>'database_id' = $1",
    )
    .bind(&id)
    .fetch_one(&mut *tx)
    .await?;
    let mut spec = source.clone();
    spec.ha = false;
    spec.replication_mode = "async".into();
    spec.hosts = request.host_id.into_iter().collect();
    spec.restore = None;
    spec.replica_of = Some(ReplicaOf {
        database_id: id.clone(),
        // Unique within the source's topology, and safe in a slot name.
        node_id: format!("r{}", count + 1),
    });
    super::databases::check_placement(&state, &spec.hosts).await?;
    check_capacity(&state, &spec).await?;

    let replica_id = uuid::Uuid::new_v4().to_string();
    let inserted = sqlx::query(
        "INSERT INTO databases (id, name, workspace, status, spec) \
         SELECT $1, $2, workspace, 'provisioning', $3 FROM databases WHERE id = $4 \
         ON CONFLICT (name) WHERE status <> 'deleted' DO NOTHING",
    )
    .bind(&replica_id)
    .bind(&request.name)
    .bind(sqlx::types::Json(&spec))
    .bind(&id)
    .execute(&mut *tx)
    .await?;
    if inserted.rows_affected() == 0 {
        return Err(ApiError::conflict(format!(
            "a database named {:?} exists",
            request.name
        )));
    }
    let operation = operations::create(&mut tx, &replica_id, Kind::CreateReplica, 1).await?;
    tx.commit().await?;
    Ok((StatusCode::ACCEPTED, Json(operation)))
}

pub async fn list(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    page: Page,
) -> Result<Json<List<Replica>>, ApiError> {
    super::databases::spec_of(&state, &id).await?;
    let rows = sqlx::query_as::<_, Replica>(&format!(
        "{REPLICA_QUERY} AND d.id > $2 ORDER BY d.id LIMIT $3"
    ))
    .bind(&id)
    .bind(page.after())
    .bind(page.fetch_limit())
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(List::from_rows(rows, &page, |r: &Replica| {
        r.id.clone()
    })))
}

/// The replica, checked to belong to `id`.
async fn replica_of(state: &AdminState, id: &str, replica_id: &str) -> Result<(), ApiError> {
    let found: Option<String> = sqlx::query_scalar(&format!(
        "SELECT d.id FROM ({REPLICA_QUERY} AND d.id = $2) d"
    ))
    .bind(id)
    .bind(replica_id)
    .fetch_optional(&state.pool)
    .await?;
    found
        .map(|_| ())
        .ok_or_else(|| ApiError::not_found("replica"))
}

/// Delete a replica. The source's cluster forgets it first.
pub async fn delete(
    State(state): State<AdminState>,
    Path((id, replica_id)): Path<(String, String)>,
) -> Result<(StatusCode, Json<Operation>), ApiError> {
    replica_of(&state, &id, &replica_id).await?;
    super::databases::delete(State(state), Path(replica_id)).await
}

/// Detach a replica into an independent one-node database. It keeps what it
/// had replayed, so it may be behind the source by what was in flight. It takes
/// its first backup before clients reach it, as any new database does.
pub async fn promote(
    State(state): State<AdminState>,
    Path((id, replica_id)): Path<(String, String)>,
) -> Result<(StatusCode, Json<Operation>), ApiError> {
    replica_of(&state, &id, &replica_id).await?;
    let mut tx = state.pool.begin().await?;
    let (status, _) = lock(&mut tx, &replica_id).await?;
    require_serving(&status)?;
    let busy: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM operations \
                        WHERE database_id = $1 AND status IN ('pending', 'running'))",
    )
    .bind(&replica_id)
    .fetch_one(&mut *tx)
    .await?;
    if busy {
        return Err(ApiError::conflict("another operation is running"));
    }
    sqlx::query("UPDATE databases SET status = 'provisioning', updated_at = now() WHERE id = $1")
        .bind(&replica_id)
        .execute(&mut *tx)
        .await?;
    let generation: i64 = sqlx::query_scalar("SELECT generation FROM databases WHERE id = $1")
        .bind(&replica_id)
        .fetch_one(&mut *tx)
        .await?;
    let operation =
        operations::create(&mut tx, &replica_id, Kind::PromoteReplica, generation).await?;
    tx.commit().await?;
    Ok((StatusCode::ACCEPTED, Json(operation)))
}
