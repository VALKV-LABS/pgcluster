//! What each database used over a period, for billing by a layer above.
//!
//! Node time comes from `usage_intervals`, which triggers keep as nodes are
//! added, resized and removed. Backup storage is what the repository holds
//! now, so a deleted database is billed for its kept backups until they are
//! purged.

use axum::extract::{Query, State};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

use super::{List, Page};
use crate::error::ApiError;
use crate::state::AdminState;

/// Hours in an average month, to turn GB-hours into GB-months.
const HOURS_PER_MONTH: f64 = 730.0;

#[derive(Debug, Deserialize)]
pub struct UsageQuery {
    from: DateTime<Utc>,
    /// Left out, the period runs to now.
    #[serde(default)]
    to: Option<DateTime<Utc>>,
    #[serde(default)]
    database_id: Option<String>,
    #[serde(default)]
    workspace: Option<String>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct DatabaseUsage {
    database_id: String,
    name: String,
    workspace: Option<String>,
    /// `database`, or `replica` for an external standby, which bills apart.
    kind: String,
    status: String,
    /// Summed over nodes: three nodes for an hour is three node-hours.
    node_hours: f64,
    vcpu_hours: f64,
    memory_gb_hours: f64,
    storage_gb_hours: f64,
    #[sqlx(skip)]
    storage_gb_months: f64,
    /// Backups the repository holds now, as WAL-G sizes them. WAL is not
    /// counted.
    backup_bytes: i64,
}

#[derive(Debug, Serialize)]
pub struct Usage {
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    #[serde(flatten)]
    list: List<DatabaseUsage>,
}

pub async fn usage(
    State(state): State<AdminState>,
    page: Page,
    Query(query): Query<UsageQuery>,
) -> Result<Json<Usage>, ApiError> {
    let now = Utc::now();
    let to = query.to.unwrap_or(now).min(now);
    if query.from >= to {
        return Err(ApiError::invalid("from must be before to, and before now"));
    }
    let mut rows = sqlx::query_as::<_, DatabaseUsage>(
        "WITH spans AS ( \
             SELECT u.database_id, u.vcpu, u.memory_gb, u.storage_gb, \
                    extract(epoch FROM least(coalesce(u.ended_at, $2), $2) \
                                     - greatest(u.started_at, $1))::float8 / 3600 AS hours \
             FROM usage_intervals u \
             WHERE u.started_at < $2 AND coalesce(u.ended_at, $2) > $1), \
         used AS ( \
             SELECT database_id, sum(hours) AS node_hours, sum(hours * vcpu) AS vcpu_hours, \
                    sum(hours * memory_gb) AS memory_gb_hours, \
                    sum(hours * storage_gb) AS storage_gb_hours \
             FROM spans GROUP BY database_id), \
         held AS ( \
             SELECT database_id, sum(size_bytes)::bigint AS backup_bytes \
             FROM backups GROUP BY database_id) \
         SELECT d.id AS database_id, d.name, d.workspace, d.status, \
                CASE WHEN d.spec->'replica_of' IS NOT NULL THEN 'replica' \
                     ELSE 'database' END AS kind, \
                coalesce(used.node_hours, 0) AS node_hours, \
                coalesce(used.vcpu_hours, 0) AS vcpu_hours, \
                coalesce(used.memory_gb_hours, 0) AS memory_gb_hours, \
                coalesce(used.storage_gb_hours, 0) AS storage_gb_hours, \
                coalesce(held.backup_bytes, 0) AS backup_bytes \
         FROM databases d \
         LEFT JOIN used ON used.database_id = d.id \
         LEFT JOIN held ON held.database_id = d.id \
         WHERE (used.database_id IS NOT NULL OR held.database_id IS NOT NULL) \
           AND ($3::text IS NULL OR d.id = $3) \
           AND ($4::text IS NULL OR d.workspace = $4) \
           AND d.id > $5 \
         ORDER BY d.id LIMIT $6",
    )
    .bind(query.from)
    .bind(to)
    .bind(&query.database_id)
    .bind(&query.workspace)
    .bind(page.after())
    .bind(page.fetch_limit())
    .fetch_all(&state.pool)
    .await?;
    for row in &mut rows {
        row.storage_gb_months = row.storage_gb_hours / HOURS_PER_MONTH;
    }
    Ok(Json(Usage {
        from: query.from,
        to,
        list: List::from_rows(rows, &page, |u: &DatabaseUsage| u.database_id.clone()),
    }))
}
