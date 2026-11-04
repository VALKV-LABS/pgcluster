//! Who changed what. Every request through the admin API that is not a read is
//! recorded with the name of the key that made it, whether it succeeded or not.

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

use super::{List, Page};
use crate::error::ApiError;
use crate::state::AdminState;

/// The database a path names, if any.
fn database_of(path: &str) -> Option<&str> {
    let rest = path.strip_prefix("/v1/databases/")?;
    let id = rest.split('/').next()?;
    (!id.is_empty()).then_some(id)
}

/// A failure to record is logged and does not undo the change: the change has
/// already happened.
pub async fn record(state: &AdminState, key: &str, method: &str, path: &str, status: StatusCode) {
    let result = sqlx::query(
        "INSERT INTO audit_log (key_name, method, path, status, database_id) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(key)
    .bind(method)
    .bind(path)
    .bind(i32::from(status.as_u16()))
    .bind(database_of(path))
    .execute(&state.pool)
    .await;
    if let Err(e) = result {
        tracing::error!(err = %e, key, method, path, "could not record a change in the audit log");
    }
}

#[derive(Debug, Deserialize)]
pub struct AuditQuery {
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    database_id: Option<String>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct Entry {
    cursor: String,
    key_name: String,
    method: String,
    path: String,
    status: i32,
    database_id: Option<String>,
    at: DateTime<Utc>,
}

/// Oldest first, from just after `page_token`.
pub async fn list(
    State(state): State<AdminState>,
    Query(query): Query<AuditQuery>,
    page: Page,
) -> Result<Json<List<Entry>>, ApiError> {
    let after: i64 = match page.after() {
        "" => 0,
        token => token
            .parse()
            .map_err(|_| ApiError::invalid("page_token is not a cursor"))?,
    };
    let rows = sqlx::query_as::<_, Entry>(
        "SELECT id::text AS cursor, key_name, method, path, status, database_id, at \
         FROM audit_log \
         WHERE id > $1 AND ($2::text IS NULL OR key_name = $2) \
           AND ($3::text IS NULL OR database_id = $3) \
         ORDER BY id LIMIT $4",
    )
    .bind(after)
    .bind(&query.key)
    .bind(&query.database_id)
    .bind(page.fetch_limit())
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(List::from_rows(rows, &page, |e: &Entry| {
        e.cursor.clone()
    })))
}

#[cfg(test)]
mod tests {
    use super::database_of;

    #[test]
    fn a_path_names_its_database() {
        assert_eq!(database_of("/v1/databases/db-1/restart"), Some("db-1"));
        assert_eq!(database_of("/v1/databases/db-1"), Some("db-1"));
        assert_eq!(database_of("/v1/databases"), None);
        assert_eq!(database_of("/v1/hosts/h-1/drain"), None);
    }
}
