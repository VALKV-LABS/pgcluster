//! Rows that record a long-running change. The reconciler runs them.

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{FromRow, Postgres, Transaction};

/// An operation that cannot finish in this time fails, with its database marked.
pub const OPERATION_TIMEOUT_SECONDS: i32 = 600;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Create,
    Update,
    Delete,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "create" => Some(Self::Create),
            "update" => Some(Self::Update),
            "delete" => Some(Self::Delete),
            _ => None,
        }
    }

    pub fn steps(self) -> &'static [&'static str] {
        match self {
            Self::Create => &["place", "wait_applied", "activate"],
            Self::Update => &["wait_applied", "settle"],
            Self::Delete => &["retract", "wait_removed", "finalize"],
        }
    }
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct Operation {
    pub id: String,
    pub database_id: String,
    pub kind: String,
    pub status: String,
    pub target_generation: i64,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

pub const OPERATION_COLUMNS: &str =
    "id, database_id, kind, status, target_generation, error, created_at, finished_at";

/// Record an operation and its steps. It runs once the transaction commits.
pub async fn create(
    tx: &mut Transaction<'_, Postgres>,
    database_id: &str,
    kind: Kind,
    target_generation: i64,
) -> Result<Operation, sqlx::Error> {
    let id = uuid::Uuid::new_v4().to_string();
    let operation = sqlx::query_as::<_, Operation>(&format!(
        "INSERT INTO operations (id, database_id, kind, target_generation, deadline) \
         VALUES ($1, $2, $3, $4, now() + make_interval(secs => $5)) \
         RETURNING {OPERATION_COLUMNS}"
    ))
    .bind(&id)
    .bind(database_id)
    .bind(kind.as_str())
    .bind(target_generation)
    .bind(f64::from(OPERATION_TIMEOUT_SECONDS))
    .fetch_one(&mut **tx)
    .await?;
    for (seq, name) in kind.steps().iter().enumerate() {
        sqlx::query("INSERT INTO operation_steps (operation_id, seq, name) VALUES ($1, $2, $3)")
            .bind(&id)
            .bind(seq as i32)
            .bind(name)
            .execute(&mut **tx)
            .await?;
    }
    Ok(operation)
}

pub async fn log(
    tx: &mut Transaction<'_, Postgres>,
    operation_id: &str,
    source: &str,
    message: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO operation_logs (operation_id, source, message) VALUES ($1, $2, $3)")
        .bind(operation_id)
        .bind(source)
        .bind(message)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
