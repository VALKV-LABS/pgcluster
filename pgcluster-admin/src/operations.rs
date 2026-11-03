//! Rows that record a long-running change. The reconciler runs them.

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::{FromRow, Postgres, Transaction};

/// An operation that cannot finish in this time fails, with its database marked.
pub const OPERATION_TIMEOUT_SECONDS: i32 = 600;
/// What seeding and backing up take depends on the data, so these get longer.
pub const DATA_OPERATION_TIMEOUT_SECONDS: i32 = 6 * 3600;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Create,
    Update,
    /// An update that needs Postgres restarted, one node at a time.
    RollingUpdate,
    ReplicationMode,
    Delete,
    Restart,
    Switchover,
    Backup,
    Replace,
    RotateCa,
    RotatePasswords,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::RollingUpdate => "rolling_update",
            Self::ReplicationMode => "replication_mode",
            Self::Delete => "delete",
            Self::Restart => "restart",
            Self::Switchover => "switchover",
            Self::Backup => "backup",
            Self::Replace => "replace",
            Self::RotateCa => "rotate_ca",
            Self::RotatePasswords => "rotate_passwords",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "create" => Some(Self::Create),
            "update" => Some(Self::Update),
            "rolling_update" => Some(Self::RollingUpdate),
            "replication_mode" => Some(Self::ReplicationMode),
            "delete" => Some(Self::Delete),
            "restart" => Some(Self::Restart),
            "switchover" => Some(Self::Switchover),
            "backup" => Some(Self::Backup),
            "replace" => Some(Self::Replace),
            "rotate_ca" => Some(Self::RotateCa),
            "rotate_passwords" => Some(Self::RotatePasswords),
            _ => None,
        }
    }

    pub fn steps(self) -> &'static [&'static str] {
        match self {
            // Clients stay out until a restorable backup exists.
            Self::Create => &[
                "place",
                "wait_applied",
                "first_backup",
                "open_access",
                "wait_applied",
                "activate",
            ],
            Self::Update => &["wait_applied", "settle"],
            // The second generation installs what needed the restart.
            Self::RollingUpdate => &[
                "wait_applied",
                "rolling_restart",
                "new_generation",
                "wait_applied",
                "settle",
            ],
            Self::ReplicationMode => &["wait_applied", "set_replication_mode", "settle"],
            Self::Delete => &["retract", "wait_removed", "finalize"],
            Self::Restart => &["rolling_restart", "settle"],
            Self::Switchover => &["switchover", "settle"],
            Self::Backup => &["backup", "settle"],
            Self::Replace => &[
                "wait_peers",
                "replace_member",
                "retract_old",
                "wait_old_removed",
                "drop_old",
                "wait_applied",
                "settle",
            ],
            // Trust both, move to the new one, then drop the old, so no node
            // ever presents a certificate another does not trust.
            Self::RotateCa => &[
                "ca_trust_both",
                "wait_applied",
                "ca_issue",
                "wait_applied",
                "ca_trust_new",
                "wait_applied",
                "settle",
            ],
            // Every consumer learns the new passwords before a role changes.
            Self::RotatePasswords => &[
                "passwords_offer",
                "wait_applied",
                "passwords_alter",
                "passwords_commit",
                "wait_applied",
                "settle",
            ],
        }
    }

    fn timeout_seconds(self) -> i32 {
        match self {
            Self::Create | Self::Backup | Self::Replace | Self::Restart => {
                DATA_OPERATION_TIMEOUT_SECONDS
            }
            _ => OPERATION_TIMEOUT_SECONDS,
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
    create_with(tx, database_id, kind, target_generation, json!({})).await
}

/// Record an operation with what its steps need to know.
pub async fn create_with(
    tx: &mut Transaction<'_, Postgres>,
    database_id: &str,
    kind: Kind,
    target_generation: i64,
    params: Value,
) -> Result<Operation, sqlx::Error> {
    let id = uuid::Uuid::new_v4().to_string();
    let operation = sqlx::query_as::<_, Operation>(&format!(
        "INSERT INTO operations (id, database_id, kind, target_generation, deadline, params) \
         VALUES ($1, $2, $3, $4, now() + make_interval(secs => $5), $6) \
         RETURNING {OPERATION_COLUMNS}"
    ))
    .bind(&id)
    .bind(database_id)
    .bind(kind.as_str())
    .bind(target_generation)
    .bind(f64::from(kind.timeout_seconds()))
    .bind(sqlx::types::Json(&params))
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

/// Record something tooling may want to react to.
pub async fn event(
    tx: &mut Transaction<'_, Postgres>,
    kind: &str,
    database_id: Option<&str>,
    node: Option<&str>,
    detail: Value,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO events (type, database_id, node, detail) VALUES ($1, $2, $3, $4)")
        .bind(kind)
        .bind(database_id)
        .bind(node)
        .bind(sqlx::types::Json(&detail))
        .execute(&mut **tx)
        .await?;
    Ok(())
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
