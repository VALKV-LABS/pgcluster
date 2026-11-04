//! Empties a draining host. Each node on it is replaced by one on another host,
//! one database at a time and one node at a time, so the new node joins before
//! the old one goes and clients see no downtime.

use anyhow::Result;
use serde_json::json;

use crate::api::databases::start_replacement;
use crate::operations::{self, event};
use crate::scheduler::{host_loads, place, Placement};
use crate::state::AdminState;

/// Start moving whatever can move now, and note the hosts that are empty.
pub async fn advance(state: &AdminState) -> Result<()> {
    // A database with an operation running waits for its turn, which also
    // keeps each database to one move at a time.
    let due: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT DISTINCT ON (n.database_id) n.database_id, n.node, n.host_id \
         FROM nodes n JOIN hosts h ON h.id = n.host_id \
         JOIN databases d ON d.id = n.database_id \
         WHERE h.draining AND NOT n.retracted \
           AND d.status IN ('active', 'degraded') AND (d.spec->>'ha')::boolean \
           AND NOT EXISTS (SELECT 1 FROM operations o \
                           WHERE o.database_id = d.id AND o.status IN ('pending', 'running')) \
         ORDER BY n.database_id, n.node",
    )
    .fetch_all(&state.pool)
    .await?;
    for (database_id, node, host_id) in due {
        move_node(state, &database_id, &node, &host_id).await?;
    }

    let mut tx = state.pool.begin().await?;
    let drained: Vec<String> = sqlx::query_scalar(
        "UPDATE hosts h SET drained_at = now(), drain_error = NULL \
         WHERE h.draining AND h.drained_at IS NULL \
           AND NOT EXISTS (SELECT 1 FROM nodes n WHERE n.host_id = h.id) \
         RETURNING h.id",
    )
    .fetch_all(&mut *tx)
    .await?;
    for host_id in drained {
        event(
            &mut tx,
            "host_drained",
            None,
            None,
            json!({ "host_id": host_id }),
        )
        .await?;
        tracing::info!(host_id, "host drained");
    }
    tx.commit().await?;
    Ok(())
}

/// Replace one node of a draining host, on the host with the most room that
/// keeps the database's nodes in separate failure domains.
async fn move_node(state: &AdminState, database_id: &str, node: &str, from: &str) -> Result<()> {
    let mut tx = state.pool.begin().await?;
    let peers: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT h.id, h.failure_domain FROM nodes n JOIN hosts h ON h.id = n.host_id \
         WHERE n.database_id = $1 AND n.node <> $2 AND NOT n.retracted",
    )
    .bind(database_id)
    .bind(node)
    .fetch_all(&mut *tx)
    .await?;
    let (name, spec): (String, sqlx::types::Json<crate::model::DatabaseSpec>) =
        sqlx::query_as("SELECT name, spec FROM databases WHERE id = $1")
            .bind(database_id)
            .fetch_one(&mut *tx)
            .await?;
    let taken: Vec<String> = peers
        .iter()
        .map(|(id, domain)| domain.clone().unwrap_or_else(|| id.clone()))
        .collect();
    let hosts: Vec<_> = host_loads(&mut tx, false)
        .await?
        .into_iter()
        .filter(|h| h.online && !h.draining && h.address.is_some() && h.id != from)
        .filter(|h| !peers.iter().any(|(id, _)| *id == h.id) && !taken.contains(&h.domain()))
        .collect();
    let target = match place(&state.scheduler, &hosts, 1, &spec.resources) {
        Placement::Placed(mut chosen) => chosen.remove(0),
        Placement::TooFewHosts => {
            let reason = "no online host is outside the failure domains of its other nodes";
            return blocked(tx, from, &format!("cannot move {name}/{node}: {reason}")).await;
        }
        Placement::NoRoom(message) => {
            return blocked(tx, from, &format!("cannot move {name}/{node}: {message}")).await;
        }
    };
    match start_replacement(state, &mut tx, database_id, node, &target.id).await {
        Ok(operation) => {
            operations::log(
                &mut tx,
                &operation.id,
                "drain",
                &format!("moving {node} off a draining host to {}", target.name),
            )
            .await?;
            sqlx::query("UPDATE hosts SET drain_error = NULL WHERE id = $1")
                .bind(from)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            tracing::info!(
                database_id,
                node,
                to = target.id,
                "moving a node off a draining host"
            );
            Ok(())
        }
        Err(e) => {
            let message = format!("cannot move {name}/{node}: {}", e.message);
            drop(tx);
            blocked(state.pool.begin().await?, from, &message).await
        }
    }
}

/// Say why the drain is stuck, where the host's status shows it.
async fn blocked(
    mut tx: sqlx::Transaction<'_, sqlx::Postgres>,
    host_id: &str,
    reason: &str,
) -> Result<()> {
    sqlx::query("UPDATE hosts SET drain_error = $2 WHERE id = $1")
        .bind(host_id)
        .bind(reason)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
