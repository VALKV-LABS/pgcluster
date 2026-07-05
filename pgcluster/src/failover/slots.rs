use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::Duration;
use tracing::{info, warn};

/// 10-second hard limit for connecting to Postgres on the failover critical path.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Ensure the physical replication slot `slot_name` exists on the Postgres
/// instance at `primary_url`.  If the slot already exists this is a no-op.
pub async fn ensure_slot(primary_url: &str, slot_name: &str) -> Result<()> {
    let pool = PgPoolOptions::new()
        .acquire_timeout(CONNECT_TIMEOUT)
        .connect(primary_url)
        .await?;
    ensure_slot_with_pool(&pool, slot_name).await?;
    pool.close().await;
    Ok(())
}

/// Ensure a slot exists using an already-open connection pool.
async fn ensure_slot_with_pool(pool: &PgPool, slot_name: &str) -> Result<()> {
    sqlx::query(
        "SELECT pg_create_physical_replication_slot($1, true, false) \
         WHERE NOT EXISTS ( \
             SELECT 1 FROM pg_replication_slots WHERE slot_name = $1 \
         )",
    )
    .bind(slot_name)
    .execute(pool)
    .await?;
    Ok(())
}

/// Drop replication slot `slot_name` on the Postgres instance at `primary_url`.
/// If the slot does not exist this is a no-op.
pub async fn drop_slot(primary_url: &str, slot_name: &str) -> Result<()> {
    let pool = PgPoolOptions::new()
        .acquire_timeout(CONNECT_TIMEOUT)
        .connect(primary_url)
        .await?;
    sqlx::query(
        "SELECT pg_drop_replication_slot($1) \
         WHERE EXISTS ( \
             SELECT 1 FROM pg_replication_slots WHERE slot_name = $1 \
         )",
    )
    .bind(slot_name)
    .execute(&pool)
    .await?;
    pool.close().await;
    Ok(())
}

/// Create slots on `new_primary_url` for each replica in `replica_node_ids`.
///
/// Opens a single connection pool shared across all slot creations so that
/// failover with N replicas does not pay N TCP/TLS handshakes.  Non-fatal:
/// logs and continues on any individual slot failure.
pub async fn ensure_slots_for_replicas(
    new_primary_url: &str,
    replica_node_ids: &[String],
    slot_prefix: &str,
) {
    let pool = match PgPoolOptions::new()
        .acquire_timeout(CONNECT_TIMEOUT)
        .connect(new_primary_url)
        .await
    {
        Ok(p) => p,
        Err(e) => {
            warn!(err = %e, "failed to connect to new primary for slot creation");
            return;
        }
    };
    for node_id in replica_node_ids {
        let slot_name = format!("{}{}", slot_prefix, node_id);
        match ensure_slot_with_pool(&pool, &slot_name).await {
            Ok(()) => info!(node_id, slot_name, "ensured replication slot on new primary"),
            Err(e) => warn!(node_id, slot_name, err = %e, "failed to ensure slot on new primary"),
        }
    }
    pool.close().await;
}

/// Drop orphaned slots on the primary.
///
/// A slot is considered orphaned when it is not active (`active = false`)
/// AND its `confirmed_flush_lsn` is more than `max_lag_bytes` behind
/// `pg_current_wal_lsn()`.  Only slots matching `slot_prefix` are touched
/// so operator-created slots are not affected.
///
/// Returns a list of slot names that were successfully dropped.
pub async fn drop_orphaned_slots(
    primary_url: &str,
    slot_prefix: &str,
    max_lag_bytes: u64,
) -> Result<Vec<String>> {
    let pool = PgPoolOptions::new()
        .acquire_timeout(CONNECT_TIMEOUT)
        .connect(primary_url)
        .await?;

    type SlotRow = (String,);
    let orphans: Vec<SlotRow> = sqlx::query_as(
        "SELECT slot_name::text
           FROM pg_replication_slots
          WHERE active = false
            AND slot_name LIKE $1
            AND (pg_current_wal_lsn() - confirmed_flush_lsn) > $2",
    )
    .bind(format!("{}%", slot_prefix))
    .bind(i64::try_from(max_lag_bytes).unwrap_or(i64::MAX))
    .fetch_all(&pool)
    .await?;

    let mut dropped = Vec::new();
    for (slot_name,) in orphans {
        match sqlx::query("SELECT pg_drop_replication_slot($1)")
            .bind(&slot_name)
            .execute(&pool)
            .await
        {
            Ok(_) => {
                info!(slot_name, "dropped orphaned replication slot");
                dropped.push(slot_name);
            }
            Err(e) => {
                warn!(slot_name, err = %e, "failed to drop orphaned slot");
            }
        }
    }

    pool.close().await;
    Ok(dropped)
}

#[cfg(test)]
mod tests {
    #[test]
    fn slot_name_format() {
        let prefix = "pgcluster_";
        let node_id = "pg2";
        assert_eq!(format!("{}{}", prefix, node_id), "pgcluster_pg2");
    }

    #[test]
    fn slot_prefix_pattern() {
        let prefix = "pgcluster_";
        let pattern = format!("{}%", prefix);
        assert_eq!(pattern, "pgcluster_%");
    }

    #[test]
    fn custom_slot_prefix_formats_correctly() {
        let prefix = "mycluster_";
        let node_id = "node1";
        let slot = format!("{}{}", prefix, node_id);
        assert_eq!(slot, "mycluster_node1");
    }
}
