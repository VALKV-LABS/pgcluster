use anyhow::Result;
use sqlx::PgPool;

/// Ensure the physical replication slot `slot_name` exists on the Postgres
/// instance at `primary_url`.  If the slot already exists this is a no-op.
pub async fn ensure_slot(primary_url: &str, slot_name: &str) -> Result<()> {
    let pool = PgPool::connect(primary_url).await?;
    sqlx::query(
        "SELECT pg_create_physical_replication_slot($1, true, false) \
         WHERE NOT EXISTS ( \
             SELECT 1 FROM pg_replication_slots WHERE slot_name = $1 \
         )",
    )
    .bind(slot_name)
    .execute(&pool)
    .await?;
    pool.close().await;
    Ok(())
}

/// Drop replication slot `slot_name` on the Postgres instance at `primary_url`.
/// If the slot does not exist this is a no-op.
pub async fn drop_slot(primary_url: &str, slot_name: &str) -> Result<()> {
    let pool = PgPool::connect(primary_url).await?;
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
