//! What happens to a database after it is deleted. Its backups, the final one
//! included, stay restorable into a new database for
//! `[lifecycle] deleted_backup_retention_days`. Then a host removes them from
//! the repository, since only hosts hold a path to it.

use std::time::Duration;

use anyhow::{Context, Result};

use crate::reconciler::{bump_host, ONLINE_WINDOW_SECONDS};
use crate::state::AdminState;

/// How long a purge that failed waits before another host tries it.
pub const PURGE_RETRY: Duration = Duration::from_secs(3600);

/// Hand each purge that is due to an online host. A database whose host went
/// away before it reported is handed to another.
pub async fn advance(state: &AdminState) -> Result<()> {
    let mut tx = state.pool.begin().await?;
    // A restore under way still reads a deleted database's backups, so its
    // purge waits for the restore to finish.
    let hosts: Vec<String> = sqlx::query_scalar::<_, Option<String>>(
        "WITH online AS (SELECT id FROM hosts \
                         WHERE enrolled_at IS NOT NULL \
                           AND last_seen_at > now() - make_interval(secs => $1)) \
         UPDATE databases d \
            SET purge_host_id = (SELECT id FROM online ORDER BY random() LIMIT 1) \
          WHERE d.purge_after <= now() AND d.purged_at IS NULL \
            AND NOT EXISTS (SELECT 1 FROM databases r \
                            WHERE r.spec->'restore'->>'database_id' = d.id \
                              AND r.status = 'provisioning') \
            AND (d.purge_host_id IS NULL \
                 OR d.purge_host_id NOT IN (SELECT id FROM online)) \
          RETURNING d.purge_host_id",
    )
    .bind(ONLINE_WINDOW_SECONDS as f64)
    .fetch_all(&mut *tx)
    .await
    .context("choose hosts to purge backups")?
    .into_iter()
    .flatten()
    .collect();
    for host in hosts {
        bump_host(&mut tx, &host).await?;
    }
    tx.commit().await?;
    Ok(())
}
