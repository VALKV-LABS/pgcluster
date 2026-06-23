use anyhow::{anyhow, Context, Result};
use sqlx::PgPool;

use crate::server::agent::ReplicaStatus;

/// Wraps a `sqlx` connection pool to the local Postgres instance and exposes
/// the queries that vk-agent needs to report status and verify LSNs.
#[derive(Debug, Clone)]
pub struct LocalPostgres {
    pool: PgPool,
}

impl LocalPostgres {
    /// Connect to the local Postgres instance using the provided URL.
    /// The URL is built by `AgentConfig::postgres_url()`.
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPool::connect(url)
            .await
            .with_context(|| format!("failed to connect to postgres at {url}"))?;
        Ok(Self { pool })
    }

    /// Return a reference to the underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Returns `true` when this node is a standby (in recovery).
    pub async fn is_in_recovery(&self) -> Result<bool> {
        let row: (bool,) = sqlx::query_as("SELECT pg_is_in_recovery()")
            .fetch_one(&self.pool)
            .await
            .context("is_in_recovery query failed")?;
        Ok(row.0)
    }

    /// Returns the last replayed WAL LSN as a `u64`.
    /// On a primary (not in recovery) this returns `NULL`; we return 0 in that case.
    pub async fn get_replay_lsn(&self) -> Result<u64> {
        let row: (Option<String>,) = sqlx::query_as("SELECT pg_last_wal_replay_lsn()::text")
            .fetch_one(&self.pool)
            .await
            .context("get_replay_lsn query failed")?;
        parse_lsn(&row.0.unwrap_or_default())
    }

    /// Returns the last received WAL LSN as a `u64` (standby only; 0 on primary).
    pub async fn get_receive_lsn(&self) -> Result<u64> {
        let row: (Option<String>,) = sqlx::query_as("SELECT pg_last_wal_receive_lsn()::text")
            .fetch_one(&self.pool)
            .await
            .context("get_receive_lsn query failed")?;
        parse_lsn(&row.0.unwrap_or_default())
    }

    /// Returns the current WAL LSN on a primary (`pg_current_wal_lsn()`).
    /// This should only be called when the node is not in recovery.
    pub async fn get_current_lsn(&self) -> Result<u64> {
        let row: (String,) = sqlx::query_as("SELECT pg_current_wal_lsn()::text")
            .fetch_one(&self.pool)
            .await
            .context("get_current_lsn query failed")?;
        parse_lsn(&row.0)
    }

    /// Returns the current timeline ID from `pg_control_checkpoint()`.
    pub async fn get_timeline(&self) -> Result<u32> {
        let row: (i32,) = sqlx::query_as("SELECT timeline_id FROM pg_control_checkpoint()")
            .fetch_one(&self.pool)
            .await
            .context("get_timeline query failed")?;
        Ok(row.0 as u32)
    }

    /// Returns rows from `pg_stat_replication` — non-empty only on a primary.
    pub async fn get_stat_replication(&self) -> Result<Vec<ReplicaStatus>> {
        type StatRow = (String, Option<String>, Option<String>, Option<i64>, String);
        let rows: Vec<StatRow> = sqlx::query_as(
            "SELECT application_name,
                        flush_lsn::text,
                        replay_lsn::text,
                        EXTRACT(EPOCH FROM replay_lag)::bigint * 1000000,
                        state
                 FROM pg_stat_replication",
        )
        .fetch_all(&self.pool)
        .await
        .context("get_stat_replication query failed")?;

        Ok(rows
            .into_iter()
            .map(|(name, flush, replay, lag, state)| ReplicaStatus {
                application_name: name,
                flush_lsn: parse_lsn(&flush.unwrap_or_default()).unwrap_or(0),
                replay_lsn: parse_lsn(&replay.unwrap_or_default()).unwrap_or(0),
                replay_lag_us: lag.unwrap_or(0),
                state,
            })
            .collect())
    }

    /// Returns the number of active non-idle connections (excluding this one).
    pub async fn get_active_connections(&self) -> Result<u32> {
        let row: (i64,) = sqlx::query_as(
            "SELECT count(*) FROM pg_stat_activity
             WHERE state != 'idle' AND pid != pg_backend_pid()",
        )
        .fetch_one(&self.pool)
        .await
        .context("get_active_connections query failed")?;
        Ok(row.0.max(0) as u32)
    }

    /// Returns the Postgres server version string (e.g. `"PostgreSQL 16.2"`).
    pub async fn get_postgres_version(&self) -> Result<String> {
        let row: (String,) = sqlx::query_as("SELECT version()")
            .fetch_one(&self.pool)
            .await
            .context("get_postgres_version query failed")?;
        // Trim to just the first token group, e.g. "PostgreSQL 16.2"
        let short = row.0.splitn(3, ' ').take(2).collect::<Vec<_>>().join(" ");
        Ok(if short.is_empty() { row.0 } else { short })
    }

    /// Returns `true` when the pool can still reach the database.
    /// A lightweight liveness check used by `GetStatus`.
    pub async fn is_running(&self) -> bool {
        sqlx::query("SELECT 1").execute(&self.pool).await.is_ok()
    }
}

/// Parse a Postgres LSN string like `"A/1B2C3D"` into a `u64`.
///
/// The format is `<high_hex>/<low_hex>` where the high part is the upper 32
/// bits and the low part is the lower 32 bits. An empty string returns `Ok(0)`.
pub fn parse_lsn(s: &str) -> Result<u64> {
    if s.is_empty() {
        return Ok(0);
    }
    let (hi, lo) = s
        .split_once('/')
        .ok_or_else(|| anyhow!("invalid LSN format: {:?}", s))?;
    let hi = u64::from_str_radix(hi.trim(), 16)
        .with_context(|| format!("invalid LSN high part: {:?}", hi))?;
    let lo = u64::from_str_radix(lo.trim(), 16)
        .with_context(|| format!("invalid LSN low part: {:?}", lo))?;
    Ok((hi << 32) | lo)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_lsn_basic() {
        // 0x00000001_1B2C3D00
        assert_eq!(parse_lsn("1/1B2C3D00").unwrap(), 0x0000000_11B2C3D00);
    }

    #[test]
    fn parse_lsn_zero() {
        assert_eq!(parse_lsn("0/0").unwrap(), 0u64);
    }

    #[test]
    fn parse_lsn_empty_returns_zero() {
        assert_eq!(parse_lsn("").unwrap(), 0u64);
    }

    #[test]
    fn parse_lsn_uppercase() {
        // 0xA = 10 in high part, 0x1B2C3D in low part
        let expected = (0xAu64 << 32) | 0x1B2C3Du64;
        assert_eq!(parse_lsn("A/1B2C3D").unwrap(), expected);
    }

    #[test]
    fn parse_lsn_invalid_missing_slash() {
        assert!(parse_lsn("1B2C3D").is_err());
    }

    #[test]
    fn parse_lsn_invalid_hex() {
        assert!(parse_lsn("Z/1B2C3D").is_err());
    }

    #[test]
    fn parse_lsn_round_trip() {
        // Simulate what Postgres returns and what we format back.
        let lsn: u64 = 0x0000_0001_2345_6789;
        let formatted = format!("{:X}/{:08X}", lsn >> 32, lsn as u32);
        assert_eq!(formatted, "1/23456789");
        assert_eq!(parse_lsn(&formatted).unwrap(), lsn);
    }

    #[test]
    fn parse_lsn_roundtrip() {
        // Spec-required alias for parse_lsn_round_trip.
        let cases = ["0/0", "0/1A2B3C", "A/DEADBEEF", "1/00000000"];
        for s in &cases {
            let lsn = parse_lsn(s).unwrap();
            let formatted = format!("{:X}/{:08X}", lsn >> 32, lsn as u32);
            assert_eq!(
                parse_lsn(&formatted).unwrap(),
                lsn,
                "roundtrip failed for {s}"
            );
        }
    }
}
