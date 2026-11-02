//! Runs operations, one stored step at a time.
//!
//! A step runs inside the transaction that claims it with `FOR UPDATE SKIP
//! LOCKED`, and that transaction also marks it done. A crash rolls back the
//! step's work and its claim together, so a restarted `pgcluster-admin` resumes
//! at the first unfinished step, and several can run side by side.

use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::{FromRow, Postgres, Transaction};

use crate::model::DatabaseSpec;
use crate::operations::{self, Kind};
use crate::secrets::{random_token, DatabaseSecrets, NodeSecrets};
use crate::state::AdminState;

/// A host that has not reported for this long counts as offline.
pub const ONLINE_WINDOW_SECONDS: i64 = 60;

/// First port of the first instance on a host, and past the last one.
const PORT_BASE_START: i32 = 20000;
const PORT_BASE_END: i32 = 32760;
const PORT_BLOCK: i32 = 10;

const WAIT_RETRY: Duration = Duration::from_secs(1);
const SWEEP_EVERY_TICKS: u32 = 10;

type Tx<'a> = Transaction<'a, Postgres>;

#[derive(Debug, FromRow)]
struct Claimed {
    operation_id: String,
    seq: i32,
    name: String,
    attempts: i32,
    database_id: String,
    kind: String,
    target_generation: i64,
    past_deadline: bool,
}

enum Outcome {
    Done,
    /// Try again soon. The message is logged the first time.
    Wait(String),
    Fail(String),
}

pub struct Reconciler {
    state: AdminState,
    tick: Duration,
}

impl Reconciler {
    pub fn new(state: AdminState, tick: Duration) -> Self {
        Self { state, tick }
    }

    pub async fn run(self) {
        let mut ticks = 0u32;
        loop {
            loop {
                match self.step_once().await {
                    Ok(true) => continue,
                    Ok(false) => break,
                    Err(e) => {
                        tracing::error!(err = format!("{e:#}"), "reconcile step failed");
                        break;
                    }
                }
            }
            ticks = ticks.wrapping_add(1);
            if ticks.is_multiple_of(SWEEP_EVERY_TICKS) {
                if let Err(e) = self.sweep().await {
                    tracing::error!(err = %e, "health sweep failed");
                }
            }
            tokio::time::sleep(self.tick).await;
        }
    }

    /// Claim and run one step. Returns whether there was one to run.
    pub async fn step_once(&self) -> Result<bool> {
        let mut tx = self.state.pool.begin().await?;
        let claimed = sqlx::query_as::<_, Claimed>(
            "SELECT s.operation_id, s.seq, s.name, s.attempts, o.database_id, o.kind, \
                    o.target_generation, o.deadline < now() AS past_deadline \
             FROM operation_steps s \
             JOIN operations o ON o.id = s.operation_id \
             WHERE s.status = 'pending' \
               AND s.not_before <= now() \
               AND o.status IN ('pending', 'running') \
               AND s.seq = (SELECT min(p.seq) FROM operation_steps p \
                            WHERE p.operation_id = s.operation_id AND p.status = 'pending') \
               AND NOT EXISTS (SELECT 1 FROM operations e \
                               WHERE e.database_id = o.database_id \
                                 AND e.status IN ('pending', 'running') \
                                 AND e.seq < o.seq) \
             ORDER BY s.not_before \
             LIMIT 1 \
             FOR UPDATE OF s SKIP LOCKED",
        )
        .fetch_optional(&mut *tx)
        .await?;
        let Some(step) = claimed else {
            return Ok(false);
        };

        // API handlers take this lock too, so a step never races a delete.
        sqlx::query("SELECT 1 FROM databases WHERE id = $1 FOR UPDATE")
            .bind(&step.database_id)
            .execute(&mut *tx)
            .await?;
        let active: Option<String> = sqlx::query_scalar(
            "SELECT status FROM operations WHERE id = $1 AND status IN ('pending', 'running')",
        )
        .bind(&step.operation_id)
        .fetch_optional(&mut *tx)
        .await?;
        if active.is_none() {
            tx.rollback().await?;
            return Ok(true);
        }

        sqlx::query("UPDATE operations SET status = 'running' WHERE id = $1")
            .bind(&step.operation_id)
            .execute(&mut *tx)
            .await?;

        let outcome = self.run_step(&mut tx, &step).await?;
        match outcome {
            Outcome::Done => {
                finish_step(&mut tx, &step).await?;
            }
            Outcome::Wait(_) if step.past_deadline => {
                let message = format!("timed out in step {}", step.name);
                fail(&mut tx, &step, &message).await?;
            }
            Outcome::Wait(message) => {
                if step.attempts == 0 {
                    operations::log(&mut tx, &step.operation_id, "reconciler", &message).await?;
                }
                sqlx::query(
                    "UPDATE operation_steps \
                     SET attempts = attempts + 1, \
                         not_before = now() + make_interval(secs => $3) \
                     WHERE operation_id = $1 AND seq = $2",
                )
                .bind(&step.operation_id)
                .bind(step.seq)
                .bind(WAIT_RETRY.as_secs_f64())
                .execute(&mut *tx)
                .await?;
            }
            Outcome::Fail(message) => {
                fail(&mut tx, &step, &message).await?;
            }
        }
        tx.commit().await?;
        Ok(true)
    }

    async fn run_step(&self, tx: &mut Tx<'_>, step: &Claimed) -> Result<Outcome> {
        match step.name.as_str() {
            "place" => self.place(tx, step).await,
            "wait_applied" => wait_applied(tx, step).await,
            "activate" => {
                set_status(tx, &step.database_id, "active", None).await?;
                Ok(Outcome::Done)
            }
            "settle" => Ok(Outcome::Done),
            "retract" => retract(tx, step).await,
            "wait_removed" => wait_removed(tx, step).await,
            "finalize" => finalize(tx, step).await,
            other => Ok(Outcome::Fail(format!("unknown step {other}"))),
        }
    }

    /// Choose a host, allocate ports, generate the instance's secrets and
    /// publish it to the host.
    async fn place(&self, tx: &mut Tx<'_>, step: &Claimed) -> Result<Outcome> {
        let (name, spec): (String, sqlx::types::Json<DatabaseSpec>) =
            sqlx::query_as("SELECT name, spec FROM databases WHERE id = $1")
                .bind(&step.database_id)
                .fetch_one(&mut **tx)
                .await?;

        let host: Option<(String, String)> = sqlx::query_as(
            "SELECT h.id, h.address FROM hosts h \
             WHERE h.last_seen_at > now() - make_interval(secs => $1) \
               AND h.address IS NOT NULL \
               AND (cardinality($2::text[]) = 0 OR h.id = ANY($2) OR h.name = ANY($2)) \
             ORDER BY (h.capacity->>'memory_gb')::float8 - coalesce(( \
                 SELECT sum((d.spec->'resources'->>'memory_gb')::float8) \
                 FROM nodes n JOIN databases d ON d.id = n.database_id \
                 WHERE n.host_id = h.id), 0) DESC, h.id \
             LIMIT 1 \
             FOR UPDATE OF h",
        )
        .bind(ONLINE_WINDOW_SECONDS as f64)
        .bind(&spec.hosts)
        .fetch_optional(&mut **tx)
        .await?;
        let Some((host_id, address)) = host else {
            return Ok(Outcome::Wait("waiting for an online host".into()));
        };

        let port_base: Option<i32> = sqlx::query_scalar(
            "SELECT p FROM generate_series($1::int, $2::int, $3::int) AS p \
             WHERE NOT EXISTS (SELECT 1 FROM nodes n WHERE n.host_id = $4 AND n.port_base = p) \
             ORDER BY p LIMIT 1",
        )
        .bind(PORT_BASE_START)
        .bind(PORT_BASE_END)
        .bind(PORT_BLOCK)
        .bind(&host_id)
        .fetch_optional(&mut **tx)
        .await?;
        let Some(port_base) = port_base else {
            return Ok(Outcome::Fail(format!("host {host_id} has no free ports")));
        };

        let ca = crate::ca::Ca::generate(&format!("pgcluster-{name}"))?;
        let mut names = vec!["127.0.0.1".to_string(), "localhost".to_string()];
        if !names.contains(&address) {
            names.insert(0, address.clone());
        }
        let issued = ca.issue_node(&names)?;
        let database_secrets = DatabaseSecrets {
            ca_cert_pem: ca.cert_pem().to_string(),
            ca_key_pem: ca.key_pem(),
            superuser_password: random_token(),
            replication_password: random_token(),
            auth_password: random_token(),
            api_key: random_token(),
        };
        let node = "n1";
        let node_secrets = NodeSecrets {
            cert_pem: issued.cert_pem,
            key_pem: issued.key_pem,
        };

        sqlx::query("UPDATE databases SET ca_cert_pem = $2, secrets = $3 WHERE id = $1")
            .bind(&step.database_id)
            .bind(ca.cert_pem())
            .bind(
                self.state
                    .secrets
                    .seal(&step.database_id, &database_secrets)?,
            )
            .execute(&mut **tx)
            .await?;
        sqlx::query(
            "INSERT INTO nodes (database_id, node, host_id, port_base, secrets) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&step.database_id)
        .bind(node)
        .bind(&host_id)
        .bind(port_base)
        .bind(
            self.state
                .secrets
                .seal(&format!("{}/{node}", step.database_id), &node_secrets)?,
        )
        .execute(&mut **tx)
        .await?;
        bump_host(tx, &host_id).await?;
        operations::log(
            tx,
            &step.operation_id,
            "reconciler",
            &format!("placed {node} on host {host_id} at port {port_base}"),
        )
        .await?;
        Ok(Outcome::Done)
    }

    /// Reflect node and host health in each database's status.
    pub async fn sweep(&self) -> Result<()> {
        const PROBLEM: &str = "(SELECT CASE \
                WHEN n.state = 'failed' THEN 'node ' || n.node || ' failed: ' || coalesce(n.error, '') \
                ELSE 'node ' || n.node || ' is on an offline host' END \
             FROM nodes n JOIN hosts h ON h.id = n.host_id \
             WHERE n.database_id = d.id AND NOT n.retracted \
               AND (n.state = 'failed' \
                    OR h.last_seen_at IS NULL \
                    OR h.last_seen_at < now() - make_interval(secs => $1)) \
             ORDER BY n.node LIMIT 1)";
        sqlx::query(&format!(
            "UPDATE databases d SET status = 'degraded', status_reason = {PROBLEM}, \
                                    updated_at = now() \
             WHERE d.status = 'active' AND {PROBLEM} IS NOT NULL"
        ))
        .bind(ONLINE_WINDOW_SECONDS as f64)
        .execute(&self.state.pool)
        .await
        .context("mark degraded databases")?;
        sqlx::query(&format!(
            "UPDATE databases d SET status = 'active', status_reason = NULL, updated_at = now() \
             WHERE d.status = 'degraded' AND {PROBLEM} IS NULL"
        ))
        .bind(ONLINE_WINDOW_SECONDS as f64)
        .execute(&self.state.pool)
        .await
        .context("mark recovered databases")?;
        Ok(())
    }
}

pub async fn bump_host(tx: &mut Tx<'_>, host_id: &str) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE hosts SET generation = generation + 1 WHERE id = $1")
        .bind(host_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn set_status(
    tx: &mut Tx<'_>,
    database_id: &str,
    status: &str,
    reason: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE databases SET status = $2, status_reason = $3, updated_at = now() WHERE id = $1",
    )
    .bind(database_id)
    .bind(status)
    .bind(reason)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn finish_step(tx: &mut Tx<'_>, step: &Claimed) -> Result<()> {
    sqlx::query(
        "UPDATE operation_steps \
         SET status = 'done', attempts = attempts + 1, finished_at = now() \
         WHERE operation_id = $1 AND seq = $2",
    )
    .bind(&step.operation_id)
    .bind(step.seq)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE operations SET status = 'succeeded', finished_at = now() \
         WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM operation_steps s \
                                       WHERE s.operation_id = $1 AND s.status = 'pending')",
    )
    .bind(&step.operation_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn fail(tx: &mut Tx<'_>, step: &Claimed, message: &str) -> Result<()> {
    sqlx::query(
        "UPDATE operation_steps SET status = 'failed', attempts = attempts + 1, \
         finished_at = now() WHERE operation_id = $1 AND seq = $2",
    )
    .bind(&step.operation_id)
    .bind(step.seq)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE operations SET status = 'failed', error = $2, finished_at = now() WHERE id = $1",
    )
    .bind(&step.operation_id)
    .bind(message)
    .execute(&mut **tx)
    .await?;
    operations::log(tx, &step.operation_id, "reconciler", message).await?;
    // A failed update leaves a database serving on its old settings.
    let status = match Kind::parse(&step.kind) {
        Some(Kind::Update) => "degraded",
        _ => "error",
    };
    set_status(tx, &step.database_id, status, Some(message)).await?;
    Ok(())
}

async fn wait_applied(tx: &mut Tx<'_>, step: &Claimed) -> Result<Outcome> {
    let nodes: Vec<(String, i64, String, Option<String>)> = sqlx::query_as(
        "SELECT node, observed_generation, state, error FROM nodes \
         WHERE database_id = $1 AND NOT retracted",
    )
    .bind(&step.database_id)
    .fetch_all(&mut **tx)
    .await?;
    for (node, observed, state, error) in nodes {
        if observed < step.target_generation || state != "running" {
            let detail = error.map(|e| format!(": {e}")).unwrap_or_default();
            return Ok(Outcome::Wait(format!(
                "waiting for {node} to apply generation {}{detail}",
                step.target_generation
            )));
        }
    }
    Ok(Outcome::Done)
}

async fn retract(tx: &mut Tx<'_>, step: &Claimed) -> Result<Outcome> {
    let hosts: Vec<String> = sqlx::query_scalar(
        "UPDATE nodes SET retracted = true WHERE database_id = $1 RETURNING host_id",
    )
    .bind(&step.database_id)
    .fetch_all(&mut **tx)
    .await?;
    for host in hosts {
        bump_host(tx, &host).await?;
    }
    Ok(Outcome::Done)
}

async fn wait_removed(tx: &mut Tx<'_>, step: &Claimed) -> Result<Outcome> {
    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*) FROM nodes WHERE database_id = $1 AND present")
            .bind(&step.database_id)
            .fetch_one(&mut **tx)
            .await?;
    if remaining == 0 {
        Ok(Outcome::Done)
    } else {
        Ok(Outcome::Wait(
            "waiting for hosts to remove instances".into(),
        ))
    }
}

async fn finalize(tx: &mut Tx<'_>, step: &Claimed) -> Result<Outcome> {
    sqlx::query("DELETE FROM nodes WHERE database_id = $1")
        .bind(&step.database_id)
        .execute(&mut **tx)
        .await?;
    sqlx::query("UPDATE databases SET secrets = NULL WHERE id = $1")
        .bind(&step.database_id)
        .execute(&mut **tx)
        .await?;
    set_status(tx, &step.database_id, "deleted", None).await?;
    Ok(Outcome::Done)
}
