//! Runs operations, one stored step at a time.
//!
//! A step runs inside the transaction that claims it with `FOR UPDATE SKIP
//! LOCKED`, and that transaction also marks it done. A crash rolls back the
//! step's work and its claim together, so a restarted `pgcluster-admin` resumes
//! at the first unfinished step, and several can run side by side.

use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::{json, Value};
use sqlx::{FromRow, Postgres, Transaction};

use crate::ca::Ca;
use crate::model::{DatabaseSpec, HA_NODES};
use crate::operations::{self, event, Kind};
use crate::scheduler::{host_loads, place, HostLoad, Placement, PORT_BLOCK};
use crate::secrets::{
    random_backup_key, random_token, DatabaseSecrets, NextPasswords, NodeSecrets,
};
use crate::state::AdminState;

/// A host that has not reported for this long counts as offline.
pub const ONLINE_WINDOW_SECONDS: i64 = 60;

const WAIT_RETRY: Duration = Duration::from_secs(1);
/// Renew a node certificate with this long left, which leaves room for a host
/// to be away for weeks before anything expires.
const CERT_RENEW_DAYS: i32 = 30;
const SWEEP_EVERY_TICKS: u32 = 10;
/// How long a node stays away before tooling is told to replace it. A reboot
/// should not bring up a new machine.
const REPLACEMENT_AFTER_SECONDS: f64 = 300.0;
/// The label of the backup every database starts with.
const FIRST_BACKUP_LABEL: &str = "first-backup";

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
    params: sqlx::types::Json<Value>,
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
                if let Err(e) = self.renew_certificates().await {
                    tracing::error!(err = format!("{e:#}"), "certificate renewal failed");
                }
                if let Err(e) = crate::drain::advance(&self.state).await {
                    tracing::error!(err = format!("{e:#}"), "host drain failed");
                }
                if let Err(e) = crate::rotation::advance(&self.state).await {
                    tracing::error!(err = format!("{e:#}"), "admin CA rotation failed");
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
                    o.target_generation, o.deadline < now() AS past_deadline, o.params \
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
            "wait_applied" => wait_applied(tx, step, None).await,
            "wait_peers" => {
                let new = step.params["new_node"].as_str().map(str::to_string);
                wait_applied(tx, step, new.as_deref()).await
            }
            "activate" => {
                set_status(tx, &step.database_id, "active", None).await?;
                Ok(Outcome::Done)
            }
            "settle" => Ok(Outcome::Done),
            "retract" => retract(tx, step).await,
            "wait_removed" => wait_removed(tx, step).await,
            "finalize" => finalize(tx, step).await,
            "first_backup" => {
                let label = json!({ "label": FIRST_BACKUP_LABEL });
                run_task(tx, step, "backup", "leader", label).await
            }
            "backup" => {
                let label = step.params["label"].clone();
                run_task(tx, step, "backup", "leader", json!({ "label": label })).await
            }
            "open_access" => {
                sqlx::query("UPDATE databases SET access_open = true WHERE id = $1")
                    .bind(&step.database_id)
                    .execute(&mut **tx)
                    .await?;
                new_generation(tx, step).await?;
                Ok(Outcome::Done)
            }
            "new_generation" => {
                new_generation(tx, step).await?;
                Ok(Outcome::Done)
            }
            "rolling_restart" => run_task(tx, step, "rolling_restart", "leader", json!({})).await,
            "resize_restart" if step.params["restart"].as_bool() == Some(true) => {
                run_task(tx, step, "rolling_restart", "leader", json!({})).await
            }
            "resize_restart" => Ok(Outcome::Done),
            "upgrade_check" => upgrade_check(tx, step).await,
            "place_replica" => self.place_replica(tx, step).await,
            "add_replica" => replica_task(tx, step, "add_replica").await,
            "detach_replica" => replica_task(tx, step, "detach_replica").await,
            "remove_replica" => match replica_of(tx, &step.database_id).await? {
                Some(_) => replica_task(tx, step, "remove_replica").await,
                None => Ok(Outcome::Done),
            },
            "promote" => promote(tx, step).await,
            "wait_version" => wait_version(tx, step).await,
            "switchover" => {
                let params = json!({ "target": step.params["target"] });
                run_task(tx, step, "switchover", "leader", params).await
            }
            "set_replication_mode" => {
                let params = json!({ "mode": step.params["mode"] });
                run_task(tx, step, "replication_mode", "leader", params).await
            }
            "replace_member" => {
                let params = step.params.0.clone();
                run_task(tx, step, "replace_member", "leader", params).await
            }
            "retract_old" => retract_old(tx, step).await,
            "wait_old_removed" => wait_old_removed(tx, step).await,
            "drop_old" => drop_old(tx, step).await,
            "ca_trust_both" => self.ca_trust_both(tx, step).await,
            "ca_issue" => self.ca_issue(tx, step).await,
            "ca_trust_new" => self.ca_trust_new(tx, step).await,
            "passwords_offer" => self.passwords_offer(tx, step).await,
            "passwords_alter" => run_task(tx, step, "alter_passwords", "primary", json!({})).await,
            "passwords_commit" => self.passwords_commit(tx, step).await,
            other => Ok(Outcome::Fail(format!("unknown step {other}"))),
        }
    }

    async fn database_secrets(
        &self,
        tx: &mut Tx<'_>,
        database_id: &str,
    ) -> Result<DatabaseSecrets> {
        let sealed: Option<Vec<u8>> =
            sqlx::query_scalar("SELECT secrets FROM databases WHERE id = $1")
                .bind(database_id)
                .fetch_one(&mut **tx)
                .await?;
        let sealed = sealed.context("the database has no secrets")?;
        self.state.secrets.open(database_id, &sealed)
    }

    async fn store_secrets(
        &self,
        tx: &mut Tx<'_>,
        database_id: &str,
        secrets: &DatabaseSecrets,
    ) -> Result<()> {
        sqlx::query("UPDATE databases SET secrets = $2, ca_cert_pem = $3 WHERE id = $1")
            .bind(database_id)
            .bind(self.state.secrets.seal(database_id, secrets)?)
            .bind(secrets.ca_bundle())
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    /// Bring in a new CA alongside the old one. Nothing presents its certificates
    /// yet, but every node, and every client that reads the bundle, trusts them.
    async fn ca_trust_both(&self, tx: &mut Tx<'_>, step: &Claimed) -> Result<Outcome> {
        let mut secrets = self.database_secrets(tx, &step.database_id).await?;
        let (name,): (String,) = sqlx::query_as("SELECT name FROM databases WHERE id = $1")
            .bind(&step.database_id)
            .fetch_one(&mut **tx)
            .await?;
        // Distinct from the old one's, so that nothing mistakes one for the other.
        let suffix = &random_token()[..8];
        let ca = Ca::generate(&format!("pgcluster-{name}-{suffix}"))?;
        secrets.previous_ca_cert_pem = Some(secrets.ca_cert_pem.clone());
        secrets.ca_cert_pem = ca.cert_pem().to_string();
        secrets.ca_key_pem = ca.key_pem();
        self.store_secrets(tx, &step.database_id, &secrets).await?;
        new_generation(tx, step).await?;
        Ok(Outcome::Done)
    }

    /// Give every node a certificate from the new CA.
    async fn ca_issue(&self, tx: &mut Tx<'_>, step: &Claimed) -> Result<Outcome> {
        let secrets = self.database_secrets(tx, &step.database_id).await?;
        let ca = Ca::from_pem(&secrets.ca_cert_pem, &secrets.ca_key_pem)?;
        let nodes: Vec<(String, String)> = sqlx::query_as(
            "SELECT n.node, h.address FROM nodes n JOIN hosts h ON h.id = n.host_id \
             WHERE n.database_id = $1 AND NOT n.retracted",
        )
        .bind(&step.database_id)
        .fetch_all(&mut **tx)
        .await?;
        for (node, address) in nodes {
            self.reissue(tx, &step.database_id, &node, &address, &ca)
                .await?;
        }
        new_generation(tx, step).await?;
        Ok(Outcome::Done)
    }

    async fn reissue(
        &self,
        tx: &mut Tx<'_>,
        database_id: &str,
        node: &str,
        address: &str,
        ca: &Ca,
    ) -> Result<()> {
        let issued = ca.issue_node(&node_names(address))?;
        let not_after = issued.not_after;
        let node_secrets = NodeSecrets {
            cert_pem: issued.cert_pem,
            key_pem: issued.key_pem,
        };
        sqlx::query(
            "UPDATE nodes SET secrets = $3, cert_not_after = $4 \
             WHERE database_id = $1 AND node = $2",
        )
        .bind(database_id)
        .bind(node)
        .bind(
            self.state
                .secrets
                .seal(&format!("{database_id}/{node}"), &node_secrets)?,
        )
        .bind(not_after)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    /// With every node on the new CA, stop trusting the old one.
    async fn ca_trust_new(&self, tx: &mut Tx<'_>, step: &Claimed) -> Result<Outcome> {
        let mut secrets = self.database_secrets(tx, &step.database_id).await?;
        secrets.previous_ca_cert_pem = None;
        self.store_secrets(tx, &step.database_id, &secrets).await?;
        new_generation(tx, step).await?;
        Ok(Outcome::Done)
    }

    /// Tell every node the passwords to come, before any role changes.
    async fn passwords_offer(&self, tx: &mut Tx<'_>, step: &Claimed) -> Result<Outcome> {
        let mut secrets = self.database_secrets(tx, &step.database_id).await?;
        secrets.next = Some(NextPasswords::generate());
        self.store_secrets(tx, &step.database_id, &secrets).await?;
        new_generation(tx, step).await?;
        Ok(Outcome::Done)
    }

    /// The roles have moved, so the new passwords become the current ones.
    async fn passwords_commit(&self, tx: &mut Tx<'_>, step: &Claimed) -> Result<Outcome> {
        let mut secrets = self.database_secrets(tx, &step.database_id).await?;
        let next = secrets.next.take().context("no passwords to commit")?;
        secrets.superuser_password = next.superuser_password;
        secrets.replication_password = next.replication_password;
        secrets.auth_password = next.auth_password;
        secrets.api_key = next.api_key;
        self.store_secrets(tx, &step.database_id, &secrets).await?;
        new_generation(tx, step).await?;
        Ok(Outcome::Done)
    }

    /// Choose hosts, allocate ports, generate each instance's secrets and publish
    /// them to the hosts. A three-node database takes one host per failure domain.
    async fn place(&self, tx: &mut Tx<'_>, step: &Claimed) -> Result<Outcome> {
        let (name, spec): (String, sqlx::types::Json<DatabaseSpec>) =
            sqlx::query_as("SELECT name, spec FROM databases WHERE id = $1")
                .bind(&step.database_id)
                .fetch_one(&mut **tx)
                .await?;

        let hosts: Vec<HostLoad> = host_loads(tx, true)
            .await?
            .into_iter()
            .filter(|h| h.online && !h.draining && h.address.is_some())
            .filter(|h| {
                spec.hosts.is_empty() || spec.hosts.contains(&h.id) || spec.hosts.contains(&h.name)
            })
            .collect();
        let count = if spec.ha { HA_NODES } else { 1 };
        let chosen = match place(&self.state.scheduler, &hosts, count, &spec.resources) {
            Placement::Placed(chosen) => chosen,
            Placement::TooFewHosts => {
                return Ok(Outcome::Wait(if spec.ha {
                    "waiting for three online hosts in separate failure domains".into()
                } else {
                    "waiting for an online host".into()
                }));
            }
            Placement::NoRoom(message) => return Ok(Outcome::Fail(message)),
        };

        let ca = Ca::generate(&format!("pgcluster-{name}"))?;
        let source_backup_key = match &spec.restore {
            Some(source) => {
                let source_secrets = self.database_secrets(tx, &source.database_id).await?;
                Some(source_secrets.backup_key)
            }
            None => None,
        };
        let database_secrets = DatabaseSecrets {
            ca_cert_pem: ca.cert_pem().to_string(),
            ca_key_pem: ca.key_pem(),
            superuser_password: random_token(),
            replication_password: random_token(),
            auth_password: random_token(),
            api_key: random_token(),
            backup_key: random_backup_key(),
            source_backup_key,
            previous_ca_cert_pem: None,
            next: None,
            proxy_certificate: None,
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
        let first = if spec.restore.is_some() {
            "restore"
        } else {
            "initdb"
        };
        for (index, host) in chosen.iter().enumerate() {
            let (host_id, address) = (&host.id, host.address.as_deref().unwrap_or_default());
            let node = format!("n{}", index + 1);
            let bootstrap = if index == 0 { first } else { "seed" };
            let added = add_node(
                &self.state,
                tx,
                &step.database_id,
                &node,
                host_id,
                address,
                bootstrap,
                &ca,
            )
            .await?;
            let Some(port_base) = added else {
                return Ok(Outcome::Fail(format!("host {host_id} has no free ports")));
            };
            operations::log(
                tx,
                &step.operation_id,
                "reconciler",
                &format!("placed {node} on host {host_id} at port {port_base}"),
            )
            .await?;
        }
        Ok(Outcome::Done)
    }

    /// Put a replica on a host away from its source. It shares the source's CA
    /// and passwords, which its data holds, and gets a backup key of its own
    /// for when it is promoted.
    async fn place_replica(&self, tx: &mut Tx<'_>, step: &Claimed) -> Result<Outcome> {
        let (spec,): (sqlx::types::Json<DatabaseSpec>,) =
            sqlx::query_as("SELECT spec FROM databases WHERE id = $1")
                .bind(&step.database_id)
                .fetch_one(&mut **tx)
                .await?;
        let source = spec
            .replica_of
            .clone()
            .context("the database is not a replica")?;
        let taken: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT h.id, h.failure_domain FROM nodes n JOIN hosts h ON h.id = n.host_id \
             WHERE n.database_id = $1 AND NOT n.retracted",
        )
        .bind(&source.database_id)
        .fetch_all(&mut **tx)
        .await?;
        let domains: Vec<String> = taken
            .iter()
            .map(|(id, domain)| domain.clone().unwrap_or_else(|| id.clone()))
            .collect();
        let hosts: Vec<HostLoad> = host_loads(tx, true)
            .await?
            .into_iter()
            .filter(|h| h.online && !h.draining && h.address.is_some())
            .filter(|h| !domains.contains(&h.domain()))
            .filter(|h| {
                spec.hosts.is_empty() || spec.hosts.contains(&h.id) || spec.hosts.contains(&h.name)
            })
            .collect();
        let host = match place(&self.state.scheduler, &hosts, 1, &spec.resources) {
            Placement::Placed(mut chosen) => chosen.remove(0),
            Placement::TooFewHosts => {
                return Ok(Outcome::Wait(
                    "waiting for an online host outside the source's failure domains".into(),
                ));
            }
            Placement::NoRoom(message) => return Ok(Outcome::Fail(message)),
        };

        let mut secrets = self.database_secrets(tx, &source.database_id).await?;
        secrets.backup_key = random_backup_key();
        secrets.source_backup_key = None;
        secrets.previous_ca_cert_pem = None;
        secrets.next = None;
        secrets.proxy_certificate = None;
        let ca = Ca::from_pem(&secrets.ca_cert_pem, &secrets.ca_key_pem)?;
        self.store_secrets(tx, &step.database_id, &secrets).await?;
        let address = host.address.clone().unwrap_or_default();
        let added = add_node(
            &self.state,
            tx,
            &step.database_id,
            "n1",
            &host.id,
            &address,
            "replica",
            &ca,
        )
        .await?;
        let Some(port_base) = added else {
            return Ok(Outcome::Fail(format!(
                "host {} has no free ports",
                host.name
            )));
        };
        // The source's nodes let the replica in.
        bump_hosts(tx, &source.database_id).await?;
        operations::log(
            tx,
            &step.operation_id,
            "reconciler",
            &format!(
                "placed the replica on host {} at port {port_base}",
                host.name
            ),
        )
        .await?;
        Ok(Outcome::Done)
    }

    /// Reissue node certificates that are near their end, as a change like any
    /// other: a new generation, published to the host, tracked by an operation.
    /// Hosts that are away are skipped until they return, and a database that is
    /// busy waits for its next turn.
    pub async fn renew_certificates(&self) -> Result<()> {
        let due: Vec<(String, String)> = sqlx::query_as(
            "SELECT n.database_id, n.node FROM nodes n \
             JOIN databases d ON d.id = n.database_id \
             JOIN hosts h ON h.id = n.host_id \
             WHERE NOT n.retracted \
               AND d.status IN ('active', 'degraded') \
               AND (n.cert_not_after IS NULL \
                    OR n.cert_not_after < now() + make_interval(days => $1)) \
               AND h.last_seen_at > now() - make_interval(secs => $2) \
               AND NOT EXISTS (SELECT 1 FROM operations o \
                               WHERE o.database_id = d.id \
                                 AND o.status IN ('pending', 'running'))",
        )
        .bind(CERT_RENEW_DAYS)
        .bind(ONLINE_WINDOW_SECONDS as f64)
        .fetch_all(&self.state.pool)
        .await
        .context("find certificates to renew")?;
        for (database_id, node) in due {
            self.renew_certificate(&database_id, &node)
                .await
                .with_context(|| format!("renew {database_id}/{node}"))?;
        }
        Ok(())
    }

    async fn renew_certificate(&self, database_id: &str, node: &str) -> Result<()> {
        let mut tx = self.state.pool.begin().await?;
        let sealed: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT secrets FROM databases \
             WHERE id = $1 AND status IN ('active', 'degraded') FOR UPDATE",
        )
        .bind(database_id)
        .fetch_optional(&mut *tx)
        .await?
        .flatten();
        // The lock may have waited out a delete or another operation.
        let busy: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM operations \
                            WHERE database_id = $1 AND status IN ('pending', 'running'))",
        )
        .bind(database_id)
        .fetch_one(&mut *tx)
        .await?;
        let Some(sealed) = sealed.filter(|_| !busy) else {
            return Ok(());
        };
        let (host_id, address): (String, String) = sqlx::query_as(
            "SELECT h.id, h.address FROM nodes n JOIN hosts h ON h.id = n.host_id \
             WHERE n.database_id = $1 AND n.node = $2",
        )
        .bind(database_id)
        .bind(node)
        .fetch_one(&mut *tx)
        .await?;

        let secrets: DatabaseSecrets = self.state.secrets.open(database_id, &sealed)?;
        let ca = Ca::from_pem(&secrets.ca_cert_pem, &secrets.ca_key_pem)?;
        self.reissue(&mut tx, database_id, node, &address, &ca)
            .await?;
        let generation: i64 = sqlx::query_scalar(
            "UPDATE databases SET generation = generation + 1, updated_at = now() \
             WHERE id = $1 RETURNING generation",
        )
        .bind(database_id)
        .fetch_one(&mut *tx)
        .await?;
        bump_host(&mut tx, &host_id).await?;
        let operation = operations::create(&mut tx, database_id, Kind::Update, generation).await?;
        operations::log(
            &mut tx,
            &operation.id,
            "reconciler",
            &format!("renewed the certificate of {node}"),
        )
        .await?;
        tx.commit().await?;
        tracing::info!(database_id, node, "renewed a node certificate");
        Ok(())
    }

    /// Tell tooling about nodes that went away, came back or need replacing.
    async fn track_nodes(&self) -> Result<()> {
        const OFFLINE: &str = "(h.last_seen_at IS NULL \
             OR h.last_seen_at < now() - make_interval(secs => $1))";
        let mut tx = self.state.pool.begin().await?;
        let left: Vec<(String, String)> = sqlx::query_as(&format!(
            "UPDATE nodes n SET offline_since = now() FROM hosts h \
             WHERE h.id = n.host_id AND n.offline_since IS NULL AND NOT n.retracted \
               AND {OFFLINE} \
             RETURNING n.database_id, n.node"
        ))
        .bind(ONLINE_WINDOW_SECONDS as f64)
        .fetch_all(&mut *tx)
        .await?;
        for (database_id, node) in left {
            event(
                &mut tx,
                "node_offline",
                Some(&database_id),
                Some(&node),
                json!({}),
            )
            .await?;
        }
        let back: Vec<(String, String)> = sqlx::query_as(&format!(
            "UPDATE nodes n SET offline_since = NULL, replacement_flagged = false FROM hosts h \
             WHERE h.id = n.host_id AND n.offline_since IS NOT NULL AND NOT n.retracted \
               AND NOT {OFFLINE} \
             RETURNING n.database_id, n.node"
        ))
        .bind(ONLINE_WINDOW_SECONDS as f64)
        .fetch_all(&mut *tx)
        .await?;
        for (database_id, node) in back {
            event(
                &mut tx,
                "node_online",
                Some(&database_id),
                Some(&node),
                json!({}),
            )
            .await?;
        }
        let lost: Vec<(String, String)> = sqlx::query_as(
            "UPDATE nodes n SET replacement_flagged = true FROM databases d \
             WHERE d.id = n.database_id AND (d.spec->>'ha')::boolean \
               AND n.offline_since < now() - make_interval(secs => $1) \
               AND NOT n.replacement_flagged AND NOT n.retracted \
             RETURNING n.database_id, n.node",
        )
        .bind(REPLACEMENT_AFTER_SECONDS)
        .fetch_all(&mut *tx)
        .await?;
        for (database_id, node) in lost {
            event(
                &mut tx,
                "replacement_needed",
                Some(&database_id),
                Some(&node),
                json!({}),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Reflect node and host health in each database's status.
    pub async fn sweep(&self) -> Result<()> {
        self.track_nodes().await?;
        const PROBLEM: &str = "coalesce((SELECT CASE \
                WHEN n.state = 'failed' THEN 'node ' || n.node || ' failed: ' || coalesce(n.error, '') \
                ELSE 'node ' || n.node || ' is on an offline host' END \
             FROM nodes n JOIN hosts h ON h.id = n.host_id \
             WHERE n.database_id = d.id AND NOT n.retracted \
               AND (n.state = 'failed' \
                    OR h.last_seen_at IS NULL \
                    OR h.last_seen_at < now() - make_interval(secs => $1)) \
             ORDER BY n.node LIMIT 1), \
             CASE WHEN (d.observed->>'sync_degraded')::boolean \
                  THEN 'sync has fallen back to async' END, \
             'backups are failing: ' || (d.observed->>'backup_error'))";
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

/// Record a node on a host, with ports and a certificate, and publish it.
/// Returns its first port, or nothing if the host has none left.
#[allow(clippy::too_many_arguments)]
pub async fn add_node(
    state: &AdminState,
    tx: &mut Tx<'_>,
    database_id: &str,
    node: &str,
    host_id: &str,
    address: &str,
    bootstrap: &str,
    ca: &Ca,
) -> Result<Option<i32>> {
    let port_base: Option<i32> = sqlx::query_scalar(
        "SELECT p FROM generate_series($1::int, $2::int, $3::int) AS p \
         WHERE NOT EXISTS (SELECT 1 FROM nodes n WHERE n.host_id = $4 AND n.port_base = p) \
         ORDER BY p LIMIT 1",
    )
    .bind(state.scheduler.port_base_start)
    .bind(state.scheduler.port_base_end)
    .bind(PORT_BLOCK)
    .bind(host_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(port_base) = port_base else {
        return Ok(None);
    };
    let issued = ca.issue_node(&node_names(address))?;
    let cert_not_after = issued.not_after;
    let node_secrets = NodeSecrets {
        cert_pem: issued.cert_pem,
        key_pem: issued.key_pem,
    };
    sqlx::query(
        "INSERT INTO nodes (database_id, node, host_id, port_base, secrets, cert_not_after, \
                            bootstrap) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(database_id)
    .bind(node)
    .bind(host_id)
    .bind(port_base)
    .bind(
        state
            .secrets
            .seal(&format!("{database_id}/{node}"), &node_secrets)?,
    )
    .bind(cert_not_after)
    .bind(bootstrap)
    .execute(&mut **tx)
    .await?;
    bump_host(tx, host_id).await?;
    Ok(Some(port_base))
}

/// The names on a node's certificate. Local processes connect over loopback.
fn node_names(address: &str) -> Vec<String> {
    let mut names = vec!["127.0.0.1".to_string(), "localhost".to_string()];
    if !names.iter().any(|name| name == address) {
        names.insert(0, address.to_string());
    }
    names
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
        Some(Kind::Create | Kind::Delete | Kind::CreateReplica | Kind::PromoteReplica) => "error",
        _ => "degraded",
    };
    set_status(tx, &step.database_id, status, Some(message)).await?;
    sqlx::query(
        "UPDATE tasks SET status = 'failed', error = 'the operation ended', finished_at = now() \
         WHERE operation_id = $1 AND status = 'pending'",
    )
    .bind(&step.operation_id)
    .execute(&mut **tx)
    .await?;
    let detail = json!({ "operation_id": step.operation_id, "kind": step.kind, "error": message });
    event(
        tx,
        "operation_failed",
        Some(&step.database_id),
        None,
        detail,
    )
    .await?;
    Ok(())
}

/// Publish the database's state again, for a step to wait on.
async fn new_generation(tx: &mut Tx<'_>, step: &Claimed) -> Result<i64> {
    let generation: i64 = sqlx::query_scalar(
        "UPDATE databases SET generation = generation + 1, updated_at = now() \
         WHERE id = $1 RETURNING generation",
    )
    .bind(&step.database_id)
    .fetch_one(&mut **tx)
    .await?;
    sqlx::query("UPDATE operations SET target_generation = $2 WHERE id = $1")
        .bind(&step.operation_id)
        .bind(generation)
        .execute(&mut **tx)
        .await?;
    bump_hosts(tx, &step.database_id).await?;
    Ok(generation)
}

/// Every host with a live node of this database has something new to read.
pub async fn bump_hosts(tx: &mut Tx<'_>, database_id: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE hosts SET generation = generation + 1 WHERE id IN \
         (SELECT host_id FROM nodes WHERE database_id = $1 AND NOT retracted)",
    )
    .bind(database_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Hand a host work and wait for it. The step's first attempt records the task,
/// and later attempts look at how it went.
async fn run_task(
    tx: &mut Tx<'_>,
    step: &Claimed,
    kind: &str,
    run_on: &str,
    params: Value,
) -> Result<Outcome> {
    let database_id = step.database_id.clone();
    run_task_on(tx, step, &database_id, kind, run_on, params).await
}

/// As `run_task`, on the nodes of `database_id`, which a replica's operations
/// use to reach their source's leader.
async fn run_task_on(
    tx: &mut Tx<'_>,
    step: &Claimed,
    database_id: &str,
    kind: &str,
    run_on: &str,
    params: Value,
) -> Result<Outcome> {
    let task: Option<(String, Option<String>)> =
        sqlx::query_as("SELECT status, error FROM tasks WHERE operation_id = $1 AND kind = $2")
            .bind(&step.operation_id)
            .bind(kind)
            .fetch_optional(&mut **tx)
            .await?;
    match task {
        None => {
            sqlx::query(
                "INSERT INTO tasks (id, operation_id, database_id, kind, run_on, params) \
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(uuid::Uuid::new_v4().to_string())
            .bind(&step.operation_id)
            .bind(database_id)
            .bind(kind)
            .bind(run_on)
            .bind(sqlx::types::Json(&params))
            .execute(&mut **tx)
            .await?;
            bump_hosts(tx, database_id).await?;
            Ok(Outcome::Wait(format!("waiting for {kind}")))
        }
        Some((status, _)) if status == "succeeded" => Ok(Outcome::Done),
        Some((status, error)) if status == "failed" => Ok(Outcome::Fail(format!(
            "{kind} failed: {}",
            error.unwrap_or_default()
        ))),
        Some(_) => Ok(Outcome::Wait(format!("waiting for {kind}"))),
    }
}

/// A host that has gone away holds up nothing on a three-node database: it reads
/// its state when it returns, and the database is serving without it.
async fn wait_applied(tx: &mut Tx<'_>, step: &Claimed, except: Option<&str>) -> Result<Outcome> {
    let nodes: Vec<(String, i64, String, Option<String>)> = sqlx::query_as(
        "SELECT n.node, n.observed_generation, n.state, n.error \
         FROM nodes n \
         JOIN hosts h ON h.id = n.host_id \
         JOIN databases d ON d.id = n.database_id \
         WHERE n.database_id = $1 AND NOT n.retracted \
           AND (n.node <> $2 OR $2 IS NULL) \
           AND NOT ((d.spec->>'ha')::boolean AND n.offline_since IS NOT NULL \
                    AND h.last_seen_at < now() - make_interval(secs => $3))",
    )
    .bind(&step.database_id)
    .bind(except)
    .bind(ONLINE_WINDOW_SECONDS as f64)
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

fn old_node(step: &Claimed) -> Result<&str> {
    step.params["old_node"]
        .as_str()
        .context("the operation names no old node")
}

/// The cluster has let go of the old node, so its host can too, and its peers
/// stop accepting it.
async fn retract_old(tx: &mut Tx<'_>, step: &Claimed) -> Result<Outcome> {
    let host: String = sqlx::query_scalar(
        "UPDATE nodes SET retracted = true WHERE database_id = $1 AND node = $2 \
         RETURNING host_id",
    )
    .bind(&step.database_id)
    .bind(old_node(step)?)
    .fetch_one(&mut **tx)
    .await?;
    bump_host(tx, &host).await?;
    new_generation(tx, step).await?;
    Ok(Outcome::Done)
}

/// A host that is gone cannot answer. It removes the instance if it returns,
/// since the instance is no longer in its desired state.
async fn wait_old_removed(tx: &mut Tx<'_>, step: &Claimed) -> Result<Outcome> {
    let remaining: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM nodes n JOIN hosts h ON h.id = n.host_id \
         WHERE n.database_id = $1 AND n.node = $2 AND n.present \
           AND h.last_seen_at > now() - make_interval(secs => $3)",
    )
    .bind(&step.database_id)
    .bind(old_node(step)?)
    .bind(ONLINE_WINDOW_SECONDS as f64)
    .fetch_one(&mut **tx)
    .await?;
    if remaining == 0 {
        Ok(Outcome::Done)
    } else {
        Ok(Outcome::Wait("waiting for the old node to go".into()))
    }
}

async fn drop_old(tx: &mut Tx<'_>, step: &Claimed) -> Result<Outcome> {
    sqlx::query("DELETE FROM nodes WHERE database_id = $1 AND node = $2")
        .bind(&step.database_id)
        .bind(old_node(step)?)
        .execute(&mut **tx)
        .await?;
    Ok(Outcome::Done)
}

async fn retract(tx: &mut Tx<'_>, step: &Claimed) -> Result<Outcome> {
    // The source stops letting a deleted replica in.
    if let Some(source) = replica_of(tx, &step.database_id).await? {
        bump_hosts(tx, &source.database_id).await?;
    }
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

fn target_version(step: &Claimed) -> Result<&str> {
    step.params["target_version"]
        .as_str()
        .context("the operation names no version")
}

/// Every host must still have the release installed. One may have been rebuilt
/// since the operation was asked for.
async fn upgrade_check(tx: &mut Tx<'_>, step: &Claimed) -> Result<Outcome> {
    let target = target_version(step)?;
    let major = target.split('.').next().unwrap_or_default();
    let missing: Vec<String> = sqlx::query_scalar(
        "SELECT h.name FROM nodes n JOIN hosts h ON h.id = n.host_id \
         WHERE n.database_id = $1 AND NOT n.retracted \
           AND h.postgres_versions->>$2 IS DISTINCT FROM $3 \
         ORDER BY h.name",
    )
    .bind(&step.database_id)
    .bind(major)
    .bind(target)
    .fetch_all(&mut **tx)
    .await?;
    if missing.is_empty() {
        Ok(Outcome::Done)
    } else {
        Ok(Outcome::Fail(format!(
            "Postgres {target} is not installed on {}",
            missing.join(", ")
        )))
    }
}

/// Every node runs the new release once it reports so.
async fn wait_version(tx: &mut Tx<'_>, step: &Claimed) -> Result<Outcome> {
    let target = target_version(step)?;
    let behind: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT node, server_version FROM nodes \
         WHERE database_id = $1 AND NOT retracted AND server_version IS DISTINCT FROM $2 \
         ORDER BY node",
    )
    .bind(&step.database_id)
    .bind(target)
    .fetch_all(&mut **tx)
    .await?;
    match behind.first() {
        None => Ok(Outcome::Done),
        Some((node, version)) => Ok(Outcome::Wait(format!(
            "waiting for {node} to run {target}, not {}",
            version.as_deref().unwrap_or("an unknown version")
        ))),
    }
}

async fn replica_of(tx: &mut Tx<'_>, database_id: &str) -> Result<Option<crate::model::ReplicaOf>> {
    let (spec,): (sqlx::types::Json<DatabaseSpec>,) =
        sqlx::query_as("SELECT spec FROM databases WHERE id = $1")
            .bind(database_id)
            .fetch_one(&mut **tx)
            .await?;
    Ok(spec.0.replica_of)
}

/// Work on a replica that its source's Raft leader does.
async fn replica_task(tx: &mut Tx<'_>, step: &Claimed, kind: &str) -> Result<Outcome> {
    let source = replica_of(tx, &step.database_id)
        .await?
        .context("the database is not a replica")?;
    let (address, port_base): (Option<String>, i32) = sqlx::query_as(
        "SELECT h.address, n.port_base FROM nodes n JOIN hosts h ON h.id = n.host_id \
         WHERE n.database_id = $1 ORDER BY n.node LIMIT 1",
    )
    .bind(&step.database_id)
    .fetch_one(&mut **tx)
    .await?;
    let address = address.unwrap_or_default();
    let params = json!({
        "node_id": source.node_id,
        "agent_addr": format!("{address}:{}", port_base + 4),
        "postgres_addr": format!("{address}:{}", port_base + 2),
    });
    run_task_on(tx, step, &source.database_id, kind, "leader", params).await
}

/// The source has let go: the replica becomes a one-node database of its own,
/// and its `pgcluster` starts a Raft group of one on the data it has.
async fn promote(tx: &mut Tx<'_>, step: &Claimed) -> Result<Outcome> {
    let (spec,): (sqlx::types::Json<DatabaseSpec>,) =
        sqlx::query_as("SELECT spec FROM databases WHERE id = $1")
            .bind(&step.database_id)
            .fetch_one(&mut **tx)
            .await?;
    let mut spec = spec.0;
    if let Some(source) = spec.replica_of.take() {
        bump_hosts(tx, &source.database_id).await?;
    }
    sqlx::query("UPDATE databases SET spec = $2 WHERE id = $1")
        .bind(&step.database_id)
        .bind(sqlx::types::Json(&spec))
        .execute(&mut **tx)
        .await?;
    sqlx::query("UPDATE nodes SET bootstrap = 'promoted' WHERE database_id = $1")
        .bind(&step.database_id)
        .execute(&mut **tx)
        .await?;
    new_generation(tx, step).await?;
    Ok(Outcome::Done)
}
