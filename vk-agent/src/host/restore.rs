//! Fills a new data directory from a backup. Restore always creates a new
//! database, so this runs on an empty instance and can start over if it fails.
//!
//! Recovery happens in a private server with no network, because a backup holds
//! the source's roles, its archiving and its name for the database. All of that
//! is put right before anything else can connect.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use sqlx::postgres::PgConnectOptions;
use sqlx::{ConnectOptions, Connection, PgConnection};

use super::fsutil::{give_to, write_private_if_changed};
use super::instance::{quote_ident, quote_literal};
use super::model::{Instance, Restore};
use super::render::{self, Layout, REPLICATION_USER, SUPERUSER};
use super::runner::{command_as, RunAs};
use crate::files::{update_auto_conf, write_signal_file};
use crate::walg::Repository;

/// The whole of recovery, which replays every WAL segment since the backup.
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(6 * 3600);
const POLL: Duration = Duration::from_millis(500);
/// Postgres's words when WAL runs out before a commit later than the target.
const TARGET_NOT_REACHED: &str = "recovery ended before configured recovery target was reached";
/// The private server trusts its socket, which only the instance's user can reach.
const RESTORE_HBA: &str = "local all all trust\n";

pub struct Restorer<'a> {
    pub instance: &'a Instance,
    pub restore: &'a Restore,
    pub layout: &'a Layout,
    pub pg_bin: &'a str,
    pub walg_path: &'a str,
    pub run_as: Option<RunAs>,
}

impl Restorer<'_> {
    fn user(&self) -> Option<(u32, u32)> {
        self.run_as.map(|u| (u.uid.as_raw(), u.gid.as_raw()))
    }

    fn log_path(&self) -> std::path::PathBuf {
        self.layout.logs().join("restore.log")
    }

    fn conf_path(&self) -> std::path::PathBuf {
        self.layout.dir.join("restore.conf")
    }

    pub async fn run(&self) -> Result<()> {
        let data = self.layout.data();
        // A restore cut short leaves a partial directory.
        if data.exists() {
            std::fs::remove_dir_all(&data).context("clear a partial data directory")?;
        }
        std::fs::create_dir_all(&data).context("create the data directory")?;
        std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o700))
            .context("secure the data directory")?;
        give_to(&data, self.run_as)?;

        let repository = Repository::create(
            self.walg_path,
            self.layout.walg_restore_config(),
            &self.restore.source,
            self.user(),
        )
        .await?;
        repository
            .fetch(&data, self.restore.target_time.as_deref())
            .await?;
        self.prepare_recovery(&data, &repository).await?;

        let result = self.recover_and_adopt(&data).await;
        self.stop(&data).await.ok();
        for file in [
            self.layout.walg_restore_config(),
            self.conf_path(),
            self.layout.dir.join("restore-hba.conf"),
        ] {
            std::fs::remove_file(file).ok();
        }
        result
    }

    /// Everything here lands in `postgresql.auto.conf`, which `ALTER SYSTEM RESET
    /// ALL` empties before the instance starts for real.
    async fn prepare_recovery(&self, data: &Path, repository: &Repository) -> Result<()> {
        std::fs::remove_file(data.join(".pgpass")).ok();
        std::fs::remove_file(data.join("standby.signal")).ok();
        write_signal_file(data, "recovery.signal").await?;
        let settings = [
            ("restore_command", repository.restore_command()),
            ("recovery_target_timeline", "latest".to_string()),
            ("recovery_target_action", "promote".to_string()),
            ("primary_conninfo", String::new()),
            ("primary_slot_name", String::new()),
            ("synchronous_standby_names", String::new()),
            ("archive_mode", "off".to_string()),
            ("archive_command", String::new()),
        ];
        for (name, value) in &settings {
            update_auto_conf(data, name, value).await?;
        }
        if let Some(target) = &self.restore.target_time {
            update_auto_conf(data, "recovery_target_time", target).await?;
        }
        give_to(&data.join("postgresql.auto.conf"), self.run_as)?;
        give_to(&data.join("recovery.signal"), self.run_as)?;

        let socket = self.layout.socket_dir();
        std::fs::create_dir_all(&socket).context("create the socket directory")?;
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o700))
            .context("secure the socket directory")?;
        give_to(&socket, self.run_as)?;
        write_private_if_changed(
            &self.conf_path(),
            &render::restore_conf(self.instance, self.layout),
            self.run_as,
        )?;
        write_private_if_changed(
            &self.layout.dir.join("restore-hba.conf"),
            RESTORE_HBA,
            self.run_as,
        )?;
        std::fs::remove_file(self.log_path()).ok();
        Ok(())
    }

    async fn start(&self, data: &Path) -> Result<()> {
        // `start` returns once it accepts connections, which a hot standby does
        // long before recovery ends. A start that fails is examined by the caller.
        let output = command_as(&format!("{}/pg_ctl", self.pg_bin), self.run_as)
            .arg("start")
            .arg("-D")
            .arg(data)
            .arg("-l")
            .arg(self.log_path())
            .args(["-w", "-t", "3600", "-o"])
            .arg(format!("--config-file='{}'", self.conf_path().display()))
            .output()
            .await
            .context("run pg_ctl")?;
        if !output.status.success() {
            tracing::warn!(
                out = %String::from_utf8_lossy(&output.stdout).trim(),
                "the restore server did not start"
            );
        }
        Ok(())
    }

    async fn stop(&self, data: &Path) -> Result<()> {
        command_as(&format!("{}/pg_ctl", self.pg_bin), self.run_as)
            .args(["stop", "-w", "-m", "fast", "-D"])
            .arg(data)
            .output()
            .await
            .context("run pg_ctl")?;
        Ok(())
    }

    fn connect_options(&self, database: &str) -> PgConnectOptions {
        PgConnectOptions::new()
            .socket(self.layout.socket_dir())
            .port(render::postgres_port(self.instance))
            .username(SUPERUSER)
            .database(database)
            .disable_statement_logging()
    }

    fn recovery_ran_out(&self) -> bool {
        std::fs::read_to_string(self.log_path()).is_ok_and(|log| log.contains(TARGET_NOT_REACHED))
    }

    async fn recover_and_adopt(&self, data: &Path) -> Result<()> {
        self.start(data).await?;
        let deadline = Instant::now() + RECOVERY_TIMEOUT;
        let mut retried = false;
        let mut connection = loop {
            match self.promoted().await {
                Ok(Some(connection)) => break connection,
                Ok(None) => {}
                Err(e) if Instant::now() >= deadline => {
                    return Err(e).context("recovery did not finish");
                }
                Err(_) => {}
            }
            // A target later than the last commit in the archive leaves nothing
            // to stop on, and Postgres gives up. The API admits only times the
            // archive covers, so all of its commits came before the target, and
            // the end of the archive is the state asked for.
            if !retried && self.recovery_ran_out() {
                tracing::info!("no commit follows the target, recovering to the end of WAL");
                update_auto_conf(data, "recovery_target_time", "").await?;
                give_to(&data.join("postgresql.auto.conf"), self.run_as)?;
                std::fs::remove_file(self.log_path()).ok();
                self.start(data).await?;
                retried = true;
            }
            if self.server_gave_up(data).await {
                let log = std::fs::read_to_string(self.log_path()).unwrap_or_default();
                let tail: Vec<&str> = log.lines().rev().take(5).collect();
                bail!("recovery failed: {}", tail.join(" | "));
            }
            tokio::time::sleep(POLL).await;
        };
        self.adopt(&mut connection).await?;
        connection.close().await.ok();
        Ok(())
    }

    /// Stopped, with no reason to try again.
    async fn server_gave_up(&self, data: &Path) -> bool {
        !data.join("postmaster.pid").exists() && !self.recovery_ran_out()
    }

    /// A connection, once recovery is over.
    async fn promoted(&self) -> Result<Option<PgConnection>> {
        let mut connection = PgConnection::connect_with(&self.connect_options("postgres")).await?;
        let recovering: bool = sqlx::query_scalar("SELECT pg_is_in_recovery()")
            .fetch_one(&mut connection)
            .await?;
        Ok((!recovering).then_some(connection))
    }

    /// Make the restored data this database's, not the source's. The source's
    /// passwords may have changed since the backup, so all of them are set here.
    async fn adopt(&self, postgres: &mut PgConnection) -> Result<()> {
        let instance = self.instance;
        let secrets = &instance.secrets;
        let source = &self.restore.source_database;
        let mut statements = vec![
            format!(
                "ALTER ROLE {SUPERUSER} PASSWORD {}",
                quote_literal(&secrets.superuser_password)
            ),
            format!(
                "ALTER ROLE {} PASSWORD {}",
                quote_ident(&instance.owner.username),
                quote_literal(&instance.owner.scram_verifier)
            ),
        ];
        if *source != instance.database_name {
            statements.push(format!(
                "ALTER DATABASE {} RENAME TO {}",
                quote_ident(source),
                quote_ident(&instance.database_name)
            ));
        }
        let replicator: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)")
                .bind(REPLICATION_USER)
                .fetch_one(&mut *postgres)
                .await
                .context("look up the replication role")?;
        if replicator {
            statements.push(format!(
                "ALTER ROLE {REPLICATION_USER} PASSWORD {}",
                quote_literal(&secrets.replication_password)
            ));
        }
        statements.push("ALTER SYSTEM RESET ALL".to_string());
        for statement in &statements {
            sqlx::query(statement)
                .execute(&mut *postgres)
                .await
                .context("adopt the restored data")?;
        }
        sqlx::query("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots")
            .execute(&mut *postgres)
            .await
            .context("drop the source's slots")?;
        sqlx::query("CHECKPOINT")
            .execute(&mut *postgres)
            .await
            .context("checkpoint")?;
        Ok(())
    }
}
