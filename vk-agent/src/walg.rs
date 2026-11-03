//! Runs WAL-G on this node. pgcluster decides what to do and when; this module
//! is the only place in vk-agent that knows WAL-G's commands and output.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use tokio::process::Command;

use crate::config::AgentConfig;
use crate::host::model::BackupTarget;
use crate::postgres::LocalPostgres;
use crate::process::PgCtl;
use crate::seed::{clear_contents, has_data, set_owner_only};
use crate::server::agent::RestoreRequest;
use crate::standby::configure_standby;

#[derive(Debug, Deserialize)]
struct ListedBackup {
    backup_name: String,
    #[serde(default)]
    compressed_size: u64,
    #[serde(default)]
    finish_time: Option<DateTime<Utc>>,
}

/// One node's view of WAL-G.
pub struct WalG {
    path: String,
    config_path: String,
    postgres_host: String,
    postgres_port: u16,
    postgres_user: String,
    postgres_password: std::sync::RwLock<Option<String>>,
}

impl WalG {
    pub fn new(config: &AgentConfig) -> Self {
        Self {
            path: config.walg_path.clone(),
            config_path: config.backup_config_path.clone(),
            postgres_host: config.postgres_host.clone(),
            postgres_port: config.postgres_port,
            postgres_user: config.postgres_user.clone(),
            postgres_password: std::sync::RwLock::new(config.postgres_password.clone()),
        }
    }

    fn command(&self) -> Command {
        let mut command = Command::new(&self.path);
        command
            .arg("--config")
            .arg(&self.config_path)
            .env("PGHOST", &self.postgres_host)
            .env("PGPORT", self.postgres_port.to_string())
            .env("PGUSER", &self.postgres_user);
        if let Some(password) = self
            .postgres_password
            .read()
            .expect("password lock")
            .as_ref()
        {
            command.env("PGPASSWORD", password);
        }
        command
    }

    /// Connect with `password` from now on.
    pub fn set_password(&self, password: Option<String>) {
        *self.postgres_password.write().expect("password lock") = password;
    }

    async fn run(&self, mut command: Command, what: &str) -> Result<String> {
        let output = command
            .output()
            .await
            .with_context(|| format!("spawn wal-g for {what}"))?;
        if !output.status.success() {
            bail!(
                "wal-g {what} failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    /// The command Postgres runs for each finished WAL segment.
    fn archive_command(&self) -> String {
        format!("{} --config {} wal-push %p", self.path, self.config_path)
    }

    /// The command Postgres runs to fetch a WAL segment during recovery.
    fn restore_command(&self) -> String {
        format!(
            "{} --config {} wal-fetch %f %p",
            self.path, self.config_path
        )
    }

    /// Write WAL-G's config, readable only by its owner because it holds
    /// credentials, then point Postgres at it. `version` is recorded last, so a
    /// failure here is retried.
    pub async fn configure(
        &self,
        pg: &LocalPostgres,
        settings: &HashMap<String, String>,
        version: &str,
    ) -> Result<()> {
        self.write_config(settings).await?;
        pg.set_archive_command(&self.archive_command()).await?;
        tokio::fs::write(self.version_path(), version)
            .await
            .context("record wal-g config version")
    }

    async fn write_config(&self, settings: &HashMap<String, String>) -> Result<()> {
        let json = serde_json::to_vec_pretty(settings).context("render wal-g config")?;
        write_owner_only(Path::new(&self.config_path), &json).await
    }

    /// The version of the config last applied here, if any.
    pub async fn config_version(&self) -> String {
        tokio::fs::read_to_string(self.version_path())
            .await
            .unwrap_or_default()
    }

    fn version_path(&self) -> String {
        format!("{}.version", self.config_path)
    }

    /// Take a backup of `data_dir`, returning its name and size.
    pub async fn backup_push(&self, data_dir: &Path) -> Result<(String, u64)> {
        let mut command = self.command();
        command.arg("backup-push").arg(data_dir);
        self.run(command, "backup-push").await?;
        let newest = self
            .list()
            .await?
            .pop()
            .context("backup-push left no backup")?;
        Ok((newest.backup_name, newest.compressed_size))
    }

    async fn list(&self) -> Result<Vec<ListedBackup>> {
        let mut command = self.command();
        command.args(["backup-list", "--json", "--detail"]);
        parse_list(&self.run(command, "backup-list").await?)
    }

    /// Keep the newest `retain_full` full backups with what they depend on, and
    /// delete `target` if named. Returns the names that remain.
    pub async fn delete(&self, retain_full: u32, target: &str) -> Result<Vec<String>> {
        if !target.is_empty() {
            let mut command = self.command();
            command.args(["delete", "target", target, "--confirm"]);
            self.run(command, "delete target").await?;
        }
        if retain_full > 0 {
            let mut command = self.command();
            command
                .args(["delete", "retain", "FULL"])
                .arg(retain_full.to_string())
                .arg("--confirm");
            self.run(command, "delete retain").await?;
        }
        Ok(self
            .list()
            .await?
            .into_iter()
            .map(|b| b.backup_name)
            .collect())
    }

    /// Check the WAL archive for gaps. Returns WAL-G's report.
    pub async fn verify(&self) -> Result<String> {
        let mut command = self.command();
        command.args(["wal-verify", "integrity", "timeline", "--json"]);
        self.run(command, "wal-verify").await
    }

    /// Fill `data_dir` from a backup and start Postgres.
    ///
    /// With `req.standby` set, the node follows the primary. Otherwise it
    /// recovers to `req.target_time`, or the end of the WAL archive, and becomes
    /// an independent primary, so it must drop the settings it inherited: it must
    /// not archive into the source's repository, and with no standbys of its
    /// own, `synchronous_standby_names` would hang every commit.
    pub async fn restore(
        &self,
        pg_ctl: &PgCtl,
        data_dir: &Path,
        req: &RestoreRequest,
    ) -> Result<()> {
        let running = pg_ctl.is_running().await.unwrap_or(false);
        if !req.force && (running || has_data(data_dir).await?) {
            bail!(
                "{} has data; restore with force to replace it",
                data_dir.display()
            );
        }
        if running {
            pg_ctl.stop_fast().await?;
        }
        clear_contents(data_dir).await?;
        self.write_config(&req.settings).await?;

        let name = if !req.backup_name.is_empty() {
            req.backup_name.clone()
        } else if req.target_time.is_empty() {
            "LATEST".to_string()
        } else {
            let target = parse_target_time(&req.target_time)?;
            newest_before(&self.list().await?, target)?
        };
        let mut command = self.command();
        command.arg("backup-fetch").arg(data_dir).arg(&name);
        self.run(command, "backup-fetch").await?;

        let conf = crate::files::update_auto_conf;
        conf(data_dir, "restore_command", &self.restore_command()).await?;
        match &req.standby {
            Some(standby) => {
                let conninfo = format!(
                    "host={} port={} user={} password={}",
                    standby.primary_host, standby.primary_port, standby.user, standby.password
                );
                configure_standby(
                    data_dir,
                    &conninfo,
                    &standby.slot_name,
                    &standby.application_name,
                )
                .await?;
            }
            None => {
                crate::files::write_signal_file(data_dir, "recovery.signal").await?;
                conf(data_dir, "recovery_target_timeline", "latest").await?;
                conf(data_dir, "recovery_target_action", "promote").await?;
                if !req.target_time.is_empty() {
                    conf(data_dir, "recovery_target_time", &req.target_time).await?;
                }
                conf(data_dir, "archive_mode", "off").await?;
                conf(data_dir, "archive_command", "").await?;
                conf(data_dir, "synchronous_standby_names", "").await?;
            }
        }

        set_owner_only(data_dir).await?;
        pg_ctl.start().await
    }
}

/// Recovery only replays forward, so a restore to a point in time must start
/// from a backup that finished before it.
fn newest_before(backups: &[ListedBackup], target: DateTime<Utc>) -> Result<String> {
    backups
        .iter()
        .filter(|b| b.finish_time.is_some_and(|finish| finish <= target))
        .max_by_key(|b| b.finish_time)
        .map(|b| b.backup_name.clone())
        .with_context(|| format!("no backup finished before {target}"))
}

/// Accepts RFC 3339 and the text form of a Postgres `timestamptz`.
fn parse_target_time(text: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(text)
        .or_else(|_| DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%#z"))
        .map(|time| time.with_timezone(&Utc))
        .with_context(|| format!("invalid target time {text:?}"))
}

/// `backup-list` prints nothing useful for an empty repository.
fn parse_list(output: &str) -> Result<Vec<ListedBackup>> {
    let trimmed = output.trim();
    if !trimmed.starts_with('[') {
        return Ok(Vec::new());
    }
    serde_json::from_str(trimmed).context("parse wal-g backup-list")
}

/// The settings for `target`, which is what an agent is given for a repository
/// it has no instance on, as a restore has for the database it came from.
pub fn repository_settings(target: &BackupTarget) -> HashMap<String, String> {
    let mut settings = HashMap::from([("WALG_COMPRESSION_METHOD".into(), "zstd".to_string())]);
    if let Some(path) = &target.path {
        settings.insert("WALG_FILE_PREFIX".into(), path.clone());
    }
    if let Some(bucket) = &target.bucket {
        let prefix = target.prefix.as_deref().unwrap_or("").trim_matches('/');
        settings.insert("WALG_S3_PREFIX".into(), format!("s3://{bucket}/{prefix}"));
    }
    let optional = [
        ("AWS_REGION", &target.region),
        ("AWS_ENDPOINT", &target.endpoint),
        ("AWS_ACCESS_KEY_ID", &target.access_key_id),
        ("AWS_SECRET_ACCESS_KEY", &target.secret_access_key),
    ];
    for (name, value) in optional {
        if let Some(value) = value {
            settings.insert(name.into(), value.clone());
        }
    }
    if target.path_style {
        settings.insert("AWS_S3_FORCE_PATH_STYLE".into(), "true".into());
    }
    if let Some(key) = &target.encryption_key {
        settings.insert("WALG_LIBSODIUM_KEY".into(), key.clone());
        settings.insert("WALG_LIBSODIUM_KEY_TRANSFORM".into(), "base64".into());
    }
    settings
}

/// A repository reached from outside any instance, by `vk-agent host`.
pub struct Repository {
    pub binary: String,
    pub config_path: PathBuf,
    /// The user to run as, as `(uid, gid)`.
    pub user: Option<(u32, u32)>,
}

impl Repository {
    /// Write the config for `target`, readable only by the user running WAL-G.
    pub async fn create(
        binary: &str,
        config_path: PathBuf,
        target: &BackupTarget,
        user: Option<(u32, u32)>,
    ) -> Result<Self> {
        let json = serde_json::to_vec_pretty(&repository_settings(target))?;
        write_owner_only(&config_path, &json).await?;
        if let Some((uid, gid)) = user {
            std::os::unix::fs::chown(&config_path, Some(uid), Some(gid))
                .context("hand over the wal-g config")?;
        }
        Ok(Self {
            binary: binary.to_string(),
            config_path,
            user,
        })
    }

    fn command(&self) -> Command {
        let mut command = Command::new(&self.binary);
        command.arg("--config").arg(&self.config_path);
        if let Some((uid, gid)) = self.user {
            command.uid(uid).gid(gid);
            if let Ok(Some(user)) = nix::unistd::User::from_uid(uid.into()) {
                command.env("HOME", user.dir);
            }
        }
        command
    }

    async fn run(&self, mut command: Command, what: &str) -> Result<String> {
        let output = command
            .output()
            .await
            .with_context(|| format!("spawn wal-g for {what}"))?;
        if !output.status.success() {
            bail!(
                "wal-g {what} failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    /// Prove the repository can be written to, read from and cleaned up.
    pub async fn check(&self, probe: &Path) -> Result<()> {
        let name = format!(
            "pgcluster-check/{}",
            probe.file_name().unwrap_or_default().to_string_lossy()
        );
        let mut put = self.command();
        put.args(["st", "put", "--no-compress", "--no-encrypt"])
            .arg(probe)
            .arg(&name);
        self.run(put, "write check").await?;
        let mut remove = self.command();
        remove.args(["st", "rm"]).arg(&name);
        self.run(remove, "cleanup check").await?;
        Ok(())
    }

    /// Fill `data_dir` from the backup that suits `target_time`.
    pub async fn fetch(&self, data_dir: &Path, target_time: Option<&str>) -> Result<()> {
        let name = match target_time {
            Some(text) => {
                let mut list = self.command();
                list.args(["backup-list", "--json", "--detail"]);
                let backups = parse_list(&self.run(list, "backup-list").await?)?;
                newest_before(&backups, parse_target_time(text)?)?
            }
            None => "LATEST".to_string(),
        };
        let mut fetch = self.command();
        fetch.arg("backup-fetch").arg(data_dir).arg(&name);
        self.run(fetch, "backup-fetch").await?;
        Ok(())
    }

    /// The command Postgres runs to fetch a WAL segment during recovery.
    pub fn restore_command(&self) -> String {
        format!(
            "{} --config {} wal-fetch %f %p",
            self.binary,
            self.config_path.display()
        )
    }
}

async fn write_owner_only(path: &Path, contents: &[u8]) -> Result<()> {
    let temporary = path.with_extension("tmp");
    tokio::fs::write(&temporary, contents)
        .await
        .with_context(|| format!("write {}", temporary.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600)).await?;
    }
    tokio::fs::rename(&temporary, path)
        .await
        .with_context(|| format!("replace {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_keeps_wal_g_order_and_reads_sizes() {
        let output = r#"[
            {"backup_name": "base_1", "compressed_size": 10, "other": true},
            {"backup_name": "base_2_D_1", "compressed_size": 20}
        ]"#;
        let list = parse_list(output).unwrap();
        assert_eq!(list[1].backup_name, "base_2_D_1");
        assert_eq!(list[1].compressed_size, 20);
    }

    fn backup(name: &str, finish: &str) -> ListedBackup {
        ListedBackup {
            backup_name: name.into(),
            compressed_size: 0,
            finish_time: Some(parse_target_time(finish).unwrap()),
        }
    }

    #[test]
    fn a_restore_starts_from_the_newest_backup_before_the_target() {
        let backups = [
            backup("old", "2026-11-01T10:00:00Z"),
            backup("right", "2026-11-01T11:00:00Z"),
            backup("too_new", "2026-11-01T12:00:00Z"),
        ];
        let target = parse_target_time("2026-11-01 11:30:00.5+00").unwrap();
        assert_eq!(newest_before(&backups, target).unwrap(), "right");
    }

    #[test]
    fn a_target_before_every_backup_is_an_error() {
        let backups = [backup("only", "2026-11-01T12:00:00Z")];
        let target = parse_target_time("2026-11-01T11:00:00Z").unwrap();
        assert!(newest_before(&backups, target).is_err());
    }

    #[test]
    fn target_times_parse_in_both_forms() {
        assert!(parse_target_time("2026-11-01T11:00:00+02:00").is_ok());
        assert!(parse_target_time("2026-11-01 11:00:00.123456+00").is_ok());
        assert!(parse_target_time("yesterday").is_err());
    }

    #[test]
    fn an_empty_repository_lists_nothing() {
        assert!(parse_list("").unwrap().is_empty());
        assert!(parse_list("No backups found\n").unwrap().is_empty());
    }

    #[tokio::test]
    async fn the_config_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("walg.json");
        write_owner_only(&path, b"{}").await.unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }
}
