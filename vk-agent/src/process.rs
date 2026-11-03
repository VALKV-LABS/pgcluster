use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tokio::process::Command;

/// Wraps `pg_ctl` subprocess management using `tokio::process::Command`.
pub struct PgCtl {
    pg_ctl_path: String,
    data_dir: String,
    config_file: Option<String>,
}

impl PgCtl {
    pub fn new(pg_ctl_path: &str, data_dir: &str) -> Self {
        Self {
            pg_ctl_path: pg_ctl_path.to_owned(),
            data_dir: data_dir.to_owned(),
            config_file: None,
        }
    }

    /// Start Postgres from `path`, in place of the data directory's config.
    pub fn with_config_file(mut self, path: Option<String>) -> Self {
        self.config_file = path;
        self
    }

    pub fn config_file(&self) -> Option<&str> {
        self.config_file.as_deref()
    }

    /// pg_ctl stop -D data_dir -m fast
    pub async fn stop_fast(&self) -> Result<()> {
        self.run_pg_ctl(&["stop", "-D", &self.data_dir, "-m", "fast"])
            .await
            .context("pg_ctl stop -m fast failed")
    }

    /// pg_ctl stop -D data_dir -m immediate  (used for self-fencing: no checkpoint)
    pub async fn stop_immediate(&self) -> Result<()> {
        self.run_pg_ctl(&["stop", "-D", &self.data_dir, "-m", "immediate"])
            .await
            .context("pg_ctl stop -m immediate failed")
    }

    /// pg_ctl start -D data_dir, waiting until Postgres accepts connections.
    pub async fn start(&self) -> Result<()> {
        let log = format!("{}/startup.log", self.data_dir);
        let mut args = vec!["start", "-D", &self.data_dir, "-l", &log, "-w", "-t", "600"];
        let options = self
            .config_file
            .as_ref()
            .map(|path| format!("--config-file='{path}'"));
        if let Some(options) = &options {
            args.extend(["-o", options]);
        }
        self.run_pg_ctl(&args).await.context("pg_ctl start failed")
    }

    /// The path of another Postgres program, from the same directory as pg_ctl.
    pub fn tool(&self, name: &str) -> PathBuf {
        Path::new(&self.pg_ctl_path).with_file_name(name)
    }

    /// pg_ctl reload -D data_dir
    pub async fn reload(&self) -> Result<()> {
        self.run_pg_ctl(&["reload", "-D", &self.data_dir])
            .await
            .context("pg_ctl reload failed")
    }

    /// pg_ctl status -D data_dir  (returns true if running)
    pub async fn is_running(&self) -> Result<bool> {
        let output = Command::new(&self.pg_ctl_path)
            .args(["status", "-D", &self.data_dir])
            .output()
            .await
            .context("failed to spawn pg_ctl status")?;

        // pg_ctl status exit codes:
        //   0 — server is running
        //   3 — server is not running
        //   4 — could not access the status (data dir wrong, etc.)
        match output.status.code() {
            Some(0) => Ok(true),
            Some(3) => Ok(false),
            Some(code) => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                anyhow::bail!("pg_ctl status exited with code {}: {}", code, stderr.trim())
            }
            None => anyhow::bail!("pg_ctl status terminated by signal"),
        }
    }

    /// Run pg_ctl with the given arguments, returning an error if exit code != 0.
    async fn run_pg_ctl(&self, args: &[&str]) -> Result<()> {
        let output = Command::new(&self.pg_ctl_path)
            .args(args)
            .output()
            .await
            .with_context(|| format!("failed to spawn pg_ctl with args: {}", args.join(" ")))?;

        if output.status.success() {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let combined = if stderr.trim().is_empty() {
            stdout.trim().to_string()
        } else {
            stderr.trim().to_string()
        };

        anyhow::bail!(
            "pg_ctl {} exited with status {}: {}",
            args.first().copied().unwrap_or(""),
            output.status,
            combined
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stores_paths() {
        let ctl = PgCtl::new(
            "/usr/lib/postgresql/16/bin/pg_ctl",
            "/var/lib/postgresql/data",
        );
        assert_eq!(ctl.pg_ctl_path, "/usr/lib/postgresql/16/bin/pg_ctl");
        assert_eq!(ctl.data_dir, "/var/lib/postgresql/data");
    }

    #[test]
    fn new_stores_default_paths() {
        let ctl = PgCtl::new("pg_ctl", "/pgdata");
        assert_eq!(ctl.pg_ctl_path, "pg_ctl");
        assert_eq!(ctl.data_dir, "/pgdata");
    }
}
