//! Makes one instance match desired state. Every step checks before it acts, so
//! a failure part way through is safe to retry.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgSslMode};
use sqlx::{ConnectOptions, Connection, PgConnection};

use super::client::AdminClient;
use super::config::HostConfig;
use super::fsutil::{give_to, write_private_if_changed};
use super::model::Instance;
use super::render::{self, Layout, REPLICATION_USER, SUPERUSER};
use super::runner::{command_as, Processes, RunAs, Runner};

const READY_TIMEOUT: Duration = Duration::from_secs(120);
const READY_POLL: Duration = Duration::from_millis(500);

/// Marks a directory as an instance of this host, and records what has been
/// applied to it. It holds no secrets beyond a verifier, which is not one.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Applied {
    pub host_id: String,
    pub generation: i64,
    pub verifier: String,
    pub extensions: Vec<String>,
}

pub fn read_applied(layout: &Layout) -> Option<Applied> {
    let text = std::fs::read_to_string(layout.applied()).ok()?;
    serde_json::from_str(&text).ok()
}

fn write_applied(layout: &Layout, applied: &Applied, run_as: Option<RunAs>) -> Result<()> {
    let text = serde_json::to_string_pretty(applied)?;
    write_private_if_changed(&layout.applied(), &text, run_as)?;
    Ok(())
}

fn quote_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

fn quote_ident(text: &str) -> String {
    format!("\"{}\"", text.replace('"', "\"\""))
}

pub struct InstanceManager<'a> {
    pub config: &'a HostConfig,
    pub runner: &'a dyn Runner,
    pub run_as: Option<RunAs>,
    pub client: &'a AdminClient,
}

impl InstanceManager<'_> {
    pub fn processes(&self, id: &str, version: &str) -> Result<Processes> {
        let vk_agent = match &self.config.vk_agent_path {
            Some(path) => path.clone(),
            None => std::env::current_exe()
                .context("find this executable")?
                .to_string_lossy()
                .into_owned(),
        };
        Ok(Processes {
            id: id.to_string(),
            layout: Layout::new(&self.config.instances_dir, id),
            vk_agent,
            pgcluster: self.config.pgcluster_path.clone(),
            pg_ctl: format!("{}/pg_ctl", self.config.pg_bin(version)),
            run_as: self.run_as,
            user: self.config.run_as.clone(),
        })
    }

    pub async fn apply(&self, instance: &Instance) -> Result<()> {
        instance.validate()?;
        let processes = self.processes(&instance.id(), &instance.postgres_version)?;
        let layout = &processes.layout;
        let pg_bin = self.config.pg_bin(&instance.postgres_version);
        if !Path::new(&pg_bin).join("initdb").exists() {
            bail!(
                "this host has no Postgres {} (looked in {pg_bin})",
                instance.postgres_version
            );
        }

        self.prepare_directories(layout)?;
        // Marks the directory as ours before anything else lands in it, so a
        // failed create can still be removed.
        let previous = read_applied(layout);
        if previous.is_none() {
            let marker = Applied {
                host_id: self.client.host_id.clone(),
                ..Default::default()
            };
            write_applied(layout, &marker, self.run_as)?;
        }

        let files = self.write_files(instance, layout, &pg_bin)?;
        let first_start = self.initialize(instance, layout, &pg_bin).await?;
        let conf_changed = write_private_if_changed(
            &layout.data().join("pgcluster-managed.conf"),
            &render::managed_conf(instance, layout),
            self.run_as,
        )?;
        write_private_if_changed(
            &layout.data().join("pg_hba.conf"),
            &render::pg_hba(),
            self.run_as,
        )?;

        self.runner.install(&processes).await?;
        if !self.runner.is_running(&processes).await? {
            self.runner.start(&processes).await?;
        } else if conf_changed || files.restart {
            self.client
                .log(instance.operation_id.as_deref(), "restarting the instance")
                .await;
            self.runner.restart(&processes).await?;
        } else if files.certificate {
            // Reloads the allowlist too, so a change to both needs only this.
            self.reload_certificates(instance, &processes).await?;
        } else if files.proxy_config {
            self.runner.reload_proxy(&processes).await?;
        }

        let mut postgres = self.connect_ready(instance, "postgres").await?;
        let applied = previous.unwrap_or_default();
        self.apply_roles(&mut postgres, instance, &applied).await?;
        postgres.close().await.ok();

        let mut database = self
            .connect_ready(instance, &instance.database_name)
            .await?;
        self.apply_extensions(&mut database, instance, &applied)
            .await?;
        database.close().await.ok();

        self.wait_for_proxy(instance).await?;
        if first_start {
            self.client
                .log(instance.operation_id.as_deref(), "instance is ready")
                .await;
        }
        write_applied(
            layout,
            &Applied {
                host_id: self.client.host_id.clone(),
                generation: instance.generation,
                verifier: instance.owner.scram_verifier.clone(),
                extensions: instance.extensions.clone(),
            },
            self.run_as,
        )
    }

    fn prepare_directories(&self, layout: &Layout) -> Result<()> {
        for dir in [&layout.dir, &layout.raft(), &layout.tls(), &layout.logs()] {
            std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
                .with_context(|| format!("secure {}", dir.display()))?;
            give_to(dir, self.run_as)?;
        }
        Ok(())
    }

    fn write_files(
        &self,
        instance: &Instance,
        layout: &Layout,
        pg_bin: &str,
    ) -> Result<FilesChanged> {
        let secrets = &instance.secrets;
        let tls = layout.tls();
        let mut restart =
            write_private_if_changed(&tls.join("ca.crt"), &secrets.ca_cert_pem, self.run_as)?;
        // Both are in place before anything is told to read them.
        let mut certificate =
            write_private_if_changed(&tls.join("node.key"), &secrets.key_pem, self.run_as)?;
        certificate |=
            write_private_if_changed(&tls.join("node.crt"), &secrets.cert_pem, self.run_as)?;
        restart |= write_private_if_changed(
            &layout.agent_config(),
            &render::agent_toml(instance, layout, pg_bin),
            self.run_as,
        )?;
        restart |=
            write_private_if_changed(&layout.env_file(), &render::env_file(instance), self.run_as)?;
        let proxy_config = write_private_if_changed(
            &layout.pgcluster_config(),
            &render::pgcluster_toml(instance, layout),
            self.run_as,
        )?;
        Ok(FilesChanged {
            restart,
            certificate,
            proxy_config,
        })
    }

    /// Move every process to a renewed certificate. Nothing restarts, so clients
    /// stay connected.
    async fn reload_certificates(&self, instance: &Instance, processes: &Processes) -> Result<()> {
        self.runner.reload_certificates(processes).await?;
        let output = command_as(&processes.pg_ctl, self.run_as)
            .arg("reload")
            .arg("-D")
            .arg(processes.layout.data())
            .output()
            .await
            .context("run pg_ctl")?;
        if !output.status.success() {
            bail!(
                "pg_ctl reload failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        self.client
            .log(
                instance.operation_id.as_deref(),
                "loaded the new certificate",
            )
            .await;
        Ok(())
    }

    /// Create the data directory once. Returns whether this call did.
    async fn initialize(&self, instance: &Instance, layout: &Layout, pg_bin: &str) -> Result<bool> {
        let data = layout.data();
        if data.join("PG_VERSION").exists() {
            return Ok(false);
        }
        // A crash during initdb leaves a partial directory, which initdb refuses.
        if data.exists() {
            std::fs::remove_dir_all(&data).context("clear a partial data directory")?;
        }
        let password_file = layout.dir.join("initdb.pw");
        write_private_if_changed(
            &password_file,
            &instance.secrets.superuser_password,
            self.run_as,
        )?;
        let initdb = format!("{pg_bin}/initdb");
        let output = command_as(&initdb, self.run_as)
            .arg("-D")
            .arg(&data)
            .arg(format!("--username={SUPERUSER}"))
            .arg(format!("--pwfile={}", password_file.display()))
            .args([
                "--auth=scram-sha-256",
                "--data-checksums",
                "--encoding=UTF8",
                "--locale=C.UTF-8",
            ])
            .output()
            .await
            .context("run initdb")?;
        std::fs::remove_file(&password_file).ok();
        if !output.status.success() {
            bail!(
                "initdb failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        use std::io::Write;
        let mut conf = std::fs::OpenOptions::new()
            .append(true)
            .open(data.join("postgresql.conf"))
            .context("open postgresql.conf")?;
        writeln!(conf, "include = 'pgcluster-managed.conf'").context("extend postgresql.conf")?;
        self.client
            .log(
                instance.operation_id.as_deref(),
                "initialized the data directory",
            )
            .await;
        Ok(true)
    }

    fn connect_options(&self, instance: &Instance, database: &str) -> PgConnectOptions {
        PgConnectOptions::new()
            .host("127.0.0.1")
            .port(render::postgres_port(instance))
            .username(SUPERUSER)
            .password(&instance.secrets.superuser_password)
            .database(database)
            .ssl_mode(PgSslMode::VerifyFull)
            .ssl_root_cert_from_pem(instance.secrets.ca_cert_pem.clone().into_bytes())
            .disable_statement_logging()
    }

    /// Connect as the superuser, waiting for Postgres to come up. The database
    /// does not exist on the first pass, so that case fails fast.
    async fn connect_ready(&self, instance: &Instance, database: &str) -> Result<PgConnection> {
        let options = self.connect_options(instance, database);
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            match PgConnection::connect_with(&options).await {
                Ok(connection) => return Ok(connection),
                Err(e) if Instant::now() >= deadline => {
                    return Err(e).with_context(|| format!("connect to {database}"))
                }
                Err(_) => tokio::time::sleep(READY_POLL).await,
            }
        }
    }

    async fn apply_roles(
        &self,
        postgres: &mut PgConnection,
        instance: &Instance,
        applied: &Applied,
    ) -> Result<()> {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)")
                .bind(REPLICATION_USER)
                .fetch_one(&mut *postgres)
                .await
                .context("look up the replication role")?;
        if !exists {
            sqlx::query(&format!(
                "CREATE ROLE {REPLICATION_USER} REPLICATION LOGIN PASSWORD {}",
                quote_literal(&instance.secrets.replication_password)
            ))
            .execute(&mut *postgres)
            .await
            .context("create the replication role")?;
        }

        let owner = &instance.owner;
        let name = quote_ident(&owner.username);
        let verifier = quote_literal(&owner.scram_verifier);
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)")
                .bind(&owner.username)
                .fetch_one(&mut *postgres)
                .await
                .context("look up the owner role")?;
        if !exists {
            sqlx::query(&format!(
                "CREATE ROLE {name} LOGIN CREATEDB CREATEROLE PASSWORD {verifier}"
            ))
            .execute(&mut *postgres)
            .await
            .context("create the owner role")?;
        } else if applied.verifier != owner.scram_verifier {
            // Only on a real change. An owner may have set a password with SQL
            // since, and an unrelated change must not undo that.
            sqlx::query(&format!("ALTER ROLE {name} PASSWORD {verifier}"))
                .execute(&mut *postgres)
                .await
                .context("set the owner's password")?;
        }

        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)")
                .bind(&instance.database_name)
                .fetch_one(&mut *postgres)
                .await
                .context("look up the database")?;
        if !exists {
            sqlx::query(&format!(
                "CREATE DATABASE {} OWNER {name}",
                quote_ident(&instance.database_name)
            ))
            .execute(&mut *postgres)
            .await
            .context("create the database")?;
        }
        Ok(())
    }

    /// Install as the superuser, so owners never need to be one.
    async fn apply_extensions(
        &self,
        database: &mut PgConnection,
        instance: &Instance,
        applied: &Applied,
    ) -> Result<()> {
        for extension in &applied.extensions {
            if !instance.extensions.contains(extension) {
                sqlx::query(&format!(
                    "DROP EXTENSION IF EXISTS {}",
                    quote_ident(extension)
                ))
                .execute(&mut *database)
                .await
                .with_context(|| format!("remove extension {extension}"))?;
            }
        }
        let owner = quote_ident(&instance.owner.username);
        for extension in &instance.extensions {
            sqlx::query(&format!(
                "CREATE EXTENSION IF NOT EXISTS {}",
                quote_ident(extension)
            ))
            .execute(&mut *database)
            .await
            .with_context(|| format!("install extension {extension}"))?;
            // What the owner needs in order to use it.
            let grant = match extension.as_str() {
                "pg_cron" => Some(format!("GRANT USAGE ON SCHEMA cron TO {owner}")),
                "pg_tle" => Some(format!("GRANT pgtle_admin TO {owner}")),
                "pg_stat_statements" => Some(format!("GRANT pg_read_all_stats TO {owner}")),
                _ => None,
            };
            if let Some(grant) = grant {
                sqlx::query(&grant)
                    .execute(&mut *database)
                    .await
                    .with_context(|| format!("grant access to {extension}"))?;
            }
            if !applied.extensions.contains(extension) {
                self.client
                    .log(
                        instance.operation_id.as_deref(),
                        &format!("installed extension {extension}"),
                    )
                    .await;
            }
        }
        Ok(())
    }

    /// Clients can use the database once the proxy has a primary, and `pgcluster`
    /// has set up the role it reads verifiers through.
    async fn wait_for_proxy(&self, instance: &Instance) -> Result<()> {
        let url = format!(
            "http://127.0.0.1:{}/health",
            render::proxy_health_port(instance)
        );
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .context("build HTTP client")?;
        let mut postgres = self.connect_ready(instance, "postgres").await?;
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            let healthy = http
                .get(&url)
                .send()
                .await
                .is_ok_and(|r| r.status().is_success());
            let auth_ready: bool = sqlx::query_scalar(
                "SELECT to_regprocedure('pgcluster.get_auth(name)') IS NOT NULL",
            )
            .fetch_one(&mut postgres)
            .await
            .context("look for the auth function")?;
            if healthy && auth_ready {
                postgres.close().await.ok();
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!("the proxy did not become ready in {READY_TIMEOUT:?}");
            }
            tokio::time::sleep(READY_POLL).await;
        }
    }

    /// Stop and delete an instance that is no longer wanted. This erases its
    /// data, so it acts only on a directory this host created.
    pub async fn remove(&self, id: &str) -> Result<bool> {
        let layout = Layout::new(&self.config.instances_dir, id);
        match read_applied(&layout) {
            Some(applied) if applied.host_id == self.client.host_id => {}
            _ => {
                tracing::warn!(id, "leaving a directory this host did not create");
                return Ok(false);
            }
        }
        let version = std::fs::read_to_string(layout.data().join("PG_VERSION"))
            .map(|v| v.trim().to_string())
            .unwrap_or_else(|_| "18".into());
        let processes = self.processes(id, &version)?;
        self.runner.remove(&processes).await?;
        tokio::fs::remove_dir_all(&layout.dir)
            .await
            .with_context(|| format!("remove {}", layout.dir.display()))?;
        tracing::info!(id, "removed instance");
        Ok(true)
    }
}

struct FilesChanged {
    /// Something a running process read at startup.
    restart: bool,
    /// The node's own certificate, which running processes can reload.
    certificate: bool,
    /// `pgcluster.toml`, which holds the allowlist.
    proxy_config: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoting_escapes() {
        assert_eq!(quote_literal("it's"), "'it''s'");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn an_applied_file_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout {
            dir: dir.path().to_path_buf(),
        };
        assert!(read_applied(&layout).is_none());
        let applied = Applied {
            host_id: "h".into(),
            generation: 3,
            verifier: "v".into(),
            extensions: vec!["vector".into()],
        };
        write_applied(&layout, &applied, None).unwrap();
        let back = read_applied(&layout).unwrap();
        assert_eq!(back.generation, 3);
        assert_eq!(back.host_id, "h");
    }
}
