//! Makes one instance match desired state. Every step checks before it acts, so
//! a failure part way through is safe to retry.

use std::collections::BTreeSet;
use std::net::IpAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgSslMode};
use sqlx::{ConnectOptions, Connection, PgConnection};

use super::client::AdminClient;
use super::config::HostConfig;
use super::firewall;
use super::fsutil::{give_to, write_private_if_changed};
use super::model::Instance;
use super::quota;
use super::render::{self, Layout, REPLICATION_USER, SUPERUSER};
use super::restore::Restorer;
use super::runner::{command_as, Processes, RunAs, Runner};

const READY_TIMEOUT: Duration = Duration::from_secs(120);
const READY_POLL: Duration = Duration::from_millis(500);

/// Marks a directory as an instance of this host, and records what has been
/// applied to it. It holds no secrets beyond a verifier, which is not one.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Applied {
    pub host_id: String,
    pub generation: i64,
    pub verifier: String,
    pub extensions: Vec<String>,
    /// The bootstrap ran to its end, so a restart must not repeat it.
    #[serde(default)]
    pub bootstrapped: bool,
}

/// How far an instance got.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    /// It matches desired state.
    Running,
    /// It waits for the Raft leader to seed it, which can take hours, so the
    /// host moves on and checks again.
    Seeding,
}

/// The time, the WAL position, the last file archived, the segment size,
/// whether this is a standby, and the server's version.
type WalPosition = (DateTime<Utc>, String, Option<String>, i64, bool, String);

/// `18.1` from `18.1 (Debian 18.1-1.pgdg120+1)` or from the output of
/// `postgres --version`.
pub fn release(text: &str) -> Option<String> {
    text.split_whitespace()
        .find(|word| word.starts_with(|c: char| c.is_ascii_digit()))
        .map(|word| word.trim_end_matches(',').to_string())
}

/// What a page header takes at the start of a WAL segment, which is where the
/// position rests once an idle server has switched segments.
const SEGMENT_HEADER: u64 = 40;
/// Enough to span an archiver a few minutes behind.
const MAX_SAMPLES: usize = 64;

/// Recent pairs of a time and the WAL position then. Every commit before that
/// time lies before that position, which is what ties a position to a time.
#[derive(Debug, Default)]
pub struct WalSamples {
    samples: std::collections::VecDeque<(DateTime<Utc>, u64)>,
}

impl WalSamples {
    fn record(&mut self, time: DateTime<Utc>, lsn: u64) {
        self.samples.push_back((time, lsn));
        if self.samples.len() > MAX_SAMPLES {
            self.samples.pop_front();
        }
    }

    /// The newest time whose position the archive has reached.
    fn covered_by(&self, archived_end: u64) -> Option<DateTime<Utc>> {
        self.samples
            .iter()
            .rev()
            .find(|(_, lsn)| lsn.saturating_sub(SEGMENT_HEADER) <= archived_end)
            .map(|(time, _)| *time)
    }
}

/// The position where `segment` ends, from a WAL file name. Other files the
/// archiver handles, such as timeline histories, say nothing about position.
fn archived_end(segment: &str, segment_size: u64) -> Option<u64> {
    if segment.len() != 24 || !segment.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let high = u64::from_str_radix(&segment[8..16], 16).ok()?;
    let low = u64::from_str_radix(&segment[16..24], 16).ok()?;
    Some((high << 32) + (low + 1) * segment_size)
}

/// The ports of a block at `port_base` that something already listens on.
pub fn ports_in_use(port_base: u16) -> Vec<u16> {
    (port_base..port_base.saturating_add(10))
        .filter(|port| std::net::TcpListener::bind(("0.0.0.0", *port)).is_err())
        .collect()
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

pub(super) fn quote_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

pub(super) fn quote_ident(text: &str) -> String {
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
            resources: None,
            proxy: true,
        })
    }

    /// The processes of `instance`, with its limits and role.
    pub fn processes_for(&self, instance: &Instance) -> Result<Processes> {
        let mut processes = self.processes(&instance.id(), &instance.postgres_version)?;
        processes.resources = Some(instance.resources.clone());
        processes.proxy = !instance.is_replica();
        Ok(processes)
    }

    /// Every address that may reach this instance's peer ports. Peers can be
    /// named, but `pg_hba.conf` and nftables want addresses.
    async fn node_addresses(&self, instance: &Instance) -> Result<Vec<IpAddr>> {
        if !instance.ha() {
            return Ok(vec![]);
        }
        let mut addresses = BTreeSet::new();
        let names = instance
            .peers
            .iter()
            .chain(&instance.replicas)
            .map(|peer| peer.address.as_str())
            .chain([instance.address.as_str()]);
        for name in names {
            let found = tokio::net::lookup_host((name, 0))
                .await
                .with_context(|| format!("resolve {name}"))?;
            addresses.extend(found.map(|socket| socket.ip()));
        }
        Ok(addresses.into_iter().collect())
    }

    pub async fn apply(&self, instance: &Instance) -> Result<Progress> {
        instance.validate()?;
        let processes = self.processes_for(instance)?;
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
            // Something outside pgcluster may hold a port in the block, and
            // would break the instance in ways harder to read than this.
            let taken = ports_in_use(instance.port_base);
            if !taken.is_empty() {
                bail!("ports {taken:?} of this instance's block are in use by another process");
            }
            let marker = Applied {
                host_id: self.client.host_id.clone(),
                ..Default::default()
            };
            write_applied(layout, &marker, self.run_as)?;
        }

        if let Some(fs) = quota::detected() {
            quota::apply(
                fs,
                &layout.dir,
                &instance.id(),
                instance.resources.storage_gb,
            )
            .await?;
        }
        let nodes = self.node_addresses(instance).await?;
        self.apply_firewall(instance, &nodes).await?;
        self.prepare_repository(instance)?;
        let files = self.write_files(instance, layout, &pg_bin, &nodes)?;
        let applied = previous.unwrap_or_default();
        let first_start = self.initialize(instance, layout, &pg_bin, &applied).await?;
        let conf = render::managed_conf(instance, layout);
        let before = std::fs::read_to_string(layout.postgres_conf()).ok();
        let conf_written = write_private_if_changed(&layout.postgres_conf(), &conf, self.run_as)?;
        // Only some settings need Postgres to restart; the rest reload.
        let conf_changed =
            conf_written && before.is_none_or(|before| render::needs_restart(&before, &conf));

        self.runner.install(&processes).await?;
        // A reload that failed is owed, though the files no longer change.
        let owed = layout.reload_owed();
        let files = FilesChanged {
            reload: files.reload || owed.exists() || (conf_written && !conf_changed),
            proxy_config: files.proxy_config,
        };
        if files.reload || files.proxy_config {
            write_private_if_changed(&owed, "", self.run_as)?;
        }
        if !self.runner.is_running(&processes).await? {
            self.runner.start(&processes).await?;
        } else if conf_changed && !instance.ha() {
            self.client
                .log(instance.operation_id.as_deref(), "restarting the instance")
                .await;
            self.runner.restart(&processes).await?;
        } else if files.reload {
            // Reloads the allowlist too, so a change to both needs only this.
            self.reload_all(instance, &processes).await?;
        } else if files.proxy_config {
            self.runner.reload_proxy(&processes).await?;
        }
        // With peers, the restart is a rolling one, which the Raft leader runs
        // when `pgcluster-admin` asks: restarting here would stop every node.
        std::fs::remove_file(&owed).ok();

        if !layout.data().join("PG_VERSION").exists() {
            return Ok(Progress::Seeding);
        }
        self.write_pgpass(instance, layout)?;

        let mut postgres = self.connect_ready(instance, "postgres").await?;
        let in_recovery: bool = sqlx::query_scalar("SELECT pg_is_in_recovery()")
            .fetch_one(&mut postgres)
            .await
            .context("ask whether this is a standby")?;
        // Replication carries everything below to standbys, which cannot write.
        // A standby keeps the old record, so that if it is promoted before the
        // primary finished, the health check notices and it finishes the job.
        let mut record = applied.clone();
        if !in_recovery {
            self.apply_roles(&mut postgres, instance, &applied).await?;
            postgres.close().await.ok();
            let mut database = self
                .connect_ready(instance, &instance.database_name)
                .await?;
            self.apply_extensions(&mut database, instance, &applied)
                .await?;
            database.close().await.ok();
            record.verifier = instance.owner.scram_verifier.clone();
            record.extensions = instance.extensions.clone();
        } else {
            postgres.close().await.ok();
        }

        if !instance.is_replica() {
            self.wait_for_proxy(instance).await?;
        }
        if first_start {
            self.client
                .log(instance.operation_id.as_deref(), "instance is ready")
                .await;
        }
        record.host_id = self.client.host_id.clone();
        record.generation = instance.generation;
        record.bootstrapped = true;
        write_applied(layout, &record, self.run_as)?;
        Ok(Progress::Running)
    }

    /// Whether this node's Postgres is the primary.
    pub async fn is_primary(&self, instance: &Instance) -> bool {
        for password in self.passwords(instance) {
            let options = self.connect_options(instance, "postgres", password);
            let Ok(mut postgres) = PgConnection::connect_with(&options).await else {
                continue;
            };
            let in_recovery: Result<bool, _> = sqlx::query_scalar("SELECT pg_is_in_recovery()")
                .fetch_one(&mut postgres)
                .await;
            postgres.close().await.ok();
            return matches!(in_recovery, Ok(false));
        }
        false
    }

    /// The release the server runs, and the time every commit before it had
    /// reached the WAL archive, by comparing `samples` of the WAL position with
    /// what the archiver has finished. Only a primary knows the second.
    pub async fn observe(
        &self,
        instance: &Instance,
        samples: &mut WalSamples,
    ) -> (Option<String>, Option<DateTime<Utc>>) {
        let password = self.passwords(instance)[0];
        let options = self.connect_options(instance, "postgres", password);
        let Ok(mut postgres) = PgConnection::connect_with(&options).await else {
            return (None, None);
        };
        let row: Result<WalPosition, _> = sqlx::query_as(
            "SELECT now(), pg_current_wal_insert_lsn()::text, a.last_archived_wal, \
                    pg_size_bytes(current_setting('wal_segment_size')), pg_is_in_recovery(), \
                    current_setting('server_version') \
             FROM pg_stat_archiver a",
        )
        .fetch_one(&mut postgres)
        .await;
        postgres.close().await.ok();
        let Ok((now, lsn, archived, segment_size, in_recovery, version)) = row else {
            return (None, None);
        };
        let version = release(&version);
        if in_recovery {
            return (version, None);
        }
        let until = (|| {
            samples.record(now, crate::postgres::parse_lsn(&lsn).ok()?);
            samples.covered_by(archived_end(archived.as_deref()?, segment_size as u64)?)
        })();
        (version, until)
    }

    /// Whether this node has become the primary with changes left to apply.
    pub async fn has_work_left(&self, instance: &Instance, layout: &Layout) -> bool {
        let Some(applied) = read_applied(layout) else {
            return false;
        };
        let current = applied.verifier == instance.owner.scram_verifier
            && applied.extensions == instance.extensions;
        if current || !layout.data().join("PG_VERSION").exists() {
            return false;
        }
        self.is_primary(instance).await
    }

    /// WAL-G needs a directory repository's root to exist before it writes.
    fn prepare_repository(&self, instance: &Instance) -> Result<()> {
        let target = instance.backup.as_ref().map(|b| &b.target);
        if let Some(path) = target.and_then(|t| t.path.as_ref()) {
            std::fs::create_dir_all(path).with_context(|| format!("create {path}"))?;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
                .with_context(|| format!("secure {path}"))?;
            give_to(Path::new(path), self.run_as)?;
        }
        Ok(())
    }

    async fn apply_firewall(&self, instance: &Instance, nodes: &[IpAddr]) -> Result<()> {
        let allowed_cidrs = instance
            .allowed_cidrs
            .iter()
            .map(|cidr| cidr.parse().context("parse a CIDR block"))
            .collect::<Result<Vec<ipnet::IpNet>>>()?;
        firewall::apply(
            self.config.firewall,
            &firewall::Rules {
                instance_id: &instance.id(),
                proxy_port: render::proxy_port(instance),
                peer_ports: render::peer_ports(instance),
                peers: nodes,
                allowed_cidrs: &allowed_cidrs,
            },
        )
        .await
    }

    /// A standby reads this file when it reconnects, so a new replication
    /// password has to reach it.
    fn write_pgpass(&self, instance: &Instance, layout: &Layout) -> Result<()> {
        let path = layout.data().join(".pgpass");
        let Ok(text) = std::fs::read_to_string(&path) else {
            return Ok(());
        };
        let Some((entry, _)) = text.trim_end().rsplit_once(':') else {
            return Ok(());
        };
        let password = &instance.secrets.replication_password;
        let updated = format!("{entry}:{password}\n");
        write_private_if_changed(&path, &updated, self.run_as)?;
        Ok(())
    }

    fn prepare_directories(&self, layout: &Layout) -> Result<()> {
        for dir in [
            &layout.dir,
            &layout.raft(),
            &layout.tls(),
            &layout.logs(),
            &layout.socket_dir(),
        ] {
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
        nodes: &[IpAddr],
    ) -> Result<FilesChanged> {
        let secrets = &instance.secrets;
        let tls = layout.tls();
        // Each of these reloads. Written first, and certificates before what
        // reads them, so nothing is told to load a file that is not there yet.
        let mut reload =
            write_private_if_changed(&tls.join("ca.crt"), &secrets.ca_cert_pem, self.run_as)?;
        reload |= write_private_if_changed(&tls.join("node.key"), &secrets.key_pem, self.run_as)?;
        reload |= write_private_if_changed(&tls.join("node.crt"), &secrets.cert_pem, self.run_as)?;
        if let (Some(cert), Some(key)) = (&secrets.proxy_cert_pem, &secrets.proxy_key_pem) {
            reload |= write_private_if_changed(&tls.join("proxy.key"), key, self.run_as)?;
            reload |= write_private_if_changed(&tls.join("proxy.crt"), cert, self.run_as)?;
        }
        reload |= write_private_if_changed(
            &layout.agent_config(),
            &render::agent_toml(instance, layout, pg_bin),
            self.run_as,
        )?;
        reload |=
            write_private_if_changed(&layout.env_file(), &render::env_file(instance), self.run_as)?;
        reload |= write_private_if_changed(&layout.pg_hba(), &render::pg_hba(nodes), self.run_as)?;
        let proxy_config = write_private_if_changed(
            &layout.pgcluster_config(),
            &render::pgcluster_toml(instance, layout),
            self.run_as,
        )?;
        Ok(FilesChanged {
            reload,
            proxy_config,
        })
    }

    /// Move every process to new certificates, passwords and peers. Nothing
    /// restarts, so clients stay connected.
    async fn reload_all(&self, instance: &Instance, processes: &Processes) -> Result<()> {
        self.runner.reload_certificates(processes).await?;
        if processes.layout.data().join("postmaster.pid").exists() {
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
        }
        self.client
            .log(
                instance.operation_id.as_deref(),
                "loaded new settings with no restart",
            )
            .await;
        Ok(())
    }

    /// Create the data directory once. Returns whether this call did. A seeded
    /// node waits for the Raft leader, which fills it through `vk-agent`.
    async fn initialize(
        &self,
        instance: &Instance,
        layout: &Layout,
        pg_bin: &str,
        applied: &Applied,
    ) -> Result<bool> {
        let data = layout.data();
        if let ("restore", Some(restore), false) = (
            instance.bootstrap.as_str(),
            &instance.restore,
            applied.bootstrapped,
        ) {
            self.client
                .log(instance.operation_id.as_deref(), "restoring from backup")
                .await;
            Restorer {
                instance,
                restore,
                layout,
                pg_bin,
                walg_path: &self.config.walg_path,
                run_as: self.run_as,
            }
            .run()
            .await?;
            write_applied(
                layout,
                &Applied {
                    host_id: self.client.host_id.clone(),
                    verifier: instance.owner.scram_verifier.clone(),
                    extensions: applied.extensions.clone(),
                    bootstrapped: true,
                    ..Default::default()
                },
                self.run_as,
            )?;
            return Ok(true);
        }
        if instance.bootstrap != "initdb" || data.join("PG_VERSION").exists() {
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
        self.client
            .log(
                instance.operation_id.as_deref(),
                "initialized the data directory",
            )
            .await;
        Ok(true)
    }

    fn connect_options(
        &self,
        instance: &Instance,
        database: &str,
        password: &str,
    ) -> PgConnectOptions {
        PgConnectOptions::new()
            .host("127.0.0.1")
            .port(render::postgres_port(instance))
            .username(SUPERUSER)
            .password(password)
            .database(database)
            .ssl_mode(PgSslMode::VerifyFull)
            .ssl_root_cert_from_pem(instance.secrets.ca_cert_pem.clone().into_bytes())
            .disable_statement_logging()
    }

    /// Connect as the superuser, waiting for Postgres to come up. The database
    /// does not exist on the first pass, so that case fails fast.
    pub async fn connect_ready(&self, instance: &Instance, database: &str) -> Result<PgConnection> {
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            // During a rotation the role may already have moved to the next one.
            let mut last = None;
            for password in self.passwords(instance) {
                let options = self.connect_options(instance, database, password);
                match PgConnection::connect_with(&options).await {
                    Ok(connection) => return Ok(connection),
                    Err(e) => last = Some(e),
                }
            }
            if Instant::now() >= deadline {
                return Err(last.expect("at least one password"))
                    .with_context(|| format!("connect to {database}"));
            }
            tokio::time::sleep(READY_POLL).await;
        }
    }

    fn passwords<'i>(&self, instance: &'i Instance) -> Vec<&'i str> {
        let secrets = &instance.secrets;
        let mut passwords = vec![secrets.superuser_password.as_str()];
        passwords.extend(secrets.next.iter().map(|n| n.superuser_password.as_str()));
        passwords
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
            // What pg_rewind reads from the primary.
            for function in [
                "pg_ls_dir(text, boolean, boolean)",
                "pg_stat_file(text, boolean)",
                "pg_read_binary_file(text)",
                "pg_read_binary_file(text, bigint, bigint, boolean)",
            ] {
                sqlx::query(&format!(
                    "GRANT EXECUTE ON FUNCTION {function} TO {REPLICATION_USER}"
                ))
                .execute(&mut *postgres)
                .await
                .context("grant rewind access")?;
            }
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
        let rename_from = match (&instance.rename_from, exists) {
            (Some(from), false) => sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)",
            )
            .bind(from)
            .fetch_one(&mut *postgres)
            .await
            .context("look up the database to rename")?
            .then_some(from),
            _ => None,
        };
        if let Some(from) = rename_from {
            // A promoted replica holds its source's database under the
            // source's name. Clients are not let in yet, so nothing holds it.
            sqlx::query(&format!(
                "ALTER DATABASE {} RENAME TO {}",
                quote_ident(from),
                quote_ident(&instance.database_name)
            ))
            .execute(&mut *postgres)
            .await
            .context("rename the database")?;
            self.client
                .log(
                    instance.operation_id.as_deref(),
                    &format!("renamed database {from} to {}", instance.database_name),
                )
                .await;
        } else if !exists {
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
        let loaded: String =
            sqlx::query_scalar("SELECT current_setting('shared_preload_libraries')")
                .fetch_one(&mut *database)
                .await
                .context("read the preloaded libraries")?;
        for extension in &instance.extensions {
            // On a three-node database, the rolling restart comes first, and
            // the operation applies a new generation after it.
            if render::needs_preload(extension) && !loaded.split(',').any(|l| l.trim() == extension)
            {
                continue;
            }
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
        firewall::remove(self.config.firewall, id).await?;
        if let Some(fs) = quota::detected() {
            if let Err(e) = quota::remove(fs, id).await {
                tracing::warn!(id, err = format!("{e:#}"), "could not lift the disk quota");
            }
        }
        tokio::fs::remove_dir_all(&layout.dir)
            .await
            .with_context(|| format!("remove {}", layout.dir.display()))?;
        tracing::info!(id, "removed instance");
        Ok(true)
    }
}

struct FilesChanged {
    /// Certificates, passwords or peers, which running processes can reload.
    reload: bool,
    /// `pgcluster.toml`, which holds the allowlist.
    proxy_config: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEGMENT: u64 = 16 * 1024 * 1024;

    #[test]
    fn a_segment_name_gives_the_position_it_ends_at() {
        let end = archived_end("000000010000000000000002", SEGMENT);
        assert_eq!(end, Some(3 * SEGMENT));
        let end = archived_end("000000020000000100000000", SEGMENT);
        assert_eq!(end, Some((1 << 32) + SEGMENT));
    }

    #[test]
    fn files_that_are_not_segments_say_nothing() {
        assert_eq!(archived_end("00000002.history", SEGMENT), None);
        let backup = "000000010000000000000002.00000028.backup";
        assert_eq!(archived_end(backup, SEGMENT), None);
    }

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(seconds, 0).unwrap()
    }

    #[test]
    fn a_time_counts_only_once_the_archive_reaches_its_position() {
        let mut samples = WalSamples::default();
        samples.record(at(1), SEGMENT);
        samples.record(at(2), SEGMENT + 500);
        samples.record(at(3), 2 * SEGMENT + 500);
        assert_eq!(samples.covered_by(SEGMENT), Some(at(1)));
        assert_eq!(samples.covered_by(2 * SEGMENT), Some(at(2)));
        assert_eq!(samples.covered_by(3 * SEGMENT), Some(at(3)));
        assert_eq!(samples.covered_by(SEGMENT / 2), None);
    }

    #[test]
    fn an_idle_server_keeps_its_window_current() {
        let mut samples = WalSamples::default();
        samples.record(at(9), SEGMENT + SEGMENT_HEADER);
        assert_eq!(samples.covered_by(SEGMENT), Some(at(9)));
    }

    #[test]
    fn a_release_is_read_from_either_spelling() {
        assert_eq!(
            release("18.1 (Debian 18.1-1.pgdg120+1)").as_deref(),
            Some("18.1")
        );
        assert_eq!(
            release("postgres (PostgreSQL) 17.6 (Debian 17.6-1.pgdg120+1)").as_deref(),
            Some("17.6")
        );
        assert_eq!(release("nothing here"), None);
    }

    #[test]
    fn a_port_in_use_is_found() {
        let held = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
        let port = held.local_addr().unwrap().port();
        assert!(ports_in_use(port).contains(&port));
        drop(held);
        assert!(!ports_in_use(port).contains(&port));
    }

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
            bootstrapped: true,
        };
        write_applied(&layout, &applied, None).unwrap();
        let back = read_applied(&layout).unwrap();
        assert_eq!(back.generation, 3);
        assert_eq!(back.host_id, "h");
    }
}
