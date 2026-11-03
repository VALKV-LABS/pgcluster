//! Work `pgcluster-admin` asks of a database rather than of a machine: a
//! switchover, a backup, a change of members. `pgcluster-admin` never calls a
//! host, and each instance's REST API listens only on loopback, so hosts pick
//! these up with desired state and run them through their local instance.
//!
//! Each task goes to every node, and runs where it can. Everything here checks
//! before it acts, so a task repeated after a crash picks up where it stopped.

use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use reqwest::{Method, StatusCode};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::Connection;

use super::instance::{quote_literal, InstanceManager};
use super::model::{raft_id, BackupReport, ClusterReport, Instance, Task};
use super::render::{self, REPLICATION_USER, SUPERUSER};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Adding a learner returns once it has caught up, which is the slow part.
const MEMBERSHIP_TIMEOUT: Duration = Duration::from_secs(120);
const POLL: Duration = Duration::from_secs(1);
const SWITCHOVER_TIMEOUT: Duration = Duration::from_secs(120);
const RESTART_TIMEOUT: Duration = Duration::from_secs(1800);
const BACKUP_TIMEOUT: Duration = Duration::from_secs(6 * 3600);
const SEED_TIMEOUT: Duration = Duration::from_secs(6 * 3600);
/// Close enough to the primary to take over from a node.
const CAUGHT_UP_BYTES: u64 = 16 * 1024 * 1024;
const AUTH_USER: &str = "pgcluster_auth";

/// This instance's own `pgcluster`, on loopback.
pub struct LocalApi {
    base: String,
    key: String,
    http: reqwest::Client,
}

#[derive(Deserialize)]
struct Health {
    raft_leader: bool,
    primary: Option<String>,
}

#[derive(Deserialize)]
struct Member {
    id: u64,
    voter: bool,
}

#[derive(Deserialize)]
struct NodeList {
    nodes: Vec<Node>,
}

#[derive(Deserialize)]
struct Node {
    node_id: String,
    role: String,
    lag_bytes: u64,
}

#[derive(Deserialize)]
struct Replication {
    degraded: bool,
    needs_reseed: Vec<String>,
}

#[derive(Deserialize)]
struct Backups {
    backups: Vec<Manifest>,
}

#[derive(Deserialize)]
struct Manifest {
    backup_id: String,
    label: String,
    completed_at: i64,
    size_bytes: u64,
}

#[derive(Deserialize)]
struct BackupStatus {
    failed_label: Option<String>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct RunStatus {
    running: bool,
    error: Option<String>,
}

impl LocalApi {
    pub fn new(instance: &Instance) -> Result<Self> {
        Ok(Self {
            base: format!("http://127.0.0.1:{}", render::rest_api_port(instance)),
            key: instance.secrets.api_key.clone(),
            http: reqwest::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()
                .context("build HTTP client")?,
        })
    }

    async fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        timeout: Duration,
    ) -> Result<(StatusCode, String)> {
        let mut request = self
            .http
            .request(method, format!("{}{path}", self.base))
            .bearer_auth(&self.key)
            .timeout(timeout);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .with_context(|| format!("call {path}"))?;
        let status = response.status();
        Ok((status, response.text().await.unwrap_or_default()))
    }

    async fn get<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T> {
        let (status, body) = self.call(Method::GET, path, None, REQUEST_TIMEOUT).await?;
        if !status.is_success() && status != StatusCode::SERVICE_UNAVAILABLE {
            bail!("{path} answered {status}: {body}");
        }
        serde_json::from_str(&body).with_context(|| format!("parse {path}"))
    }

    async fn post(&self, path: &str, body: Value) -> Result<()> {
        let (status, text) = self
            .call(Method::POST, path, Some(body), MEMBERSHIP_TIMEOUT)
            .await?;
        if !status.is_success() {
            bail!("{path} answered {status}: {text}");
        }
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (status, text) = self
            .call(Method::DELETE, path, None, MEMBERSHIP_TIMEOUT)
            .await?;
        if !status.is_success() && status != StatusCode::NOT_FOUND {
            bail!("{path} answered {status}: {text}");
        }
        Ok(())
    }

    async fn health(&self) -> Result<Health> {
        self.get("/health").await
    }

    pub async fn is_leader(&self) -> bool {
        self.health().await.is_ok_and(|h| h.raft_leader)
    }

    async fn primary(&self) -> Result<Option<String>> {
        Ok(self.health().await?.primary)
    }

    async fn nodes(&self) -> Result<Vec<Node>> {
        Ok(self.get::<NodeList>("/api/nodes").await?.nodes)
    }

    /// The standby closest to the primary.
    async fn best_standby(&self) -> Result<String> {
        self.nodes()
            .await?
            .into_iter()
            .filter(|n| n.role.eq_ignore_ascii_case("replica"))
            .min_by_key(|n| n.lag_bytes)
            .map(|n| n.node_id)
            .context("no standby can take over")
    }

    async fn switchover(&self, target: Option<&str>) -> Result<Value> {
        let primary = self.primary().await?;
        // A repeat of a task that already got this far.
        if target.is_some() && primary.as_deref() == target {
            return Ok(json!({ "primary": target }));
        }
        let target = match target {
            Some(target) => target.to_string(),
            None => self.best_standby().await?,
        };
        self.post("/api/switchover", json!({ "target_node_id": target }))
            .await?;
        let deadline = Instant::now() + SWITCHOVER_TIMEOUT;
        loop {
            if self.primary().await.ok().flatten().as_deref() == Some(target.as_str()) {
                return Ok(json!({ "primary": target }));
            }
            if Instant::now() >= deadline {
                bail!("{target} did not become the primary in {SWITCHOVER_TIMEOUT:?}");
            }
            tokio::time::sleep(POLL).await;
        }
    }

    async fn rolling_restart(&self) -> Result<Value> {
        self.post("/api/restart", json!({})).await?;
        let deadline = Instant::now() + RESTART_TIMEOUT;
        loop {
            let status: RunStatus = self.get("/api/restart").await?;
            if !status.running {
                return match status.error {
                    Some(error) => Err(anyhow!(error)),
                    None => Ok(json!({})),
                };
            }
            if Instant::now() >= deadline {
                bail!("the rolling restart did not finish in {RESTART_TIMEOUT:?}");
            }
            tokio::time::sleep(POLL).await;
        }
    }

    async fn backup(&self, label: &str) -> Result<Value> {
        self.post("/api/backups", json!({ "label": label })).await?;
        let deadline = Instant::now() + BACKUP_TIMEOUT;
        loop {
            let backups: Backups = self.get("/api/backups").await?;
            if let Some(found) = backups.backups.iter().find(|b| b.label == label) {
                return Ok(json!({ "backup_id": found.backup_id }));
            }
            let status: BackupStatus = self.get("/api/backups/status").await?;
            if status.failed_label.as_deref() == Some(label) {
                return Err(anyhow!(status.error.unwrap_or_default()));
            }
            if Instant::now() >= deadline {
                bail!("the backup did not finish in {BACKUP_TIMEOUT:?}");
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Bring in the new node, wait for it, and only then let the old one go, so
    /// the cluster never runs short of voters.
    async fn replace_member(&self, params: &Value) -> Result<Value> {
        let text = |key: &str| {
            params[key]
                .as_str()
                .with_context(|| format!("replace_member needs {key}"))
        };
        let (old, new) = (text("old_node")?, text("new_node")?);
        let new_id = raft_id(new);

        let members: Vec<Member> = self.get("/api/members").await?;
        if !members.iter().any(|m| m.id == new_id) {
            self.post(
                "/api/members",
                json!({ "id": new_id, "addr": text("raft_addr")? }),
            )
            .await?;
        }
        if !self.nodes().await?.iter().any(|n| n.node_id == new) {
            self.post(
                "/api/nodes/add",
                json!({
                    "node_id": new,
                    "agent_addr": text("agent_addr")?,
                    "postgres_addr": text("postgres_addr")?,
                    "priority": 90,
                }),
            )
            .await?;
        }
        self.wait_for_standby(new).await?;

        let members: Vec<Member> = self.get("/api/members").await?;
        if members.iter().any(|m| m.id == new_id && !m.voter) {
            self.post(&format!("/api/members/{new_id}/promote"), json!({}))
                .await?;
        }
        if self.primary().await?.as_deref() == Some(old) {
            self.switchover(Some(new)).await?;
        }
        self.delete(&format!("/api/nodes/{old}")).await?;
        self.delete(&format!("/api/members/{}", raft_id(old)))
            .await?;
        Ok(json!({}))
    }

    async fn wait_for_standby(&self, node: &str) -> Result<()> {
        let deadline = Instant::now() + SEED_TIMEOUT;
        loop {
            let ready = self.nodes().await?.iter().any(|n| {
                n.node_id == node
                    && n.role.eq_ignore_ascii_case("replica")
                    && n.lag_bytes <= CAUGHT_UP_BYTES
            });
            if ready {
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!("{node} did not catch up in {SEED_TIMEOUT:?}");
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// What the leader knows, for `pgcluster-admin` to record.
    pub async fn cluster_report(&self, database_id: &str) -> Result<ClusterReport> {
        let replication: Replication = self.get("/api/replication").await?;
        let failovers: Vec<Value> = self.get("/api/failover/history").await?;
        let backups: Backups = self.get("/api/backups").await?;
        let status: BackupStatus = self.get("/api/backups/status").await?;
        Ok(ClusterReport {
            database_id: database_id.to_string(),
            primary: self.primary().await?,
            sync_degraded: replication.degraded,
            needs_reseed: replication.needs_reseed,
            failovers,
            backups: backups
                .backups
                .into_iter()
                .map(|b| BackupReport {
                    id: b.backup_id,
                    label: b.label,
                    completed_at: b.completed_at,
                    size_bytes: b.size_bytes,
                })
                .collect(),
            backup_error: status.error,
        })
    }
}

/// Whether this node should run `task` now.
pub async fn is_ours(manager: &InstanceManager<'_>, instance: &Instance, task: &Task) -> bool {
    match task.on.as_str() {
        "leader" => match LocalApi::new(instance) {
            Ok(api) => api.is_leader().await,
            Err(_) => false,
        },
        "primary" => manager.is_primary(instance).await,
        _ => false,
    }
}

/// Run `task` on this node.
pub async fn run(manager: &InstanceManager<'_>, instance: &Instance, task: &Task) -> Result<Value> {
    let api = LocalApi::new(instance)?;
    match task.kind.as_str() {
        "switchover" => {
            let target = task.params["target"].as_str();
            api.switchover(target).await
        }
        "rolling_restart" => api.rolling_restart().await,
        "replication_mode" => {
            let mode = task.params["mode"].as_str().context("no mode")?;
            api.post("/api/replication/mode", json!({ "mode": mode }))
                .await?;
            Ok(json!({}))
        }
        "backup" => {
            let label = task.params["label"].as_str().context("no label")?;
            api.backup(label).await
        }
        "replace_member" => api.replace_member(&task.params).await,
        "alter_passwords" => alter_passwords(manager, instance).await,
        other => bail!("unknown task {other}"),
    }
}

/// Move the roles to their next passwords. Replication carries this to standbys,
/// so it runs once, on the primary.
async fn alter_passwords(manager: &InstanceManager<'_>, instance: &Instance) -> Result<Value> {
    let next = instance
        .secrets
        .next
        .as_ref()
        .context("no passwords to move to")?;
    let mut postgres = manager.connect_ready(instance, "postgres").await?;
    for (role, password) in [
        (REPLICATION_USER, &next.replication_password),
        (AUTH_USER, &next.auth_password),
        (SUPERUSER, &next.superuser_password),
    ] {
        sqlx::query(&format!(
            "ALTER ROLE {role} PASSWORD {}",
            quote_literal(password)
        ))
        .execute(&mut postgres)
        .await
        .with_context(|| format!("change the password of {role}"))?;
    }
    postgres.close().await.ok();
    Ok(json!({}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_api_is_reached_on_this_instances_port() {
        let api = LocalApi::new(&super::super::model::tests::instance()).unwrap();
        assert_eq!(api.base, "http://127.0.0.1:20007");
    }
}
