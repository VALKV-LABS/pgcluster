//! `vk-agent host`: the host agent. One runs on every machine, opens no port,
//! and makes the machine match what `pgcluster-admin` asks for.
//!
//! It keeps its last desired state on disk and leaves instances running while
//! `pgcluster-admin` is away. It takes no part in failover: each cluster's Raft
//! group decides that.

pub mod capacity;
pub mod client;
pub mod config;
pub mod firewall;
pub mod fsutil;
pub mod instance;
pub mod model;
pub mod quota;
pub mod render;
pub mod restore;
pub mod runner;
pub mod tasks;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};

use client::{AdminClient, Poll};
use config::HostConfig;
use instance::{read_applied, InstanceManager, Progress, WalSamples};
use model::{DesiredState, Instance, InstanceReport, Report, RepositoryCheck, Task, TaskResult};
use render::Layout;
use runner::{RunAs, Runner};

const ENROLL_RETRY: Duration = Duration::from_secs(5);
const ERROR_RETRY: Duration = Duration::from_secs(2);
/// How soon a task waiting for its node gets another look.
const TASK_TICK: Duration = Duration::from_secs(2);
/// How often to look at each cluster, while the leader's report is due.
const WATCH_TICK: Duration = Duration::from_secs(5);

/// What the next report says.
#[derive(Debug, Default, Clone)]
pub struct Status {
    pub observed_generation: i64,
    pub instances: HashMap<String, InstanceReport>,
}

pub type SharedStatus = Arc<Mutex<Status>>;
pub type SharedDesired = Arc<RwLock<Option<DesiredState>>>;

fn desired_path(config: &HostConfig) -> PathBuf {
    Path::new(&config.state_dir).join("desired.json")
}

pub fn load_desired(config: &HostConfig) -> Result<Option<DesiredState>> {
    match std::fs::read_to_string(desired_path(config)) {
        Ok(text) => Ok(Some(
            serde_json::from_str(&text).context("parse the saved desired state")?,
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).context("read the saved desired state"),
    }
}

/// It holds secrets, so only its owner can read it.
fn save_desired(config: &HostConfig, desired: &DesiredState) -> Result<()> {
    let text = serde_json::to_vec(desired)?;
    fsutil::write_private(&desired_path(config), &text)
}

fn read_token(config: &HostConfig) -> Result<String> {
    if let Ok(token) = std::env::var("VK_AGENT_ENROLLMENT_TOKEN") {
        return Ok(token);
    }
    let path = config
        .enrollment_token_file
        .as_deref()
        .context("set enrollment_token_file or VK_AGENT_ENROLLMENT_TOKEN to enrol this host")?;
    std::fs::read_to_string(path).with_context(|| format!("read {path}"))
}

/// Instance directories on disk, whatever desired state says.
fn instances_on_disk(config: &HostConfig) -> Result<Vec<String>> {
    let mut ids = Vec::new();
    for entry in std::fs::read_dir(&config.instances_dir).context("list instances")? {
        let entry = entry.context("list instances")?;
        if entry.path().is_dir() {
            ids.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
    Ok(ids)
}

pub struct HostAgent {
    config: HostConfig,
    runner: Arc<dyn Runner>,
    run_as: Option<RunAs>,
    client: AdminClient,
    status: SharedStatus,
    desired: SharedDesired,
}

impl HostAgent {
    fn manager(&self) -> InstanceManager<'_> {
        InstanceManager {
            config: &self.config,
            runner: self.runner.as_ref(),
            run_as: self.run_as,
            client: &self.client,
        }
    }

    /// Make this machine match `desired`. Returns whether all of it now does.
    pub async fn reconcile(&self, desired: &DesiredState) -> bool {
        let manager = self.manager();
        let mut all_ok = true;
        let mut reports = HashMap::new();

        for instance in &desired.instances {
            let id = instance.id();
            let observed = read_applied(&Layout::new(&self.config.instances_dir, &id))
                .map_or(0, |a| a.generation);
            let report = |observed_generation, state, error| InstanceReport {
                database_id: instance.database_id.clone(),
                node: instance.node.clone(),
                observed_generation,
                state,
                error,
                archived_until: None,
                server_version: None,
            };
            self.status
                .lock()
                .expect("status lock")
                .instances
                .entry(id.clone())
                .or_insert_with(|| report(observed, "starting", None));
            match manager.apply(instance).await {
                Ok(Progress::Running) => {
                    reports.insert(id, report(instance.generation, "running", None));
                }
                Ok(Progress::Seeding) => {
                    all_ok = false;
                    reports.insert(id, report(observed, "seeding", None));
                }
                Err(e) => {
                    all_ok = false;
                    let message = format!("{e:#}");
                    tracing::error!(id, err = message, "could not apply instance");
                    self.client
                        .log(instance.operation_id.as_deref(), &message)
                        .await;
                    reports.insert(id, report(observed, "failed", Some(message)));
                }
            }
        }

        match instances_on_disk(&self.config) {
            Ok(ids) => {
                for id in ids {
                    if desired.instances.iter().any(|i| i.id() == id) {
                        continue;
                    }
                    if let Err(e) = manager.remove(&id).await {
                        all_ok = false;
                        tracing::error!(id, err = format!("{e:#}"), "could not remove instance");
                    }
                }
            }
            Err(e) => {
                all_ok = false;
                tracing::error!(err = format!("{e:#}"), "could not list instances");
            }
        }

        let mut status = self.status.lock().expect("status lock");
        status.instances = reports;
        if all_ok {
            status.observed_generation = desired.generation;
        }
        all_ok
    }

    /// Between changes, notice an instance that stopped.
    async fn health_check(&self, desired: &DesiredState) -> bool {
        for instance in &desired.instances {
            let id = instance.id();
            let running = match self.manager().processes_for(instance) {
                Ok(processes) => self.runner.is_running(&processes).await.unwrap_or(false),
                Err(_) => false,
            };
            if !running {
                tracing::warn!(id, "instance is not running");
                return false;
            }
            let layout = Layout::new(&self.config.instances_dir, &id);
            if self.manager().has_work_left(instance, &layout).await {
                tracing::info!(id, "promoted with changes left to apply");
                return false;
            }
        }
        true
    }

    fn publish(&self, state: DesiredState) {
        if let Some(ca) = &state.admin_ca {
            if let Err(e) = self.client.trust(&ca.bundle_pem) {
                tracing::error!(err = format!("{e:#}"), "could not update the trusted CAs");
            }
        }
        *self.desired.write().expect("desired lock") = Some(state);
    }

    async fn run(self) -> Result<()> {
        let mut desired = load_desired(&self.config)?;
        if let Some(saved) = &desired {
            tracing::info!(
                generation = saved.generation,
                "resuming saved desired state"
            );
            self.publish(saved.clone());
            self.reconcile(saved).await;
        }
        let wait = Duration::from_secs(self.config.report_interval_seconds);
        let mut needs_retry = false;
        loop {
            let generation = desired.as_ref().map_or(0, |d| d.generation);
            match self.client.poll(generation, wait).await {
                Ok(Poll::Changed(state)) => {
                    tracing::info!(generation = state.generation, "desired state changed");
                    if let Err(e) = save_desired(&self.config, &state) {
                        tracing::error!(err = format!("{e:#}"), "could not save desired state");
                    }
                    self.publish(state.clone());
                    needs_retry = !self.reconcile(&state).await;
                    desired = Some(state);
                }
                Ok(Poll::Unchanged) => {
                    if let Some(state) = &desired {
                        if needs_retry || !self.health_check(state).await {
                            needs_retry = !self.reconcile(state).await;
                        }
                    }
                }
                // Instances keep running, so all this costs is staleness.
                Err(e) => {
                    tracing::warn!(err = format!("{e:#}"), "cannot reach pgcluster-admin");
                    tokio::time::sleep(ERROR_RETRY).await;
                }
            }
            let renew = desired
                .as_ref()
                .and_then(|d| d.admin_ca.as_ref())
                .is_some_and(|ca| ca.renew_client_certificate);
            let rotated = if renew {
                self.client.renew_certificate().await
            } else {
                self.client.rotate_if_due().await
            };
            if let Err(e) = rotated {
                tracing::warn!(err = format!("{e:#}"), "could not rotate the certificate");
            }
        }
    }
}

/// Report on a timer of its own, so a slow change does not read as a dead host.
fn spawn_reporter(config: HostConfig, client: AdminClient, status: SharedStatus) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        let mut last: Option<(i64, Vec<InstanceReport>)> = None;
        let mut since_sent = u64::MAX;
        loop {
            ticker.tick().await;
            since_sent = since_sent.saturating_add(1);
            let snapshot = status.lock().expect("status lock").clone();
            let mut instances: Vec<_> = snapshot.instances.into_values().collect();
            instances.sort_by(|a, b| a.database_id.cmp(&b.database_id));
            let current = (snapshot.observed_generation, instances.clone());
            // Send a change straight away, and otherwise on the interval.
            if last.as_ref() == Some(&current) && since_sent < config.report_interval_seconds {
                continue;
            }
            match build_report(&config, &client, snapshot.observed_generation, instances) {
                Ok(report) => match client.report(&report).await {
                    Ok(()) => {
                        last = Some(current);
                        since_sent = 0;
                    }
                    Err(e) => tracing::warn!(err = format!("{e:#}"), "could not report"),
                },
                Err(e) => tracing::error!(err = format!("{e:#}"), "could not build a report"),
            }
        }
    });
}

/// What a background loop needs to act on an instance.
#[derive(Clone)]
struct Worker {
    config: HostConfig,
    runner: Arc<dyn Runner>,
    run_as: Option<RunAs>,
    client: AdminClient,
}

impl Worker {
    fn manager(&self) -> InstanceManager<'_> {
        InstanceManager {
            config: &self.config,
            runner: self.runner.as_ref(),
            run_as: self.run_as,
            client: &self.client,
        }
    }

    async fn run_task(&self, instance: &Instance, task: &Task) -> bool {
        let manager = self.manager();
        if !tasks::is_ours(&manager, instance, task).await {
            return false;
        }
        tracing::info!(id = task.id, kind = task.kind, "running a task");
        self.client
            .log(
                instance.operation_id.as_deref(),
                &format!("{} on {}", task.kind, instance.node),
            )
            .await;
        let result = match tasks::run(&manager, instance, task).await {
            Ok(output) => TaskResult {
                success: true,
                error: None,
                output,
            },
            Err(e) => TaskResult {
                success: false,
                error: Some(format!("{e:#}")),
                output: serde_json::Value::Null,
            },
        };
        if let Err(e) = self.client.task_result(&task.id, &result).await {
            tracing::warn!(err = format!("{e:#}"), "could not report a task");
            return false;
        }
        true
    }

    async fn check_repository(&self, check: &RepositoryCheck) {
        let dir = Path::new(&self.config.state_dir);
        let result = async {
            check.target.validate()?;
            let probe = dir.join(format!("repository-check-{}", check.id));
            std::fs::write(&probe, b"pgcluster").context("write a probe")?;
            let user = self.run_as.map(|u| (u.uid.as_raw(), u.gid.as_raw()));
            if let Some((uid, gid)) = user {
                std::os::unix::fs::chown(&probe, Some(uid), Some(gid)).context("chown a probe")?;
            }
            let repository = crate::walg::Repository::create(
                &self.config.walg_path,
                dir.join(format!("repository-check-{}.json", check.id)),
                &check.target,
                user,
            )
            .await?;
            let outcome = repository.check(&probe).await;
            std::fs::remove_file(&probe).ok();
            std::fs::remove_file(&repository.config_path).ok();
            outcome
        }
        .await;
        let error = result.err().map(|e| format!("{e:#}"));
        if let Err(e) = self
            .client
            .repository_check(&check.id, error.as_deref())
            .await
        {
            tracing::warn!(
                err = format!("{e:#}"),
                "could not report a repository check"
            );
        }
    }
}

/// Run tasks as they appear, each on its own, so a long one holds up nothing.
fn spawn_tasks(worker: Worker, desired: SharedDesired) {
    tokio::spawn(async move {
        let running: Arc<Mutex<HashSet<String>>> = Arc::default();
        let mut ticker = tokio::time::interval(TASK_TICK);
        loop {
            ticker.tick().await;
            let Some(state) = desired.read().expect("desired lock").clone() else {
                continue;
            };
            let claim = |id: &str| running.lock().expect("task lock").insert(id.to_string());
            for check in state.repository_checks {
                if claim(&check.id) {
                    let (worker, running) = (worker.clone(), running.clone());
                    tokio::spawn(async move {
                        worker.check_repository(&check).await;
                        running.lock().expect("task lock").remove(&check.id);
                    });
                }
            }
            for instance in state.instances {
                for task in instance.tasks.clone() {
                    if !claim(&task.id) {
                        continue;
                    }
                    let (worker, running) = (worker.clone(), running.clone());
                    let instance = instance.clone();
                    tokio::spawn(async move {
                        worker.run_task(&instance, &task).await;
                        running.lock().expect("task lock").remove(&task.id);
                    });
                }
            }
        }
    });
}

/// Watch each instance: the leader says how its cluster stands, and the primary
/// says how far the WAL archive reaches.
fn spawn_watcher(worker: Worker, desired: SharedDesired, status: SharedStatus) {
    tokio::spawn(async move {
        let mut samples: HashMap<String, WalSamples> = HashMap::new();
        let mut ticker = tokio::time::interval(WATCH_TICK);
        let mut ticks = 0u64;
        let report_every = (worker.config.report_interval_seconds / WATCH_TICK.as_secs()).max(1);
        loop {
            ticker.tick().await;
            ticks += 1;
            let instances = match desired.read().expect("desired lock").as_ref() {
                Some(state) => state.instances.clone(),
                None => continue,
            };
            let manager = worker.manager();
            for instance in &instances {
                let id = instance.id();
                let layout = Layout::new(&worker.config.instances_dir, &id);
                if !layout.data().join("PG_VERSION").exists() {
                    continue;
                }
                let (version, until) = manager
                    .observe(instance, samples.entry(id.clone()).or_default())
                    .await;
                if let Some(report) = status.lock().expect("status lock").instances.get_mut(&id) {
                    report.archived_until = until.map(|time| time.to_rfc3339());
                    if version.is_some() {
                        report.server_version = version;
                    }
                }
                if !ticks.is_multiple_of(report_every) {
                    continue;
                }
                let Ok(api) = tasks::LocalApi::new(instance) else {
                    continue;
                };
                if !api.is_leader().await {
                    continue;
                }
                match api.cluster_report(&instance.database_id).await {
                    Ok(report) => {
                        if let Err(e) = worker.client.cluster_report(&report).await {
                            tracing::debug!(err = format!("{e:#}"), "no cluster report");
                        }
                    }
                    Err(e) => tracing::debug!(err = format!("{e:#}"), "no cluster report"),
                }
            }
        }
    });
}

fn build_report(
    config: &HostConfig,
    client: &AdminClient,
    observed_generation: i64,
    instances: Vec<InstanceReport>,
) -> Result<Report> {
    let address = match &config.advertise_address {
        Some(address) => address.clone(),
        None => capacity::detect_address(&config.admin_url)?,
    };
    Ok(Report {
        observed_generation,
        version: env!("CARGO_PKG_VERSION").to_string(),
        address,
        capacity: capacity::measure(&config.instances_dir)?,
        instances,
        ca_fingerprint: client.ca_fingerprint()?,
        postgres_versions: capacity::postgres_versions(config),
    })
}

/// Run the host agent until the process is stopped.
pub async fn run(config_path: &str) -> Result<()> {
    let config = HostConfig::from_file(config_path)?;
    std::fs::create_dir_all(&config.state_dir)
        .with_context(|| format!("create {}", config.state_dir))?;
    std::fs::create_dir_all(&config.instances_dir)
        .with_context(|| format!("create {}", config.instances_dir))?;
    let run_as = config.run_as.as_deref().map(RunAs::lookup).transpose()?;
    quota::init(config.disk_quota, &config.instances_dir)?;

    // An unreachable server is worth waiting for, but a refused token is not:
    // retrying cannot make it work.
    let client = loop {
        let token = || read_token(&config);
        match AdminClient::connect(&config.admin_url, &config.state_dir, token).await {
            Ok(client) => break client,
            Err(e) => {
                let message = format!("{e:#}");
                let permanent = [
                    "invalid, expired",
                    "does not match",
                    "malformed",
                    "set enrollment",
                ]
                .iter()
                .any(|text| message.contains(text));
                if permanent {
                    return Err(e);
                }
                tracing::warn!(err = message, "waiting for pgcluster-admin");
                tokio::time::sleep(ENROLL_RETRY).await;
            }
        }
    };
    tracing::info!(host_id = client.host_id, "vk-agent host running");

    let status: SharedStatus = Arc::default();
    let desired: SharedDesired = Arc::default();
    let runner: Arc<dyn Runner> = Arc::from(runner::build(&config));
    let worker = Worker {
        config: config.clone(),
        runner: runner.clone(),
        run_as,
        client: client.clone(),
    };
    spawn_reporter(config.clone(), client.clone(), status.clone());
    spawn_tasks(worker.clone(), desired.clone());
    spawn_watcher(worker, desired.clone(), status.clone());
    let agent = HostAgent {
        runner,
        config,
        run_as,
        client,
        status,
        desired,
    };
    agent.run().await
}
