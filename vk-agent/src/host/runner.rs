//! Starts and stops an instance's processes. `vk-agent` owns Postgres, so an
//! instance is two processes: it, and `pgcluster`.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use nix::sys::signal::{kill, Signal};
use nix::unistd::{Gid, Pid, Uid, User};
use tokio::process::Command;

use super::config::{HostConfig, RunnerKind};
use super::model::Resources;
use super::render::Layout;

const STOP_TIMEOUT: Duration = Duration::from_secs(30);
const STOP_POLL: Duration = Duration::from_millis(100);

/// The user instances run as, when this agent runs as someone else.
#[derive(Debug, Clone, Copy)]
pub struct RunAs {
    pub uid: Uid,
    pub gid: Gid,
    /// Its home directory. Tools such as libpq look there for files, and must
    /// not look in the home of the user that started them.
    pub home: &'static str,
}

impl RunAs {
    pub fn lookup(name: &str) -> Result<Self> {
        let user = User::from_name(name)
            .with_context(|| format!("look up user {name}"))?
            .with_context(|| format!("no user named {name}"))?;
        let home = user.dir.to_string_lossy().into_owned();
        Ok(Self {
            uid: user.uid,
            gid: user.gid,
            // Looked up once per process, so leaking it costs nothing.
            home: Box::leak(home.into_boxed_str()),
        })
    }
}

pub fn command_as(program: &str, run_as: Option<RunAs>) -> Command {
    let mut command = Command::new(program);
    if let Some(user) = run_as {
        command
            .uid(user.uid.as_raw())
            .gid(user.gid.as_raw())
            .env("HOME", user.home);
    }
    command
}

/// What a runner needs to know about one instance.
#[derive(Debug, Clone)]
pub struct Processes {
    pub id: String,
    pub layout: Layout,
    pub vk_agent: String,
    pub pgcluster: String,
    pub pg_ctl: String,
    pub run_as: Option<RunAs>,
    pub user: Option<String>,
    /// What the instance may use. Set when applying desired state.
    pub resources: Option<Resources>,
    /// Whether it runs `pgcluster`. An external standby does not.
    pub proxy: bool,
}

impl Processes {
    /// The units this instance runs, `vk-agent` first.
    fn units(&self) -> Vec<String> {
        let mut units = vec![agent_unit_name(&self.id)];
        if self.proxy {
            units.push(proxy_unit_name(&self.id));
        }
        units
    }
}

// async_trait generates the attribute, so this lint cannot be fixed at the source.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait Runner: Send + Sync {
    /// Record how to run the instance. Doing this again changes nothing.
    async fn install(&self, processes: &Processes) -> Result<()>;
    async fn start(&self, processes: &Processes) -> Result<()>;
    async fn stop(&self, processes: &Processes) -> Result<()>;
    async fn restart(&self, processes: &Processes) -> Result<()>;
    /// Have `pgcluster` read its allowlist again, keeping connected clients.
    async fn reload_proxy(&self, processes: &Processes) -> Result<()>;
    /// Have `vk-agent` and `pgcluster` read their certificates again.
    async fn reload_certificates(&self, processes: &Processes) -> Result<()>;
    async fn is_running(&self, processes: &Processes) -> Result<bool>;
    /// Stop the instance and forget it. Its directory stays.
    async fn remove(&self, processes: &Processes) -> Result<()>;
}

pub fn build(config: &HostConfig) -> Box<dyn Runner> {
    match config.runner {
        RunnerKind::Systemd => Box::new(Systemd {
            unit_dir: PathBuf::from(&config.systemd_unit_dir),
        }),
        RunnerKind::Plain => Box::new(Plain),
    }
}

// ── systemd ───────────────────────────────────────────────────────────────────

pub struct Systemd {
    pub unit_dir: PathBuf,
}

fn agent_unit_name(id: &str) -> String {
    format!("vk-instance-{id}-agent.service")
}

fn proxy_unit_name(id: &str) -> String {
    format!("vk-instance-{id}-pgcluster.service")
}

fn slice_name(id: &str) -> String {
    format!("vk-instance-{id}.slice")
}

/// The cgroup v2 limits both of an instance's units share, so a busy database
/// cannot take another's CPU, memory or disk bandwidth.
pub struct Limits {
    pub cpu_quota_percent: u64,
    pub memory_max_bytes: u64,
    pub io_weight: u64,
}

impl Limits {
    pub fn of(resources: &Resources) -> Self {
        Self {
            cpu_quota_percent: (resources.vcpu * 100.0).round().max(1.0) as u64,
            memory_max_bytes: (resources.memory_gb * 1024.0 * 1024.0 * 1024.0) as u64,
            // In proportion to CPU, so bandwidth under contention follows size.
            io_weight: (resources.vcpu * 100.0).round().clamp(1.0, 10000.0) as u64,
        }
    }

    fn properties(&self) -> [String; 3] {
        [
            format!("CPUQuota={}%", self.cpu_quota_percent),
            format!("MemoryMax={}", self.memory_max_bytes),
            format!("IOWeight={}", self.io_weight),
        ]
    }
}

pub fn slice_unit(id: &str, limits: &Limits) -> String {
    let [cpu, memory, io] = limits.properties();
    format!(
        "[Unit]\n\
         Description=Resource limits for {id}\n\
         \n\
         [Slice]\n\
         CPUAccounting=yes\n\
         MemoryAccounting=yes\n\
         IOAccounting=yes\n\
         {cpu}\n\
         {memory}\n\
         {io}\n"
    )
}

/// SIGINT is a fast shutdown for Postgres, and stops `vk-agent` too. Postgres is
/// not `vk-agent`'s child, but they share this unit's control group.
pub fn agent_unit(processes: &Processes) -> String {
    let user = user_line(processes);
    format!(
        "[Unit]\n\
         Description=vk-agent and Postgres for {id}\n\
         After=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         Slice={slice}\n\
         {user}\
         ExecStart={program} {config}\n\
         Restart=always\n\
         RestartSec=2\n\
         KillMode=control-group\n\
         KillSignal=SIGINT\n\
         TimeoutStopSec=90\n\
         LimitNOFILE=65536\n\
         \n\
         [Install]\n\
         WantedBy=multi-user.target\n",
        id = processes.id,
        slice = slice_name(&processes.id),
        program = processes.vk_agent,
        config = processes.layout.agent_config().display(),
    )
}

pub fn proxy_unit(processes: &Processes) -> String {
    let user = user_line(processes);
    format!(
        "[Unit]\n\
         Description=pgcluster for {id}\n\
         After=network-online.target {agent}\n\
         \n\
         [Service]\n\
         Type=simple\n\
         Slice={slice}\n\
         {user}\
         EnvironmentFile={env}\n\
         ExecStart={program} server --config {config}\n\
         Restart=always\n\
         RestartSec=2\n\
         LimitNOFILE=65536\n\
         \n\
         [Install]\n\
         WantedBy=multi-user.target\n",
        id = processes.id,
        slice = slice_name(&processes.id),
        agent = agent_unit_name(&processes.id),
        env = processes.layout.env_file().display(),
        program = processes.pgcluster,
        config = processes.layout.pgcluster_config().display(),
    )
}

fn user_line(processes: &Processes) -> String {
    processes
        .user
        .as_ref()
        .map(|user| format!("User={user}\n"))
        .unwrap_or_default()
}

async fn systemctl(args: &[&str]) -> Result<()> {
    let output = Command::new("systemctl")
        .args(args)
        .output()
        .await
        .context("run systemctl")?;
    if !output.status.success() {
        bail!(
            "systemctl {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

#[async_trait]
impl Runner for Systemd {
    async fn install(&self, processes: &Processes) -> Result<()> {
        let agent = self.unit_dir.join(agent_unit_name(&processes.id));
        let proxy = self.unit_dir.join(proxy_unit_name(&processes.id));
        let mut units = vec![
            (agent, agent_unit(processes)),
            (proxy, proxy_unit(processes)),
        ];
        let slice = slice_name(&processes.id);
        let limits = processes.resources.as_ref().map(Limits::of);
        if let Some(limits) = &limits {
            units.push((
                self.unit_dir.join(&slice),
                slice_unit(&processes.id, limits),
            ));
        }
        let mut changed = false;
        for (path, text) in units {
            if tokio::fs::read_to_string(&path).await.ok().as_ref() != Some(&text) {
                tokio::fs::write(&path, text)
                    .await
                    .with_context(|| format!("write {}", path.display()))?;
                changed = true;
            }
        }
        if changed {
            systemctl(&["daemon-reload"]).await?;
            // Applies new limits to a running slice at once, with no restart.
            if let Some(limits) = &limits {
                let properties = limits.properties();
                let mut args = vec!["set-property", "--runtime", slice.as_str()];
                args.extend(properties.iter().map(String::as_str));
                systemctl(&args).await?;
            }
        }
        Ok(())
    }

    /// `enable`, so an instance returns after a reboot with no help from
    /// `pgcluster-admin`.
    async fn start(&self, processes: &Processes) -> Result<()> {
        let units = processes.units();
        let mut args = vec!["enable", "--now"];
        args.extend(units.iter().map(String::as_str));
        systemctl(&args).await
    }

    async fn stop(&self, processes: &Processes) -> Result<()> {
        let agent = agent_unit_name(&processes.id);
        let proxy = proxy_unit_name(&processes.id);
        systemctl(&["disable", "--now", &proxy, &agent]).await
    }

    async fn restart(&self, processes: &Processes) -> Result<()> {
        for unit in processes.units() {
            systemctl(&["restart", &unit]).await?;
        }
        Ok(())
    }

    /// SIGHUP to the main process, from systemd itself, so it needs no `kill`
    /// on the machine.
    async fn reload_proxy(&self, processes: &Processes) -> Result<()> {
        let proxy = proxy_unit_name(&processes.id);
        systemctl(&["kill", "--kill-whom=main", "--signal=HUP", &proxy]).await
    }

    async fn reload_certificates(&self, processes: &Processes) -> Result<()> {
        let units = processes.units();
        let mut args = vec!["kill", "--kill-whom=main", "--signal=HUP"];
        args.extend(units.iter().map(String::as_str));
        systemctl(&args).await
    }

    async fn is_running(&self, processes: &Processes) -> Result<bool> {
        for unit in processes.units() {
            let status = Command::new("systemctl")
                .args(["is-active", "--quiet", &unit])
                .status()
                .await
                .context("run systemctl")?;
            if !status.success() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn remove(&self, processes: &Processes) -> Result<()> {
        self.stop(processes).await.ok();
        for name in [
            agent_unit_name(&processes.id),
            proxy_unit_name(&processes.id),
            slice_name(&processes.id),
        ] {
            let path = self.unit_dir.join(name);
            if path.exists() {
                tokio::fs::remove_file(&path)
                    .await
                    .with_context(|| format!("remove {}", path.display()))?;
            }
        }
        systemctl(&["daemon-reload"]).await
    }
}

// ── Plain processes ───────────────────────────────────────────────────────────

/// Detached processes tracked with pid files, for development and tests. They
/// outlive this agent, as units do, but nothing restarts them after a reboot.
pub struct Plain;

fn pid_path(layout: &Layout, name: &str) -> PathBuf {
    layout.dir.join(format!("{name}.pid"))
}

/// The pid in `file`, if that process is alive and still runs `config`. The
/// check on its arguments stops a recycled pid from being mistaken for it.
fn live_pid(file: &Path, config: &Path) -> Option<Pid> {
    let pid: i32 = std::fs::read_to_string(file).ok()?.trim().parse().ok()?;
    let cmdline = std::fs::read(format!("/proc/{pid}/cmdline")).ok()?;
    let wanted = config.to_string_lossy();
    let matches = cmdline
        .split(|b| *b == 0)
        .any(|arg| arg == wanted.as_bytes());
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let zombie = stat
        .rsplit_once(')')
        .is_some_and(|(_, rest)| rest.trim_start().starts_with('Z'));
    (matches && !zombie).then_some(Pid::from_raw(pid))
}

async fn spawn_detached(
    processes: &Processes,
    name: &str,
    program: &str,
    args: &[&str],
    config: &Path,
    env: &[(String, String)],
) -> Result<()> {
    let pid_file = pid_path(&processes.layout, name);
    if live_pid(&pid_file, config).is_some() {
        return Ok(());
    }
    let log = processes.layout.logs().join(format!("{name}.log"));
    let open_log = || {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log)
            .with_context(|| format!("open {}", log.display()))
    };
    let mut command = command_as(program, processes.run_as);
    command
        .args(args)
        .envs(env.iter().cloned())
        .stdin(Stdio::null())
        .stdout(open_log()?)
        .stderr(open_log()?)
        // Its own process group, so a signal to this agent does not reach it.
        .process_group(0);
    let mut child = command
        .spawn()
        .with_context(|| format!("start {program}"))?;
    let pid = child.id().context("the process exited at once")?;
    tokio::fs::write(&pid_file, pid.to_string())
        .await
        .context("write pid file")?;
    // Collect the exit status, or a dead child would linger as a zombie.
    tokio::spawn(async move {
        let _ = child.wait().await;
    });
    Ok(())
}

async fn signal_and_wait(pid_file: &Path, config: &Path, signal: Signal) -> Result<()> {
    let Some(pid) = live_pid(pid_file, config) else {
        return Ok(());
    };
    kill(pid, signal).context("signal process")?;
    let deadline = Instant::now() + STOP_TIMEOUT;
    while live_pid(pid_file, config).is_some() {
        if Instant::now() >= deadline {
            kill(pid, Signal::SIGKILL).ok();
            break;
        }
        tokio::time::sleep(STOP_POLL).await;
    }
    Ok(())
}

fn read_env_file(path: &Path) -> Result<Vec<(String, String)>> {
    let text = std::fs::read_to_string(path).context("read instance.env")?;
    Ok(text
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect())
}

#[async_trait]
impl Runner for Plain {
    async fn install(&self, _processes: &Processes) -> Result<()> {
        Ok(())
    }

    async fn start(&self, processes: &Processes) -> Result<()> {
        let layout = &processes.layout;
        let agent_config = layout.agent_config();
        let agent_arg = agent_config.to_string_lossy();
        spawn_detached(
            processes,
            "vk-agent",
            &processes.vk_agent,
            &[&agent_arg],
            &agent_config,
            &[],
        )
        .await?;
        if !processes.proxy {
            return Ok(());
        }
        let proxy_config = layout.pgcluster_config();
        let proxy_arg = proxy_config.to_string_lossy();
        spawn_detached(
            processes,
            "pgcluster",
            &processes.pgcluster,
            &["server", "--config", &proxy_arg],
            &proxy_config,
            &read_env_file(&layout.env_file())?,
        )
        .await
    }

    async fn stop(&self, processes: &Processes) -> Result<()> {
        let layout = &processes.layout;
        signal_and_wait(
            &pid_path(layout, "pgcluster"),
            &layout.pgcluster_config(),
            Signal::SIGTERM,
        )
        .await?;
        signal_and_wait(
            &pid_path(layout, "vk-agent"),
            &layout.agent_config(),
            Signal::SIGTERM,
        )
        .await?;
        // Postgres outlives vk-agent here, with no control group to end both.
        let data = layout.data();
        if data.join("postmaster.pid").exists() {
            let output = command_as(&processes.pg_ctl, processes.run_as)
                .args(["stop", "-w", "-m", "fast", "-D"])
                .arg(&data)
                .output()
                .await
                .context("run pg_ctl")?;
            if !output.status.success() {
                tracing::warn!(
                    stderr = %String::from_utf8_lossy(&output.stderr).trim(),
                    "pg_ctl stop failed"
                );
            }
        }
        Ok(())
    }

    async fn restart(&self, processes: &Processes) -> Result<()> {
        self.stop(processes).await?;
        self.start(processes).await
    }

    async fn reload_proxy(&self, processes: &Processes) -> Result<()> {
        if !processes.proxy {
            return Ok(());
        }
        let layout = &processes.layout;
        let pid = live_pid(&pid_path(layout, "pgcluster"), &layout.pgcluster_config())
            .context("pgcluster is not running")?;
        kill(pid, Signal::SIGHUP).context("signal pgcluster")
    }

    async fn reload_certificates(&self, processes: &Processes) -> Result<()> {
        let layout = &processes.layout;
        let pid = live_pid(&pid_path(layout, "vk-agent"), &layout.agent_config())
            .context("vk-agent is not running")?;
        kill(pid, Signal::SIGHUP).context("signal vk-agent")?;
        self.reload_proxy(processes).await
    }

    async fn is_running(&self, processes: &Processes) -> Result<bool> {
        let layout = &processes.layout;
        let agent = live_pid(&pid_path(layout, "vk-agent"), &layout.agent_config());
        let proxy = live_pid(&pid_path(layout, "pgcluster"), &layout.pgcluster_config());
        Ok(agent.is_some() && (proxy.is_some() || !processes.proxy))
    }

    async fn remove(&self, processes: &Processes) -> Result<()> {
        self.stop(processes).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn processes() -> Processes {
        Processes {
            id: "db-1-n1".into(),
            layout: Layout::new("/instances", "db-1-n1"),
            vk_agent: "/usr/local/bin/vk-agent".into(),
            pgcluster: "/usr/local/bin/pgcluster".into(),
            pg_ctl: "/usr/lib/postgresql/18/bin/pg_ctl".into(),
            run_as: None,
            user: Some("postgres".into()),
            resources: None,
            proxy: true,
        }
    }

    #[test]
    fn a_replica_runs_no_pgcluster() {
        let mut p = processes();
        assert_eq!(p.units().len(), 2);
        p.proxy = false;
        assert_eq!(p.units(), ["vk-instance-db-1-n1-agent.service"]);
    }

    #[test]
    fn both_units_run_in_the_instances_slice() {
        let p = processes();
        assert!(agent_unit(&p).contains("Slice=vk-instance-db-1-n1.slice"));
        assert!(proxy_unit(&p).contains("Slice=vk-instance-db-1-n1.slice"));
    }

    #[test]
    fn the_slice_caps_cpu_memory_and_io() {
        let limits = Limits::of(&Resources {
            vcpu: 1.5,
            memory_gb: 2.0,
            storage_gb: 10.0,
        });
        let unit = slice_unit("db-1-n1", &limits);
        assert!(unit.contains("CPUQuota=150%"), "{unit}");
        assert!(unit.contains("MemoryMax=2147483648"), "{unit}");
        assert!(unit.contains("IOWeight=150"), "{unit}");
        assert!(unit.contains("[Slice]"), "{unit}");
    }

    #[test]
    fn the_agent_unit_stops_postgres_with_a_fast_shutdown() {
        let unit = agent_unit(&processes());
        assert!(unit.contains("KillSignal=SIGINT"), "{unit}");
        assert!(unit.contains("KillMode=control-group"), "{unit}");
        assert!(unit.contains("User=postgres"), "{unit}");
        assert!(unit.contains("Restart=always"), "{unit}");
        assert!(unit.contains("/usr/local/bin/vk-agent /instances/db-1-n1/vk-agent.toml"));
    }

    #[test]
    fn the_proxy_unit_reads_secrets_from_a_file() {
        let unit = proxy_unit(&processes());
        assert!(unit.contains("EnvironmentFile=/instances/db-1-n1/instance.env"));
        assert!(!unit.contains("PASSWORD"));
    }

    #[test]
    fn a_missing_pid_file_means_not_running() {
        assert!(live_pid(Path::new("/no/such/file"), Path::new("/x")).is_none());
    }

    #[test]
    fn a_recycled_pid_is_not_mistaken_for_the_instance() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("x.pid");
        std::fs::write(&file, std::process::id().to_string()).unwrap();
        assert!(live_pid(&file, Path::new("/some/config.toml")).is_none());
    }

    #[test]
    fn the_env_file_parses() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("instance.env");
        std::fs::write(&file, "A=1\nB=x=y\n").unwrap();
        let env = read_env_file(&file).unwrap();
        assert_eq!(env[1], ("B".to_string(), "x=y".to_string()));
    }
}
