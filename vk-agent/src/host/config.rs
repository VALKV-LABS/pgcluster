use serde::{Deserialize, Serialize};

/// Settings for `vk-agent host`, read from a TOML file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostConfig {
    /// URL of the agent API, such as `https://admin.internal:8444`.
    pub admin_url: String,
    /// Holds this host's identity and its last desired state.
    #[serde(default = "default_state_dir")]
    pub state_dir: String,
    /// Each instance gets a directory here, with its data and config.
    #[serde(default = "default_instances_dir")]
    pub instances_dir: String,
    /// File holding the enrolment token. `VK_AGENT_ENROLLMENT_TOKEN` also works.
    /// Only the first start needs it.
    #[serde(default)]
    pub enrollment_token_file: Option<String>,
    /// The address clients and peers use to reach this host. Left out, it is
    /// the address this host uses to reach `admin_url`.
    #[serde(default)]
    pub advertise_address: Option<String>,
    #[serde(default)]
    pub runner: RunnerKind,
    /// User that owns instances. Set it when `vk-agent host` runs as root.
    #[serde(default)]
    pub run_as: Option<String>,
    /// Where each Postgres major's programs are. `{version}` is replaced.
    #[serde(default = "default_pg_bin_template")]
    pub pg_bin_template: String,
    /// Path to the `pgcluster` binary.
    #[serde(default = "default_pgcluster_path")]
    pub pgcluster_path: String,
    /// Path to the `vk-agent` binary. Defaults to this executable.
    #[serde(default)]
    pub vk_agent_path: Option<String>,
    /// Directory for unit files, with the systemd runner.
    #[serde(default = "default_unit_dir")]
    pub systemd_unit_dir: String,
    #[serde(default = "default_report_interval")]
    pub report_interval_seconds: u64,
    /// How this host enforces which machines reach an instance's ports.
    #[serde(default)]
    pub firewall: FirewallKind,
    /// Path to the WAL-G binary.
    #[serde(default = "default_walg_path")]
    pub walg_path: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FirewallKind {
    /// One nftables table per instance. Needs `nft` and root.
    #[default]
    Nftables,
    /// Leave ports open, for development on one machine.
    None,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunnerKind {
    /// For production: instances survive this agent and restart on boot.
    #[default]
    Systemd,
    /// For development and tests.
    Plain,
}

fn default_state_dir() -> String {
    "/var/lib/vk-agent-host".into()
}
fn default_instances_dir() -> String {
    "/var/lib/pgcluster/instances".into()
}
fn default_pg_bin_template() -> String {
    "/usr/lib/postgresql/{version}/bin".into()
}
fn default_pgcluster_path() -> String {
    "/usr/local/bin/pgcluster".into()
}
fn default_unit_dir() -> String {
    "/etc/systemd/system".into()
}
fn default_report_interval() -> u64 {
    15
}
fn default_walg_path() -> String {
    "wal-g".into()
}

impl HostConfig {
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("cannot read host config {path}: {e}"))?;
        let cfg: Self = toml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("invalid host config {path}: {e}"))?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if !self.admin_url.starts_with("https://") {
            anyhow::bail!("admin_url must start with https://");
        }
        if self.report_interval_seconds == 0 {
            anyhow::bail!("report_interval_seconds must be > 0");
        }
        if !self.pg_bin_template.contains("{version}") {
            anyhow::bail!("pg_bin_template must contain {{version}}");
        }
        Ok(())
    }

    pub fn pg_bin(&self, version: &str) -> String {
        self.pg_bin_template.replace("{version}", version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> anyhow::Result<HostConfig> {
        let cfg: HostConfig = toml::from_str(text)?;
        cfg.validate()?;
        Ok(cfg)
    }

    #[test]
    fn defaults_are_for_production() {
        let cfg = parse("admin_url = \"https://a:8444\"").unwrap();
        assert_eq!(cfg.runner, RunnerKind::Systemd);
        assert_eq!(cfg.pg_bin("18"), "/usr/lib/postgresql/18/bin");
    }

    #[test]
    fn plaintext_is_refused() {
        assert!(parse("admin_url = \"http://a:8444\"").is_err());
    }

    #[test]
    fn unknown_settings_are_refused() {
        assert!(parse("admin_url = \"https://a\"\nrunnr = \"plain\"").is_err());
    }
}
