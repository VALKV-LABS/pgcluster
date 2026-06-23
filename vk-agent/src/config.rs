use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    /// Address this agent listens on for gRPC calls from pgcluster.
    /// Default: "0.0.0.0:7001"
    #[serde(default = "default_listen")]
    pub listen_addr: String,

    /// Postgres data directory (PGDATA) — where signal files are written.
    pub data_dir: String,

    /// How to reach local Postgres.
    /// On Linux: a Unix socket path like "/var/run/postgresql".
    /// On other platforms or for TCP: "host=127.0.0.1 port=5432".
    #[serde(default = "default_pg_host")]
    pub postgres_host: String,

    #[serde(default = "default_pg_port")]
    pub postgres_port: u16,

    #[serde(default = "default_pg_user")]
    pub postgres_user: String,

    #[serde(default)]
    pub postgres_password: Option<String>,

    #[serde(default = "default_pg_dbname")]
    pub postgres_dbname: String,

    /// If pgcluster doesn't send a heartbeat within this many seconds,
    /// the agent enters safe mode and refuses promote commands.
    #[serde(default = "default_heartbeat_timeout")]
    pub heartbeat_timeout_seconds: u64,

    /// If pgcluster heartbeats are absent for this many seconds *after* safe mode
    /// is entered, vk-agent calls `pg_ctl stop -m immediate` and exits, self-fencing
    /// the local postgres against split-brain writes in a network partition.
    /// Set to 0 or omit to disable (default: disabled).
    #[serde(default)]
    pub fence_timeout_seconds: Option<u64>,

    /// Optional TLS configuration for the gRPC server.
    #[serde(default)]
    pub tls: AgentTlsConfig,

    /// Path to pg_ctl binary (used for stop/start commands).
    #[serde(default = "default_pg_ctl")]
    pub pg_ctl_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AgentTlsConfig {
    pub ca_cert: Option<String>,
    pub cert: Option<String>,
    pub key: Option<String>,
}

fn default_listen() -> String {
    "0.0.0.0:7001".into()
}
fn default_pg_host() -> String {
    "/var/run/postgresql".into()
}
fn default_pg_port() -> u16 {
    5432
}
fn default_pg_user() -> String {
    "postgres".into()
}
fn default_pg_dbname() -> String {
    "postgres".into()
}
fn default_heartbeat_timeout() -> u64 {
    10
}
fn default_pg_ctl() -> String {
    "pg_ctl".into()
}

impl AgentConfig {
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("cannot read agent config {}: {}", path, e))?;
        let cfg: Self = toml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("invalid agent config {}: {}", path, e))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.data_dir.is_empty() {
            anyhow::bail!("data_dir must not be empty");
        }
        use std::net::SocketAddr;
        self.listen_addr
            .parse::<SocketAddr>()
            .map_err(|e| anyhow::anyhow!("listen_addr {:?} is invalid: {}", self.listen_addr, e))?;
        Ok(())
    }

    /// Build a sqlx Postgres connection URL for the local Postgres instance.
    pub fn postgres_url(&self) -> String {
        if self.postgres_host.starts_with('/') {
            // Unix socket
            format!(
                "postgresql://{}@%2F{}:{}/{}",
                self.postgres_user,
                self.postgres_host.trim_start_matches('/'),
                self.postgres_port,
                self.postgres_dbname
            )
        } else {
            let pwd = self.postgres_password.as_deref().unwrap_or("");
            if pwd.is_empty() {
                format!(
                    "postgresql://{}@{}:{}/{}",
                    self.postgres_user,
                    self.postgres_host,
                    self.postgres_port,
                    self.postgres_dbname
                )
            } else {
                format!(
                    "postgresql://{}:{}@{}:{}/{}",
                    self.postgres_user,
                    pwd,
                    self.postgres_host,
                    self.postgres_port,
                    self.postgres_dbname
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_values_applied() {
        let toml = r#"
data_dir = "/var/lib/postgresql/data"
listen_addr = "0.0.0.0:7001"
"#;
        let cfg: AgentConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.heartbeat_timeout_seconds, 10);
        assert_eq!(cfg.postgres_user, "postgres");
        assert_eq!(cfg.postgres_dbname, "postgres");
    }

    #[test]
    fn tcp_postgres_url() {
        let mut cfg = AgentConfig {
            listen_addr: "0.0.0.0:7001".into(),
            data_dir: "/pgdata".into(),
            postgres_host: "127.0.0.1".into(),
            postgres_port: 5432,
            postgres_user: "postgres".into(),
            postgres_password: None,
            postgres_dbname: "postgres".into(),
            heartbeat_timeout_seconds: 10,
            fence_timeout_seconds: None,
            tls: Default::default(),
            pg_ctl_path: "pg_ctl".into(),
        };
        assert_eq!(
            cfg.postgres_url(),
            "postgresql://postgres@127.0.0.1:5432/postgres"
        );
        cfg.postgres_password = Some("secret".into());
        assert_eq!(
            cfg.postgres_url(),
            "postgresql://postgres:secret@127.0.0.1:5432/postgres"
        );
    }
}
