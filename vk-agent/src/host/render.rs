//! Turns one instance of desired state into its files. Pure functions, so
//! applying the same state twice writes the same bytes.

use std::path::{Path, PathBuf};

use serde_json::json;

use super::model::Instance;

// Offsets into an instance's block of ten ports.
const PROXY: u16 = 0;
const PROXY_ADMIN: u16 = 1;
const POSTGRES: u16 = 2;
const RAFT: u16 = 3;
const AGENT: u16 = 4;
const PROXY_HEALTH: u16 = 6;
const REST_API: u16 = 7;
const METRICS: u16 = 8;

/// A one-node instance talks to itself over loopback.
const LOCAL: &str = "127.0.0.1";

pub const REPLICATION_USER: &str = "replicator";
pub const SUPERUSER: &str = "postgres";

/// Extensions that must be in `shared_preload_libraries` to install.
const NEEDS_PRELOAD: &[&str] = &["pg_tle", "pg_cron"];

/// Where an instance's files live.
#[derive(Debug, Clone)]
pub struct Layout {
    pub dir: PathBuf,
}

impl Layout {
    pub fn new(instances_dir: &str, id: &str) -> Self {
        Self {
            dir: Path::new(instances_dir).join(id),
        }
    }

    pub fn data(&self) -> PathBuf {
        self.dir.join("pgdata")
    }
    pub fn raft(&self) -> PathBuf {
        self.dir.join("raft")
    }
    pub fn tls(&self) -> PathBuf {
        self.dir.join("tls")
    }
    pub fn logs(&self) -> PathBuf {
        self.dir.join("logs")
    }
    pub fn pgcluster_config(&self) -> PathBuf {
        self.dir.join("pgcluster.toml")
    }
    pub fn agent_config(&self) -> PathBuf {
        self.dir.join("vk-agent.toml")
    }
    pub fn env_file(&self) -> PathBuf {
        self.dir.join("instance.env")
    }
    pub fn applied(&self) -> PathBuf {
        self.dir.join("applied.json")
    }
}

pub fn postgres_port(instance: &Instance) -> u16 {
    instance.port_base + POSTGRES
}

pub fn proxy_health_port(instance: &Instance) -> u16 {
    instance.port_base + PROXY_HEALTH
}

fn local(instance: &Instance, offset: u16) -> String {
    format!("{LOCAL}:{}", instance.port_base + offset)
}

fn path(p: PathBuf) -> String {
    p.to_string_lossy().into_owned()
}

/// `pgcluster`'s config. Everything but the proxy stays on loopback.
pub fn pgcluster_toml(instance: &Instance, layout: &Layout) -> String {
    let tls = layout.tls();
    let config = json!({
        "cluster": {
            "name": instance.database_name,
            "data_dir": path(layout.raft()),
        },
        "raft": {
            "node_id": 1,
            "bootstrap": true,
            "peers": [{ "id": 1, "addr": local(instance, RAFT) }],
        },
        "nodes": {
            "node": [{
                "id": instance.node,
                "agent_addr": local(instance, AGENT),
                "postgres_addr": local(instance, POSTGRES),
                "priority": 100,
            }],
        },
        "replication": {
            "replication_user": REPLICATION_USER,
            "replication_mode": instance.replication_mode,
        },
        "proxy": {
            "listen_addr": format!("0.0.0.0:{}", instance.port_base + PROXY),
            "admin_listen_addr": local(instance, PROXY_ADMIN),
            "health_listen_addr": local(instance, PROXY_HEALTH),
            "allowed_cidrs": instance.allowed_cidrs,
            "require_tls": true,
        },
        "tls": {
            "ca_cert": path(tls.join("ca.crt")),
            "cert": path(tls.join("node.crt")),
            "key": path(tls.join("node.key")),
        },
        "metrics": { "listen_addr": local(instance, METRICS) },
        "api": {
            "listen_addr": local(instance, REST_API),
            "api_keys": [instance.secrets.api_key],
        },
    });
    toml::to_string(&config).expect("a JSON object serializes to TOML")
}

/// `vk-agent`'s config, in instance mode.
pub fn agent_toml(instance: &Instance, layout: &Layout, pg_bin: &str) -> String {
    let tls = layout.tls();
    let config = json!({
        "listen_addr": local(instance, AGENT),
        "data_dir": path(layout.data()),
        "postgres_host": LOCAL,
        "postgres_port": postgres_port(instance),
        "postgres_user": SUPERUSER,
        "postgres_password": instance.secrets.superuser_password,
        "postgres_dbname": "postgres",
        "pg_ctl_path": format!("{pg_bin}/pg_ctl"),
        "backup_config_path": path(layout.dir.join("walg.json")),
        "tls": {
            "ca_cert": path(tls.join("ca.crt")),
            "cert": path(tls.join("node.crt")),
            "key": path(tls.join("node.key")),
        },
    });
    toml::to_string(&config).expect("a JSON object serializes to TOML")
}

/// Environment for `pgcluster`.
pub fn env_file(instance: &Instance) -> String {
    format!(
        "PGCLUSTER_AUTH_PASSWORD={}\nPG_REPLICATION_PASSWORD={}\n",
        instance.secrets.auth_password, instance.secrets.replication_password
    )
}

/// What `shared_preload_libraries` needs for these extensions.
fn preload(extensions: &[String]) -> String {
    let mut libraries = vec!["pg_stat_statements"];
    libraries.extend(
        NEEDS_PRELOAD
            .iter()
            .copied()
            .filter(|lib| extensions.iter().any(|e| e == lib)),
    );
    libraries.join(",")
}

/// Settings the data directory includes. Each of them needs a restart to change,
/// so a change to this file restarts the instance.
pub fn managed_conf(instance: &Instance, layout: &Layout) -> String {
    let tls = layout.tls();
    let tls_file = |name: &str| tls.join(name).to_string_lossy().into_owned();
    let shared_buffers_mb = (instance.resources.memory_gb * 1024.0 / 4.0).max(32.0) as u64;
    let mut conf = format!(
        "# Written by vk-agent host. Changes here are overwritten.\n\
         listen_addresses = '{LOCAL}'\n\
         port = {port}\n\
         unix_socket_directories = ''\n\
         password_encryption = 'scram-sha-256'\n\
         wal_level = replica\n\
         archive_mode = off\n\
         max_wal_senders = 10\n\
         max_replication_slots = 10\n\
         hot_standby = on\n\
         ssl = on\n\
         ssl_ca_file = '{ca}'\n\
         ssl_cert_file = '{cert}'\n\
         ssl_key_file = '{key}'\n\
         shared_buffers = '{shared_buffers_mb}MB'\n\
         shared_preload_libraries = '{preload}'\n",
        port = postgres_port(instance),
        ca = tls_file("ca.crt"),
        cert = tls_file("node.crt"),
        key = tls_file("node.key"),
        preload = preload(&instance.extensions),
    );
    if instance.extensions.iter().any(|e| e == "pg_cron") {
        conf.push_str(&format!(
            "cron.database_name = '{}'\n",
            instance.database_name
        ));
    }
    conf
}

/// Postgres accepts only this host's own proxy and agent: clients never reach
/// it, so the allowlist is applied at the proxy, which sees their addresses.
pub fn pg_hba() -> String {
    "# Written by vk-agent host. Changes here are overwritten.\n\
     # TYPE  DATABASE     USER  ADDRESS       METHOD\n\
     hostssl all          all   127.0.0.1/32  scram-sha-256\n\
     hostssl replication  all   127.0.0.1/32  scram-sha-256\n"
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::super::model::tests::instance;
    use super::*;

    fn layout() -> Layout {
        Layout::new("/instances", "db-1-n1")
    }

    #[test]
    fn ports_follow_the_spec() {
        let text = pgcluster_toml(&instance(), &layout());
        for expected in [
            "0.0.0.0:20000",
            "127.0.0.1:20001",
            "127.0.0.1:20002",
            "127.0.0.1:20003",
            "127.0.0.1:20004",
            "127.0.0.1:20006",
            "127.0.0.1:20007",
            "127.0.0.1:20008",
        ] {
            assert!(text.contains(expected), "{expected} missing from\n{text}");
        }
    }

    #[test]
    fn rendering_is_repeatable() {
        let (i, l) = (instance(), layout());
        assert_eq!(pgcluster_toml(&i, &l), pgcluster_toml(&i, &l));
        assert_eq!(managed_conf(&i, &l), managed_conf(&i, &l));
    }

    #[test]
    fn preload_extensions_reach_the_conf() {
        let mut i = instance();
        i.extensions = vec!["pg_cron".into(), "pg_tle".into(), "vector".into()];
        let conf = managed_conf(&i, &layout());
        assert!(
            conf.contains("'pg_stat_statements,pg_tle,pg_cron'"),
            "{conf}"
        );
        assert!(conf.contains("cron.database_name = 'orders'"), "{conf}");
    }

    #[test]
    fn pg_stat_statements_is_always_preloaded() {
        let mut i = instance();
        i.extensions.clear();
        let conf = managed_conf(&i, &layout());
        assert!(conf.contains("shared_preload_libraries = 'pg_stat_statements'"));
        assert!(!conf.contains("cron."));
    }

    #[test]
    fn a_quarter_of_memory_goes_to_shared_buffers() {
        assert!(managed_conf(&instance(), &layout()).contains("shared_buffers = '2048MB'"));
    }

    #[test]
    fn postgres_refuses_everything_but_local_tls() {
        for line in pg_hba().lines().filter(|l| !l.starts_with('#')) {
            assert!(line.starts_with("hostssl"), "{line}");
            assert!(line.contains("127.0.0.1/32"), "{line}");
            assert!(line.contains("scram-sha-256"), "{line}");
        }
    }

    #[test]
    fn an_empty_allowlist_is_written_as_empty() {
        let mut i = instance();
        i.allowed_cidrs.clear();
        let text = pgcluster_toml(&i, &layout());
        assert!(text.contains("allowed_cidrs = []"), "{text}");
    }
}
