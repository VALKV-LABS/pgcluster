//! Turns one instance of desired state into its files. Pure functions, so
//! applying the same state twice writes the same bytes.

use std::net::IpAddr;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

use super::model::{raft_id, BackupTarget, Instance};

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
const ANY: &str = "0.0.0.0";

/// The first node's priority, which makes it the first primary.
const FIRST_PRIORITY: u32 = 100;
const OTHER_PRIORITY: u32 = 90;

/// Weekly full, daily delta.
const BACKUP_INTERVAL_SECONDS: u64 = 86_400;
const DELTA_STEPS: u32 = 6;

/// The name the encryption key goes by in the secrets file.
const BACKUP_KEY_ENV: &str = "PGCLUSTER_BACKUP_KEY";

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
    /// Present while running processes have yet to read changed files.
    pub fn reload_owed(&self) -> PathBuf {
        self.dir.join("reload.owed")
    }
    /// Postgres's config. A base backup or rewind copies the data directory from
    /// another node, so what is specific to this node lives out here.
    pub fn postgres_conf(&self) -> PathBuf {
        self.dir.join("postgresql.conf")
    }
    pub fn pg_hba(&self) -> PathBuf {
        self.dir.join("pg_hba.conf")
    }
    pub fn walg_config(&self) -> PathBuf {
        self.dir.join("walg.json")
    }
    /// For WAL-G while restoring, pointing at the source's repository.
    pub fn walg_restore_config(&self) -> PathBuf {
        self.dir.join("walg-restore.json")
    }
    /// Holds the socket of the private server a restore runs.
    pub fn socket_dir(&self) -> PathBuf {
        self.dir.join("run")
    }
}

pub fn postgres_port(instance: &Instance) -> u16 {
    instance.port_base + POSTGRES
}

pub fn proxy_health_port(instance: &Instance) -> u16 {
    instance.port_base + PROXY_HEALTH
}

pub fn rest_api_port(instance: &Instance) -> u16 {
    instance.port_base + REST_API
}

pub fn proxy_port(instance: &Instance) -> u16 {
    instance.port_base + PROXY
}

/// The ports only the database's own nodes may reach.
pub fn peer_ports(instance: &Instance) -> [u16; 3] {
    [POSTGRES, RAFT, AGENT].map(|offset| instance.port_base + offset)
}

fn local(instance: &Instance, offset: u16) -> String {
    format!("{LOCAL}:{}", instance.port_base + offset)
}

/// Where a port is bound: a node with peers has to be reachable by them.
fn bound(instance: &Instance, offset: u16) -> String {
    let host = if instance.ha() { ANY } else { LOCAL };
    format!("{host}:{}", instance.port_base + offset)
}

/// Where peers reach a port of this node.
fn reachable(instance: &Instance, offset: u16) -> String {
    if instance.ha() {
        format!("{}:{}", instance.address, instance.port_base + offset)
    } else {
        local(instance, offset)
    }
}

fn priority(node: &str) -> u32 {
    if raft_id(node) == 1 {
        FIRST_PRIORITY
    } else {
        OTHER_PRIORITY
    }
}

/// This node and its peers, as `[raft.peers]` and `[[nodes.node]]`.
fn members(instance: &Instance) -> (Vec<Value>, Vec<Value>) {
    let mut raft = vec![json!({
        "id": instance.raft_id(),
        "addr": reachable(instance, RAFT),
    })];
    let mut nodes = vec![json!({
        "id": instance.node,
        "agent_addr": reachable(instance, AGENT),
        "postgres_addr": reachable(instance, POSTGRES),
        "priority": priority(&instance.node),
    })];
    for peer in &instance.peers {
        let addr = |offset: u16| format!("{}:{}", peer.address, peer.port_base + offset);
        raft.push(json!({ "id": raft_id(&peer.node), "addr": addr(RAFT) }));
        nodes.push(json!({
            "id": peer.node,
            "agent_addr": addr(AGENT),
            "postgres_addr": addr(POSTGRES),
            "priority": priority(&peer.node),
        }));
    }
    (raft, nodes)
}

/// `pgcluster`'s `[backup]`, if the database has backups.
fn backup_section(instance: &Instance) -> Option<Value> {
    let backup = instance.backup.as_ref()?;
    let target: &BackupTarget = &backup.target;
    let mut section = json!({
        "interval_seconds": BACKUP_INTERVAL_SECONDS,
        "delta_steps": DELTA_STEPS,
        "retain_full": backup.retain_full,
        "encryption_key_env": BACKUP_KEY_ENV,
    });
    if let Some(path) = &target.path {
        section["posix"] = json!({ "path": path });
    }
    if let Some(bucket) = &target.bucket {
        let mut s3 = json!({
            "bucket": bucket,
            "prefix": target.prefix.clone().unwrap_or_default(),
            "path_style": target.path_style,
        });
        if let Some(region) = &target.region {
            s3["region"] = json!(region);
        }
        if let Some(endpoint) = &target.endpoint {
            s3["endpoint"] = json!(endpoint);
        }
        section["s3"] = s3;
    }
    Some(section)
}

fn path(p: PathBuf) -> String {
    p.to_string_lossy().into_owned()
}

/// `pgcluster`'s config. On a one-node database everything but the proxy stays
/// on loopback.
pub fn pgcluster_toml(instance: &Instance, layout: &Layout) -> String {
    let tls = layout.tls();
    let (raft_peers, nodes) = members(instance);
    let mut tls_section = json!({
        "ca_cert": path(tls.join("ca.crt")),
        "cert": path(tls.join("node.crt")),
        "key": path(tls.join("node.key")),
    });
    if instance.secrets.proxy_cert_pem.is_some() {
        tls_section["proxy_cert"] = json!(path(tls.join("proxy.crt")));
        tls_section["proxy_key"] = json!(path(tls.join("proxy.key")));
    }
    let mut api_keys = vec![instance.secrets.api_key.clone()];
    api_keys.extend(instance.secrets.next.iter().map(|n| n.api_key.clone()));
    let config = json!({
        "cluster": {
            "name": instance.database_name,
            "data_dir": path(layout.raft()),
            "secrets_file": path(layout.env_file()),
        },
        "raft": {
            "node_id": instance.raft_id(),
            // Only the first node forms the group. Later ones join as learners.
            "bootstrap": instance.raft_id() == 1,
            "peers": raft_peers,
        },
        "nodes": { "node": nodes },
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
        "tls": tls_section,
        "metrics": { "listen_addr": local(instance, METRICS) },
        "api": {
            "listen_addr": local(instance, REST_API),
            "api_keys": api_keys,
        },
    });
    let mut config = config;
    if let Some(section) = backup_section(instance) {
        config["backup"] = section;
    }
    toml::to_string(&config).expect("a JSON object serializes to TOML")
}

/// `vk-agent`'s config, in instance mode.
pub fn agent_toml(instance: &Instance, layout: &Layout, pg_bin: &str) -> String {
    let tls = layout.tls();
    let config = json!({
        "listen_addr": bound(instance, AGENT),
        "data_dir": path(layout.data()),
        "postgres_host": LOCAL,
        "postgres_port": postgres_port(instance),
        "postgres_user": SUPERUSER,
        "postgres_password": instance.secrets.superuser_password,
        "postgres_dbname": "postgres",
        "pg_ctl_path": format!("{pg_bin}/pg_ctl"),
        "postgres_config_file": path(layout.postgres_conf()),
        "backup_config_path": path(layout.walg_config()),
        "tls": {
            "ca_cert": path(tls.join("ca.crt")),
            "cert": path(tls.join("node.crt")),
            "key": path(tls.join("node.key")),
        },
    });
    toml::to_string(&config).expect("a JSON object serializes to TOML")
}

/// What `pgcluster` reads for passwords and repository credentials. SIGHUP reads
/// it again, so none of these need a restart to change.
pub fn env_file(instance: &Instance) -> String {
    let secrets = &instance.secrets;
    let mut text = format!(
        "PGCLUSTER_AUTH_PASSWORD={}\nPG_REPLICATION_PASSWORD={}\n",
        secrets.auth_password, secrets.replication_password
    );
    if let Some(backup) = &instance.backup {
        let target = &backup.target;
        let optional = [
            (BACKUP_KEY_ENV, &target.encryption_key),
            ("AWS_ACCESS_KEY_ID", &target.access_key_id),
            ("AWS_SECRET_ACCESS_KEY", &target.secret_access_key),
        ];
        for (name, value) in optional {
            if let Some(value) = value {
                text.push_str(&format!("{name}={value}\n"));
            }
        }
    }
    if let Some(next) = &secrets.next {
        text.push_str(&format!(
            "PGCLUSTER_AUTH_PASSWORD_NEXT={}\nPG_REPLICATION_PASSWORD_NEXT={}\n",
            next.auth_password, next.replication_password
        ));
    }
    text
}

pub fn needs_preload(extension: &str) -> bool {
    NEEDS_PRELOAD.contains(&extension)
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

/// Postgres's config, which it starts from in place of the data directory's.
/// Each setting here needs a restart to change, so a change to this file restarts
/// the instance, or on a three-node database calls for a rolling restart.
pub fn managed_conf(instance: &Instance, layout: &Layout) -> String {
    let tls = layout.tls();
    let tls_file = |name: &str| tls.join(name).to_string_lossy().into_owned();
    let shared_buffers_mb = (instance.resources.memory_gb * 1024.0 / 4.0).max(32.0) as u64;
    let listen = if instance.ha() { "*" } else { LOCAL };
    let mut conf = format!(
        "# Written by vk-agent host. Changes here are overwritten.\n\
         include_if_exists = '{initdb_conf}'\n\
         hba_file = '{hba}'\n\
         listen_addresses = '{listen}'\n\
         port = {port}\n\
         unix_socket_directories = ''\n\
         password_encryption = 'scram-sha-256'\n\
         wal_level = replica\n\
         archive_mode = on\n\
         archive_timeout = 60\n\
         max_wal_senders = 10\n\
         max_replication_slots = 10\n\
         hot_standby = on\n\
         ssl = on\n\
         ssl_ca_file = '{ca}'\n\
         ssl_cert_file = '{cert}'\n\
         ssl_key_file = '{key}'\n\
         shared_buffers = '{shared_buffers_mb}MB'\n\
         shared_preload_libraries = '{preload}'\n",
        initdb_conf = layout.data().join("postgresql.conf").display(),
        hba = layout.pg_hba().display(),
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
    // With no backups, each WAL segment is dropped once done with, rather than
    // kept for an archive that never comes. Archiving stays on, so turning
    // backups on needs no restart: the agent's own `archive_command`, set with
    // ALTER SYSTEM, takes the place of this one.
    if instance.backup.is_none() {
        conf.push_str("archive_command = '/bin/true'\n");
    }
    conf
}

/// Settings in the managed config that Postgres takes on a reload.
const RELOADABLE: &[&str] = &["archive_command"];

/// Whether moving from config `before` to `after` takes a restart, rather
/// than a reload.
pub fn needs_restart(before: &str, after: &str) -> bool {
    let restart_lines = |conf: &str| -> Vec<String> {
        conf.lines()
            .filter(|line| {
                let name = line.split('=').next().unwrap_or_default().trim();
                !RELOADABLE.contains(&name)
            })
            .map(str::to_string)
            .collect()
    };
    restart_lines(before) != restart_lines(after)
}

/// Postgres accepts only this node's proxy and agent, and those of its peers:
/// clients never reach it, so the allowlist is applied at the proxy, which sees
/// their addresses. `nodes` holds the address of every node with a Postgres
/// peers can reach, this one included.
pub fn pg_hba(nodes: &[IpAddr]) -> String {
    let mut text = "# Written by vk-agent host. Changes here are overwritten.\n\
         # TYPE  DATABASE     USER  ADDRESS       METHOD\n\
         hostssl all          all   127.0.0.1/32  scram-sha-256\n\
         hostssl replication  all   127.0.0.1/32  scram-sha-256\n"
        .to_string();
    for ip in nodes {
        let bits = if ip.is_ipv4() { 32 } else { 128 };
        text.push_str(&format!(
            "hostssl all          all   {ip}/{bits}  scram-sha-256\n"
        ));
        text.push_str(&format!(
            "hostssl replication  all   {ip}/{bits}  scram-sha-256\n"
        ));
    }
    text
}

/// A private server for finishing a restore: no network, and trust for whoever
/// can open a socket in a directory only the instance's user can.
pub fn restore_conf(instance: &Instance, layout: &Layout) -> String {
    format!(
        "include_if_exists = '{initdb_conf}'\n\
         hba_file = '{hba}'\n\
         listen_addresses = ''\n\
         port = {port}\n\
         unix_socket_directories = '{socket}'\n\
         hot_standby = on\n\
         archive_mode = off\n\
         shared_preload_libraries = '{preload}'\n",
        initdb_conf = layout.data().join("postgresql.conf").display(),
        hba = layout.dir.join("restore-hba.conf").display(),
        port = postgres_port(instance),
        socket = layout.socket_dir().display(),
        preload = preload(&instance.extensions),
    )
}

#[cfg(test)]
mod tests {
    use super::super::model::tests::instance;
    use super::super::model::{NextPasswords, Peer};
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
    fn postgres_refuses_everything_but_tls_from_its_own_nodes() {
        let text = pg_hba(&["10.0.0.2".parse().unwrap(), "2001:db8::2".parse().unwrap()]);
        assert!(text.contains("10.0.0.2/32"), "{text}");
        assert!(text.contains("2001:db8::2/128"), "{text}");
        for line in text.lines().filter(|l| !l.starts_with('#')) {
            assert!(line.starts_with("hostssl"), "{line}");
            assert!(line.contains("scram-sha-256"), "{line}");
        }
        assert_eq!(pg_hba(&[]).lines().filter(|l| l.contains("/")).count(), 2);
    }

    fn three_nodes() -> Instance {
        let mut i = instance();
        i.node = "n2".into();
        i.address = "10.0.0.2".into();
        i.peers = vec![
            Peer {
                node: "n1".into(),
                address: "10.0.0.1".into(),
                port_base: 20010,
            },
            Peer {
                node: "n3".into(),
                address: "10.0.0.3".into(),
                port_base: 20000,
            },
        ];
        i
    }

    #[test]
    fn a_node_with_peers_reaches_them_at_each_ones_own_ports() {
        let text = pgcluster_toml(&three_nodes(), &layout());
        let config: toml::Value = toml::from_str(&text).unwrap();
        assert_eq!(config["raft"]["node_id"].as_integer(), Some(2));
        assert_eq!(config["raft"]["bootstrap"].as_bool(), Some(false));
        let peers = config["raft"]["peers"].as_array().unwrap();
        let addrs: Vec<&str> = peers.iter().map(|p| p["addr"].as_str().unwrap()).collect();
        assert_eq!(
            addrs,
            ["10.0.0.2:20003", "10.0.0.1:20013", "10.0.0.3:20003"]
        );
        let nodes = config["nodes"]["node"].as_array().unwrap();
        let first = nodes
            .iter()
            .find(|n| n["id"].as_str() == Some("n1"))
            .unwrap();
        assert_eq!(first["postgres_addr"].as_str(), Some("10.0.0.1:20012"));
        assert_eq!(first["priority"].as_integer(), Some(100));
    }

    #[test]
    fn a_node_with_peers_listens_for_them() {
        let l = layout();
        let i = three_nodes();
        assert!(managed_conf(&i, &l).contains("listen_addresses = '*'"));
        assert!(agent_toml(&i, &l, "/bin").contains("0.0.0.0:20004"));
        // The REST API and metrics stay local, peers or not.
        let text = pgcluster_toml(&i, &l);
        assert!(text.contains("127.0.0.1:20007"), "{text}");
        assert!(text.contains("127.0.0.1:20008"), "{text}");
    }

    #[test]
    fn postgres_never_reads_this_nodes_settings_from_the_data_directory() {
        let l = layout();
        let conf = managed_conf(&instance(), &l);
        assert!(
            conf.contains("hba_file = '/instances/db-1-n1/pg_hba.conf'"),
            "{conf}"
        );
        assert!(agent_toml(&instance(), &l, "/bin").contains("postgres_config_file"));
    }

    #[test]
    fn a_database_without_backups_discards_wal_and_names_no_repository() {
        let (mut i, l) = (instance(), layout());
        i.backup = None;
        let conf = managed_conf(&i, &l);
        assert!(conf.contains("archive_mode = on"), "{conf}");
        assert!(conf.contains("archive_command = '/bin/true'"), "{conf}");
        let config: toml::Value = toml::from_str(&pgcluster_toml(&i, &l)).unwrap();
        assert!(config.get("backup").is_none());
        assert!(!env_file(&i).contains(BACKUP_KEY_ENV));
        assert!(!managed_conf(&instance(), &l).contains("archive_command"));
    }

    #[test]
    fn turning_backups_on_reloads_and_other_changes_restart() {
        let (with, l) = (instance(), layout());
        let mut without = instance();
        without.backup = None;
        let (on, off) = (managed_conf(&with, &l), managed_conf(&without, &l));
        assert!(!needs_restart(&off, &on));
        let mut bigger = instance();
        bigger.resources.memory_gb *= 2.0;
        assert!(needs_restart(&on, &managed_conf(&bigger, &l)));
    }

    #[test]
    fn wal_is_archived_and_backups_are_encrypted_per_database() {
        let (i, l) = (instance(), layout());
        assert!(managed_conf(&i, &l).contains("archive_mode = on"));
        let config: toml::Value = toml::from_str(&pgcluster_toml(&i, &l)).unwrap();
        let backup = &config["backup"];
        assert_eq!(backup["posix"]["path"].as_str(), Some("/backups/db-1"));
        assert_eq!(backup["retain_full"].as_integer(), Some(2));
        assert_eq!(backup["encryption_key_env"].as_str(), Some(BACKUP_KEY_ENV));
        assert!(env_file(&i).contains("PGCLUSTER_BACKUP_KEY=a2V5\n"));
    }

    #[test]
    fn next_passwords_are_offered_alongside_the_current_ones() {
        let mut i = instance();
        i.secrets.next = Some(NextPasswords {
            superuser_password: "s2".into(),
            replication_password: "r2".into(),
            auth_password: "a2".into(),
            api_key: "k2".into(),
        });
        let text = pgcluster_toml(&i, &layout());
        assert!(
            text.contains("\"api\", \"k2\"") || text.contains("api_keys = [\"api\", \"k2\"]"),
            "{text}"
        );
        assert!(env_file(&i).contains("PG_REPLICATION_PASSWORD_NEXT=r2\n"));
    }

    #[test]
    fn a_custom_certificate_is_for_the_proxy_alone() {
        let mut i = instance();
        let text = pgcluster_toml(&i, &layout());
        assert!(!text.contains("proxy_cert"), "{text}");
        i.secrets.proxy_cert_pem = Some("cert".into());
        i.secrets.proxy_key_pem = Some("key".into());
        let config: toml::Value = toml::from_str(&pgcluster_toml(&i, &layout())).unwrap();
        assert_eq!(
            config["tls"]["proxy_cert"].as_str(),
            Some("/instances/db-1-n1/tls/proxy.crt")
        );
        assert_eq!(
            config["tls"]["cert"].as_str(),
            Some("/instances/db-1-n1/tls/node.crt")
        );
    }

    #[test]
    fn an_empty_allowlist_is_written_as_empty() {
        let mut i = instance();
        i.allowed_cidrs.clear();
        let text = pgcluster_toml(&i, &layout());
        assert!(text.contains("allowed_cidrs = []"), "{text}");
    }
}
