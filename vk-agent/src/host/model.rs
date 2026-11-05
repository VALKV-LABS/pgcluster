//! The agent API's types, from `openapi/agent.yaml`.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Resources {
    pub vcpu: f64,
    pub memory_gb: f64,
    pub storage_gb: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DesiredState {
    pub generation: i64,
    pub instances: Vec<Instance>,
    /// Backup repositories this host should prove it can write to.
    #[serde(default)]
    pub repository_checks: Vec<RepositoryCheck>,
    /// Which CAs to trust for the agent API, during a rotation of the admin CA.
    #[serde(default)]
    pub admin_ca: Option<AdminCa>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminCa {
    pub bundle_pem: String,
    /// Ask for a new client certificate from the CA now in use.
    pub renew_client_certificate: bool,
}

/// Where a database's backups live. S3-compatible or POSIX.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct BackupTarget {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub path_style: bool,
    #[serde(default)]
    pub access_key_id: Option<String>,
    #[serde(default)]
    pub secret_access_key: Option<String>,
    /// Base64, 32 bytes. Data is encrypted with it before it leaves the node.
    #[serde(default)]
    pub encryption_key: Option<String>,
}

impl std::fmt::Debug for BackupTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BackupTarget({})", self.kind)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepositoryCheck {
    pub id: String,
    pub target: BackupTarget,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupPolicy {
    pub target: BackupTarget,
    /// Full backups to keep, with the deltas and WAL that depend on them.
    pub retain_full: u32,
}

/// What to restore a new database from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Restore {
    pub source: BackupTarget,
    /// Recover to this time. Left out, to the end of the WAL archive.
    #[serde(default)]
    pub target_time: Option<String>,
    /// The name the database has inside the backup.
    pub source_database: String,
}

/// One-off work for an instance. Runs on the node that is the Raft leader, or
/// the Postgres primary, because only that node can do it. Either can move, so
/// every node of the database gets the task and each checks for itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub kind: String,
    pub on: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Peer {
    pub node: String,
    pub address: String,
    pub port_base: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Owner {
    pub username: String,
    pub scram_verifier: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct InstanceSecrets {
    pub ca_cert_pem: String,
    pub cert_pem: String,
    pub key_pem: String,
    pub superuser_password: String,
    pub replication_password: String,
    pub auth_password: String,
    pub api_key: String,
    /// Set during a rotation, until the roles have moved to them.
    #[serde(default)]
    pub next: Option<NextPasswords>,
    /// A certificate the proxy presents in place of `cert_pem`.
    #[serde(default)]
    pub proxy_cert_pem: Option<String>,
    #[serde(default)]
    pub proxy_key_pem: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct NextPasswords {
    pub superuser_password: String,
    pub replication_password: String,
    pub auth_password: String,
    pub api_key: String,
}

impl std::fmt::Debug for InstanceSecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InstanceSecrets(..)")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instance {
    pub database_id: String,
    pub database_name: String,
    pub node: String,
    pub generation: i64,
    #[serde(default)]
    pub operation_id: Option<String>,
    pub postgres_version: String,
    pub resources: Resources,
    pub port_base: u16,
    /// The address peers reach this node at.
    pub address: String,
    pub peers: Vec<Peer>,
    pub bootstrap: String,
    #[serde(default)]
    pub restore: Option<Restore>,
    /// `None` when the database has no backups: nothing is archived.
    #[serde(default)]
    pub backup: Option<BackupPolicy>,
    #[serde(default)]
    pub tasks: Vec<Task>,
    pub replication_mode: String,
    pub allowed_cidrs: Vec<String>,
    pub extensions: Vec<String>,
    pub owner: Owner,
    pub secrets: InstanceSecrets,
    /// `member`, or `replica` for an external standby of another database.
    #[serde(default = "member")]
    pub role: String,
    /// The name the database has in its data, such as a promoted replica's
    /// source's. It is renamed to `database_name` once it can be written.
    #[serde(default)]
    pub rename_from: Option<String>,
    /// External standbys of this database, which reach its Postgres, and whose
    /// agents its leader reaches. They take no part in Raft.
    #[serde(default)]
    pub replicas: Vec<Peer>,
}

fn member() -> String {
    "member".into()
}

impl Instance {
    /// Names a directory and systemd units.
    pub fn id(&self) -> String {
        format!("{}-{}", self.database_id, self.node)
    }

    /// The node's Raft ID, which is the number in its name.
    pub fn raft_id(&self) -> u64 {
        raft_id(&self.node)
    }

    pub fn ha(&self) -> bool {
        !self.peers.is_empty()
    }

    /// An external standby runs Postgres and `vk-agent`, and no `pgcluster`:
    /// its source's cluster drives it.
    pub fn is_replica(&self) -> bool {
        self.role == "replica"
    }

    /// Anything can serve the agent API, and these values reach paths, unit
    /// files and SQL, so a host checks them for itself.
    pub fn validate(&self) -> Result<()> {
        let plain_id = |text: &str| {
            !text.is_empty()
                && text.len() <= 64
                && text
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        };
        if !plain_id(&self.database_id) || !plain_id(&self.node) {
            bail!("invalid instance ID {:?}", self.id());
        }
        if !self.postgres_version.chars().all(|c| c.is_ascii_digit())
            || self.postgres_version.is_empty()
        {
            bail!("invalid Postgres version {:?}", self.postgres_version);
        }
        check_identifier(&self.database_name)?;
        if let Some(from) = &self.rename_from {
            check_identifier(from)?;
        }
        check_identifier(&self.owner.username)?;
        check_verifier(&self.owner.scram_verifier)?;
        for extension in &self.extensions {
            if extension.is_empty()
                || !extension
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "_-".contains(c))
            {
                bail!("invalid extension name {extension:?}");
            }
        }
        match self.role.as_str() {
            "member" | "replica" => {}
            other => bail!("unsupported role {other:?}"),
        }
        match (self.bootstrap.as_str(), &self.restore) {
            ("initdb" | "seed" | "replica" | "promoted", _) => {}
            ("restore", Some(restore)) => {
                check_identifier(&restore.source_database)?;
                if let Some(target) = &restore.target_time {
                    check_conf_value(target)?;
                }
            }
            ("restore", None) => bail!("a restore names no source"),
            (other, _) => bail!("unsupported bootstrap {other:?}"),
        }
        if self.port_base > u16::MAX - 10 {
            bail!("port_base {} leaves no room for a block", self.port_base);
        }
        check_address(&self.address)?;
        check_node_name(&self.node)?;
        for peer in &self.peers {
            check_address(&peer.address)?;
            check_node_name(&peer.node)?;
            if peer.port_base > u16::MAX - 10 {
                bail!("port_base {} leaves no room for a block", peer.port_base);
            }
        }
        for replica in &self.replicas {
            check_address(&replica.address)?;
            let plain = !replica.node.is_empty()
                && replica.node.len() <= 32
                && replica
                    .node
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
            if !plain || replica.port_base > u16::MAX - 10 {
                bail!("invalid replica {:?}", replica.node);
            }
        }
        for cidr in &self.allowed_cidrs {
            cidr.parse::<ipnet::IpNet>()
                .with_context(|| format!("invalid CIDR block {cidr:?}"))?;
        }
        if let Some(backup) = &self.backup {
            backup.target.validate()?;
        }
        if let Some(restore) = &self.restore {
            restore.source.validate()?;
        }
        Ok(())
    }
}

impl BackupTarget {
    /// These reach config files and a command line.
    pub fn validate(&self) -> Result<()> {
        match self.kind.as_str() {
            "posix" => match self.path.as_deref() {
                Some(path) if path.starts_with('/') => check_conf_value(path),
                _ => bail!("a posix repository needs an absolute path"),
            },
            "s3" => {
                let Some(bucket) = self.bucket.as_deref().filter(|b| !b.is_empty()) else {
                    bail!("an s3 repository needs a bucket");
                };
                check_conf_value(bucket)?;
                self.prefix.as_deref().map_or(Ok(()), check_conf_value)
            }
            other => bail!("unsupported repository type {other:?}"),
        }
    }
}

pub fn raft_id(node: &str) -> u64 {
    node.strip_prefix('n')
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}

fn check_node_name(node: &str) -> Result<()> {
    if raft_id(node) == 0 {
        bail!("invalid node name {node:?}");
    }
    Ok(())
}

fn check_address(address: &str) -> Result<()> {
    let plain = !address.is_empty()
        && address.len() <= 253
        && address
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || ".-_:".contains(c));
    if !plain {
        bail!("invalid address {address:?}");
    }
    Ok(())
}

/// A value that lands inside quotes in a config file.
fn check_conf_value(value: &str) -> Result<()> {
    if value.contains(['\'', '\\', '\n', '\r']) {
        bail!("invalid value {value:?}");
    }
    Ok(())
}

fn check_identifier(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 63
        && !name.starts_with(|c: char| c.is_ascii_digit())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if !ok {
        bail!("invalid identifier {name:?}");
    }
    Ok(())
}

fn check_verifier(verifier: &str) -> Result<()> {
    let ok = verifier.starts_with("SCRAM-SHA-256$")
        && verifier
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-$:+/=".contains(c));
    if !ok {
        bail!("owner.scram_verifier is not a SCRAM verifier");
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub observed_generation: i64,
    pub version: String,
    pub address: String,
    pub capacity: Resources,
    pub instances: Vec<InstanceReport>,
    /// The hash of the CA bundle this host trusts.
    pub ca_fingerprint: String,
    /// Each Postgres major installed here, and its full version.
    pub postgres_versions: std::collections::BTreeMap<String, String>,
}

/// One outcome of a task.
#[derive(Debug, Clone, Serialize)]
pub struct TaskResult {
    pub success: bool,
    pub error: Option<String>,
    pub output: Value,
}

/// What the Raft leader knows, for `pgcluster-admin` to record. It is reported
/// and never decided there: each cluster's Raft group owns these facts.
#[derive(Debug, Clone, Serialize)]
pub struct ClusterReport {
    pub database_id: String,
    pub primary: Option<String>,
    pub sync_degraded: bool,
    pub needs_reseed: Vec<String>,
    pub failovers: Vec<Value>,
    pub backups: Vec<BackupReport>,
    pub backup_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BackupReport {
    pub id: String,
    pub label: String,
    pub completed_at: i64,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InstanceReport {
    pub database_id: String,
    pub node: String,
    pub observed_generation: i64,
    pub state: &'static str,
    pub error: Option<String>,
    /// Every commit before this time is in the WAL archive. Only the primary
    /// reports it.
    #[serde(default)]
    pub archived_until: Option<String>,
    /// The release the running server reports, such as `18.1`.
    #[serde(default)]
    pub server_version: Option<String>,
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub fn instance() -> Instance {
        Instance {
            database_id: "db-1".into(),
            database_name: "orders".into(),
            node: "n1".into(),
            generation: 1,
            operation_id: None,
            postgres_version: "18".into(),
            resources: Resources {
                vcpu: 2.0,
                memory_gb: 8.0,
                storage_gb: 100.0,
            },
            port_base: 20000,
            address: "10.0.0.1".into(),
            peers: vec![],
            bootstrap: "initdb".into(),
            restore: None,
            backup: Some(BackupPolicy {
                target: BackupTarget {
                    kind: "posix".into(),
                    path: Some("/backups/db-1".into()),
                    encryption_key: Some("a2V5".into()),
                    ..Default::default()
                },
                retain_full: 2,
            }),
            tasks: vec![],
            replication_mode: "async".into(),
            allowed_cidrs: vec!["198.51.100.0/24".into()],
            extensions: vec!["vector".into()],
            owner: Owner {
                username: "app_owner".into(),
                scram_verifier: "SCRAM-SHA-256$4096:c2FsdA==$a:b".into(),
            },
            secrets: InstanceSecrets {
                ca_cert_pem: "ca".into(),
                cert_pem: "cert".into(),
                key_pem: "key".into(),
                superuser_password: "super".into(),
                replication_password: "repl".into(),
                auth_password: "auth".into(),
                api_key: "api".into(),
                next: None,
                proxy_cert_pem: None,
                proxy_key_pem: None,
            },
            role: "member".into(),
            rename_from: None,
            replicas: vec![],
        }
    }

    #[test]
    fn a_replica_is_named_apart_and_checked() {
        let mut i = instance();
        i.role = "replica".into();
        i.bootstrap = "replica".into();
        i.validate().unwrap();
        assert!(i.is_replica());
        i.role = "leader".into();
        assert!(i.validate().is_err());

        let mut i = instance();
        i.replicas.push(Peer {
            node: "r1".into(),
            address: "10.1.0.1".into(),
            port_base: 20000,
        });
        i.validate().unwrap();
        i.replicas[0].node = "r1; drop".into();
        assert!(i.validate().is_err());
    }

    #[test]
    fn a_good_instance_passes() {
        instance().validate().unwrap();
    }

    #[test]
    fn hostile_values_are_refused() {
        let mut i = instance();
        i.database_id = "../../etc".into();
        assert!(i.validate().is_err());

        let mut i = instance();
        i.database_name = "x\"; DROP DATABASE postgres; --".into();
        assert!(i.validate().is_err());

        let mut i = instance();
        i.owner.scram_verifier = "SCRAM-SHA-256$' OR 1=1".into();
        assert!(i.validate().is_err());

        let mut i = instance();
        i.extensions = vec!["x\" CASCADE".into()];
        assert!(i.validate().is_err());

        let mut i = instance();
        i.postgres_version = "18/../..".into();
        assert!(i.validate().is_err());
    }

    #[test]
    fn hostile_peers_and_repositories_are_refused() {
        let mut i = instance();
        i.peers.push(Peer {
            node: "n2".into(),
            address: "10.0.0.2; drop".into(),
            port_base: 20000,
        });
        assert!(i.validate().is_err());

        let mut i = instance();
        i.backup.as_mut().unwrap().target.path = Some("/backups' ; x".into());
        assert!(i.validate().is_err());

        let mut i = instance();
        i.backup.as_mut().unwrap().target.path = Some("relative".into());
        assert!(i.validate().is_err());

        let mut i = instance();
        i.allowed_cidrs = vec!["everyone".into()];
        assert!(i.validate().is_err());
    }

    #[test]
    fn a_node_can_seed_or_restore() {
        let mut i = instance();
        i.bootstrap = "seed".into();
        i.validate().unwrap();

        i.bootstrap = "restore".into();
        assert!(i.validate().is_err(), "a restore needs a source");
        i.restore = Some(Restore {
            source: i.backup.clone().unwrap().target,
            target_time: Some("2026-11-01T10:00:00Z".into()),
            source_database: "orders".into(),
        });
        i.validate().unwrap();
    }

    #[test]
    fn raft_ids_come_from_node_names() {
        assert_eq!(raft_id("n4"), 4);
        assert_eq!(raft_id("x"), 0);
    }
}
