//! Types stored in `databases.spec`, sent to hosts, and the rules for them.

use serde::{Deserialize, Serialize};

use std::collections::HashSet;

use crate::error::ApiError;
use crate::secrets::{DatabaseSecrets, NextPasswords, NodeSecrets, RepositoryCredentials};

/// The extensions on the node image, by `CREATE EXTENSION` name.
pub const EXTENSIONS: &[&str] = &[
    "uuid-ossp",
    "pgcrypto",
    "pg_trgm",
    "unaccent",
    "vector",
    "pg_stat_statements",
    "pg_tle",
    "postgis",
    "pg_cron",
];

/// Those that must be in `shared_preload_libraries` to install.
pub const NEEDS_PRELOAD: &[&str] = &["pg_tle", "pg_cron"];

const POSTGRES_VERSIONS: &[&str] = &["16", "17", "18"];

/// Three, because pgcluster needs three Raft voters for quorum.
pub const HA_NODES: usize = 3;

pub const REPLICATION_MODES: &[&str] = &["async", "sync", "sync_strict"];

/// Names `pgcluster` and Postgres keep for themselves.
const RESERVED_ROLES: &[&str] = &["postgres", "replicator", "pgcluster_auth", "public", "none"];
const RESERVED_DATABASES: &[&str] = &["postgres", "template0", "template1"];

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Resources {
    pub vcpu: f64,
    pub memory_gb: f64,
    pub storage_gb: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Owner {
    pub username: String,
    pub scram_verifier: String,
}

/// What a database should look like.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatabaseSpec {
    pub version: String,
    pub ha: bool,
    pub hosts: Vec<String>,
    pub resources: Resources,
    pub replication_mode: String,
    pub allowed_cidrs: Vec<String>,
    pub extensions: Vec<String>,
    pub owner: Owner,
    /// `None` for a database created without backups, until they are turned on.
    pub backup: Option<BackupSettings>,
    /// Set when this database begins as a copy of another's backups.
    #[serde(default)]
    pub restore: Option<RestoreSource>,
    /// Set while this database is an external standby of another, in its own
    /// region. Promotion clears it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_of: Option<ReplicaOf>,
    /// The database's name inside Postgres, when its data came from another
    /// database by another name. The host renames it once it can write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplicaOf {
    pub database_id: String,
    /// What the source cluster calls this standby.
    pub node_id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateReplica {
    pub name: String,
    /// The host to run on, by ID or name. Left out, the scheduler chooses one
    /// outside the source's failure domains.
    #[serde(default)]
    pub host_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupSettings {
    pub repository_id: String,
    pub retention_days: u32,
}

impl BackupSettings {
    /// Weekly full backups, with one to spare so that a window of this many
    /// days survives the deletion of the oldest.
    pub fn retain_full(&self) -> u32 {
        self.retention_days.div_ceil(7) + 1
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RestoreSource {
    pub database_id: String,
    pub database_name: String,
    pub target_time: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateDatabase {
    pub name: String,
    #[serde(default)]
    pub workspace: Option<String>,
    pub version: String,
    #[serde(default)]
    pub ha: bool,
    #[serde(default)]
    pub hosts: Vec<String>,
    pub resources: Resources,
    #[serde(default)]
    pub replication_mode: Option<String>,
    pub owner: Owner,
    #[serde(default)]
    pub allowed_cidrs: Vec<String>,
    #[serde(default)]
    pub extensions: Vec<String>,
    /// A database with backups becomes active only once it has a restorable
    /// backup. Required, so that opting out is always explicit.
    pub backup: BackupRequest,
}

/// A backup policy, or `{ "enabled": false }` for none.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupRequest {
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub repository_id: Option<String>,
    #[serde(default)]
    pub retention_days: Option<u32>,
}

impl BackupRequest {
    /// The policy asked for, or `None` when backups are off.
    pub fn into_settings(self) -> Result<Option<BackupSettings>, ApiError> {
        if self.enabled == Some(false) {
            if self.repository_id.is_some() || self.retention_days.is_some() {
                return Err(ApiError::invalid(
                    "backup with enabled false takes no repository_id or retention_days",
                ));
            }
            return Ok(None);
        }
        let (Some(repository_id), Some(retention_days)) = (self.repository_id, self.retention_days)
        else {
            return Err(ApiError::invalid(
                "backup needs repository_id and retention_days, or enabled false",
            ));
        };
        check_retention(retention_days)?;
        Ok(Some(BackupSettings {
            repository_id,
            retention_days,
        }))
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateDatabase {
    #[serde(default)]
    pub resources: Option<Resources>,
    #[serde(default)]
    pub replication_mode: Option<String>,
    #[serde(default)]
    pub retention_days: Option<u32>,
    /// Turns backups on for a database created without them.
    #[serde(default)]
    pub backup: Option<BackupRequest>,
}

impl CreateDatabase {
    /// Check the request, and turn it into the stored spec.
    pub fn into_spec(self) -> Result<(String, Option<String>, DatabaseSpec), ApiError> {
        check_database_name(&self.name)?;
        if !POSTGRES_VERSIONS.contains(&self.version.as_str()) {
            return Err(ApiError::invalid(format!(
                "version must be one of {}",
                POSTGRES_VERSIONS.join(", ")
            )));
        }
        let mode = self.replication_mode.as_deref().unwrap_or("async");
        check_replication_mode(mode, self.ha)?;
        check_hosts(&self.hosts, self.ha)?;
        let backup = self.backup.into_settings()?;
        let r = &self.resources;
        if !(r.vcpu > 0.0 && r.memory_gb > 0.0 && r.storage_gb > 0.0) {
            return Err(ApiError::invalid("resources must all be above zero"));
        }
        check_owner(&self.owner)?;
        check_cidrs(&self.allowed_cidrs)?;
        check_extensions(&self.extensions)?;
        let spec = DatabaseSpec {
            version: self.version,
            ha: self.ha,
            hosts: self.hosts,
            resources: self.resources,
            replication_mode: mode.to_string(),
            allowed_cidrs: self.allowed_cidrs,
            extensions: self.extensions,
            owner: self.owner,
            backup,
            restore: None,
            replica_of: None,
            data_name: None,
        };
        Ok((self.name, self.workspace, spec))
    }
}

pub fn check_database_name(name: &str) -> Result<(), ApiError> {
    check_identifier("name", name)?;
    if RESERVED_DATABASES.contains(&name) {
        return Err(ApiError::invalid(format!("name {name:?} is reserved")));
    }
    Ok(())
}

pub fn check_replication_mode(mode: &str, ha: bool) -> Result<(), ApiError> {
    if !REPLICATION_MODES.contains(&mode) {
        return Err(ApiError::invalid(format!(
            "replication_mode must be one of {}",
            REPLICATION_MODES.join(", ")
        )));
    }
    if mode != "async" && !ha {
        return Err(ApiError::invalid("replication_mode needs ha: true"));
    }
    Ok(())
}

pub fn check_retention(days: u32) -> Result<(), ApiError> {
    if days == 0 {
        return Err(ApiError::invalid("retention_days must be at least 1"));
    }
    Ok(())
}

pub fn check_hosts(hosts: &[String], ha: bool) -> Result<(), ApiError> {
    let wanted = if ha { HA_NODES } else { 1 };
    if !hosts.is_empty() && ((ha && hosts.len() != wanted) || hosts.len() > wanted) {
        return Err(ApiError::invalid(if ha {
            "a three-node database takes three hosts, or none to choose for you"
        } else {
            "a one-node database takes one host"
        }));
    }
    if hosts.iter().collect::<HashSet<_>>().len() != hosts.len() {
        return Err(ApiError::invalid("each node needs a host of its own"));
    }
    Ok(())
}

/// Choose hosts for `count` nodes from `candidates`, best first: one for each
/// failure domain. A host with no domain is a domain of its own.
pub fn choose_hosts<T>(
    candidates: Vec<T>,
    count: usize,
    domain: impl Fn(&T) -> String,
) -> Option<Vec<T>> {
    let mut used = HashSet::new();
    let chosen: Vec<T> = candidates
        .into_iter()
        .filter(|host| used.insert(domain(host)))
        .take(count)
        .collect();
    (chosen.len() == count).then_some(chosen)
}

/// The number of a node's name, which is also its Raft ID.
pub fn node_number(node: &str) -> i32 {
    node.strip_prefix('n')
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateRepository {
    pub name: String,
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
}

/// A repository's location. Its credentials are held apart, encrypted.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RepositoryConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub path_style: bool,
}

impl CreateRepository {
    pub fn into_parts(
        self,
    ) -> Result<(String, String, RepositoryConfig, RepositoryCredentials), ApiError> {
        check_host_name(&self.name)?;
        let plain = |value: &Option<String>| {
            value
                .as_deref()
                .is_none_or(|v| !v.contains(['\'', '\\', '\n', '\r']))
        };
        if !plain(&self.path) || !plain(&self.bucket) || !plain(&self.prefix) {
            return Err(ApiError::invalid(
                "path, bucket and prefix cannot hold quotes",
            ));
        }
        match self.kind.as_str() {
            "posix" => {
                if !self.path.as_deref().is_some_and(|p| p.starts_with('/')) {
                    return Err(ApiError::invalid(
                        "a posix repository needs an absolute path",
                    ));
                }
            }
            "s3" => {
                if self.bucket.as_deref().is_none_or(str::is_empty) {
                    return Err(ApiError::invalid("an s3 repository needs a bucket"));
                }
            }
            _ => return Err(ApiError::invalid("type must be s3 or posix")),
        }
        if self.access_key_id.is_some() != self.secret_access_key.is_some() {
            return Err(ApiError::invalid(
                "set both access_key_id and secret_access_key, or neither to use an instance role",
            ));
        }
        Ok((
            self.name,
            self.kind,
            RepositoryConfig {
                path: self.path,
                bucket: self.bucket,
                prefix: self.prefix,
                region: self.region,
                endpoint: self.endpoint,
                path_style: self.path_style,
            },
            RepositoryCredentials {
                access_key_id: self.access_key_id,
                secret_access_key: self.secret_access_key,
            },
        ))
    }
}

/// Lowercase letters, digits and underscores, starting with a letter or
/// underscore. Keeping names this plain means they never need escaping.
fn check_identifier(field: &str, value: &str) -> Result<(), ApiError> {
    let mut chars = value.chars();
    let starts_well = chars
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_');
    let rest_ok = chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if !starts_well || !rest_ok || value.len() > 63 {
        return Err(ApiError::invalid(format!(
            "{field} must be 1 to 63 lowercase letters, digits and underscores, \
             and start with a letter or underscore"
        )));
    }
    Ok(())
}

fn check_owner(owner: &Owner) -> Result<(), ApiError> {
    check_identifier("owner.username", &owner.username)?;
    if RESERVED_ROLES.contains(&owner.username.as_str()) || owner.username.starts_with("pg_") {
        return Err(ApiError::invalid(format!(
            "owner.username {:?} is reserved",
            owner.username
        )));
    }
    check_verifier(&owner.scram_verifier)
}

/// The shape Postgres stores: `SCRAM-SHA-256$<iterations>:<salt>$<key>:<key>`.
/// Anything else would be taken as a plain password.
pub fn check_verifier(verifier: &str) -> Result<(), ApiError> {
    let shape_ok = verifier
        .strip_prefix("SCRAM-SHA-256$")
        .and_then(|rest| rest.split_once('$'))
        .is_some_and(|(params, keys)| {
            let iterations_ok = params
                .split_once(':')
                .is_some_and(|(n, salt)| n.parse::<u32>().is_ok() && !salt.is_empty());
            let keys_ok = keys
                .split_once(':')
                .is_some_and(|(a, b)| !a.is_empty() && !b.is_empty());
            iterations_ok && keys_ok
        });
    let chars_ok = verifier
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "-$:+/=".contains(c));
    if shape_ok && chars_ok {
        Ok(())
    } else {
        Err(ApiError::invalid(
            "scram_verifier must be a SCRAM-SHA-256 verifier, not a password",
        ))
    }
}

pub fn check_cidrs(cidrs: &[String]) -> Result<(), ApiError> {
    for cidr in cidrs {
        cidr.parse::<ipnet::IpNet>()
            .map_err(|_| ApiError::invalid(format!("{cidr:?} is not a CIDR block")))?;
    }
    Ok(())
}

pub fn check_extensions(extensions: &[String]) -> Result<(), ApiError> {
    for extension in extensions {
        if !EXTENSIONS.contains(&extension.as_str()) {
            return Err(ApiError::invalid(format!(
                "{extension:?} is not available. Choose from {}",
                EXTENSIONS.join(", ")
            )));
        }
    }
    Ok(())
}

pub fn check_host_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty()
        || name.len() > 63
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return Err(ApiError::invalid(
            "name must be 1 to 63 letters, digits, dots, dashes and underscores",
        ));
    }
    Ok(())
}

// ── Agent API ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct DesiredState {
    pub generation: i64,
    pub instances: Vec<Instance>,
    pub repository_checks: Vec<RepositoryCheck>,
    pub admin_ca: Option<AdminCa>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdminCa {
    pub bundle_pem: String,
    pub renew_client_certificate: bool,
}

/// A repository as a node reaches it.
#[derive(Clone, Serialize)]
pub struct BackupTarget {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(flatten)]
    pub config: RepositoryConfig,
    #[serde(flatten)]
    pub credentials: RepositoryCredentials,
    pub encryption_key: Option<String>,
}

impl BackupTarget {
    /// Each database keeps its backups under its own ID.
    pub fn for_database(
        kind: &str,
        mut config: RepositoryConfig,
        credentials: RepositoryCredentials,
        database_id: &str,
        encryption_key: String,
    ) -> Self {
        let under = |base: Option<String>| {
            let base = base.unwrap_or_default();
            format!("{}/{database_id}", base.trim_end_matches('/'))
        };
        if kind == "posix" {
            config.path = Some(under(config.path));
        } else {
            config.prefix = Some(under(config.prefix).trim_start_matches('/').to_string());
        }
        Self {
            kind: kind.to_string(),
            config,
            credentials,
            encryption_key: Some(encryption_key),
        }
    }
}

impl std::fmt::Debug for BackupTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BackupTarget({})", self.kind)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RepositoryCheck {
    pub id: String,
    pub target: BackupTarget,
}

#[derive(Debug, Clone, Serialize)]
pub struct InstanceBackup {
    pub target: BackupTarget,
    pub retain_full: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct InstanceRestore {
    pub source: BackupTarget,
    pub target_time: Option<String>,
    pub source_database: String,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct Task {
    pub id: String,
    pub kind: String,
    #[sqlx(rename = "run_on")]
    pub on: String,
    pub params: sqlx::types::Json<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Instance {
    pub database_id: String,
    pub database_name: String,
    pub node: String,
    pub generation: i64,
    pub operation_id: Option<String>,
    pub postgres_version: String,
    pub resources: Resources,
    pub port_base: i32,
    pub address: String,
    pub peers: Vec<Peer>,
    pub bootstrap: String,
    pub restore: Option<InstanceRestore>,
    /// `None` when the database has no backups: nothing is archived.
    pub backup: Option<InstanceBackup>,
    pub tasks: Vec<Task>,
    pub replication_mode: String,
    pub allowed_cidrs: Vec<String>,
    pub extensions: Vec<String>,
    pub owner: Owner,
    pub secrets: InstanceSecrets,
    /// `member`, or `replica` for an external standby of another database,
    /// which runs Postgres and `vk-agent` but no `pgcluster`.
    pub role: String,
    /// The name the database has in its data, to rename from.
    pub rename_from: Option<String>,
    /// External standbys of this database. They reach its Postgres and its
    /// leader reaches their agents, but they take no part in Raft.
    pub replicas: Vec<Peer>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Peer {
    pub node: String,
    pub address: String,
    pub port_base: i32,
}

#[derive(Clone, Serialize)]
pub struct InstanceSecrets {
    pub ca_cert_pem: String,
    pub cert_pem: String,
    pub key_pem: String,
    pub superuser_password: String,
    pub replication_password: String,
    pub auth_password: String,
    pub api_key: String,
    pub next: Option<NextPasswords>,
    /// What the proxy presents to clients, if not the node's own certificate.
    pub proxy_cert_pem: Option<String>,
    pub proxy_key_pem: Option<String>,
}

impl InstanceSecrets {
    pub fn merge(database: DatabaseSecrets, node: NodeSecrets) -> Self {
        let (proxy_cert_pem, proxy_key_pem) = match database.proxy_certificate.clone() {
            Some(c) => (Some(c.cert_pem), Some(c.key_pem)),
            None => (None, None),
        };
        Self {
            proxy_cert_pem,
            proxy_key_pem,
            ca_cert_pem: database.ca_bundle(),
            cert_pem: node.cert_pem,
            key_pem: node.key_pem,
            superuser_password: database.superuser_password,
            replication_password: database.replication_password,
            auth_password: database.auth_password,
            api_key: database.api_key,
            next: database.next,
        }
    }
}

impl std::fmt::Debug for InstanceSecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InstanceSecrets(..)")
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Report {
    pub observed_generation: i64,
    pub version: String,
    pub address: String,
    pub capacity: Resources,
    pub instances: Vec<InstanceReport>,
    #[serde(default)]
    pub ca_fingerprint: Option<String>,
    /// Each Postgres major the host has, and its full version.
    #[serde(default)]
    pub postgres_versions: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TaskResult {
    pub success: bool,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub output: serde_json::Value,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct RepositoryCheckResult {
    #[serde(default)]
    pub error: Option<String>,
}

/// What a cluster's Raft leader reports.
#[derive(Debug, Clone, Deserialize)]
pub struct ClusterReport {
    pub database_id: String,
    #[serde(default)]
    pub primary: Option<String>,
    #[serde(default)]
    pub sync_degraded: bool,
    #[serde(default)]
    pub needs_reseed: Vec<String>,
    #[serde(default)]
    pub failovers: Vec<serde_json::Value>,
    #[serde(default)]
    pub backups: Vec<BackupReport>,
    #[serde(default)]
    pub backup_error: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BackupReport {
    pub id: String,
    pub label: String,
    pub completed_at: i64,
    pub size_bytes: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InstanceReport {
    pub database_id: String,
    pub node: String,
    pub observed_generation: i64,
    pub state: String,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub archived_until: Option<chrono::DateTime<chrono::Utc>>,
    /// What the running server reports, such as `18.1`.
    #[serde(default)]
    pub server_version: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    const VERIFIER: &str = "SCRAM-SHA-256$4096:c2FsdA==$c3RvcmVk+/==:c2VydmVy";

    fn request() -> CreateDatabase {
        CreateDatabase {
            name: "orders".into(),
            workspace: None,
            version: "18".into(),
            ha: false,
            hosts: vec![],
            resources: Resources {
                vcpu: 1.0,
                memory_gb: 1.0,
                storage_gb: 10.0,
            },
            replication_mode: None,
            owner: Owner {
                username: "app_owner".into(),
                scram_verifier: VERIFIER.into(),
            },
            allowed_cidrs: vec!["198.51.100.0/24".into()],
            extensions: vec!["vector".into()],
            backup: BackupRequest {
                enabled: None,
                repository_id: Some("repo-1".into()),
                retention_days: Some(7),
            },
        }
    }

    fn rejected(change: impl FnOnce(&mut CreateDatabase)) -> ApiError {
        let mut r = request();
        change(&mut r);
        r.into_spec().unwrap_err()
    }

    #[test]
    fn a_good_request_becomes_a_spec() {
        let (name, _, spec) = request().into_spec().unwrap();
        assert_eq!(name, "orders");
        assert_eq!(spec.replication_mode, "async");
    }

    #[test]
    fn names_that_would_need_quoting_are_refused() {
        rejected(|r| r.name = "Orders".into());
        rejected(|r| r.name = "a; DROP TABLE x".into());
        rejected(|r| r.name = "a\"b".into());
        rejected(|r| r.name = String::new());
        rejected(|r| r.name = "x".repeat(64));
    }

    #[test]
    fn reserved_names_are_refused() {
        rejected(|r| r.name = "postgres".into());
        rejected(|r| r.owner.username = "postgres".into());
        rejected(|r| r.owner.username = "pgcluster_auth".into());
        rejected(|r| r.owner.username = "pg_monitor".into());
    }

    #[test]
    fn a_plain_password_is_not_a_verifier() {
        rejected(|r| r.owner.scram_verifier = "hunter2".into());
        rejected(|r| r.owner.scram_verifier = "md5abcdef".into());
        rejected(|r| r.owner.scram_verifier = format!("{VERIFIER}'; DROP ROLE x; --"));
    }

    #[test]
    fn replication_modes_need_ha() {
        rejected(|r| r.replication_mode = Some("sync".into()));
        let mut r = request();
        r.ha = true;
        r.replication_mode = Some("sync_strict".into());
        assert_eq!(r.into_spec().unwrap().2.replication_mode, "sync_strict");
    }

    #[test]
    fn an_unknown_replication_mode_is_refused() {
        rejected(|r| {
            r.ha = true;
            r.replication_mode = Some("fast".into());
        });
    }

    #[test]
    fn hosts_must_suit_the_size_of_the_database() {
        let hosts = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        rejected(|r| r.hosts = hosts(&["a", "b"]));
        rejected(|r| {
            r.ha = true;
            r.hosts = hosts(&["a", "b"]);
        });
        rejected(|r| {
            r.ha = true;
            r.hosts = hosts(&["a", "b", "b"]);
        });
        let mut r = request();
        r.ha = true;
        r.hosts = hosts(&["a", "b", "c"]);
        assert!(r.into_spec().is_ok());
    }

    #[test]
    fn nodes_land_in_separate_failure_domains() {
        let hosts = vec![("a", "r1"), ("b", "r1"), ("c", "r2"), ("d", "r3")];
        let chosen = choose_hosts(hosts, 3, |h| h.1.to_string()).unwrap();
        assert_eq!(chosen, vec![("a", "r1"), ("c", "r2"), ("d", "r3")]);
    }

    #[test]
    fn two_domains_cannot_hold_three_nodes() {
        let hosts = vec![("a", "r1"), ("b", "r1"), ("c", "r2")];
        assert!(choose_hosts(hosts, 3, |h| h.1.to_string()).is_none());
    }

    #[test]
    fn a_retention_period_keeps_whole_weeks_and_one_more() {
        let retain = |retention_days| {
            BackupSettings {
                repository_id: String::new(),
                retention_days,
            }
            .retain_full()
        };
        assert_eq!(retain(1), 2);
        assert_eq!(retain(7), 2);
        assert_eq!(retain(8), 3);
        assert_eq!(retain(30), 6);
    }

    #[test]
    fn a_repository_is_checked_for_what_its_type_needs() {
        let repository = |kind: &str| CreateRepository {
            name: "main".into(),
            kind: kind.into(),
            path: None,
            bucket: None,
            prefix: None,
            region: None,
            endpoint: None,
            path_style: false,
            access_key_id: None,
            secret_access_key: None,
        };
        assert!(repository("posix").into_parts().is_err());
        assert!(repository("s3").into_parts().is_err());
        assert!(repository("ftp").into_parts().is_err());
        let mut good = repository("posix");
        good.path = Some("/backups".into());
        assert!(good.clone().into_parts().is_ok());
        good.path = Some("/backups' x".into());
        assert!(good.into_parts().is_err());
        let mut half = repository("s3");
        half.bucket = Some("b".into());
        half.access_key_id = Some("a".into());
        assert!(half.into_parts().is_err());
    }

    #[test]
    fn each_database_gets_its_own_place_in_a_repository() {
        let config = RepositoryConfig {
            path: Some("/backups/".into()),
            ..Default::default()
        };
        let target = BackupTarget::for_database(
            "posix",
            config,
            RepositoryCredentials::default(),
            "db-1",
            "key".into(),
        );
        assert_eq!(target.config.path.as_deref(), Some("/backups/db-1"));

        let config = RepositoryConfig {
            bucket: Some("b".into()),
            ..Default::default()
        };
        let target = BackupTarget::for_database(
            "s3",
            config,
            RepositoryCredentials::default(),
            "db-1",
            "key".into(),
        );
        assert_eq!(target.config.prefix.as_deref(), Some("db-1"));
    }

    #[test]
    fn a_target_never_shows_credentials_in_debug_output() {
        let target = BackupTarget::for_database(
            "s3",
            RepositoryConfig::default(),
            RepositoryCredentials {
                access_key_id: Some("private".into()),
                secret_access_key: Some("private".into()),
            },
            "db-1",
            "private".into(),
        );
        assert!(!format!("{target:?}").contains("private"));
    }

    #[test]
    fn unknown_extensions_and_bad_blocks_are_refused() {
        rejected(|r| r.extensions = vec!["plpython3u".into()]);
        rejected(|r| r.allowed_cidrs = vec!["everyone".into()]);
    }

    #[test]
    fn an_empty_allowlist_is_allowed() {
        let mut r = request();
        r.allowed_cidrs.clear();
        assert!(r.into_spec().is_ok());
    }

    #[test]
    fn secrets_do_not_show_in_debug_output() {
        let secrets = InstanceSecrets {
            ca_cert_pem: String::new(),
            cert_pem: String::new(),
            key_pem: "private".into(),
            superuser_password: "private".into(),
            replication_password: String::new(),
            auth_password: String::new(),
            api_key: String::new(),
            next: None,
            proxy_cert_pem: None,
            proxy_key_pem: Some("private".into()),
        };
        assert!(!format!("{secrets:?}").contains("private"));
    }
}
