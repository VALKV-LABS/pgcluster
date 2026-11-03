//! Types stored in `databases.spec`, sent to hosts, and the rules for them.

use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::secrets::{DatabaseSecrets, NodeSecrets};

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

const POSTGRES_VERSIONS: &[&str] = &["16", "17", "18"];

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
    #[serde(default)]
    pub backup: Option<serde_json::Value>,
}

impl CreateDatabase {
    /// Check the request, and turn it into the stored spec.
    pub fn into_spec(self) -> Result<(String, Option<String>, DatabaseSpec), ApiError> {
        check_identifier("name", &self.name)?;
        if RESERVED_DATABASES.contains(&self.name.as_str()) {
            return Err(ApiError::invalid(format!(
                "name {:?} is reserved",
                self.name
            )));
        }
        if !POSTGRES_VERSIONS.contains(&self.version.as_str()) {
            return Err(ApiError::invalid(format!(
                "version must be one of {}",
                POSTGRES_VERSIONS.join(", ")
            )));
        }
        if self.ha {
            return Err(ApiError::not_supported(
                "three-node databases are not available yet",
            ));
        }
        if self.backup.is_some() {
            return Err(ApiError::not_supported("backups are not available yet"));
        }
        let mode = self.replication_mode.as_deref().unwrap_or("async");
        if mode != "async" {
            return Err(ApiError::invalid("replication_mode needs ha: true"));
        }
        if self.hosts.len() > 1 {
            return Err(ApiError::invalid("a one-node database takes one host"));
        }
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
        };
        Ok((self.name, self.workspace, spec))
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
    pub peers: Vec<Peer>,
    pub bootstrap: String,
    pub replication_mode: String,
    pub allowed_cidrs: Vec<String>,
    pub extensions: Vec<String>,
    pub owner: Owner,
    pub secrets: InstanceSecrets,
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
}

impl InstanceSecrets {
    pub fn merge(database: DatabaseSecrets, node: NodeSecrets) -> Self {
        Self {
            ca_cert_pem: database.ca_cert_pem,
            cert_pem: node.cert_pem,
            key_pem: node.key_pem,
            superuser_password: database.superuser_password,
            replication_password: database.replication_password,
            auth_password: database.auth_password,
            api_key: database.api_key,
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
}

#[derive(Debug, Clone, Deserialize)]
pub struct InstanceReport {
    pub database_id: String,
    pub node: String,
    pub observed_generation: i64,
    pub state: String,
    #[serde(default)]
    pub error: Option<String>,
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
            backup: None,
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
    fn what_later_stages_deliver_says_so() {
        let err = rejected(|r| r.ha = true);
        assert_eq!(err.code, "not_supported");
        let err = rejected(|r| r.backup = Some(serde_json::json!({})));
        assert_eq!(err.code, "not_supported");
    }

    #[test]
    fn replication_modes_need_ha() {
        rejected(|r| r.replication_mode = Some("sync".into()));
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
        };
        assert!(!format!("{secrets:?}").contains("private"));
    }
}
