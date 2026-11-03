//! The agent API's types, from `openapi/agent.yaml`.

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

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
    pub peers: Vec<Peer>,
    pub bootstrap: String,
    pub replication_mode: String,
    pub allowed_cidrs: Vec<String>,
    pub extensions: Vec<String>,
    pub owner: Owner,
    pub secrets: InstanceSecrets,
}

impl Instance {
    /// Names a directory and systemd units.
    pub fn id(&self) -> String {
        format!("{}-{}", self.database_id, self.node)
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
        if self.bootstrap != "initdb" {
            bail!("unsupported bootstrap {:?}", self.bootstrap);
        }
        if !self.peers.is_empty() {
            bail!("this host agent runs one-node databases only");
        }
        if self.port_base > u16::MAX - 10 {
            bail!("port_base {} leaves no room for a block", self.port_base);
        }
        Ok(())
    }
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
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InstanceReport {
    pub database_id: String,
    pub node: String,
    pub observed_generation: i64,
    pub state: &'static str,
    pub error: Option<String>,
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
            peers: vec![],
            bootstrap: "initdb".into(),
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
            },
        }
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
    fn peers_wait_for_the_three_node_stage() {
        let mut i = instance();
        i.peers.push(Peer {
            node: "n2".into(),
            address: "10.0.0.2".into(),
            port_base: 20000,
        });
        assert!(i.validate().is_err());
    }
}
