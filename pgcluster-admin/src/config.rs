use serde::Deserialize;

/// Settings for `pgcluster-admin`, read from a TOML file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConfig {
    /// Environment variable holding the metadata database URL.
    #[serde(default = "default_database_url_env")]
    pub database_url_env: String,
    /// Environment variable holding a base64 32-byte key, which encrypts
    /// instance secrets at rest. Losing it loses access to those secrets.
    #[serde(default = "default_secret_key_env")]
    pub secret_key_env: String,
    /// Environment variable holding the key being replaced. With this set, the
    /// next start seals everything with `secret_key_env`, and it can then go.
    #[serde(default = "default_old_secret_key_env")]
    pub old_secret_key_env: String,
    pub api: ApiConfig,
    #[serde(default)]
    pub agent_api: AgentApiConfig,
    pub tls: TlsConfig,
    /// Hosts to create at startup, with tokens generated elsewhere. The AWS
    /// template uses this so that hosts need no admin key to enrol.
    #[serde(default)]
    pub bootstrap_hosts: Vec<BootstrapHost>,
    /// How much of each host is handed out, and which ports.
    #[serde(default)]
    pub scheduler: crate::scheduler::SchedulerConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiConfig {
    #[serde(default = "default_api_listen")]
    pub listen_addr: String,
    /// SHA-256 of each accepted API key, in hex, so a copy of this file cannot
    /// call the API. `pgcluster-admin generate-key` makes a key and its hash.
    /// There is no way to run without one.
    #[serde(default)]
    pub api_key_hashes: Vec<String>,
    /// Named keys, so the audit log says which one made each change, and one
    /// can be revoked alone: remove it here and send SIGHUP.
    #[serde(default)]
    pub keys: Vec<NamedKey>,
    /// Development only: serve the admin API in plaintext.
    #[serde(default)]
    pub insecure_plaintext: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedKey {
    pub name: String,
    /// SHA-256 of the key, in hex.
    pub sha256: String,
}

impl ApiConfig {
    /// Every accepted key, by name. Keys in `api_key_hashes` are `key-1`,
    /// `key-2` and so on, in order.
    pub fn named_keys(&self) -> Vec<(String, String)> {
        self.api_key_hashes
            .iter()
            .enumerate()
            .map(|(i, hash)| (format!("key-{}", i + 1), hash.clone()))
            .chain(self.keys.iter().map(|k| (k.name.clone(), k.sha256.clone())))
            .collect()
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let keys = self.named_keys();
        if keys.is_empty() {
            anyhow::bail!(
                "[api] needs at least one key, in [[api.keys]] or api_key_hashes. \
                 Run pgcluster-admin generate-key to make a key"
            );
        }
        let mut names = std::collections::HashSet::new();
        for (name, hash) in &keys {
            if !is_sha256(hash) {
                anyhow::bail!("[api] the hash of key {name:?} must be 64 hex digits");
            }
            let plain = !name.is_empty()
                && name.len() <= 63
                && name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c));
            if !plain {
                anyhow::bail!("[api] key name {name:?} must be 1 to 63 letters, digits, dots, dashes and underscores");
            }
            if !names.insert(name.as_str()) {
                anyhow::bail!("[api] two keys are named {name:?}");
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentApiConfig {
    #[serde(default = "default_agent_listen")]
    pub listen_addr: String,
}

impl Default for AgentApiConfig {
    fn default() -> Self {
        Self {
            listen_addr: default_agent_listen(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Holds `ca.crt` and `ca.key`, which are created if missing.
    pub ca_dir: String,
    /// Hostnames or IP addresses hosts and API clients use to reach this
    /// service. They go on its server certificate.
    pub server_names: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapHost {
    pub name: String,
    #[serde(default)]
    pub failure_domain: Option<String>,
    /// SHA-256 of the token, in hex.
    pub token_sha256: String,
    /// Counted from the first start that sees this host.
    #[serde(default = "default_bootstrap_token_ttl")]
    pub token_ttl_minutes: u32,
}

fn default_database_url_env() -> String {
    "PGCLUSTER_ADMIN_DATABASE_URL".into()
}
fn default_secret_key_env() -> String {
    "PGCLUSTER_ADMIN_SECRET_KEY".into()
}
fn default_old_secret_key_env() -> String {
    "PGCLUSTER_ADMIN_OLD_SECRET_KEY".into()
}
fn default_api_listen() -> String {
    "0.0.0.0:8443".into()
}
fn default_agent_listen() -> String {
    "0.0.0.0:8444".into()
}
fn default_bootstrap_token_ttl() -> u32 {
    60
}

fn is_sha256(text: &str) -> bool {
    text.len() == 64 && text.bytes().all(|b| b.is_ascii_hexdigit())
}

impl AdminConfig {
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("cannot read config {path}: {e}"))?;
        let cfg: Self =
            toml::from_str(&content).map_err(|e| anyhow::anyhow!("invalid config {path}: {e}"))?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        self.api.validate()?;
        for (name, addr) in [
            ("api", &self.api.listen_addr),
            ("agent_api", &self.agent_api.listen_addr),
        ] {
            addr.parse::<std::net::SocketAddr>()
                .map_err(|e| anyhow::anyhow!("[{name}] invalid listen_addr {addr:?}: {e}"))?;
        }
        if self.tls.ca_dir.is_empty() {
            anyhow::bail!("[tls] ca_dir must not be empty");
        }
        if self.tls.server_names.is_empty() {
            anyhow::bail!("[tls] server_names must contain at least one name");
        }
        self.scheduler.validate()?;
        for host in &self.bootstrap_hosts {
            if !is_sha256(&host.token_sha256) {
                anyhow::bail!(
                    "[[bootstrap_hosts]] {} token_sha256 must be 64 hex digits",
                    host.name
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
[api]
api_key_hashes = ["ab0d8f2c6a1e4b7790c3de15f24a6b8c1d3e5f7091a2b4c6d8e0f1a3b5c7d9e1"]

[tls]
ca_dir       = "/ca"
server_names = ["admin.internal"]
"#;

    fn parse(text: &str) -> anyhow::Result<AdminConfig> {
        let cfg: AdminConfig = toml::from_str(text)?;
        cfg.validate()?;
        Ok(cfg)
    }

    #[test]
    fn defaults_apply() {
        let cfg = parse(VALID).unwrap();
        assert_eq!(cfg.api.listen_addr, "0.0.0.0:8443");
        assert_eq!(cfg.agent_api.listen_addr, "0.0.0.0:8444");
        assert!(!cfg.api.insecure_plaintext);
    }

    #[test]
    fn api_keys_are_required() {
        let start = VALID.find("api_key_hashes").unwrap();
        let end = VALID[start..].find('\n').unwrap() + start;
        let err = parse(&VALID.replace(&VALID[start..end], ""))
            .unwrap_err()
            .to_string();
        assert!(err.contains("at least one key"), "{err}");
    }

    #[test]
    fn a_plain_key_in_place_of_a_hash_is_refused() {
        let start = VALID.find("ab0d").unwrap();
        let text = VALID.replace(&VALID[start..start + 64], "a-plain-key");
        assert!(parse(&text).is_err());
    }

    #[test]
    fn the_old_setting_is_refused_rather_than_ignored() {
        let text = VALID.replace("api_key_hashes", "api_keys");
        assert!(parse(&text).is_err());
    }

    #[test]
    fn named_keys_join_the_unnamed_ones() {
        let hash = "ab0d8f2c6a1e4b7790c3de15f24a6b8c1d3e5f7091a2b4c6d8e0f1a3b5c7d9e1";
        let text = format!("{VALID}\n[[api.keys]]\nname = \"ci\"\nsha256 = \"{hash}\"\n");
        let cfg = parse(&text).unwrap();
        let names: Vec<String> = cfg.api.named_keys().into_iter().map(|(n, _)| n).collect();
        assert_eq!(names, ["key-1", "ci"]);
        let twice = text.replace("name = \"ci\"", "name = \"key-1\"");
        assert!(parse(&twice).is_err());
        let bad = text.replace("name = \"ci\"", "name = \"a b\"");
        assert!(parse(&bad).is_err());
    }

    #[test]
    fn a_bootstrap_host_needs_a_real_hash() {
        let text = format!("{VALID}\n[[bootstrap_hosts]]\nname = \"a\"\ntoken_sha256 = \"abc\"\n");
        assert!(parse(&text).is_err());
    }
}
