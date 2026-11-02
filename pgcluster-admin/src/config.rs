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
    pub api: ApiConfig,
    #[serde(default)]
    pub agent_api: AgentApiConfig,
    pub tls: TlsConfig,
    /// Hosts to create at startup, with tokens generated elsewhere. The AWS
    /// template uses this so that hosts need no admin key to enrol.
    #[serde(default)]
    pub bootstrap_hosts: Vec<BootstrapHost>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiConfig {
    #[serde(default = "default_api_listen")]
    pub listen_addr: String,
    /// Bearer tokens accepted by the admin API. There is no way to run without.
    #[serde(default)]
    pub api_keys: Vec<String>,
    /// Development only: serve the admin API in plaintext.
    #[serde(default)]
    pub insecure_plaintext: bool,
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
fn default_api_listen() -> String {
    "0.0.0.0:8443".into()
}
fn default_agent_listen() -> String {
    "0.0.0.0:8444".into()
}
fn default_bootstrap_token_ttl() -> u32 {
    60
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
        if self.api.api_keys.is_empty() {
            anyhow::bail!("[api] api_keys must contain at least one key");
        }
        if self.api.api_keys.iter().any(|k| k.trim().is_empty()) {
            anyhow::bail!("[api] api_keys must not contain an empty key");
        }
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
        for host in &self.bootstrap_hosts {
            let hash = &host.token_sha256;
            if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
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
api_keys = ["key"]

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
        let text = VALID.replace(r#"api_keys = ["key"]"#, "");
        let err = parse(&text).unwrap_err().to_string();
        assert!(err.contains("api_keys"), "{err}");
    }

    #[test]
    fn an_empty_key_is_refused() {
        let text = VALID.replace(r#"["key"]"#, r#"["  "]"#);
        assert!(parse(&text).is_err());
    }

    #[test]
    fn a_bootstrap_host_needs_a_real_hash() {
        let text = format!("{VALID}\n[[bootstrap_hosts]]\nname = \"a\"\ntoken_sha256 = \"abc\"\n");
        assert!(parse(&text).is_err());
    }
}
