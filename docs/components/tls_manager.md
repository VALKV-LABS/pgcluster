# Component: TLS Manager (`tls_manager`)

## High-Level Function

All internal communication in pgcluster is TLS-encrypted: pgcluster ↔ pgcluster (Raft), pgcluster ↔ vk-agent (gRPC), and optionally pgcluster proxy ↔ Postgres backends. The TLS manager handles certificate loading, validation, and automatic self-signed cert generation for development deployments.

---

## Architecture

### Trust Model

```
CA Certificate
  ├── pgcluster server certs (for Raft gRPC peer connections)
  ├── vk-agent server certs (presented to pgcluster)
  └── Client certs (optional, for mutual TLS to agents)
```

All pgcluster instances and vk-agents share the same CA. A new node joining the cluster needs a cert signed by that CA. `pgcluster init` can generate all certs automatically for single-cluster deployments.

### Certificate Sources

| Source | Use case |
|--------|----------|
| PEM files (config) | Production — certs managed by Vault, cert-manager, etc. |
| Auto-generated (rcgen) | Development / `pgcluster init` — self-signed CA + leaf certs |
| ACME / Let's Encrypt | Future (Milestone 2) — for proxy → client TLS with public CAs |

---

## Detailed Implementation Plan

### 1. Module Layout

```
src/
  tls/
    mod.rs          # TlsManager, load(), acceptor(), connector()
    config.rs       # TlsConfig (paths, mode)
    generate.rs     # Self-signed CA + leaf cert generation (rcgen)
    reload.rs       # Certificate hot-reload without restart
```

### 2. TlsManager

```rust
pub struct TlsManager {
    config: TlsConfig,
    server_config: Arc<RwLock<Arc<rustls::ServerConfig>>>,
    client_config: Arc<RwLock<Arc<rustls::ClientConfig>>>,
}

pub struct TlsConfig {
    pub ca_cert_path: PathBuf,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub verify_peers: bool,   // mutual TLS (default: true)
    pub auto_generate: bool,  // Generate if files don't exist (dev mode)
}

impl TlsManager {
    pub fn load(config: TlsConfig) -> Result<Self> {
        if config.auto_generate && !config.cert_path.exists() {
            generate::generate_self_signed(&config)?;
            log::info!("Generated self-signed TLS certs in {:?}", config.cert_path.parent());
        }

        let ca = load_ca_cert(&config.ca_cert_path)?;
        let (cert_chain, key) = load_cert_and_key(&config.cert_path, &config.key_path)?;

        let server_config = build_server_config(cert_chain.clone(), key.clone(), &ca, config.verify_peers)?;
        let client_config = build_client_config(cert_chain, key, &ca)?;

        Ok(TlsManager {
            config,
            server_config: Arc::new(RwLock::new(Arc::new(server_config))),
            client_config: Arc::new(RwLock::new(Arc::new(client_config))),
        })
    }

    /// Returns a TLS acceptor for incoming gRPC / Raft connections
    pub fn server_acceptor(&self) -> tokio_rustls::TlsAcceptor {
        tokio_rustls::TlsAcceptor::from(self.server_config.read().unwrap().clone())
    }

    /// Returns a TLS connector for outbound connections to peers / agents
    pub fn client_connector(&self) -> tokio_rustls::TlsConnector {
        tokio_rustls::TlsConnector::from(self.client_config.read().unwrap().clone())
    }

    /// Reload certs from disk without restarting (used by config reload)
    pub fn reload(&self) -> Result<()> {
        let ca = load_ca_cert(&self.config.ca_cert_path)?;
        let (cert_chain, key) = load_cert_and_key(&self.config.cert_path, &self.config.key_path)?;
        *self.server_config.write().unwrap() = Arc::new(
            build_server_config(cert_chain.clone(), key.clone(), &ca, self.config.verify_peers)?
        );
        *self.client_config.write().unwrap() = Arc::new(build_client_config(cert_chain, key, &ca)?);
        log::info!("TLS certificates reloaded");
        Ok(())
    }
}
```

### 3. Self-Signed Certificate Generation

```rust
pub fn generate_self_signed(config: &TlsConfig) -> Result<()> {
    use rcgen::{Certificate, CertificateParams, DistinguishedName, KeyPair, PKCS_ECDSA_P256_SHA256};

    // Generate CA
    let mut ca_params = CertificateParams::new(vec!["pgcluster-ca".to_string()]);
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = Certificate::from_params(ca_params)?;

    // Generate leaf cert
    let mut leaf_params = CertificateParams::new(vec!["pgcluster".to_string()]);
    leaf_params.distinguished_name = DistinguishedName::new();
    let leaf_cert = Certificate::from_params(leaf_params)?;
    let leaf_signed = leaf_cert.serialize_pem_with_signer(&ca_cert)?;

    std::fs::write(&config.ca_cert_path, ca_cert.serialize_pem()?)?;
    std::fs::write(&config.cert_path, leaf_signed)?;
    std::fs::write(&config.key_path, leaf_cert.serialize_private_key_pem())?;

    Ok(())
}
```

### 4. Integration Points

- `raft_consensus` uses `tls_manager.client_connector()` for outbound Raft gRPC connections.
- `raft_consensus` uses `tls_manager.server_acceptor()` for incoming Raft gRPC connections.
- `vk_agent` (agent client in pgcluster) uses `tls_manager.client_connector()`.
- `proxy_layer` optionally uses `tls_manager.server_acceptor()` for client → proxy TLS.
- `rest_api` uses `tls_manager.server_acceptor()` for HTTPS on the API port.
- `pgcluster config reload` calls `tls_manager.reload()` for cert rotation without restart.
