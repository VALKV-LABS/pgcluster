//! TLS certificate management for vk-agent's gRPC server.
//!
//! Supports two modes:
//! - **Auto-generate** (`auto_generate = true`): creates a self-signed CA and
//!   a server certificate in `data_dir/certs/` on first start.  Subsequent
//!   starts reuse the files from disk.
//! - **Load from files** (`cert` + `key` set): reads PEM files at the given
//!   paths.

use std::path::Path;

use anyhow::{bail, Context, Result};
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, SanType};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls_pemfile;

use crate::config::AgentTlsConfig;

/// Load or generate TLS cert + key PEM bytes.
///
/// Returns `(ca_cert_pem, cert_pem, key_pem)`.  The CA cert is the one that
/// pgcluster should use to verify this agent's certificate.
pub fn load_or_generate(
    data_dir: &Path,
    cfg: &AgentTlsConfig,
) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    if let (Some(cert_path), Some(key_path)) = (&cfg.cert, &cfg.key) {
        // Load explicitly configured files.
        let cert =
            std::fs::read(cert_path).with_context(|| format!("read cert file {cert_path}"))?;
        let key = std::fs::read(key_path).with_context(|| format!("read key file {key_path}"))?;
        let ca = if let Some(ca_path) = &cfg.ca_cert {
            std::fs::read(ca_path).with_context(|| format!("read CA cert file {ca_path}"))?
        } else {
            cert.clone() // self-signed: use the cert itself as CA
        };
        return Ok((ca, cert, key));
    }

    if !cfg.auto_generate {
        bail!("TLS is not configured: set [tls] auto_generate=true or provide cert+key paths");
    }

    let certs_dir = data_dir.join("certs");
    let ca_path = certs_dir.join("agent-ca.crt");
    let cert_path = certs_dir.join("agent.crt");
    let key_path = certs_dir.join("agent.key");

    // Reuse existing files if all three are present.
    if ca_path.exists() && cert_path.exists() && key_path.exists() {
        let ca = std::fs::read(&ca_path).context("read agent-ca.crt")?;
        let cert = std::fs::read(&cert_path).context("read agent.crt")?;
        let key = std::fs::read(&key_path).context("read agent.key")?;
        tracing::debug!(dir = %certs_dir.display(), "reusing existing agent TLS certs");
        return Ok((ca, cert, key));
    }

    // Generate a new CA + server cert.
    std::fs::create_dir_all(&certs_dir)
        .with_context(|| format!("create certs dir {}", certs_dir.display()))?;

    let ca_key =
        KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).context("generate CA keypair")?;
    let mut ca_params =
        CertificateParams::new(vec!["vk-agent-ca".to_string()]).context("CA params")?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "vk-agent-ca");
    let ca_cert = ca_params.self_signed(&ca_key).context("self-sign CA")?;
    let ca_pem = ca_cert.pem().into_bytes();

    let node_key =
        KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).context("generate node keypair")?;
    let dns_names = vec!["localhost".to_string(), "vk-agent".to_string()];
    let mut node_params = CertificateParams::new(dns_names).context("node cert params")?;
    node_params
        .distinguished_name
        .push(DnType::CommonName, "vk-agent");
    node_params
        .subject_alt_names
        .push(SanType::IpAddress(std::net::IpAddr::V4(
            std::net::Ipv4Addr::LOCALHOST,
        )));
    let node_cert = node_params
        .signed_by(&node_key, &ca_cert, &ca_key)
        .context("sign node cert")?;
    let cert_pem = node_cert.pem().into_bytes();
    let key_pem = node_key.serialize_pem().into_bytes();

    std::fs::write(&ca_path, &ca_pem).context("write agent-ca.crt")?;
    std::fs::write(&cert_path, &cert_pem).context("write agent.crt")?;
    std::fs::write(&key_path, &key_pem).context("write agent.key")?;

    tracing::info!(dir = %certs_dir.display(), "generated agent TLS certificates");
    Ok((ca_pem, cert_pem, key_pem))
}

/// Build a `rustls::ServerConfig` from PEM-encoded cert and key bytes.
pub fn build_server_config(cert_pem: &[u8], key_pem: &[u8]) -> Result<rustls::ServerConfig> {
    let certs: Vec<CertificateDer<'static>> = {
        let mut reader = std::io::BufReader::new(cert_pem);
        rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .context("parse cert PEM")?
    };
    let key: PrivateKeyDer<'static> = {
        let mut reader = std::io::BufReader::new(key_pem);
        rustls_pemfile::private_key(&mut reader)
            .context("parse key PEM")?
            .context("no private key found")?
    };

    rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("build server TLS config")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn auto_generate_creates_cert_files() {
        let dir = TempDir::new().unwrap();
        let cfg = AgentTlsConfig {
            auto_generate: true,
            ..Default::default()
        };
        let (ca, cert, key) = load_or_generate(dir.path(), &cfg).unwrap();
        assert!(!ca.is_empty());
        assert!(!cert.is_empty());
        assert!(!key.is_empty());
        // Check files were written
        assert!(dir.path().join("certs/agent-ca.crt").exists());
        assert!(dir.path().join("certs/agent.crt").exists());
        assert!(dir.path().join("certs/agent.key").exists());
    }

    #[test]
    fn auto_generate_reuses_existing_files() {
        let dir = TempDir::new().unwrap();
        let cfg = AgentTlsConfig {
            auto_generate: true,
            ..Default::default()
        };
        let (ca1, cert1, _) = load_or_generate(dir.path(), &cfg).unwrap();
        let (ca2, cert2, _) = load_or_generate(dir.path(), &cfg).unwrap();
        // Must return the same cert on second call.
        assert_eq!(ca1, ca2);
        assert_eq!(cert1, cert2);
    }

    #[test]
    fn no_tls_config_returns_error() {
        let dir = TempDir::new().unwrap();
        let cfg = AgentTlsConfig::default(); // auto_generate=false, no paths
        let result = load_or_generate(dir.path(), &cfg);
        assert!(result.is_err());
    }

    #[test]
    fn generated_cert_builds_valid_server_config() {
        // rustls requires a CryptoProvider; ring is the only provider in vk-agent.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let dir = TempDir::new().unwrap();
        let cfg = AgentTlsConfig {
            auto_generate: true,
            ..Default::default()
        };
        let (_ca, cert, key) = load_or_generate(dir.path(), &cfg).unwrap();
        build_server_config(&cert, &key).expect("server config should build from generated certs");
    }
}
