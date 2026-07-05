//! Development certificate generation using `rcgen` 0.13.
//!
//! Produces a self-signed CA and a node certificate signed by that CA.
//! Certificates are written to `data_dir/certs/` on first run and reused on
//! subsequent runs.  Regenerating the CA on every restart would invalidate all
//! distributed trust anchors and break Raft peer / vk-agent TLS connections.

use std::net::{IpAddr, Ipv4Addr};
use std::path::Path;

use anyhow::{Context, Result};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, SanType, PKCS_ECDSA_P256_SHA256,
};

/// Load existing dev TLS certificates from `data_dir/certs/`, or generate and
/// persist new ones if they do not yet exist.
///
/// Returns `(ca_cert_pem, cert_pem, key_pem)` as PEM-encoded byte vectors.
///
/// File layout:
/// - `data_dir/certs/ca.crt`   — CA certificate (shared trust anchor)
/// - `data_dir/certs/node.crt` — node certificate (signed by CA)
/// - `data_dir/certs/node.key` — node private key
///
/// The CA cert is written to a predictable path so that vk-agents and peer nodes
/// can be configured with `ca_cert = "/path/to/data_dir/certs/ca.crt"` to trust
/// pgcluster's certificate chain.
pub fn generate_dev_certs(node_id: u64, data_dir: &Path) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    let certs_dir = data_dir.join("certs");
    let ca_path = certs_dir.join("ca.crt");
    let cert_path = certs_dir.join("node.crt");
    let key_path = certs_dir.join("node.key");

    // Reuse existing certs if all three files are present.  This preserves the
    // CA across restarts so that distributed trust anchors remain valid.
    if ca_path.exists() && cert_path.exists() && key_path.exists() {
        let ca_pem = std::fs::read(&ca_path)
            .with_context(|| format!("read existing ca.crt from {}", ca_path.display()))?;
        let cert_pem = std::fs::read(&cert_path)
            .with_context(|| format!("read existing node.crt from {}", cert_path.display()))?;
        let key_pem = std::fs::read(&key_path)
            .with_context(|| format!("read existing node.key from {}", key_path.display()))?;
        tracing::info!(
            node_id,
            dir = %certs_dir.display(),
            "loaded existing dev TLS certificates"
        );
        return Ok((ca_pem, cert_pem, key_pem));
    }

    // ── 1. Generate self-signed CA ────────────────────────────────────────────
    let ca_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).context("generate CA keypair")?;

    let mut ca_params =
        CertificateParams::new(vec!["pgcluster-ca".to_string()]).context("build CA params")?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "pgcluster-ca");

    let ca_cert = ca_params
        .self_signed(&ca_key)
        .context("self-sign CA certificate")?;

    let ca_cert_pem = ca_cert.pem().into_bytes();

    // ── 2. Generate node cert signed by the CA ────────────────────────────────
    let node_key =
        KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).context("generate node keypair")?;

    // DNS SANs include common container/service names.  Production deployments
    // should use explicit cert/key paths with proper SANs for their hostnames.
    let dns_names = vec![
        "localhost".to_string(),
        "pgcluster".to_string(),
        format!("pgcluster-{}", node_id),
    ];
    let mut node_params = CertificateParams::new(dns_names).context("build node cert params")?;

    node_params
        .distinguished_name
        .push(DnType::CommonName, format!("pgcluster-node-{}", node_id));

    // Add IP SANs for loopback access.
    node_params
        .subject_alt_names
        .push(SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)));

    let node_cert = node_params
        .signed_by(&node_key, &ca_cert, &ca_key)
        .context("sign node certificate with CA")?;

    let cert_pem = node_cert.pem().into_bytes();
    let key_pem = node_key.serialize_pem().into_bytes();

    // ── 3. Persist to data_dir/certs/ ────────────────────────────────────────
    std::fs::create_dir_all(&certs_dir)
        .with_context(|| format!("create certs dir {:?}", certs_dir))?;

    std::fs::write(&ca_path, &ca_cert_pem).context("write ca.crt")?;
    std::fs::write(&cert_path, &cert_pem).context("write node.crt")?;
    std::fs::write(&key_path, &key_pem).context("write node.key")?;

    tracing::info!(
        node_id,
        dir = %certs_dir.display(),
        "generated and persisted dev TLS certificates"
    );

    Ok((ca_cert_pem, cert_pem, key_pem))
}
