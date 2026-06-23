//! Development certificate generation using `rcgen` 0.13.
//!
//! Produces a self-signed CA and a node certificate signed by that CA.
//! These are suitable for development and testing only — not for production.

use std::net::{IpAddr, Ipv4Addr};
use std::path::Path;

use anyhow::{Context, Result};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, SanType, PKCS_ECDSA_P256_SHA256,
};

/// Generate development TLS certificates.
///
/// Returns `(ca_cert_pem, cert_pem, key_pem)` as PEM-encoded byte vectors.
///
/// Also writes the files to `data_dir/certs/`:
/// - `ca.crt`  — CA certificate
/// - `node.crt` — node certificate (signed by CA)
/// - `node.key` — node private key
pub fn generate_dev_certs(node_id: u64, data_dir: &Path) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
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

    // CertificateParams::new() sets DNS SANs from the string list.
    // We then replace subject_alt_names to add the IP SAN explicitly.
    let dns_names = vec!["localhost".to_string(), "pgcluster".to_string()];
    let mut node_params = CertificateParams::new(dns_names).context("build node cert params")?;

    node_params
        .distinguished_name
        .push(DnType::CommonName, format!("pgcluster-node-{}", node_id));

    // Add the 127.0.0.1 IP SAN explicitly.
    node_params
        .subject_alt_names
        .push(SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)));

    let node_cert = node_params
        .signed_by(&node_key, &ca_cert, &ca_key)
        .context("sign node certificate with CA")?;

    let cert_pem = node_cert.pem().into_bytes();
    let key_pem = node_key.serialize_pem().into_bytes();

    // ── 3. Write to data_dir/certs/ ──────────────────────────────────────────
    let certs_dir = data_dir.join("certs");
    std::fs::create_dir_all(&certs_dir)
        .with_context(|| format!("create certs dir {:?}", certs_dir))?;

    std::fs::write(certs_dir.join("ca.crt"), &ca_cert_pem).context("write ca.crt")?;
    std::fs::write(certs_dir.join("node.crt"), &cert_pem).context("write node.crt")?;
    std::fs::write(certs_dir.join("node.key"), &key_pem).context("write node.key")?;

    tracing::info!(
        node_id,
        dir = %certs_dir.display(),
        "generated dev TLS certificates"
    );

    Ok((ca_cert_pem, cert_pem, key_pem))
}
