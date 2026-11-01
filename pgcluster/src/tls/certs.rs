//! Certificate generation for `pgcluster certs`, using `rcgen` 0.13.
//!
//! Produces a cluster CA and one certificate per name, for small deployments
//! and the local stack. Larger deployments can issue the same files from their
//! own CA.

use std::fs::OpenOptions;
use std::io::Write;
use std::net::IpAddr;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use anyhow::{Context, Result};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose, SanType,
    PKCS_ECDSA_P256_SHA256,
};

/// Create `ca.crt` and `ca.key` in `dir` unless they exist, then create
/// `<name>.crt` and `<name>.key` for each name that does not have them.
///
/// Each name is a hostname or IP address, and becomes the certificate's subject
/// alternative name, so it must match the address peers connect to. Certificates
/// are valid as both server and client, because every gRPC peer is both.
/// Existing files are never replaced, so a repeated run keeps the CA.
pub fn generate(dir: &Path, names: &[String]) -> Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;

    let ca_cert_path = dir.join("ca.crt");
    let ca_key_path = dir.join("ca.key");

    let ca_key = if ca_key_path.exists() {
        let pem = std::fs::read_to_string(&ca_key_path).context("read ca.key")?;
        KeyPair::from_pem(&pem).context("parse ca.key")?
    } else {
        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).context("generate CA key")?;
        write_private(&ca_key_path, key.serialize_pem().as_bytes())?;
        key
    };

    // Signing again with the stored key gives an issuer identical to ca.crt.
    let ca_cert = ca_params()?.self_signed(&ca_key).context("self-sign CA")?;
    if !ca_cert_path.exists() {
        std::fs::write(&ca_cert_path, ca_cert.pem()).context("write ca.crt")?;
    }

    for name in names {
        let cert_path = dir.join(format!("{name}.crt"));
        let key_path = dir.join(format!("{name}.key"));
        if cert_path.exists() && key_path.exists() {
            continue;
        }

        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).context("generate node key")?;
        let mut params = CertificateParams::new(Vec::<String>::new()).context("cert params")?;
        params.distinguished_name.push(DnType::CommonName, name);
        params.subject_alt_names.push(match name.parse::<IpAddr>() {
            Ok(ip) => SanType::IpAddress(ip),
            Err(_) => SanType::DnsName(
                name.as_str()
                    .try_into()
                    .with_context(|| format!("invalid hostname {name:?}"))?,
            ),
        });
        let cert = params
            .signed_by(&key, &ca_cert, &ca_key)
            .with_context(|| format!("sign certificate for {name}"))?;

        std::fs::write(&cert_path, cert.pem()).with_context(|| format!("write {name}.crt"))?;
        write_private(&key_path, key.serialize_pem().as_bytes())?;
    }

    Ok(())
}

fn ca_params() -> Result<CertificateParams> {
    let mut params = CertificateParams::new(Vec::<String>::new()).context("CA params")?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params
        .distinguished_name
        .push(DnType::CommonName, "pgcluster-ca");
    Ok(params)
}

/// Write a private key that only its owner can read.
fn write_private(path: &Path, contents: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options
        .open(path)
        .with_context(|| format!("create {}", path.display()))?;
    file.write_all(contents)
        .with_context(|| format!("write {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_ca_and_named_certificates() {
        let dir = tempfile::tempdir().unwrap();
        generate(dir.path(), &["pgcluster-1".into(), "10.0.0.1".into()]).unwrap();
        for file in [
            "ca.crt",
            "ca.key",
            "pgcluster-1.crt",
            "pgcluster-1.key",
            "10.0.0.1.crt",
            "10.0.0.1.key",
        ] {
            assert!(dir.path().join(file).exists(), "{file}");
        }
    }

    #[test]
    fn a_second_run_keeps_the_ca_and_can_add_a_name() {
        let dir = tempfile::tempdir().unwrap();
        generate(dir.path(), &["a".into()]).unwrap();
        let ca = std::fs::read(dir.path().join("ca.crt")).unwrap();
        let cert = std::fs::read(dir.path().join("a.crt")).unwrap();

        generate(dir.path(), &["a".into(), "b".into()]).unwrap();

        assert_eq!(ca, std::fs::read(dir.path().join("ca.crt")).unwrap());
        assert_eq!(cert, std::fs::read(dir.path().join("a.crt")).unwrap());
        assert!(dir.path().join("b.crt").exists());
    }

    #[cfg(unix)]
    #[test]
    fn private_keys_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        generate(dir.path(), &["a".into()]).unwrap();
        for file in ["ca.key", "a.key"] {
            let mode = std::fs::metadata(dir.path().join(file))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600, "{file}");
        }
    }
}
