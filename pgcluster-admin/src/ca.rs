//! The certificate authorities `pgcluster-admin` runs: one for hosts and this
//! service, and one for each database, which signs that cluster's certificates.

use std::fs::OpenOptions;
use std::io::Write;
use std::net::IpAddr;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use anyhow::{bail, Context, Result};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, CertificateSigningRequestParams, DnType,
    ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose, SanType, PKCS_ECDSA_P256_SHA256,
};
use sha2::{Digest, Sha256};

/// How long a host certificate lasts. Hosts rotate well before this.
pub const HOST_CERT_DAYS: i64 = 90;

pub struct Ca {
    cert: Certificate,
    key: KeyPair,
    cert_pem: String,
}

/// A certificate and its private key, in PEM.
pub struct Issued {
    pub cert_pem: String,
    pub key_pem: String,
}

impl Ca {
    /// A new CA held in memory.
    pub fn generate(common_name: &str) -> Result<Self> {
        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).context("generate CA key")?;
        let mut params = CertificateParams::new(Vec::<String>::new()).context("CA params")?;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params
            .distinguished_name
            .push(DnType::CommonName, common_name);
        let cert = params.self_signed(&key).context("self-sign CA")?;
        let cert_pem = cert.pem();
        Ok(Self {
            cert,
            key,
            cert_pem,
        })
    }

    pub fn from_pem(cert_pem: &str, key_pem: &str) -> Result<Self> {
        let key = KeyPair::from_pem(key_pem).context("parse CA key")?;
        let params = CertificateParams::from_ca_cert_pem(cert_pem).context("parse CA cert")?;
        // Signing again with the stored key gives an issuer identical to the file.
        let cert = params.self_signed(&key).context("load CA")?;
        Ok(Self {
            cert,
            key,
            cert_pem: cert_pem.to_string(),
        })
    }

    /// Load `ca.crt` and `ca.key` from `dir`, creating them on the first run.
    pub fn load_or_create(dir: &Path) -> Result<Self> {
        let cert_path = dir.join("ca.crt");
        let key_path = dir.join("ca.key");
        if cert_path.exists() && key_path.exists() {
            let cert = std::fs::read_to_string(&cert_path).context("read ca.crt")?;
            let key = std::fs::read_to_string(&key_path).context("read ca.key")?;
            return Self::from_pem(&cert, &key);
        }
        std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        let ca = Self::generate("pgcluster-admin-ca")?;
        let mut key_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&key_path)
            .with_context(|| format!("create {}", key_path.display()))?;
        key_file
            .write_all(ca.key.serialize_pem().as_bytes())
            .context("write ca.key")?;
        std::fs::write(&cert_path, &ca.cert_pem).context("write ca.crt")?;
        Ok(ca)
    }

    pub fn cert_pem(&self) -> &str {
        &self.cert_pem
    }

    pub fn key_pem(&self) -> String {
        self.key.serialize_pem()
    }

    /// The SHA-256 of the certificate's PEM text, carried in enrolment tokens.
    pub fn fingerprint(&self) -> String {
        hex::encode(Sha256::digest(self.cert_pem.as_bytes()))
    }

    /// A certificate for this service's own listeners.
    pub fn issue_server(&self, names: &[String]) -> Result<Issued> {
        let mut params = named_params(names)?;
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        self.issue(params)
    }

    /// A certificate for a Postgres node. Every peer there is client and server,
    /// so it carries no extended key usage, as `pgcluster certs` does.
    pub fn issue_node(&self, names: &[String]) -> Result<Issued> {
        self.issue(named_params(names)?)
    }

    fn issue(&self, params: CertificateParams) -> Result<Issued> {
        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).context("generate key")?;
        let cert = params
            .signed_by(&key, &self.cert, &self.key)
            .context("sign certificate")?;
        Ok(Issued {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem(),
        })
    }

    /// Sign a host's request. Only the public key comes from the request: the
    /// identity is always `host_id`, so a host cannot ask for another's name.
    pub fn sign_host_csr(&self, csr_pem: &str, host_id: &str) -> Result<String> {
        let csr = CertificateSigningRequestParams::from_pem(csr_pem)
            .map_err(|e| anyhow::anyhow!("invalid certificate request: {e}"))?;
        let mut params = CertificateParams::new(Vec::<String>::new())?;
        params.distinguished_name.push(DnType::CommonName, host_id);
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let now = rcgen::date_time_ymd(1970, 1, 1)
            + std::time::Duration::from_secs(chrono::Utc::now().timestamp() as u64);
        params.not_before = now - std::time::Duration::from_secs(300);
        params.not_after = now + std::time::Duration::from_secs(HOST_CERT_DAYS as u64 * 86_400);
        let signed = CertificateSigningRequestParams {
            params,
            public_key: csr.public_key,
        }
        .signed_by(&self.cert, &self.key)
        .context("sign host certificate")?;
        Ok(signed.pem())
    }
}

fn named_params(names: &[String]) -> Result<CertificateParams> {
    let Some(first) = names.first() else {
        bail!("a certificate needs at least one name");
    };
    let mut params = CertificateParams::new(Vec::<String>::new())?;
    params.distinguished_name.push(DnType::CommonName, first);
    for name in names {
        params.subject_alt_names.push(match name.parse::<IpAddr>() {
            Ok(ip) => SanType::IpAddress(ip),
            Err(_) => SanType::DnsName(
                name.as_str()
                    .try_into()
                    .with_context(|| format!("invalid hostname {name:?}"))?,
            ),
        });
    }
    Ok(params)
}

#[cfg(test)]
mod tests {
    use super::*;
    use x509_parser::prelude::{parse_x509_pem, X509Certificate};

    fn csr(common_name: &str) -> String {
        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
        let mut params = CertificateParams::new(vec!["evil.example.com".to_string()]).unwrap();
        params
            .distinguished_name
            .push(DnType::CommonName, common_name);
        params.serialize_request(&key).unwrap().pem().unwrap()
    }

    fn with_parsed<T>(pem: &str, check: impl FnOnce(&X509Certificate<'_>) -> T) -> T {
        let (_, parsed) = parse_x509_pem(pem.as_bytes()).unwrap();
        check(&parsed.parse_x509().unwrap())
    }

    #[test]
    fn a_ca_survives_a_reload_and_keeps_its_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        let first = Ca::load_or_create(dir.path()).unwrap();
        let second = Ca::load_or_create(dir.path()).unwrap();
        assert_eq!(first.fingerprint(), second.fingerprint());
    }

    #[test]
    fn the_key_file_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        Ca::load_or_create(dir.path()).unwrap();
        let mode = std::fs::metadata(dir.path().join("ca.key"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn a_host_certificate_takes_only_the_public_key_from_the_request() {
        let ca = Ca::generate("test").unwrap();
        let pem = ca.sign_host_csr(&csr("another-host"), "host-1").unwrap();
        with_parsed(&pem, |cert| {
            let cn = cert.subject().iter_common_name().next().unwrap();
            assert_eq!(cn.as_str().unwrap(), "host-1");
            assert!(!cert.is_ca());
            assert!(cert.subject_alternative_name().unwrap().is_none());
            let eku = cert.extended_key_usage().unwrap().unwrap().value;
            assert!(eku.client_auth && !eku.server_auth);
            let life = cert.validity().not_after.timestamp() - chrono::Utc::now().timestamp();
            assert!((0..=HOST_CERT_DAYS * 86_400).contains(&life));
        });
    }

    #[test]
    fn garbage_is_not_a_request() {
        let ca = Ca::generate("test").unwrap();
        assert!(ca.sign_host_csr("nonsense", "host-1").is_err());
    }

    #[test]
    fn a_node_certificate_carries_every_name() {
        let ca = Ca::generate("db").unwrap();
        let issued = ca
            .issue_node(&["10.0.0.1".into(), "127.0.0.1".into(), "localhost".into()])
            .unwrap();
        with_parsed(&issued.cert_pem, |cert| {
            let names = cert.subject_alternative_name().unwrap().unwrap();
            assert_eq!(names.value.general_names.len(), 3);
        });
    }
}
