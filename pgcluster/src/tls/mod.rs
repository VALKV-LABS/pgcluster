//! TLS manager: loads certificates and provides `TlsAcceptor` / `TlsConnector`
//! handles for the rest of the codebase.

pub mod certs;

use std::io::BufReader;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::RootCertStore;
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::config::TlsConfig;

// ── TlsManager ────────────────────────────────────────────────────────────────

/// Holds pre-built TLS configs for both inbound and outbound connections.
///
/// * `server_tls` — used by the proxy and Raft peer listener.
/// * `client_tls` — used for outbound connections to Postgres backends and
///   other pgcluster nodes.
pub struct TlsManager {
    client_tls: Arc<rustls::ClientConfig>,
    server_tls: Arc<rustls::ServerConfig>,
    /// Raw PEM bytes kept so callers can build tonic/tokio-rustls handles.
    ca_pem: Vec<u8>,
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
}

impl TlsManager {
    /// Build a `TlsManager` from the PEM files named in the config.
    pub fn new(config: &TlsConfig) -> Result<Self> {
        let (Some(ca_path), Some(cert_path), Some(key_path)) =
            (&config.ca_cert, &config.cert, &config.key)
        else {
            bail!("[tls] ca_cert, cert and key are all required");
        };

        let ca_cert_pem =
            std::fs::read(ca_path).with_context(|| format!("read CA cert file {ca_path}"))?;
        let cert_pem =
            std::fs::read(cert_path).with_context(|| format!("read cert file {cert_path}"))?;
        let key_pem =
            std::fs::read(key_path).with_context(|| format!("read key file {key_path}"))?;

        let server_tls =
            build_server_config(&cert_pem, &key_pem).context("build server TLS config")?;
        let client_tls = build_client_config(&ca_cert_pem).context("build client TLS config")?;

        Ok(Self {
            client_tls: Arc::new(client_tls),
            server_tls: Arc::new(server_tls),
            ca_pem: ca_cert_pem,
            cert_pem,
            key_pem,
        })
    }

    /// PEM-encoded CA certificate used to verify peer/client certificates.
    pub fn ca_cert_pem(&self) -> &[u8] {
        &self.ca_pem
    }

    /// Returns a `TlsAcceptor` for incoming client / peer connections.
    pub fn server_tls_acceptor(&self) -> TlsAcceptor {
        TlsAcceptor::from(Arc::clone(&self.server_tls))
    }

    /// Returns a `TlsConnector` for outbound connections to backends or peers.
    pub fn client_tls_connector(&self) -> TlsConnector {
        TlsConnector::from(Arc::clone(&self.client_tls))
    }

    /// Returns a clone of the underlying `rustls::ServerConfig` for use with
    /// low-level rustls integrations.
    pub fn server_rustls_config(&self) -> Arc<rustls::ServerConfig> {
        Arc::clone(&self.server_tls)
    }

    /// Returns a clone of the underlying `rustls::ClientConfig` for use with
    /// low-level rustls integrations.
    pub fn client_rustls_config(&self) -> Arc<rustls::ClientConfig> {
        Arc::clone(&self.client_tls)
    }

    /// Build a tonic `ServerTlsConfig` that requires a client certificate from
    /// the cluster CA.
    pub fn tonic_server_tls_config(&self) -> tonic::transport::ServerTlsConfig {
        let identity = tonic::transport::Identity::from_pem(&self.cert_pem, &self.key_pem);
        let ca = tonic::transport::Certificate::from_pem(&self.ca_pem);
        tonic::transport::ServerTlsConfig::new()
            .identity(identity)
            .client_ca_root(ca)
    }

    /// Build a tonic `ClientTlsConfig` that verifies the server against the
    /// cluster CA and presents this node's certificate.
    pub fn tonic_client_tls_config(&self) -> tonic::transport::ClientTlsConfig {
        let identity = tonic::transport::Identity::from_pem(&self.cert_pem, &self.key_pem);
        let ca = tonic::transport::Certificate::from_pem(&self.ca_pem);
        tonic::transport::ClientTlsConfig::new()
            .ca_certificate(ca)
            .identity(identity)
    }

    /// Dev-mode: creates configs that skip certificate verification entirely.
    ///
    /// **Never use in production.**
    pub fn insecure() -> Self {
        let server_tls = build_insecure_server_config();
        let client_tls = build_insecure_client_config();
        Self {
            client_tls: Arc::new(client_tls),
            server_tls: Arc::new(server_tls),
            ca_pem: Vec::new(),
            cert_pem: Vec::new(),
            key_pem: Vec::new(),
        }
    }
}

// ── Builders ──────────────────────────────────────────────────────────────────

fn parse_certs(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>> {
    let mut reader = BufReader::new(pem);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .context("parse certificate PEM")?;
    if certs.is_empty() {
        bail!("no certificates found in PEM data");
    }
    Ok(certs)
}

fn parse_private_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>> {
    let mut reader = BufReader::new(pem);

    // Try PKCS#8 first, then fall back to RSA / EC
    let key = rustls_pemfile::private_key(&mut reader)
        .context("parse private key PEM")?
        .context("no private key found in PEM data")?;

    Ok(key)
}

fn build_server_config(cert_pem: &[u8], key_pem: &[u8]) -> Result<rustls::ServerConfig> {
    let certs = parse_certs(cert_pem)?;
    let key = parse_private_key(key_pem)?;

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("configure server TLS with cert+key")?;

    Ok(config)
}

fn build_client_config(ca_cert_pem: &[u8]) -> Result<rustls::ClientConfig> {
    let mut root_store = RootCertStore::empty();

    let ca_certs = parse_certs(ca_cert_pem)?;
    for cert in ca_certs {
        root_store.add(cert).context("add CA cert to root store")?;
    }

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(config)
}

// ── Insecure helpers (dev only) ───────────────────────────────────────────────

/// A `rustls::ServerConfig` that uses a throwaway self-signed cert.
fn build_insecure_server_config() -> rustls::ServerConfig {
    // Generate an ephemeral cert just to satisfy rustls (we have no stored certs).
    use rcgen::{CertificateParams, KeyPair, PKCS_ECDSA_P256_SHA256};

    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("generate ephemeral key");
    let params = CertificateParams::new(vec!["pgcluster-insecure".to_string()])
        .expect("build insecure cert params");
    let cert = params.self_signed(&key).expect("self-sign insecure cert");

    let cert_der = CertificateDer::from(cert.der().to_vec());
    let key_der = PrivateKeyDer::try_from(key.serialize_der()).expect("serialize ephemeral key");

    rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .expect("build insecure server config")
}

/// A `rustls::ClientConfig` that accepts any server certificate.
fn build_insecure_client_config() -> rustls::ClientConfig {
    #[derive(Debug)]
    struct AcceptAny;

    impl rustls::client::danger::ServerCertVerifier for AcceptAny {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &rustls::pki_types::ServerName<'_>,
            _ocsp_response: &[u8],
            _now: rustls::pki_types::UnixTime,
        ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            vec![
                rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
                rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
                rustls::SignatureScheme::RSA_PSS_SHA256,
                rustls::SignatureScheme::RSA_PSS_SHA384,
                rustls::SignatureScheme::RSA_PSS_SHA512,
                rustls::SignatureScheme::RSA_PKCS1_SHA256,
                rustls::SignatureScheme::RSA_PKCS1_SHA384,
                rustls::SignatureScheme::RSA_PKCS1_SHA512,
                rustls::SignatureScheme::ED25519,
            ]
        }
    }

    rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAny))
        .with_no_client_auth()
}
