//! Serves an axum router over TLS, and tells handlers which host is calling.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use axum::body::Body;
use axum::http::Request;
use axum::Router;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_rustls::TlsAcceptor;
use tower::ServiceExt;
use x509_parser::prelude::*;

use crate::ca::Issued;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// A listener's TLS settings. A rotation of the admin CA swaps them, and open
/// connections keep theirs.
pub type SharedAcceptor = Arc<ArcSwap<TlsAcceptor>>;

pub fn shared(acceptor: TlsAcceptor) -> SharedAcceptor {
    Arc::new(ArcSwap::from_pointee(acceptor))
}

/// The host ID on the caller's client certificate, if it presented one.
#[derive(Clone, Debug)]
pub struct PeerHost(pub Option<String>);

fn parse_identity(
    issued: &Issued,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let certs = rustls_pemfile::certs(&mut issued.cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .context("parse server certificate")?;
    let key = rustls_pemfile::private_key(&mut issued.key_pem.as_bytes())
        .context("parse server key")?
        .context("no server key")?;
    Ok((certs, key))
}

/// Check that a certificate and key parse, belong together and are in date.
pub fn check_certificate(cert_pem: &str, key_pem: &str) -> Result<()> {
    let certs = rustls_pemfile::certs(&mut cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .context("parse the certificate")?;
    let first = certs.first().context("no certificate in certificate_pem")?;
    let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())
        .context("parse the private key")?
        .context("no private key in private_key_pem")?;
    let (_, parsed) = X509Certificate::from_der(first).context("read the certificate")?;
    if !parsed.validity().is_valid() {
        anyhow::bail!("the certificate is not valid now");
    }
    let signer = rustls::crypto::ring::sign::any_supported_type(&key)
        .context("the key type is not supported")?;
    let public = signer
        .public_key()
        .context("the key's public half cannot be read")?;
    if public.as_ref() != parsed.public_key().raw {
        anyhow::bail!("the private key does not belong to the certificate");
    }
    Ok(())
}

/// TLS with no client certificates, for the admin API.
pub fn server_tls(issued: &Issued) -> Result<TlsAcceptor> {
    let (certs, key) = parse_identity(issued)?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("build TLS config")?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// TLS for the agent API, trusting client certificates from `cas`. One is
/// optional here because a host has none until it enrols; `agent::Host` requires
/// one on every later route.
pub fn agent_tls(cas: &[&str], issued: &Issued) -> Result<TlsAcceptor> {
    let (certs, key) = parse_identity(issued)?;
    let mut roots = RootCertStore::empty();
    for mut pem in cas.iter().map(|pem| pem.as_bytes()) {
        for cert in rustls_pemfile::certs(&mut pem) {
            roots.add(cert.context("parse CA")?).context("add CA")?;
        }
    }
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .allow_unauthenticated()
        .build()
        .context("build client verifier")?;
    let config = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .context("build TLS config")?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn host_from_cert(cert: &CertificateDer<'_>) -> Option<String> {
    let (_, parsed) = X509Certificate::from_der(cert).ok()?;
    let cn = parsed.subject().iter_common_name().next()?.as_str().ok()?;
    Some(cn.to_string())
}

/// Caps on a listener's connections. A connection that has not shown a client
/// certificate is capped per address and in total, while a host's certificate
/// lifts it out of those caps, so a flood can neither crowd out enrolled hosts
/// nor, from one address, everyone else.
#[derive(Debug, Clone)]
pub struct Limits {
    /// Every connection, of any kind.
    pub total: usize,
    /// TLS handshakes in progress.
    pub handshakes: usize,
    /// Connections without a client certificate, handshakes included.
    pub anonymous: usize,
    /// Connections without a client certificate from one address.
    pub per_address: usize,
    pub handshake_timeout: Duration,
    /// How long a client has to send a request's headers.
    pub header_timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            total: 4096,
            handshakes: 256,
            anonymous: 512,
            per_address: 32,
            handshake_timeout: HANDSHAKE_TIMEOUT,
            header_timeout: Duration::from_secs(30),
        }
    }
}

struct Gate {
    limits: Limits,
    total: Arc<Semaphore>,
    handshakes: Arc<Semaphore>,
    anonymous: Arc<Semaphore>,
    per_address: Mutex<HashMap<IpAddr, usize>>,
}

/// What an anonymous connection holds until it ends or shows a certificate.
struct Anonymous {
    gate: Arc<Gate>,
    address: IpAddr,
    _permit: OwnedSemaphorePermit,
}

impl Drop for Anonymous {
    fn drop(&mut self) {
        let mut counts = self.gate.per_address.lock().expect("gate lock");
        if let Some(count) = counts.get_mut(&self.address) {
            *count -= 1;
            if *count == 0 {
                counts.remove(&self.address);
            }
        }
    }
}

impl Gate {
    fn new(limits: Limits) -> Arc<Self> {
        Arc::new(Self {
            total: Arc::new(Semaphore::new(limits.total)),
            handshakes: Arc::new(Semaphore::new(limits.handshakes)),
            anonymous: Arc::new(Semaphore::new(limits.anonymous)),
            per_address: Mutex::default(),
            limits,
        })
    }

    fn anonymous(self: &Arc<Self>, address: IpAddr) -> Option<Anonymous> {
        let permit = self.anonymous.clone().try_acquire_owned().ok()?;
        let mut counts = self.per_address.lock().expect("gate lock");
        let count = counts.entry(address).or_default();
        if *count >= self.limits.per_address {
            return None;
        }
        *count += 1;
        drop(counts);
        Some(Anonymous {
            gate: self.clone(),
            address,
            _permit: permit,
        })
    }
}

/// Accept connections until the task is dropped.
pub async fn serve(listener: TcpListener, acceptor: SharedAcceptor, app: Router) {
    serve_with(listener, acceptor, app, Limits::default()).await
}

pub async fn serve_with(
    listener: TcpListener,
    acceptor: SharedAcceptor,
    app: Router,
    limits: Limits,
) {
    let gate = Gate::new(limits);
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                tracing::error!(err = %e, "accept failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        // Refused at once, so a flood costs as little as it can.
        let Ok(total) = gate.total.clone().try_acquire_owned() else {
            tracing::debug!(%peer, "refused: too many connections");
            continue;
        };
        let Some(anonymous) = gate.anonymous(peer.ip()) else {
            tracing::debug!(%peer, "refused: too many connections from this address");
            continue;
        };
        let Ok(handshake) = gate.handshakes.clone().try_acquire_owned() else {
            tracing::debug!(%peer, "refused: too many handshakes");
            continue;
        };
        let acceptor = acceptor.load_full();
        let app = app.clone();
        let limits = gate.limits.clone();
        tokio::spawn(async move {
            let _total = total;
            let result =
                serve_connection(stream, peer, acceptor, app, limits, handshake, anonymous).await;
            if let Err(e) = result {
                tracing::debug!(%peer, err = %e, "connection ended");
            }
        });
    }
}

async fn serve_connection(
    stream: tokio::net::TcpStream,
    _peer: SocketAddr,
    acceptor: Arc<TlsAcceptor>,
    app: Router,
    limits: Limits,
    handshake: OwnedSemaphorePermit,
    anonymous: Anonymous,
) -> Result<()> {
    let tls = tokio::time::timeout(limits.handshake_timeout, acceptor.accept(stream))
        .await
        .context("TLS handshake timed out")?
        .context("TLS handshake")?;
    drop(handshake);
    let host = tls
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|chain| chain.first())
        .and_then(host_from_cert);
    // A verified certificate is an enrolled host: it leaves the caps that
    // strangers share.
    let _anonymous = if host.is_some() {
        drop(anonymous);
        None
    } else {
        Some(anonymous)
    };
    let service = hyper::service::service_fn(move |mut request: Request<Incoming>| {
        request.extensions_mut().insert(PeerHost(host.clone()));
        app.clone().oneshot(request.map(Body::new))
    });
    let mut builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(limits.header_timeout);
    builder
        .serve_connection(TokioIo::new(tls), service)
        .await
        .map_err(|e| anyhow::anyhow!("serve connection: {e}"))
}

#[cfg(test)]
mod tests {
    use super::check_certificate;

    fn pair() -> (String, String) {
        let rcgen::CertifiedKey { cert, key_pair } =
            rcgen::generate_simple_self_signed(vec!["db.example.com".into()]).unwrap();
        (cert.pem(), key_pair.serialize_pem())
    }

    #[test]
    fn a_matching_pair_passes() {
        let (cert, key) = pair();
        check_certificate(&cert, &key).unwrap();
    }

    #[test]
    fn a_key_from_another_pair_is_refused() {
        let (cert, _) = pair();
        let (_, other) = pair();
        let err = check_certificate(&cert, &other).unwrap_err().to_string();
        assert!(err.contains("does not belong"), "{err}");
    }

    #[test]
    fn junk_is_refused() {
        let (cert, key) = pair();
        assert!(check_certificate("not a certificate", &key).is_err());
        assert!(check_certificate(&cert, "not a key").is_err());
    }
}
