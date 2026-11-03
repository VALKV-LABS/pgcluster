//! Serves an axum router over TLS, and tells handlers which host is calling.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use axum::body::Body;
use axum::http::Request;
use axum::Router;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
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

/// Accept connections until the task is dropped.
pub async fn serve(listener: TcpListener, acceptor: SharedAcceptor, app: Router) {
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                tracing::error!(err = %e, "accept failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let acceptor = acceptor.load_full();
        let app = app.clone();
        tokio::spawn(async move {
            if let Err(e) = serve_connection(stream, peer, acceptor, app).await {
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
) -> Result<()> {
    let tls = tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream))
        .await
        .context("TLS handshake timed out")?
        .context("TLS handshake")?;
    let host = tls
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|chain| chain.first())
        .and_then(host_from_cert);
    let service = hyper::service::service_fn(move |mut request: Request<Incoming>| {
        request.extensions_mut().insert(PeerHost(host.clone()));
        app.clone().oneshot(request.map(Body::new))
    });
    hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
        .serve_connection(TokioIo::new(tls), service)
        .await
        .map_err(|e| anyhow::anyhow!("serve connection: {e}"))
}
