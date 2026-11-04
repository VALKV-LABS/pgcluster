//! A single pooled TCP connection to a Postgres backend.

use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context as TaskContext, Poll};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

use super::ssl::SSL_REQUEST_BYTES;

// ── BackendStream ─────────────────────────────────────────────────────────────

/// A connection to Postgres: TLS, or plain TCP in development.
pub enum BackendStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for BackendStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            BackendStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            BackendStream::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for BackendStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            BackendStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            BackendStream::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            BackendStream::Plain(s) => Pin::new(s).poll_flush(cx),
            BackendStream::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            BackendStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            BackendStream::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Ask Postgres for TLS on `stream`, then verify its certificate against the
/// cluster CA and the hostname in `addr`. A server that declines is an error,
/// never a fallback to plain TCP.
async fn upgrade_to_tls(
    mut stream: TcpStream,
    addr: &str,
    connector: &TlsConnector,
) -> Result<BackendStream> {
    stream
        .write_all(&SSL_REQUEST_BYTES)
        .await
        .context("send SSL request")?;
    stream.flush().await.context("flush SSL request")?;

    match stream.read_u8().await.context("read SSL response")? {
        b'S' => {}
        b'N' => bail!("backend {addr} refused TLS; set ssl = on in postgresql.conf"),
        other => bail!("backend {addr} sent unexpected SSL response {other:#x}"),
    }

    let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);
    let server_name = ServerName::try_from(host.to_owned())
        .with_context(|| format!("invalid backend hostname {host:?}"))?;
    let tls = connector
        .connect(server_name, stream)
        .await
        .with_context(|| format!("TLS handshake with backend {addr}"))?;
    Ok(BackendStream::Tls(Box::new(tls)))
}

// ── BackendConnection ─────────────────────────────────────────────────────────

/// A single connection to a Postgres backend node.
///
/// Authentication and the startup handshake are **not** performed here —
/// they are handled by the pool layer so that connections can be reused across
/// client sessions in transaction-mode pooling.
///
/// The `stream` is protected by a `tokio::sync::Mutex` so that it can be
/// accessed mutably through the `Arc<BackendConnection>` that the pool hands
/// out. Only one task holds the lock at a time (the `ProxyConnection` that
/// checked it out).
pub struct BackendConnection {
    /// Logical node identifier (e.g. "pg1").
    pub node_id: String,

    /// The stream to the backend — locked for exclusive I/O access.
    pub stream: Mutex<BackendStream>,

    /// Whether this connection is currently checked out by a client.
    pub in_use: AtomicBool,

    /// When the connection was first established.
    pub created_at: Instant,

    /// Time of the most recent activity (last read or write).
    pub last_used: Mutex<Instant>,
}

impl std::fmt::Debug for BackendConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendConnection")
            .field("node_id", &self.node_id)
            .field("in_use", &self.in_use.load(Ordering::Relaxed))
            .field("created_at", &self.created_at)
            .finish_non_exhaustive()
    }
}

impl BackendConnection {
    /// Open a new connection to `addr` and wrap it as a `BackendConnection`.
    ///
    /// `node_id` is the logical cluster node identifier (e.g. `"pg1"`).
    /// `addr`    is the TCP address of the Postgres backend (e.g. `"127.0.0.1:5432"`).
    /// `tls`     verifies and encrypts the connection; `None` is plain TCP, for
    ///           development.
    pub async fn connect(node_id: &str, addr: &str, tls: Option<&TlsConnector>) -> Result<Self> {
        let tcp = TcpStream::connect(addr)
            .await
            .with_context(|| format!("connect to backend {node_id} at {addr}"))?;

        let stream = match tls {
            Some(connector) => upgrade_to_tls(tcp, addr, connector).await?,
            None => BackendStream::Plain(tcp),
        };

        tracing::debug!(node_id, addr, "backend connection established");

        Ok(Self {
            node_id: node_id.to_owned(),
            stream: Mutex::new(stream),
            in_use: AtomicBool::new(false),
            created_at: Instant::now(),
            last_used: Mutex::new(Instant::now()),
        })
    }

    /// Returns `true` if the connection appears to be alive.
    ///
    /// Polls the stream once: `Pending` means nothing has arrived, so it is
    /// alive. End of stream, an error, or unexpected data all mean it is not.
    /// Polling through the stream lets TLS consume its own records, such as
    /// session tickets, instead of mistaking them for data.
    ///
    /// Returns `true` conservatively if the stream lock is held (in-use).
    pub fn is_alive(&self) -> bool {
        match self.stream.try_lock() {
            Ok(mut guard) => {
                let mut byte = [0u8; 1];
                let mut buf = ReadBuf::new(&mut byte);
                let mut cx = TaskContext::from_waker(futures::task::noop_waker_ref());
                Pin::new(&mut *guard)
                    .poll_read(&mut cx, &mut buf)
                    .is_pending()
            }
            // Lock is held by a task doing I/O — assume alive.
            Err(_) => true,
        }
    }

    /// Returns `true` if the connection has been idle longer than `timeout_secs`.
    pub async fn is_idle_too_long(&self, timeout_secs: u64) -> bool {
        self.last_used.lock().await.elapsed().as_secs() > timeout_secs
    }

    /// Mark this connection as in-use (checked out from the pool).
    pub fn mark_in_use(&self) {
        self.in_use.store(true, Ordering::Release);
    }

    /// Mark this connection as idle (returned to the pool).
    pub fn mark_idle(&self) {
        self.in_use.store(false, Ordering::Release);
    }

    /// Touch `last_used` to reset the idle timer.
    pub async fn touch(&self) {
        *self.last_used.lock().await = Instant::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TlsConfig;
    use crate::tls::{certs, TlsManager};
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::net::TcpListener;

    fn manager(dir: &TempDir, name: &str) -> TlsManager {
        let _ = rustls::crypto::ring::default_provider().install_default();
        certs::generate(dir.path(), &[name.to_string()]).unwrap();
        let file = |f: String| Some(dir.path().join(f).to_str().unwrap().to_string());
        TlsManager::new(&TlsConfig {
            ca_cert: file("ca.crt".into()),
            cert: file(format!("{name}.crt")),
            key: file(format!("{name}.key")),
            insecure_plaintext: false,
            ..Default::default()
        })
        .unwrap()
    }

    /// A fake Postgres that answers the SSL request with `reply`, then, on `S`,
    /// echoes one TLS message and closes when asked.
    async fn fake_postgres(tls: Option<TlsManager>, reply: u8) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut tcp, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 8];
            tcp.read_exact(&mut request).await.unwrap();
            assert_eq!(request, SSL_REQUEST_BYTES);
            tcp.write_all(&[reply]).await.unwrap();
            if let Some(tls) = tls {
                let Ok(mut stream) = tls.server_tls_acceptor().accept(tcp).await else {
                    return;
                };
                let mut buf = [0u8; 4];
                if stream.read_exact(&mut buf).await.is_ok() {
                    stream.write_all(&buf).await.unwrap();
                    stream.flush().await.unwrap();
                    stream.shutdown().await.unwrap();
                }
            }
        });
        port
    }

    #[tokio::test]
    async fn traffic_is_encrypted_and_verified() {
        let dir = tempfile::tempdir().unwrap();
        let server = manager(&dir, "localhost");
        let connector = server.client_tls_connector();
        let port = fake_postgres(Some(server), b'S').await;

        let conn =
            BackendConnection::connect("pg1", &format!("localhost:{port}"), Some(&connector))
                .await
                .unwrap();
        assert!(conn.is_alive(), "an idle TLS connection must read as alive");

        let mut stream = conn.stream.lock().await;
        assert!(matches!(*stream, BackendStream::Tls(_)));
        stream.write_all(b"ping").await.unwrap();
        stream.flush().await.unwrap();
        let mut reply = [0u8; 4];
        stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"ping");
    }

    #[tokio::test]
    async fn closed_tls_connection_reads_as_dead() {
        let dir = tempfile::tempdir().unwrap();
        let server = manager(&dir, "localhost");
        let connector = server.client_tls_connector();
        let port = fake_postgres(Some(server), b'S').await;

        let conn =
            BackendConnection::connect("pg1", &format!("localhost:{port}"), Some(&connector))
                .await
                .unwrap();
        {
            let mut stream = conn.stream.lock().await;
            stream.write_all(b"ping").await.unwrap();
            stream.flush().await.unwrap();
            let mut reply = [0u8; 4];
            stream.read_exact(&mut reply).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!conn.is_alive());
    }

    #[tokio::test]
    async fn server_that_declines_tls_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let connector = manager(&dir, "localhost").client_tls_connector();
        let port = fake_postgres(None, b'N').await;

        let err = BackendConnection::connect("pg1", &format!("localhost:{port}"), Some(&connector))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("refused TLS"), "{err}");
    }

    #[tokio::test]
    async fn certificate_from_another_ca_is_rejected() {
        let server_dir = tempfile::tempdir().unwrap();
        let client_dir = tempfile::tempdir().unwrap();
        let server = manager(&server_dir, "localhost");
        let connector = manager(&client_dir, "localhost").client_tls_connector();
        let port = fake_postgres(Some(server), b'S').await;

        assert!(
            BackendConnection::connect("pg1", &format!("localhost:{port}"), Some(&connector))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn certificate_for_another_host_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let server = manager(&dir, "other-host");
        let connector = server.client_tls_connector();
        let port = fake_postgres(Some(server), b'S').await;

        assert!(
            BackendConnection::connect("pg1", &format!("localhost:{port}"), Some(&connector))
                .await
                .is_err()
        );
    }
}
