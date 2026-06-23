//! SSL / TLS negotiation for incoming client connections.
//!
//! The Postgres SSL negotiation protocol:
//! 1. Client sends an 8-byte "SSL request" packet.
//! 2. Server responds with `S` (willing) or `N` (not willing).
//! 3. If `S`, the TCP stream is upgraded to TLS in-place.
//! 4. After TLS (or if no TLS), the client sends the regular startup message.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::{Context as AnyhowContext, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;

// ── SSL request magic ─────────────────────────────────────────────────────────

/// The 8 bytes that constitute a Postgres SSL request.
/// `\x00\x00\x00\x08` (length = 8) + `\x04\xd2\x16\x2f` (SSL request code).
const SSL_REQUEST_BYTES: [u8; 8] = [0x00, 0x00, 0x00, 0x08, 0x04, 0xd2, 0x16, 0x2f];

// ── MaybeTlsStream ────────────────────────────────────────────────────────────

/// A stream that is either a plain TCP socket or a TLS-wrapped socket.
///
/// Both variants implement `AsyncRead + AsyncWrite` so the rest of the proxy
/// can work without knowing whether TLS is active.
pub enum MaybeTlsStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

// `Box<T>: Unpin` for all T, and `TcpStream: Unpin`, so this is safe.
impl Unpin for MaybeTlsStream {}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

// ── SSL negotiation (owned stream — used by ProxyConnection) ──────────────────

/// Handle the Postgres SSL negotiation dance.
///
/// Takes ownership of `stream` and returns a [`MaybeTlsStream`].
///
/// Peeks at the first 8 bytes:
/// - If they match the SSL request magic **and** a `TlsAcceptor` is provided,
///   consumes those 8 bytes, sends `'S'`, and upgrades the stream to TLS.
/// - If they match the SSL request magic but no acceptor is provided, sends
///   `'N'` and continues on plain TCP.
/// - If they do NOT match the SSL request magic, the client is not requesting
///   TLS. The bytes remain in the kernel receive buffer; the startup message
///   will be read by the caller on the returned plain stream.
pub async fn handle_ssl_owned(
    mut stream: TcpStream,
    tls_acceptor: Option<&TlsAcceptor>,
) -> Result<MaybeTlsStream> {
    // Peek at the first 8 bytes without consuming them.
    let mut peek_buf = [0u8; 8];
    let peeked = peek_stream(&mut stream, &mut peek_buf).await?;

    if peeked == 8 && peek_buf == SSL_REQUEST_BYTES {
        // Consume the SSL request packet (8 bytes).
        let mut consume = [0u8; 8];
        stream
            .read_exact(&mut consume)
            .await
            .context("consume SSL request")?;

        if let Some(acceptor) = tls_acceptor {
            // Willing to upgrade: send 'S' then perform the TLS handshake.
            stream.write_all(b"S").await.context("send 'S'")?;
            stream.flush().await.context("flush 'S'")?;

            let tls_stream = acceptor
                .accept(stream)
                .await
                .context("TLS handshake with client")?;

            tracing::debug!("client connection upgraded to TLS");
            return Ok(MaybeTlsStream::Tls(Box::new(tls_stream)));
        } else {
            // TLS not configured — decline.
            stream.write_all(b"N").await.context("send 'N'")?;
            stream.flush().await.context("flush 'N'")?;
            tracing::debug!("TLS not configured; client will use plain TCP");
            // Startup message will arrive next on the plain stream.
            return Ok(MaybeTlsStream::Plain(stream));
        }
    }

    // No SSL request — the 8 bytes are still unread in the kernel buffer.
    tracing::debug!("no SSL request; plain TCP connection");
    Ok(MaybeTlsStream::Plain(stream))
}

/// Reference variant that works on `&mut TcpStream` (cannot perform TLS upgrade).
///
/// Useful for testing or when the caller cannot give up ownership.  Always
/// declines TLS with `'N'` even if an acceptor is provided.
#[allow(dead_code)]
pub async fn handle_ssl_negotiation(
    stream: &mut TcpStream,
    _tls_acceptor: Option<&TlsAcceptor>,
) -> Result<()> {
    let mut peek_buf = [0u8; 8];
    let n = peek_stream(stream, &mut peek_buf).await?;

    if n == 8 && peek_buf == SSL_REQUEST_BYTES {
        // Consume the SSL request.
        let mut consume = [0u8; 8];
        stream
            .read_exact(&mut consume)
            .await
            .context("consume SSL request bytes")?;

        // Cannot upgrade without stream ownership — always decline.
        stream.write_all(b"N").await.context("send SSL 'N' byte")?;
        stream.flush().await.context("flush SSL 'N'")?;
    }
    // Non-SSL: bytes remain buffered, caller reads startup message normally.
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Peek up to `buf.len()` bytes from `stream` without consuming them.
async fn peek_stream(stream: &TcpStream, buf: &mut [u8]) -> Result<usize> {
    loop {
        match stream.peek(buf).await {
            Ok(n) => return Ok(n),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                tokio::task::yield_now().await;
            }
            Err(e) => return Err(e).context("peek client stream"),
        }
    }
}
