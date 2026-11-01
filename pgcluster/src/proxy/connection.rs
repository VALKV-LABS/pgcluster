//! Per-client connection state machine.
//!
//! A `ProxyConnection` is spawned for each accepted TCP connection.
//! It handles:
//!   1. SSL negotiation
//!   2. Startup message parsing
//!   3. Auth pass-through to the primary backend
//!   4. Per-statement routing in a message loop

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use super::{
    backend::{BackendConnection, BackendStream},
    pool::ConnectionPool,
    protocol::{
        self, read_frontend_message, read_startup_message, write_error_response, FrontendMessage,
        PROTOCOL_V3,
    },
    router::Router,
    session::{RouteTarget, SessionState, TxnState},
    ssl::{handle_ssl_owned, MaybeTlsStream},
};

// ── ProxyConnection ───────────────────────────────────────────────────────────

/// Run the full proxy session state machine for one client connection.
///
/// # Protocol flow
///
/// ```text
/// Client connects (TCP)
///   ↓
/// [SSL negotiation] — upgrade to TLS if requested + acceptor present
///   ↓
/// Read StartupMessage → parse database + user
///   ↓
/// Auth pass-through → connect to primary, forward startup + auth exchange
///   ↓
/// Message routing loop:
///   ├─ Query(sql) → classify → pick backend → forward → pipe response
///   ├─ Parse/Bind/Execute/… → forward to current backend
///   └─ Terminate → break
/// ```
pub struct ProxyConnection {
    client: MaybeTlsStream,
    session: SessionState,
    router: Arc<Router>,
    pool: Arc<ConnectionPool>,
    /// Current backend (node_id, postgres_addr) — held while in a transaction.
    current_backend: Option<(Arc<BackendConnection>, String, String)>, // (conn, node_id, addr)
}

impl ProxyConnection {
    /// Create and immediately run a new proxy connection.
    pub async fn run(
        stream: TcpStream,
        router: Arc<Router>,
        pool: Arc<ConnectionPool>,
        tls: Option<Arc<tokio_rustls::TlsAcceptor>>,
    ) -> Result<()> {
        // ── 1. SSL negotiation ────────────────────────────────────────────
        let tls_ref = tls.as_deref();
        let mut client = handle_ssl_owned(stream, tls_ref).await?;

        // ── 2. Read startup message ───────────────────────────────────────
        let startup = read_startup_message(&mut client)
            .await
            .context("read client startup message")?;

        let mut session = SessionState::default();

        let (database, user, startup_bytes) = match startup {
            FrontendMessage::StartupMessage { ref params } => {
                let db = params.get("database").cloned().unwrap_or_default();
                let user = params.get("user").cloned().unwrap_or_default();
                let appname = params.get("application_name").cloned().unwrap_or_default();

                session.database = db.clone();
                session.user = user.clone();
                session.application_name = appname;

                let bytes = encode_startup_message(params);
                (db, user, bytes)
            }
            FrontendMessage::SslRequest | FrontendMessage::CancelRequest { .. } => {
                bail!("unexpected message type at startup");
            }
            _ => bail!("expected StartupMessage, got something else"),
        };

        // ── 3. Auth pass-through to the primary ───────────────────────────
        // Refuse new connections while the proxy is draining (switchover in progress).
        if router.is_draining() {
            write_error_response(
                &mut client,
                "FATAL",
                "57P01",
                "server is draining connections for a planned switchover; reconnect shortly",
            )
            .await?;
            return Ok(());
        }

        let (primary_node_id, primary_addr) =
            match router.primary_node_id().zip(router.primary_addr()) {
                Some(pair) => pair,
                None => {
                    write_error_response(&mut client, "FATAL", "57P03", "no primary available")
                        .await?;
                    return Ok(());
                }
            };

        // Always open a FRESH TCP connection for the startup+auth exchange.
        // We bypass pool.acquire() here because pooled connections are already
        // authenticated; sending a startup message to them would cause Postgres
        // to log "invalid frontend message type 0" and close the connection.
        let auth_conn = Arc::new(match pool.connect(&primary_node_id, &primary_addr).await {
            Ok(c) => c,
            Err(e) => {
                write_error_response(
                    &mut client,
                    "FATAL",
                    "08006",
                    &format!("could not connect to primary: {e}"),
                )
                .await?;
                return Ok(());
            }
        });

        // Forward startup and pipe the auth exchange.
        {
            let mut bstream = auth_conn.stream.lock().await;
            bstream
                .write_all(&startup_bytes)
                .await
                .context("forward startup")?;
            auth_passthrough(&mut client, &mut *bstream).await?;
        }

        // ── 4. Message routing loop ───────────────────────────────────────
        // Inject the now-authenticated connection into the pool so it can be
        // reused for subsequent query routing in this session (and others).
        pool.inject(&database, &user, &primary_addr, &primary_node_id, auth_conn)
            .await;

        let mut current: Option<(Arc<BackendConnection>, String, String)> = None;

        loop {
            let msg = match read_frontend_message(&mut client).await {
                Ok(m) => m,
                Err(e) => {
                    tracing::debug!("client disconnected: {e}");
                    break;
                }
            };

            match msg {
                // ── Terminate ─────────────────────────────────────────────
                FrontendMessage::Terminate => {
                    tracing::debug!(user = %session.user, db = %session.database, "client terminated");
                    break;
                }

                // ── Simple query ──────────────────────────────────────────
                FrontendMessage::Query(ref sql) => {
                    let intent = protocol::classify_statement(sql);
                    let target = session.route_intent(&intent);

                    // Determine (node_id, addr) for this statement.
                    let (node_id, addr) = if let Some((_, ref nid, ref addr)) = current {
                        // Sticky to current backend while in a transaction.
                        (nid.clone(), addr.clone())
                    } else {
                        // Between transactions: disconnect if the proxy is draining
                        // so clients reconnect to the (new) primary after switchover.
                        if router.is_draining() && matches!(target, RouteTarget::Primary) {
                            write_error_response(
                                &mut client,
                                "FATAL",
                                "57P01",
                                "server is draining connections for a planned switchover; reconnect shortly",
                            )
                            .await?;
                            break;
                        }
                        match resolve_backend(&router, &target) {
                            Some(pair) => pair,
                            None => {
                                write_error_response(
                                    &mut client,
                                    "ERROR",
                                    "57P03",
                                    "no backend available",
                                )
                                .await?;
                                continue;
                            }
                        }
                    };

                    let conn = match ensure_backend(
                        &mut current,
                        &pool,
                        &database,
                        &user,
                        &node_id,
                        &addr,
                    )
                    .await
                    {
                        Ok(c) => c,
                        Err(e) => {
                            write_error_response(
                                &mut client,
                                "ERROR",
                                "08006",
                                &format!("backend connection failed: {e}"),
                            )
                            .await?;
                            continue;
                        }
                    };

                    let raw = encode_query_message(sql);
                    let rfq = forward_and_pipe(&mut client, &conn, &raw).await?;
                    session.update_from_ready_for_query(rfq);

                    if session.txn_state == TxnState::Idle {
                        if let Some((c, nid, a)) = current.take() {
                            pool.release(&database, &user, &a, &nid, c).await;
                        }
                    }
                }

                // ── Extended query messages ────────────────────────────────
                FrontendMessage::Parse { .. }
                | FrontendMessage::Bind { .. }
                | FrontendMessage::Execute { .. }
                | FrontendMessage::Describe { .. }
                | FrontendMessage::Sync
                | FrontendMessage::Flush
                | FrontendMessage::Close { .. }
                | FrontendMessage::CopyData(_)
                | FrontendMessage::CopyDone
                | FrontendMessage::CopyFail(_) => {
                    let (node_id, addr) = if let Some((_, ref nid, ref addr)) = current {
                        (nid.clone(), addr.clone())
                    } else {
                        match router.primary_node_id().zip(router.primary_addr()) {
                            Some(pair) => pair,
                            None => {
                                write_error_response(&mut client, "ERROR", "57P03", "no primary")
                                    .await?;
                                continue;
                            }
                        }
                    };

                    let conn = match ensure_backend(
                        &mut current,
                        &pool,
                        &database,
                        &user,
                        &node_id,
                        &addr,
                    )
                    .await
                    {
                        Ok(c) => c,
                        Err(e) => {
                            write_error_response(
                                &mut client,
                                "ERROR",
                                "08006",
                                &format!("backend unavailable: {e}"),
                            )
                            .await?;
                            continue;
                        }
                    };

                    let raw = encode_raw_message(&msg);
                    let is_sync = matches!(msg, FrontendMessage::Sync);

                    if is_sync {
                        let rfq = forward_and_pipe(&mut client, &conn, &raw).await?;
                        session.update_from_ready_for_query(rfq);
                        if session.txn_state == TxnState::Idle {
                            if let Some((c, nid, a)) = current.take() {
                                pool.release(&database, &user, &a, &nid, c).await;
                            }
                        }
                    } else {
                        // For non-Sync messages send and pipe until next RFQ.
                        let rfq = forward_and_pipe(&mut client, &conn, &raw).await?;
                        session.update_from_ready_for_query(rfq);
                        if session.txn_state == TxnState::Idle {
                            if let Some((c, nid, a)) = current.take() {
                                pool.release(&database, &user, &a, &nid, c).await;
                            }
                        }
                    }
                }

                FrontendMessage::PasswordMessage(_) => {
                    tracing::warn!("unexpected PasswordMessage in main loop");
                }

                FrontendMessage::StartupMessage { .. }
                | FrontendMessage::SslRequest
                | FrontendMessage::CancelRequest { .. } => {
                    tracing::warn!("unexpected startup-phase message in main loop");
                }
            }
        }

        // Release any remaining backend on disconnect.
        if let Some((c, nid, a)) = current.take() {
            pool.release(&database, &user, &a, &nid, c).await;
        }

        Ok(())
    }
}

// ── Routing helpers ───────────────────────────────────────────────────────────

fn resolve_backend(router: &Router, target: &RouteTarget) -> Option<(String, String)> {
    match target {
        RouteTarget::Primary => router.primary_node_id().zip(router.primary_addr()),
        // A session is only authenticated on the primary. The proxy holds no
        // credentials of its own, so it cannot log in to a replica for the
        // client, and a connection that skips startup is dropped by Postgres.
        RouteTarget::ReplicaOrPrimary => router.primary_node_id().zip(router.primary_addr()),
    }
}

/// Ensure `current` holds a connection to `(node_id, addr)`.
///
/// If `current` already points to the correct backend, returns a clone of
/// the `Arc`.  Otherwise acquires a new connection from the pool.
async fn ensure_backend(
    current: &mut Option<(Arc<BackendConnection>, String, String)>,
    pool: &ConnectionPool,
    database: &str,
    user: &str,
    node_id: &str,
    addr: &str,
) -> Result<Arc<BackendConnection>> {
    if let Some((ref conn, ref nid, ref a)) = *current {
        if nid == node_id && a == addr {
            return Ok(Arc::clone(conn));
        }
    }

    // Acquire a new connection.
    let conn = pool.acquire(database, user, addr, node_id).await?;
    *current = Some((Arc::clone(&conn), node_id.to_owned(), addr.to_owned()));
    Ok(conn)
}

// ── I/O helpers ───────────────────────────────────────────────────────────────

/// Forward `raw_msg` to `backend`, then pipe all response messages back to
/// `client` until we see a `ReadyForQuery`.
///
/// Returns the transaction status byte from the `ReadyForQuery`.
async fn forward_and_pipe(
    client: &mut MaybeTlsStream,
    conn: &Arc<BackendConnection>,
    raw_msg: &[u8],
) -> Result<u8> {
    let mut backend = conn.stream.lock().await;

    backend
        .write_all(raw_msg)
        .await
        .context("forward message to backend")?;
    backend.flush().await.context("flush to backend")?;

    pipe_until_ready_for_query(client, &mut backend).await
}

/// Pipe backend response messages to `client` until a `ReadyForQuery` is seen.
///
/// Uses an accumulating buffer so a `ReadyForQuery` that spans two TCP reads
/// is still detected rather than causing an infinite read loop.
async fn pipe_until_ready_for_query(
    client: &mut MaybeTlsStream,
    backend: &mut BackendStream,
) -> Result<u8> {
    let mut acc: Vec<u8> = Vec::new();
    let mut buf = vec![0u8; 65536];

    loop {
        let n = backend
            .read(&mut buf)
            .await
            .context("read backend response")?;

        if n == 0 {
            bail!("backend closed connection unexpectedly");
        }

        client
            .write_all(&buf[..n])
            .await
            .context("forward response to client")?;
        client.flush().await.context("flush to client")?;

        acc.extend_from_slice(&buf[..n]);

        // Scan acc for ReadyForQuery, properly advancing past complete messages.
        let mut pos = 0usize;
        let mut last_complete = 0usize;

        while pos + 5 <= acc.len() {
            let msg_type = acc[pos];
            let length =
                u32::from_be_bytes([acc[pos + 1], acc[pos + 2], acc[pos + 3], acc[pos + 4]])
                    as usize;

            if msg_type == b'Z' && length == 5 && pos + 6 <= acc.len() {
                return Ok(acc[pos + 5]);
            }

            let next = pos + 1 + length;
            if next <= pos || next > acc.len() {
                // Malformed or incomplete message — wait for more data.
                break;
            }
            last_complete = next;
            pos = next;
        }

        // Discard fully-scanned bytes; keep only the unscanned tail so the
        // next iteration continues where we left off.
        if last_complete > 0 {
            acc.drain(0..last_complete);
        }
    }
}

/// Pipe the auth exchange between `client` and `backend` until the backend
/// sends `ReadyForQuery`.
///
/// Waits on both directions at once, because either side may be the one
/// holding up the exchange. SCRAM clients can take many milliseconds to answer
/// a challenge, and the backend sends nothing until they do.
async fn auth_passthrough<C, B>(client: &mut C, backend: &mut B) -> Result<()>
where
    C: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    let mut acc: Vec<u8> = Vec::new();
    let mut backend_buf = vec![0u8; 8192];
    let mut client_buf = vec![0u8; 8192];

    loop {
        tokio::select! {
            read = backend.read(&mut backend_buf) => {
                let n = read.context("read backend auth")?;
                if n == 0 {
                    bail!("backend closed during auth");
                }

                client
                    .write_all(&backend_buf[..n])
                    .await
                    .context("forward auth to client")?;
                client.flush().await.context("flush auth to client")?;

                acc.extend_from_slice(&backend_buf[..n]);

                // Scan for ReadyForQuery across the accumulated buffer.
                let mut pos = 0usize;
                let mut last_complete = 0usize;

                while pos + 5 <= acc.len() {
                    let msg_type = acc[pos];
                    let length = u32::from_be_bytes([
                        acc[pos + 1],
                        acc[pos + 2],
                        acc[pos + 3],
                        acc[pos + 4],
                    ]) as usize;

                    if msg_type == b'Z' && length == 5 && pos + 6 <= acc.len() {
                        return Ok(());
                    }

                    let next = pos + 1 + length;
                    if next <= pos || next > acc.len() {
                        break;
                    }
                    last_complete = next;
                    pos = next;
                }

                if last_complete > 0 {
                    acc.drain(0..last_complete);
                }
            }
            read = client.read(&mut client_buf) => {
                let n = read.context("read client auth")?;
                if n == 0 {
                    bail!("client closed during auth");
                }

                backend
                    .write_all(&client_buf[..n])
                    .await
                    .context("forward client auth")?;
                backend.flush().await.context("flush client auth")?;
            }
        }
    }
}

// ── Message encoding helpers ──────────────────────────────────────────────────

/// Re-encode the startup message (protocol 3.0 format) for forwarding.
pub(crate) fn encode_startup_message(
    params: &std::collections::HashMap<String, String>,
) -> Vec<u8> {
    let mut body: Vec<u8> = Vec::new();

    // Protocol version 3.0
    body.extend_from_slice(&PROTOCOL_V3.to_be_bytes());

    for (k, v) in params {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0); // terminator

    // Prepend 4-byte total length (includes the length field itself)
    let total_len = (4u32 + body.len() as u32).to_be_bytes();
    let mut msg = total_len.to_vec();
    msg.extend(body);
    msg
}

/// Encode a simple Query message ('Q').
fn encode_query_message(sql: &str) -> Vec<u8> {
    let mut body = sql.as_bytes().to_vec();
    body.push(0); // null terminator
    let length = (4u32 + body.len() as u32).to_be_bytes();
    let mut msg = vec![b'Q'];
    msg.extend_from_slice(&length);
    msg.extend(body);
    msg
}

/// Re-encode a frontend message back to wire bytes for forwarding.
fn encode_raw_message(msg: &FrontendMessage) -> Vec<u8> {
    match msg {
        FrontendMessage::Sync => encode_type_only(b'S'),
        FrontendMessage::Flush => encode_type_only(b'H'),
        FrontendMessage::CopyDone => encode_type_only(b'c'),

        FrontendMessage::CopyData(data) => build_msg(b'd', data),

        FrontendMessage::CopyFail(reason) => {
            let mut body = reason.as_bytes().to_vec();
            body.push(0);
            build_msg(b'f', &body)
        }

        FrontendMessage::Describe { kind, name } => {
            let mut body = vec![*kind];
            body.extend_from_slice(name.as_bytes());
            body.push(0);
            build_msg(b'D', &body)
        }

        FrontendMessage::Close { kind, name } => {
            let mut body = vec![*kind];
            body.extend_from_slice(name.as_bytes());
            body.push(0);
            build_msg(b'C', &body)
        }

        FrontendMessage::Execute { portal, max_rows } => {
            let mut body = portal.as_bytes().to_vec();
            body.push(0);
            body.extend_from_slice(&max_rows.to_be_bytes());
            build_msg(b'E', &body)
        }

        FrontendMessage::Parse {
            name,
            query,
            param_types,
        } => {
            let mut body = name.as_bytes().to_vec();
            body.push(0);
            body.extend_from_slice(query.as_bytes());
            body.push(0);
            body.extend_from_slice(&(param_types.len() as u16).to_be_bytes());
            for &t in param_types {
                body.extend_from_slice(&t.to_be_bytes());
            }
            build_msg(b'P', &body)
        }

        FrontendMessage::Bind {
            portal,
            statement,
            params,
        } => {
            let mut body: Vec<u8> = Vec::new();
            body.extend_from_slice(portal.as_bytes());
            body.push(0);
            body.extend_from_slice(statement.as_bytes());
            body.push(0);
            // Format codes: 0 (use text for all)
            body.extend_from_slice(&0u16.to_be_bytes());
            // Parameter values
            body.extend_from_slice(&(params.len() as u16).to_be_bytes());
            for p in params {
                body.extend_from_slice(&(p.len() as i32).to_be_bytes());
                body.extend_from_slice(p);
            }
            // Result format codes: 0 (text)
            body.extend_from_slice(&0u16.to_be_bytes());
            build_msg(b'B', &body)
        }

        // These should not be re-encoded here.
        _ => vec![],
    }
}

fn encode_type_only(type_byte: u8) -> Vec<u8> {
    // type_byte + length=4
    let mut msg = vec![type_byte];
    msg.extend_from_slice(&4u32.to_be_bytes());
    msg
}

fn build_msg(type_byte: u8, body: &[u8]) -> Vec<u8> {
    let length = (4u32 + body.len() as u32).to_be_bytes();
    let mut msg = vec![type_byte];
    msg.extend_from_slice(&length);
    msg.extend_from_slice(body);
    msg
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::duplex;

    const AUTH_CHALLENGE: &[u8] = b"R\x00\x00\x00\x04";
    const AUTH_DONE: &[u8] = b"Z\x00\x00\x00\x05I";

    /// The backend sends nothing after its challenge until the client answers,
    /// and a SCRAM client can take far longer than one poll to do that.
    #[tokio::test]
    async fn auth_waits_for_a_slow_client() {
        let (mut client, mut proxy_client) = duplex(64);
        let (mut proxy_backend, mut backend) = duplex(64);

        let backend_task = tokio::spawn(async move {
            backend.write_all(AUTH_CHALLENGE).await.unwrap();
            let mut answer = [0u8; 6];
            backend.read_exact(&mut answer).await.unwrap();
            assert_eq!(&answer, b"answer");
            backend.write_all(AUTH_DONE).await.unwrap();
            backend
        });

        let client_task = tokio::spawn(async move {
            let mut challenge = [0u8; 5];
            client.read_exact(&mut challenge).await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            client.write_all(b"answer").await.unwrap();
            let mut done = [0u8; 6];
            client.read_exact(&mut done).await.unwrap();
            assert_eq!(&done, AUTH_DONE);
            client
        });

        tokio::time::timeout(
            Duration::from_secs(5),
            auth_passthrough(&mut proxy_client, &mut proxy_backend),
        )
        .await
        .expect("auth must not deadlock")
        .unwrap();

        backend_task.await.unwrap();
        client_task.await.unwrap();
    }

    #[tokio::test]
    async fn client_that_leaves_during_auth_ends_the_exchange() {
        let (client, mut proxy_client) = duplex(64);
        let (mut proxy_backend, _backend) = duplex(64);
        drop(client);

        let err = auth_passthrough(&mut proxy_client, &mut proxy_backend)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("client closed"), "{err}");
    }
}
