//! Per-client connection state machine.
//!
//! A `ProxyConnection` is spawned for each accepted TCP connection.
//! It handles:
//!   1. SSL negotiation
//!   2. Startup message parsing
//!   3. SCRAM with the client, then logging in to backends as that client
//!   4. Per-statement routing in a message loop

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::{
    auth::AuthQuery,
    backend::{BackendConnection, BackendStream},
    limits::LoginGuard,
    pool::{ConnectionPool, Login},
    prepared::{encode_empty, ClientStatements},
    protocol::{
        self, read_frontend_frame, read_startup_message, write_error_response, FrontendMessage,
        PROTOCOL_V3,
    },
    router::Router,
    scram::{authenticate_client, BackendLoginError, ClientAuthError},
    session::{RouteTarget, SessionState, TxnState},
    ssl::{handle_ssl_owned, MaybeTlsStream},
};

const NO_QUORUM_MESSAGE: &str =
    "proxy lost contact with the cluster majority; reconnect to another node";

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
/// Authenticate with SCRAM from the stored verifier, then log in to the primary
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
        auth: Arc<AuthQuery>,
        require_tls: bool,
        mut pending: LoginGuard,
    ) -> Result<()> {
        // ── 1. SSL negotiation ────────────────────────────────────────────
        let tls_ref = tls.as_deref();
        let mut client = handle_ssl_owned(stream, tls_ref).await?;
        if require_tls && matches!(client, MaybeTlsStream::Plain(_)) {
            write_error_response(&mut client, "FATAL", "28000", "this server requires TLS").await?;
            return Ok(());
        }

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

        // ── 3. Authentication ───────────────────────────
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

        // Without quorum the other side may already be promoting a new primary.
        if !router.has_quorum() {
            write_error_response(&mut client, "FATAL", "57P03", NO_QUORUM_MESSAGE).await?;
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

        // The proxy authenticates the client itself, from the verifier stored
        // for the role. That gives it what it needs to log in to any backend.
        let verifier = match auth.verifier(&primary_addr, &user).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(err = %format!("{e:#}"), "could not fetch a password verifier");
                write_error_response(
                    &mut client,
                    "FATAL",
                    "57P03",
                    "the proxy cannot authenticate clients yet; try again shortly",
                )
                .await?;
                return Ok(());
            }
        };
        let credentials = match authenticate_client(&mut client, verifier).await {
            Ok(c) => c,
            Err(ClientAuthError::Rejected) => {
                let message = format!("password authentication failed for user \"{user}\"");
                write_error_response(&mut client, "FATAL", "28P01", &message).await?;
                return Ok(());
            }
            Err(ClientAuthError::Protocol(e)) => return Err(e),
        };
        let login = Login {
            startup: startup_bytes,
            credentials,
        };

        let (auth_conn, rest) = match pool.login(&primary_node_id, &primary_addr, &login).await {
            Ok(pair) => pair,
            Err(BackendLoginError::Refused(frame)) => {
                client.write_all(&frame).await.context("forward refusal")?;
                client.flush().await.context("flush refusal")?;
                return Ok(());
            }
            Err(BackendLoginError::Failed(e)) => {
                write_error_response(
                    &mut client,
                    "FATAL",
                    "08006",
                    &format!("could not connect to primary: {e:#}"),
                )
                .await?;
                return Ok(());
            }
        };
        client.write_all(&rest).await.context("forward startup")?;
        client.flush().await.context("flush startup")?;
        pending.finish();

        // ── 4. Message routing loop ───────────────────────────────────────
        // Inject the now-authenticated connection into the pool so it can be
        // reused for subsequent query routing in this session (and others).
        pool.inject(
            &database,
            &user,
            &primary_addr,
            &primary_node_id,
            Arc::new(auth_conn),
        )
        .await;

        let mut current: Option<(Arc<BackendConnection>, String, String)> = None;
        let mut statements = ClientStatements::new(pool.max_prepared_statements());

        loop {
            let (msg, raw) = match read_frontend_frame(&mut client).await {
                Ok(frame) => frame,
                Err(e) => {
                    tracing::debug!("client disconnected: {e}");
                    break;
                }
            };

            if !router.has_quorum() && !matches!(msg, FrontendMessage::Terminate) {
                write_error_response(&mut client, "FATAL", "57P03", NO_QUORUM_MESSAGE).await?;
                break;
            }

            match msg {
                // ── Terminate ─────────────────────────────────────────────
                FrontendMessage::Terminate => {
                    tracing::debug!(user = %session.user, db = %session.database, "client terminated");
                    break;
                }

                // ── Simple query ──────────────────────────────────────────
                FrontendMessage::Query(ref sql) => {
                    if let Some(tag) = statements.answer_locally(sql) {
                        let mut body = tag.as_bytes().to_vec();
                        body.push(0);
                        protocol::write_message(&mut client, b'C', &body).await?;
                        protocol::write_ready_for_query(&mut client, session.txn_status_byte())
                            .await?;
                        continue;
                    }

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
                        &login,
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

                    statements.query(sql);
                    let rfq = forward_and_relay(&mut client, &conn, &raw, &mut statements).await?;
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
                        &login,
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

                    // Named statements are renamed for the backend, and sent
                    // again to a backend that lacks them.
                    let out = {
                        let mut held = conn.statements.lock().expect("statements lock");
                        statements.outgoing(&msg, &raw, &mut held)
                    };

                    // A backend answers a pipeline only at Sync, and an open
                    // COPY only at its end. Waiting after any other message
                    // would hang, because the client is still sending.
                    if answers_now(&msg) {
                        let rfq =
                            forward_and_relay(&mut client, &conn, &out, &mut statements).await?;
                        session.update_from_ready_for_query(rfq);
                        if session.txn_state == TxnState::Idle {
                            if let Some((c, nid, a)) = current.take() {
                                pool.release(&database, &user, &a, &nid, c).await;
                            }
                        }
                    } else if !out.is_empty() {
                        forward(&conn, &out).await?;
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
        RouteTarget::ReplicaOrPrimary => router.read_backend(),
    }
}

/// Whether the backend replies to this message before reading more.
fn answers_now(msg: &FrontendMessage) -> bool {
    matches!(
        msg,
        FrontendMessage::Sync | FrontendMessage::CopyDone | FrontendMessage::CopyFail(_)
    )
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
    login: &Login,
) -> Result<Arc<BackendConnection>> {
    if let Some((ref conn, ref nid, ref a)) = *current {
        if nid == node_id && a == addr {
            return Ok(Arc::clone(conn));
        }
    }

    // Acquire a new connection.
    let conn = pool.acquire(database, user, addr, node_id, login).await?;
    *current = Some((Arc::clone(&conn), node_id.to_owned(), addr.to_owned()));
    Ok(conn)
}

// ── I/O helpers ───────────────────────────────────────────────────────────────

/// Forward `raw_msg` to `backend`, then relay its replies to `client` until
/// a `ReadyForQuery`.
///
/// Returns the transaction status byte from the `ReadyForQuery`.
async fn forward_and_relay(
    client: &mut MaybeTlsStream,
    conn: &Arc<BackendConnection>,
    raw_msg: &[u8],
    statements: &mut ClientStatements,
) -> Result<u8> {
    let mut backend = conn.stream.lock().await;

    if !raw_msg.is_empty() {
        backend
            .write_all(raw_msg)
            .await
            .context("forward message to backend")?;
        backend.flush().await.context("flush to backend")?;
    }

    relay_until_ready_for_query(client, &mut backend, conn, statements).await
}

/// Forward `raw_msg` to the backend without waiting for a reply.
async fn forward(conn: &Arc<BackendConnection>, raw_msg: &[u8]) -> Result<()> {
    let mut backend = conn.stream.lock().await;
    backend
        .write_all(raw_msg)
        .await
        .context("forward message to backend")?;
    backend.flush().await.context("flush to backend")
}

/// Relay backend messages to `client` until a `ReadyForQuery` is seen.
///
/// Replies to requests the proxy made itself are dropped, and replies it
/// owes the client for requests it did not send are added in their place.
/// Large messages, such as wide rows, stream through as they arrive.
async fn relay_until_ready_for_query(
    client: &mut MaybeTlsStream,
    backend: &mut BackendStream,
    conn: &BackendConnection,
    statements: &mut ClientStatements,
) -> Result<u8> {
    let mut acc: Vec<u8> = Vec::new();
    let mut buf = vec![0u8; 65536];
    let mut out: Vec<u8> = Vec::new();
    // Bytes of a message still to arrive, and whether they go to the client.
    let mut body_left = 0usize;
    let mut body_forward = true;

    loop {
        while let Some(kind) = statements.next_synthetic() {
            out.extend_from_slice(&encode_empty(kind));
        }
        if !out.is_empty() {
            client
                .write_all(&out)
                .await
                .context("forward response to client")?;
            client.flush().await.context("flush to client")?;
            out.clear();
        }

        let n = backend
            .read(&mut buf)
            .await
            .context("read backend response")?;
        if n == 0 {
            bail!("backend closed connection unexpectedly");
        }

        let mut chunk = &buf[..n];
        if body_left > 0 {
            let take = body_left.min(chunk.len());
            if body_forward {
                out.extend_from_slice(&chunk[..take]);
            }
            body_left -= take;
            chunk = &chunk[take..];
        }
        acc.extend_from_slice(chunk);

        let mut pos = 0usize;
        let mut status = None;
        while status.is_none() && body_left == 0 && pos + 5 <= acc.len() {
            let kind = acc[pos];
            let length =
                u32::from_be_bytes([acc[pos + 1], acc[pos + 2], acc[pos + 3], acc[pos + 4]])
                    as usize;
            if length < 4 {
                bail!("backend sent a malformed message");
            }
            let total = 1 + length;
            let available = total.min(acc.len() - pos);
            // ReadyForQuery and CopyInResponse end the relay, so wait for
            // all of them.
            if matches!(kind, b'Z' | b'G') && available < total {
                break;
            }

            while let Some(synthetic) = statements.next_synthetic() {
                out.extend_from_slice(&encode_empty(synthetic));
            }
            let forward = {
                let mut held = conn.statements.lock().expect("statements lock");
                statements.on_reply(kind, &mut held)
            };
            if forward {
                out.extend_from_slice(&acc[pos..pos + available]);
            }

            match kind {
                b'Z' => status = Some(acc[pos + 5]),
                // CopyInResponse: the backend now waits for the client's data,
                // so report a transaction in progress to stay on this connection.
                b'G' => status = Some(b'T'),
                _ => {}
            }
            body_left = total - available;
            body_forward = forward;
            pos += available;
        }
        acc.drain(..pos);

        if !out.is_empty() {
            client
                .write_all(&out)
                .await
                .context("forward response to client")?;
            client.flush().await.context("flush to client")?;
            out.clear();
        }
        if let Some(status) = status {
            return Ok(status);
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
