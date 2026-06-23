//! A single pooled TCP connection to a Postgres backend.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use anyhow::{Context, Result};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

// ── BackendConnection ─────────────────────────────────────────────────────────

/// A single raw TCP connection to a Postgres backend node.
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

    /// The raw TCP stream to the backend — locked for exclusive I/O access.
    pub stream: Mutex<TcpStream>,

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
    /// Open a new TCP connection to `addr` and wrap it as a `BackendConnection`.
    ///
    /// `node_id` is the logical cluster node identifier (e.g. `"pg1"`).
    /// `addr`    is the TCP address of the Postgres backend (e.g. `"127.0.0.1:5432"`).
    pub async fn connect(node_id: &str, addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .with_context(|| format!("connect to backend {node_id} at {addr}"))?;

        tracing::debug!(node_id, addr, "backend TCP connection established");

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
    /// Uses a non-blocking try-lock + peek: if the backend has closed the
    /// socket we will see `Ok(0)` (EOF); if no data is available we get
    /// `WouldBlock`, which means the connection is alive.
    ///
    /// Returns `true` conservatively if the stream lock is held (in-use).
    pub fn is_alive(&self) -> bool {
        match self.stream.try_lock() {
            Ok(guard) => {
                let mut buf = [0u8; 1];
                match guard.try_read(&mut buf) {
                    Ok(0) => false, // EOF — backend closed
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => true,
                    _ => false,
                }
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
