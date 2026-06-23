//! Per-(database, user, backend) connection pool.
//!
//! Each pool slot holds a [`BackendConnection`] that can be borrowed by a
//! proxy session and returned when the transaction completes (transaction-mode
//! pooling, the PgBouncer default).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use dashmap::DashMap;

use super::backend::BackendConnection;
use crate::config::PoolConfig;

// ── PoolKey ───────────────────────────────────────────────────────────────────

/// Composite key that uniquely identifies a pool bucket.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct PoolKey {
    pub database: String,
    pub user: String,
    /// `"<node_id>:<addr>"` — e.g. `"pg1:10.0.0.1:5432"`
    pub backend: String,
}

impl PoolKey {
    pub fn new(database: &str, user: &str, node_id: &str, addr: &str) -> Self {
        Self {
            database: database.to_owned(),
            user: user.to_owned(),
            backend: format!("{node_id}:{addr}"),
        }
    }

    fn string_key(&self) -> String {
        format!("{}|{}|{}", self.database, self.user, self.backend)
    }
}

// ── Internal pool bucket ──────────────────────────────────────────────────────

struct PoolBucket {
    idle: tokio::sync::Mutex<Vec<Arc<BackendConnection>>>,
    /// Total connections alive (idle + in-use).
    total: std::sync::atomic::AtomicUsize,
    max: usize,
    /// Notified when a connection is released back to the bucket.
    released: tokio::sync::Notify,
}

impl PoolBucket {
    fn new(max: usize) -> Self {
        Self {
            idle: tokio::sync::Mutex::new(Vec::new()),
            total: std::sync::atomic::AtomicUsize::new(0),
            max,
            released: tokio::sync::Notify::new(),
        }
    }

    fn total(&self) -> usize {
        self.total.load(std::sync::atomic::Ordering::Acquire)
    }

    fn increment(&self) -> usize {
        self.total.fetch_add(1, std::sync::atomic::Ordering::AcqRel)
    }

    fn decrement(&self) {
        self.total.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

// ── ConnectionPool ────────────────────────────────────────────────────────────

/// A global connection pool indexed by `(database, user, backend_addr)`.
pub struct ConnectionPool {
    // DashMap key is the string form of PoolKey for simpler hashing.
    pools: DashMap<String, Arc<PoolBucket>>,
    config: PoolConfig,
}

impl ConnectionPool {
    pub fn new(config: PoolConfig) -> Self {
        Self {
            pools: DashMap::new(),
            config,
        }
    }

    /// Borrow an idle connection for `(database, user, backend_addr)`.
    ///
    /// - If an idle healthy connection exists it is returned immediately.
    /// - Otherwise a new TCP connection is opened (if under the pool limit).
    /// - If the limit is reached the call waits up to `connect_timeout_seconds`.
    pub async fn acquire(
        &self,
        database: &str,
        user: &str,
        backend_addr: &str,
        node_id: &str,
    ) -> Result<Arc<BackendConnection>> {
        let key = PoolKey::new(database, user, node_id, backend_addr);
        let skey = key.string_key();
        let max = self.config.max_connections_per_db_user as usize;
        let bucket = self
            .pools
            .entry(skey)
            .or_insert_with(|| Arc::new(PoolBucket::new(max)))
            .clone();

        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(self.config.connect_timeout_seconds);

        loop {
            // ── Fast path: take an idle healthy connection ────────────────
            {
                let mut idle = bucket.idle.lock().await;
                while let Some(conn) = idle.pop() {
                    if conn.is_alive() {
                        conn.mark_in_use();
                        return Ok(conn);
                    } else {
                        // Broken — discard
                        bucket.decrement();
                    }
                }
            }

            // ── Try to open a new connection ──────────────────────────────
            let prev_total = bucket.total();
            if prev_total < bucket.max {
                // Reserve a slot before the async connect
                bucket.increment();
                match BackendConnection::connect(node_id, backend_addr).await {
                    Ok(conn) => {
                        conn.mark_in_use();
                        return Ok(Arc::new(conn));
                    }
                    Err(e) => {
                        bucket.decrement();
                        return Err(e).context("open new backend connection");
                    }
                }
            }

            // ── Wait for a connection to be released ──────────────────────
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "connection pool exhausted for {}/{} → {}",
                    database,
                    user,
                    backend_addr
                );
            }

            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let _ = tokio::time::timeout(remaining, bucket.released.notified()).await;
        }
    }

    /// Return a connection to the pool.
    ///
    /// If the connection is broken, or we already have more idle connections
    /// than the `max_connections_per_db_user` limit, it is discarded.
    pub async fn release(
        &self,
        database: &str,
        user: &str,
        backend_addr: &str,
        node_id: &str,
        conn: Arc<BackendConnection>,
    ) {
        let key = PoolKey::new(database, user, node_id, backend_addr);
        let skey = key.string_key();

        let bucket = match self.pools.get(&skey) {
            Some(b) => b.clone(),
            None => return, // pool was dropped (e.g. backend drained)
        };

        conn.mark_idle();

        if conn.is_alive() {
            let _ = conn.touch().await;
            let mut idle = bucket.idle.lock().await;
            idle.push(conn);
            drop(idle);
            bucket.released.notify_one();
        } else {
            // Broken connection — discard
            bucket.decrement();
        }
    }

    /// Donate a pre-authenticated connection to the pool.
    ///
    /// Use this after completing startup+auth on a fresh TCP connection so the
    /// pool can hand it out for subsequent query routing.  Unlike `release`,
    /// this path increments the total-connection counter (since `acquire` was
    /// never called for this connection).
    pub async fn inject(
        &self,
        database: &str,
        user: &str,
        backend_addr: &str,
        node_id: &str,
        conn: Arc<BackendConnection>,
    ) {
        let key = PoolKey::new(database, user, node_id, backend_addr);
        let skey = key.string_key();
        let max = self.config.max_connections_per_db_user as usize;

        let bucket = self
            .pools
            .entry(skey)
            .or_insert_with(|| Arc::new(PoolBucket::new(max)))
            .clone();

        bucket.increment();
        conn.mark_idle();
        let _ = conn.touch().await;
        let mut idle = bucket.idle.lock().await;
        idle.push(conn);
        drop(idle);
        bucket.released.notify_one();
    }

    /// Remove all idle connections to `backend_addr` (used after a failover).
    ///
    /// In-use connections will be discarded when they are next `release()`d.
    pub async fn drain_backend(&self, backend_addr: &str) {
        let keys_to_drain: Vec<String> = self
            .pools
            .iter()
            .filter(|entry| entry.key().contains(backend_addr))
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys_to_drain {
            if let Some(bucket) = self.pools.get(&key) {
                let mut idle = bucket.idle.lock().await;
                let removed = idle.len();
                idle.clear();
                drop(idle);
                // Adjust counter for the connections we just discarded.
                for _ in 0..removed {
                    bucket.decrement();
                }
                tracing::info!(backend = backend_addr, removed, "drained pool");
            }
        }
    }

    /// Background task: remove connections idle longer than `idle_timeout_seconds`.
    pub async fn evict_idle(&self) {
        let timeout = self.config.idle_timeout_seconds;

        for entry in self.pools.iter() {
            let bucket = entry.value().clone();
            let mut idle = bucket.idle.lock().await;
            let before = idle.len();
            let mut kept = Vec::with_capacity(before);

            for conn in idle.drain(..) {
                if conn.is_idle_too_long(timeout).await {
                    bucket.decrement();
                } else {
                    kept.push(conn);
                }
            }

            let evicted = before - kept.len();
            if evicted > 0 {
                tracing::debug!(evicted, "evicted idle backend connections");
            }

            *idle = kept;
        }
    }
}
