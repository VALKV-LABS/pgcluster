# Component: Connection Pool (`connection_pool`)

## High-Level Function

The connection pool maintains a set of pre-warmed Postgres backend connections per `(database, user, backend_addr)` triple. When a proxy client needs a backend, it borrows a connection from the pool instead of opening a new TCP connection and re-authenticating. This eliminates per-query TCP + TLS + Postgres auth overhead, which can add 5–20ms per connection under load.

---

## Architecture

### Pool Structure

```
ConnectionPool (global)
  │
  ├── PoolKey("mydb", "alice", "10.0.0.1:5432") → Pool[0..24]
  ├── PoolKey("mydb", "alice", "10.0.0.2:5432") → Pool[0..24]
  ├── PoolKey("mydb", "bob",   "10.0.0.1:5432") → Pool[0..24]
  └── ...
```

Each `Pool` is a fixed-size set of `BackendConnection` objects. A connection can be:
- **Idle** — available for the next client
- **In use** — borrowed by a proxy connection
- **Broken** — TCP error detected; will be replaced on next acquisition attempt

### Pool Modes

| Mode | Behavior |
|------|----------|
| **Session mode** | One backend connection per client session for its entire lifetime (like PgBouncer session mode) |
| **Transaction mode** | Backend connection released back to pool after each COMMIT/ROLLBACK (like PgBouncer transaction mode — higher multiplexing, most applications work correctly) |
| **Statement mode** | Released after each statement — only safe for simple queries with no state |

Default: **transaction mode** (matches PgBouncer's recommended default for most apps).

### Connection Lifecycle

```
Client needs backend:
  pool.acquire(db, user, backend_addr)
    │
    ├── Idle connection available? → return it (mark In Use)
    │
    └── No idle connections:
          total_connections < max_per_db_user?
            → open new Postgres connection, authenticate, return it
          else:
            → wait up to connect_timeout_seconds for one to become idle
            → timeout → error to client ("too many connections")

Client releases connection:
  pool.release(conn)
    │
    ├── conn is healthy (no error) → return to idle queue
    └── conn is broken → discard, decrement count
```

---

## Detailed Implementation Plan

### 1. Module Layout

```
src/
  pool/
    mod.rs            # ConnectionPool, PoolKey, acquire/release
    backend_conn.rs   # BackendConnection — one pooled Postgres connection
    authenticator.rs  # Postgres auth handshake for new backend connections
    health_check.rs   # Idle connection keepalive / validity check
```

### 2. ConnectionPool

```rust
pub struct ConnectionPool {
    pools: DashMap<PoolKey, Arc<SinglePool>>,
    config: PoolConfig,
}

#[derive(Hash, Eq, PartialEq, Clone)]
pub struct PoolKey {
    pub database: String,
    pub user: String,
    pub backend_addr: String,
}

pub struct PoolConfig {
    pub max_per_db_user: usize,       // default: 25
    pub idle_timeout_secs: u64,       // default: 600
    pub connect_timeout_secs: u64,    // default: 5
    pub keepalive_interval_secs: u64, // default: 60
}

pub struct SinglePool {
    idle: Mutex<VecDeque<BackendConnection>>,
    total: AtomicUsize,
    max: usize,
    notify: Notify,  // Wake waiters when connection released
}

impl ConnectionPool {
    pub async fn acquire(&self, key: &PoolKey) -> Result<BackendConnection> {
        let pool = self.pools.entry(key.clone())
            .or_insert_with(|| Arc::new(SinglePool::new(self.config.max_per_db_user)))
            .clone();

        let deadline = Instant::now() + Duration::from_secs(self.config.connect_timeout_secs);

        loop {
            // Fast path: take idle connection
            {
                let mut idle = pool.idle.lock().await;
                while let Some(conn) = idle.pop_front() {
                    if conn.is_alive().await { return Ok(conn); }
                    pool.total.fetch_sub(1, Ordering::SeqCst);
                }
            }

            // Open new connection if under limit
            let total = pool.total.load(Ordering::SeqCst);
            if total < pool.max {
                if pool.total.compare_exchange(total, total + 1, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                    match self.open_backend_connection(key).await {
                        Ok(conn) => return Ok(conn),
                        Err(e) => {
                            pool.total.fetch_sub(1, Ordering::SeqCst);
                            return Err(e);
                        }
                    }
                }
            }

            // Wait for a connection to be released
            if Instant::now() > deadline {
                return Err(anyhow::anyhow!("connection pool exhausted for {}/{}", key.database, key.user));
            }
            tokio::time::timeout(
                deadline.saturating_duration_since(Instant::now()),
                pool.notify.notified()
            ).await.ok();
        }
    }

    pub async fn release(&self, key: &PoolKey, conn: BackendConnection) {
        if let Some(pool) = self.pools.get(key) {
            if conn.is_alive().await && !conn.in_transaction() {
                pool.idle.lock().await.push_back(conn);
                pool.notify.notify_one();
            } else {
                pool.total.fetch_sub(1, Ordering::SeqCst);
            }
        }
    }

    async fn open_backend_connection(&self, key: &PoolKey) -> Result<BackendConnection> {
        let stream = tokio::time::timeout(
            Duration::from_secs(self.config.connect_timeout_secs),
            TcpStream::connect(&key.backend_addr)
        ).await??;

        let conn = BackendConnection::handshake(stream, &key.database, &key.user).await?;
        Ok(conn)
    }
}
```

### 3. BackendConnection

```rust
pub struct BackendConnection {
    stream: TcpStream,
    in_transaction: bool,
    created_at: Instant,
    last_used: Instant,
}

impl BackendConnection {
    pub async fn handshake(stream: TcpStream, database: &str, user: &str) -> Result<Self> {
        // Send StartupMessage, handle auth (pass-through trust or md5/scram)
        send_startup_message(&stream, database, user).await?;
        handle_auth_exchange(&stream).await?;
        // Consume ParameterStatus and BackendKeyData messages
        consume_startup_messages(&stream).await?;
        Ok(BackendConnection { stream, in_transaction: false, created_at: Instant::now(), last_used: Instant::now() })
    }

    pub async fn is_alive(&self) -> bool {
        // Non-blocking peek: if the connection has been closed by the backend,
        // a zero-byte read will be returned immediately.
        let mut buf = [0u8; 1];
        match self.stream.try_read(&mut buf) {
            Ok(0) => false,  // EOF
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => true,  // No data = alive
            _ => false,
        }
    }

    pub fn is_idle_too_long(&self, timeout_secs: u64) -> bool {
        self.last_used.elapsed().as_secs() > timeout_secs
    }
}
```

### 4. Topology Change — Pool Drain

When the primary changes (failover or switchover), the pool for the old primary address must be drained. In-use connections complete their current transaction; idle connections are discarded immediately:

```rust
pub async fn drain_backend(&self, backend_addr: &str) {
    let to_remove: Vec<_> = self.pools.iter()
        .filter(|entry| entry.key().backend_addr == backend_addr)
        .map(|entry| entry.key().clone())
        .collect();

    for key in to_remove {
        if let Some((_, pool)) = self.pools.remove(&key) {
            // Discard all idle connections to this backend
            let mut idle = pool.idle.lock().await;
            idle.clear();
            // In-use connections will be discarded on release (is_alive check)
        }
    }
}
```

### 5. Idle Connection Reaper

A background task periodically removes connections that have been idle too long:

```rust
pub async fn idle_reaper(pool: Arc<ConnectionPool>, config: PoolConfig) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    loop {
        interval.tick().await;
        for entry in pool.pools.iter() {
            let mut idle = entry.idle.lock().await;
            idle.retain(|conn| !conn.is_idle_too_long(config.idle_timeout_secs));
        }
    }
}
```

### 6. Integration Points

- `proxy_layer` calls `pool.acquire()` when routing a client to a backend and `pool.release()` on transaction completion.
- `failover_engine` and `switchover_engine` call `pool.drain_backend(old_primary_addr)` after topology change.
- `topology_store` change events (via Raft state machine watch) trigger pool drain when primary changes.
