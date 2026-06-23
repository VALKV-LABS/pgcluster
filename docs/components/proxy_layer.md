# Component: Proxy Layer (`proxy_layer`)

## High-Level Function

The proxy layer is the client-facing entry point of pgcluster. It speaks the PostgreSQL wire protocol natively, so clients connect to pgcluster exactly as they would to a Postgres server — no driver changes, no special connection strings. The proxy routes write transactions to the current primary and read queries to the replica pool, with lag-aware load balancing.

The proxy runs on all pgcluster instances (not just the Raft leader), so clients can connect to any instance. Each proxy reads the current topology from its local Raft state machine replica — no network hop needed for routing decisions.

---

## Architecture

### Routing Logic

```
Client connects → proxy
  │
  ├── SSL negotiation (if requested)
  ├── Startup / auth handshake → pass through to backend
  │
  └── Per-message routing:
        Track transaction state:
          Idle       → next statement determines routing
          InTxn      → sticky to backend chosen at BEGIN
          Failed     → sticky until ROLLBACK
        
        On BEGIN (or first statement in auto-commit):
          if read-only hint (BEGIN READ ONLY, SET TRANSACTION READ ONLY):
            → route to least-lagged replica in pool
          else:
            → route to primary
        
        Statements during InTxn:
          → sticky to same backend (all in one connection)
```

### Session Affinity

Once a connection is assigned to a backend, it stays there until:
- Transaction commits (`COMMIT` / `ROLLBACK`) — then released back to pool
- Connection closes

`SET` commands and `LISTEN` / `NOTIFY` require session affinity (they have per-connection state). These always go to primary.

### Lag-Aware Read Routing

Replicas are ranked by replication lag (bytes behind primary). Replicas exceeding `max_replica_lag_bytes` are excluded from the read pool:

```rust
fn select_read_replica(
    replicas: &[ReplicaBackend],
    max_lag_bytes: u64,
    current_primary_lsn: u64,
) -> Option<&ReplicaBackend> {
    replicas.iter()
        .filter(|r| r.is_connected())
        .filter(|r| current_primary_lsn.saturating_sub(r.flush_lsn) <= max_lag_bytes)
        .min_by_key(|r| r.active_connection_count)  // least loaded
}
```

---

## Detailed Implementation Plan

### 1. Module Layout

```
src/
  proxy/
    mod.rs            # ProxyServer — listen loop, spawn connection handlers
    connection.rs     # ProxyConnection — per-client state machine
    router.rs         # Route decisions: which backend?
    backend.rs        # BackendConnection — pooled Postgres connection
    pool.rs           # ConnectionPool — per-(db, user) pool
    protocol.rs       # Postgres wire protocol framing (read/write packets)
    session.rs        # Session state: txn mode, SET vars, prepared stmts
    ssl.rs            # TLS upgrade for client connections
```

### 2. Connection State Machine

```rust
pub struct ProxyConnection {
    client: TcpStream,
    backend: Option<Arc<BackendConnection>>,
    session: SessionState,
    router: Arc<Router>,
    pool: Arc<ConnectionPool>,
}

#[derive(Debug, PartialEq)]
pub enum TxnState { Idle, InTransaction, Failed }

pub struct SessionState {
    pub txn_state: TxnState,
    pub is_read_only: bool,
    pub database: String,
    pub user: String,
    pub application_name: String,
    pub set_vars: HashMap<String, String>,
}

impl ProxyConnection {
    pub async fn run(&mut self) -> Result<()> {
        // 1. Handle SSL upgrade
        self.handle_ssl().await?;
        // 2. Startup message + auth passthrough
        let (db, user) = self.handle_startup().await?;
        self.session.database = db;
        self.session.user = user;

        // 3. Message routing loop
        loop {
            let msg = self.read_client_message().await?;
            match &msg {
                FrontendMessage::Query(sql) => self.route_simple_query(sql).await?,
                FrontendMessage::Parse { .. } => self.route_extended_query(msg).await?,
                FrontendMessage::Terminate => break,
                _ => self.forward_to_backend(msg).await?,
            }
        }
        Ok(())
    }

    async fn route_simple_query(&mut self, sql: &str) -> Result<()> {
        if self.session.txn_state == TxnState::Idle {
            let intent = classify_statement(sql);
            self.backend = Some(self.pool.acquire(
                &self.session.database,
                &self.session.user,
                intent,
                &self.router,
            ).await?);
        }
        self.forward_to_backend(FrontendMessage::Query(sql.to_string())).await?;
        self.update_txn_state_from_ready_for_query().await
    }

    async fn update_txn_state_from_ready_for_query(&mut self) -> Result<()> {
        // ReadyForQuery 'Z' carries txn status byte: 'I', 'T', or 'E'
        let rfq = self.read_backend_until_ready_for_query().await?;
        self.session.txn_state = match rfq.status {
            b'I' => { self.release_backend(); TxnState::Idle }
            b'T' => TxnState::InTransaction,
            b'E' => TxnState::Failed,
            _    => TxnState::Idle,
        };
        Ok(())
    }
}
```

### 3. Statement Classification

```rust
pub enum StatementIntent { Write, Read, SetLocal }

pub fn classify_statement(sql: &str) -> StatementIntent {
    let trimmed = sql.trim_start().to_uppercase();
    if trimmed.starts_with("SELECT") || trimmed.starts_with("TABLE") || trimmed.starts_with("VALUES") {
        StatementIntent::Read
    } else if trimmed.starts_with("SET") || trimmed.starts_with("BEGIN READ ONLY") {
        StatementIntent::SetLocal
    } else {
        // INSERT, UPDATE, DELETE, BEGIN, DDL, COPY, etc.
        StatementIntent::Write
    }
}
```

### 4. Router

```rust
pub struct Router {
    topology: Arc<RwLock<ClusterTopology>>,
    node_configs: HashMap<String, NodeConfig>,
    config: ProxyConfig,
}

impl Router {
    pub fn primary_addr(&self) -> Option<String> {
        let topology = self.topology.blocking_read();
        let primary_id = &topology.primary_node_id;
        self.node_configs.get(primary_id).map(|c| c.postgres_addr.clone())
    }

    pub fn best_replica_addr(&self, current_primary_lsn: u64) -> Option<String> {
        let topology = self.topology.blocking_read();
        let max_lag = self.config.read_routing.max_replica_lag_bytes;

        topology.node_roles.iter()
            .filter(|(_, role)| **role == NodeRole::Replica)
            .filter(|(id, _)| {
                let lag = current_primary_lsn
                    .saturating_sub(topology.last_flush_lsns.get(*id).copied().unwrap_or(0));
                lag <= max_lag
            })
            .map(|(id, _)| self.node_configs[id].postgres_addr.clone())
            .next()
    }
}
```

### 5. Admin Database (port 5433)

pgcluster exposes a virtual admin database on a separate port. Clients can connect to it and run:

```sql
-- Cluster status
SELECT * FROM pgcluster.nodes;

-- Trigger switchover
SELECT pgcluster.switchover('pg2');

-- View replication lag
SELECT * FROM pgcluster.replication_lag;

-- Add a node
SELECT pgcluster.add_node('pg4', '10.0.0.4:7001', '10.0.0.4:5432', 70);
```

This is a custom virtual schema served entirely by pgcluster — the queries never reach a Postgres backend.

### 6. Integration Points

- Reads `ClusterTopology` from `raft_consensus` state machine (local, no network hop).
- `connection_pool` manages per-(database, user) pools to each backend.
- On topology change (Raft state machine update), in-flight connections to old primary are gracefully drained; new connections route to new primary immediately.
- `tls_manager` provides the TLS acceptor for client connections.
- `ha_status` HTTP server shares the same port binding infrastructure but runs on a different port (:8008).
