# Component: pgrust Native Integration (`pgrust_native`)

## High-Level Function

When pgcluster manages pgrust nodes (as opposed to vanilla Postgres), it can eliminate the `vk-agent` sidecar entirely. pgrust exposes a dedicated native control port that speaks a lightweight binary protocol. This unlocks sub-500ms failover and deeper integration (live config push, schema migration coordination, streaming backup) that the vk-agent approach cannot provide.

This is a **Milestone 3** component. Milestone 1 and 2 use vk-agent.

---

## Architecture

### Why Agent-Free is Better

| Capability | vk-agent approach | pgrust native |
|------------|------------------|---------------|
| Promote | Write file, wait for Postgres to detect it | Direct in-process call |
| Failover detection | vk-agent polls Postgres every 500ms | pgrust pushes events to pgcluster |
| Failover time | ~3–5s (file + detect + restart) | < 500ms (in-process promote) |
| Config update | Update file + SIGHUP + wait for reload | Push config live, applied instantly |
| Backup | Run pg_basebackup subprocess | Stream directly from pgrust storage |
| Schema migration | External tool coordination | Native DDL lock coordination |

### Native Control Port

pgrust listens on a dedicated control port (default `:5433`, separate from the Postgres wire protocol port `:5432`). The protocol is a lightweight binary framing:

```
+-----+--------+----------+------------------+
| Tag | Len    | Seq      | Payload          |
| u8  | u32 BE | u32 BE   | variable         |
+-----+--------+----------+------------------+
```

Tags:
```
0x01  StatusRequest    → StatusResponse
0x02  PromoteRequest   → PromoteResponse
0x03  DemoteRequest    → DemoteResponse
0x04  PushConfig       → ConfigAck
0x05  StartBackup      → (streaming BackupChunk frames)
0x06  SubscribeEvents  → (streaming EventFrame frames)
0x07  Heartbeat        → HeartbeatAck
```

### Event Push (Replaces Polling)

Instead of pgcluster polling vk-agent every 500ms, pgrust pushes events to pgcluster over the `SubscribeEvents` stream:

```
pgcluster opens SubscribeEvents stream → pgrust sends:
  EventFrame { type: LsnAdvance, flush_lsn: 0x4A001F8 }   (on every WAL flush)
  EventFrame { type: ReplicaConnected, name: "replica1" }
  EventFrame { type: ReplicaDisconnected, name: "replica1" }
  EventFrame { type: CheckpointComplete, lsn: 0x4A00000 }
  EventFrame { type: SlotAdvanced, slot: "pgcluster_pg2", lsn: 0x49FF800 }
```

This makes pgcluster aware of LSN advances in real time rather than at polling intervals. Failover candidate selection immediately has current LSN data.

---

## Detailed Implementation Plan

### 1. Module Layout (in pgcluster)

```
src/
  pgrust_native/
    mod.rs          # PgrustrNativeClient — connects to pgrust control port
    protocol.rs     # Binary framing, message types, encode/decode
    client.rs       # Promote, Demote, GetStatus, PushConfig calls
    event_stream.rs # SubscribeEvents → feed into topology_store + node_monitor
    backup_stream.rs # StartBackup streaming → backup_coordinator
```

### 2. Protocol Types

```rust
#[derive(Debug)]
pub enum ControlMessage {
    StatusRequest,
    StatusResponse(NodeStatusPayload),
    PromoteRequest { expected_lsn: u64 },
    PromoteResponse { success: bool, promoted_lsn: u64, new_tli: u32, error: String },
    DemoteRequest { primary_conninfo: String, slot_name: String },
    DemoteResponse { success: bool, error: String },
    PushConfig(ConfigPatch),
    ConfigAck { applied: bool },
    SubscribeEvents,
    EventFrame(ClusterEvent),
    StartBackup { label: String, compress: bool },
    BackupChunk { data: Vec<u8>, start_lsn: u64, end_lsn: u64, is_final: bool },
    Heartbeat { leader_id: u64, term: u64 },
    HeartbeatAck,
}

#[derive(Debug)]
pub struct NodeStatusPayload {
    pub is_primary: bool,
    pub current_lsn: u64,
    pub flush_lsn: u64,
    pub timeline: u32,
    pub active_connections: u32,
    pub replicas: Vec<ReplicaStatus>,
}

#[derive(Debug)]
pub enum ClusterEvent {
    LsnAdvance { flush_lsn: u64 },
    ReplicaConnected { application_name: String, flush_lsn: u64 },
    ReplicaDisconnected { application_name: String },
    CheckpointComplete { lsn: u64 },
    SlotAdvanced { slot_name: String, flush_lsn: u64 },
    ServerError { message: String },
}
```

### 3. pgrust Control Server (pgrust side)

pgrust implements the server side of this protocol in its `pg_server` module:

```rust
pub async fn run_control_server(
    port: u16,
    promotion_mgr: Arc<PromotionManager>,
    wal: Arc<WalEngine>,
    sender_registry: Arc<WalSenderRegistry>,
    tls: Arc<TlsConfig>,
) {
    let listener = TcpListener::bind(("0.0.0.0", port)).await.unwrap();
    // ... accept + TLS upgrade + dispatch ControlMessage frames
}

async fn handle_control_message(msg: ControlMessage, ctx: &ControlContext) -> ControlMessage {
    match msg {
        ControlMessage::PromoteRequest { expected_lsn } => {
            // Direct in-process promote — no file I/O
            let result = ctx.promotion_mgr.run_promotion_sequence().await;
            ControlMessage::PromoteResponse {
                success: result.is_ok(),
                promoted_lsn: ctx.wal.current_lsn(),
                new_tli: ctx.timeline.current_tli(),
                error: result.err().map(|e| e.to_string()).unwrap_or_default(),
            }
        }
        ControlMessage::SubscribeEvents => {
            // Start streaming events — never returns until connection closes
            stream_events(ctx).await
        }
        // ...
    }
}
```

### 4. Failover Time Improvement

With vk-agent:
```
health check fails (500ms × 3) = 1500ms detection
+ write promote.signal = ~1ms
+ Postgres detects signal (polls every 1s) = up to 1000ms
+ pg_is_in_recovery() polling (100ms × up to 10) = up to 1000ms
Total: ~3500ms
```

With pgrust native:
```
event push (LsnAdvance stops) = detected in < 150ms (Raft heartbeat)
+ send PromoteRequest = ~1ms RTT
+ in-process promote = ~50ms (stop replay, fork timeline, write checkpoint)
+ PromoteResponse received = ~52ms
Total: ~200-500ms
```

### 5. ConfigPatch — Live Config Push

```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct ConfigPatch {
    /// Key-value pairs to apply to pgrust's running config
    pub values: HashMap<String, serde_json::Value>,
}
// Examples:
// { "max_connections": 200 }
// { "synchronous_commit": "remote_write" }
// { "synchronous_standby_names": "ANY 1 (replica1, replica2)" }
```

pgcluster pushes config changes to pgrust when topology changes (e.g., after failover, update `synchronous_standby_names` to reflect the new replica set).

### 6. Detection: pgrust vs vanilla Postgres

pgcluster auto-detects whether a node is pgrust:

```rust
pub async fn probe_node(postgres_addr: &str) -> NodeType {
    // Try control port first
    if TcpStream::connect(format!("{}:5433", host_of(postgres_addr))).await.is_ok() {
        NodeType::Pgrust
    } else {
        NodeType::VanillaPostgres
    }
}
```

If the control port is reachable: use `pgrust_native`. Otherwise: use `vk_agent`.

### 7. Integration Points

- `node_monitor` subscribes to pgrust event stream instead of polling vk-agent.
- `failover_engine` calls `pgrust_native_client.promote()` instead of `agent.promote()`.
- `switchover_engine` calls `pgrust_native_client.promote()` and `pgrust_native_client.demote()`.
- `backup_coordinator` calls `pgrust_native_client.start_backup()` for streaming backup.
- `config_manager` pushes config patches via `pgrust_native_client.push_config()` after topology changes.
- pgcluster selects `pgrust_native` vs `vk_agent` per-node at startup based on control port probe.
