# Component: Node Agent (`vk-agent`)

## High-Level Function

`vk-agent` is a small Rust binary that runs as a sidecar alongside each Postgres node. It is the bridge between pgcluster's control plane and the local Postgres instance. Its design is intentionally minimal: **it never makes decisions**. It only executes commands sent by the pgcluster Raft leader and reports local state back.

This separation is why pgcluster avoids split-brain: there is exactly one decision-maker (the Raft leader). All agents are pure executors.

---

## Architecture

### What vk-agent Does

```
pgcluster Raft leader
       │ gRPC (TLS)
       ▼
   vk-agent
       │
       ├── Execute: write promote.signal
       ├── Execute: write standby.signal
       ├── Execute: update primary_conninfo in postgresql.auto.conf
       ├── Execute: send SIGHUP to Postgres (config reload)
       ├── Execute: graceful Postgres stop (pg_ctl stop -m fast)
       ├── Report: pg_is_in_recovery(), LSNs, lag, connection count
       └── Report: local Postgres process health (PID alive?)
```

### What vk-agent Does NOT Do

- Does not decide whether to promote
- Does not watch for primary failure
- Does not participate in leader election
- Does not communicate with other vk-agents
- Does not modify `postgresql.conf` (only `postgresql.auto.conf`)

### Agent Heartbeat (Fencing Mechanism)

The agent requires a heartbeat from pgcluster at least every `agent_heartbeat_timeout` seconds (default 10s). If the heartbeat stops — meaning pgcluster lost quorum or is unreachable — the agent enters **safe mode**:

```
Heartbeat lost for > 10s:
  → Agent stops accepting promote commands
  → Logs: "pgcluster unreachable — entering safe mode"
  → Does NOT stop Postgres (read queries continue to work on standby)
  → Resumes normal operation once pgcluster reconnects
```

This prevents a scenario where a network-partitioned agent autonomously promotes a replica while the real primary is still running.

---

## Detailed Implementation Plan

### 1. Binary and Module Layout

```
vk-agent/
  src/
    main.rs          # Start gRPC server, connect to local Postgres
    server.rs        # AgentService gRPC implementation
    postgres.rs      # Local Postgres queries (pg_is_in_recovery, LSNs)
    files.rs         # Signal file writes, postgresql.auto.conf update
    process.rs       # pg_ctl stop, SIGHUP via kill()
    heartbeat.rs     # Heartbeat tracker, safe mode enforcement
    config.rs        # Agent config (data_dir, postgres socket, pgcluster addr)
```

### 2. gRPC Service Definition

```protobuf
// proto/agent.proto
syntax = "proto3";
package pgcluster.agent;

service AgentService {
  // Promote this node to primary
  rpc Promote(PromoteRequest) returns (PromoteResponse);
  // Demote this node to replica of a new primary
  rpc Demote(DemoteRequest) returns (DemoteResponse);
  // Get current node status
  rpc GetStatus(StatusRequest) returns (StatusResponse);
  // Heartbeat from pgcluster leader (keeps agent in active mode)
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
  // Gracefully stop Postgres (used before node maintenance)
  rpc StopPostgres(StopRequest) returns (StopResponse);
  // Reload Postgres config (SIGHUP)
  rpc ReloadConfig(ReloadRequest) returns (ReloadResponse);
}

message PromoteRequest {
  uint64 expected_lsn = 1;   // Sanity check: agent verifies its LSN is >= this
}
message PromoteResponse {
  bool success = 1;
  string error = 2;
  uint64 promoted_at_lsn = 3;
  uint32 new_timeline = 4;
}

message DemoteRequest {
  string new_primary_conninfo = 1;
  string slot_name = 2;
}
message DemoteResponse {
  bool success = 1;
  string error = 2;
}

message StatusResponse {
  bool is_in_recovery = 1;
  uint64 received_lsn = 2;
  uint64 replayed_lsn = 3;
  uint64 sent_lsn = 4;        // Primary only
  uint32 timeline = 5;
  string postgres_version = 6;
  uint32 active_connections = 7;
  bool postgres_running = 8;
  repeated ReplicaStatus replicas = 9;  // Primary only: pg_stat_replication
}

message ReplicaStatus {
  string application_name = 1;
  uint64 flush_lsn = 2;
  uint64 replay_lsn = 3;
  int64 replay_lag_us = 4;
}
```

### 3. Promote Implementation

```rust
async fn promote(&self, req: PromoteRequest) -> Result<PromoteResponse> {
    // 1. Sanity check: our LSN should be >= expected
    let our_lsn = self.postgres.get_replay_lsn().await?;
    if our_lsn < req.expected_lsn {
        return Ok(PromoteResponse {
            success: false,
            error: format!("LSN too low: have {:X}/{:08X}, need {:X}/{:08X}",
                our_lsn >> 32, our_lsn as u32,
                req.expected_lsn >> 32, req.expected_lsn as u32),
            ..Default::default()
        });
    }

    // 2. Write promote.signal to data directory
    let signal = self.config.data_dir.join("promote.signal");
    tokio::fs::write(&signal, b"").await?;
    log::info!("promote.signal written");

    // 3. Wait for Postgres to complete promotion (pg_is_in_recovery() → false)
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if !self.postgres.is_in_recovery().await? {
            break;
        }
        if Instant::now() > deadline {
            return Ok(PromoteResponse { success: false, error: "promotion timeout".into(), ..Default::default() });
        }
    }

    let new_lsn = self.postgres.get_current_lsn().await?;
    let new_tli = self.postgres.get_timeline().await?;
    log::info!("Promotion complete: LSN={:X}/{:08X} TLI={}", new_lsn >> 32, new_lsn as u32, new_tli);

    Ok(PromoteResponse { success: true, promoted_at_lsn: new_lsn, new_timeline: new_tli, ..Default::default() })
}
```

### 4. Demote Implementation

```rust
async fn demote(&self, req: DemoteRequest) -> Result<DemoteResponse> {
    // 1. Update primary_conninfo in postgresql.auto.conf
    self.files.update_auto_conf("primary_conninfo", &req.new_primary_conninfo).await?;
    self.files.update_auto_conf("primary_slot_name", &req.slot_name).await?;
    self.files.update_auto_conf("recovery_target_timeline", "latest").await?;

    // 2. Write standby.signal
    let signal = self.config.data_dir.join("standby.signal");
    tokio::fs::write(&signal, b"").await?;

    // 3. Restart Postgres in standby mode
    //    (Postgres must restart to read new recovery config)
    self.process.stop_fast().await?;
    self.process.start().await?;

    // 4. Wait for streaming to begin
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        if self.postgres.is_in_recovery().await.unwrap_or(false) {
            break;
        }
        if Instant::now() > deadline {
            return Ok(DemoteResponse { success: false, error: "demote: standby streaming timeout".into() });
        }
    }

    log::info!("Demotion complete — streaming from {}", req.new_primary_conninfo);
    Ok(DemoteResponse { success: true, ..Default::default() })
}
```

### 5. Local Postgres Queries

```rust
pub struct LocalPostgres {
    pool: PgPool,  // sqlx connection pool to local Postgres via Unix socket
}

impl LocalPostgres {
    pub async fn is_in_recovery(&self) -> Result<bool> {
        let row: (bool,) = sqlx::query_as("SELECT pg_is_in_recovery()")
            .fetch_one(&self.pool).await?;
        Ok(row.0)
    }

    pub async fn get_replay_lsn(&self) -> Result<u64> {
        let row: (Option<String>,) = sqlx::query_as(
            "SELECT pg_last_wal_replay_lsn()::text"
        ).fetch_one(&self.pool).await?;
        parse_lsn(&row.0.unwrap_or_default())
    }

    pub async fn get_current_lsn(&self) -> Result<u64> {
        let row: (String,) = sqlx::query_as("SELECT pg_current_wal_lsn()::text")
            .fetch_one(&self.pool).await?;
        parse_lsn(&row.0)
    }

    pub async fn get_timeline(&self) -> Result<u32> {
        let row: (i64,) = sqlx::query_as(
            "SELECT timeline_id FROM pg_control_checkpoint()"
        ).fetch_one(&self.pool).await?;
        Ok(row.0 as u32)
    }

    pub async fn get_stat_replication(&self) -> Result<Vec<ReplicaStatus>> {
        sqlx::query_as::<_, (String, Option<String>, Option<String>, Option<i64>)>(
            "SELECT application_name, flush_lsn::text, replay_lsn::text,
                    EXTRACT(EPOCH FROM replay_lag)::bigint * 1000000
             FROM pg_stat_replication"
        )
        .fetch_all(&self.pool).await
        .map(|rows| rows.into_iter().map(|(name, flush, replay, lag)| ReplicaStatus {
            application_name: name,
            flush_lsn: parse_lsn(&flush.unwrap_or_default()).unwrap_or(0),
            replay_lsn: parse_lsn(&replay.unwrap_or_default()).unwrap_or(0),
            replay_lag_us: lag.unwrap_or(0),
        }).collect())
    }
}
```

### 6. Heartbeat Safe Mode

```rust
pub struct HeartbeatTracker {
    last_seen: Arc<Mutex<Instant>>,
    timeout: Duration,
    in_safe_mode: Arc<AtomicBool>,
}

impl HeartbeatTracker {
    pub fn touch(&self) {
        *self.last_seen.lock().unwrap() = Instant::now();
        self.in_safe_mode.store(false, Ordering::SeqCst);
    }

    pub fn spawn_watchdog(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let elapsed = self.last_seen.lock().unwrap().elapsed();
                if elapsed > self.timeout && !self.in_safe_mode.load(Ordering::SeqCst) {
                    self.in_safe_mode.store(true, Ordering::SeqCst);
                    log::warn!("pgcluster heartbeat lost ({:?}) — entering safe mode", elapsed);
                }
            }
        });
    }

    pub fn is_safe_mode(&self) -> bool {
        self.in_safe_mode.load(Ordering::SeqCst)
    }
}

// In AgentService::promote():
if self.heartbeat.is_safe_mode() {
    return Ok(PromoteResponse {
        success: false,
        error: "agent in safe mode — pgcluster heartbeat lost".into(),
        ..Default::default()
    });
}
```

### 7. Integration Points

- pgcluster Raft leader calls `AgentService::Promote()` during failover.
- pgcluster calls `AgentService::Demote()` to re-point a node to a new primary.
- pgcluster's `node_monitor` calls `AgentService::GetStatus()` every `health_check_interval_ms`.
- pgcluster calls `AgentService::Heartbeat()` every `heartbeat_interval_ms`.
- `vk-agent` connects to local Postgres via Unix domain socket (no network, no auth overhead).
