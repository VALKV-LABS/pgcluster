# Component: Backup Coordinator (`backup_coordinator`)

## High-Level Function

The backup coordinator manages base backups across the cluster. Rather than running `pg_basebackup` against the primary (which adds load), it directs backup requests to a designated replica. It tracks backup state (in-progress, completed, failed), stores backup manifests, and can optionally stream backups to object storage (S3, GCS, Azure Blob).

This is a Milestone 2 component. Milestone 1 ships without it.

---

## Architecture

### Backup Strategy

```
Operator: pgcluster backup create --label "daily-2026-06-19"
  │
  ▼
1. Select backup source: prefer designated replica → fall back to primary
  │
  ▼
2. Call vk-agent::StartBackup() on source node
   vk-agent executes: pg_basebackup -h localhost -U replicator -F tar -z
  │
  ▼
3. Stream tar output chunks via vk-agent gRPC streaming RPC
   → pgcluster receives chunks → uploads to object storage OR writes locally
  │
  ▼
4. On completion: store BackupManifest in topology_store (via Raft)
  │
  ▼
5. Emit backup_complete event
```

### Backup Manifest

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    pub backup_id: String,           // UUID
    pub label: String,
    pub source_node: String,
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub timeline: u32,
    pub started_at: i64,
    pub completed_at: i64,
    pub size_bytes: u64,
    pub location: BackupLocation,    // Local path or object storage URI
    pub sha256: String,              // Checksum of the tar archive
    pub postgres_version: String,
    pub status: BackupStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackupLocation {
    Local { path: PathBuf },
    S3 { bucket: String, key: String },
    Gcs { bucket: String, object: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BackupStatus { InProgress, Completed, Failed }
```

### PITR Coordination

When a PITR restore is needed, the backup coordinator:
1. Lists available backups via `pgcluster backup list`
2. Selects the most recent backup before the target time
3. Downloads it from object storage to the target node
4. Configures `recovery_target_time` in `postgresql.auto.conf`
5. Restores WAL segments from archive to cover the gap to target time
6. Starts Postgres in recovery mode via vk-agent

---

## Detailed Implementation Plan

### 1. Module Layout

```
src/
  backup/
    mod.rs              # BackupCoordinator, create_backup(), list_backups()
    source_selector.rs  # Pick best backup source (prefer replica)
    stream.rs           # Stream pg_basebackup output via vk-agent gRPC
    storage.rs          # Object storage upload (S3/GCS/Azure via object_store crate)
    manifest.rs         # BackupManifest write/read, Raft storage
    pitr.rs             # PITR restore coordination
```

### 2. vk-agent Backup RPC Extensions

```protobuf
// Added to agent.proto for Milestone 2:
service AgentService {
  // ... existing RPCs ...
  rpc StartBackup(BackupRequest) returns (stream BackupChunk);
  rpc AbortBackup(AbortBackupRequest) returns (AbortBackupResponse);
}

message BackupRequest {
  string label = 1;
  bool   fast_checkpoint = 2;
  bool   compress = 3;
}

message BackupChunk {
  bytes  data = 1;       // tar chunk bytes (up to 64KB per message)
  uint64 bytes_sent = 2;
  bool   is_final = 3;
  uint64 start_lsn = 4;  // Set on first chunk
  uint64 end_lsn = 5;    // Set on final chunk
  uint32 timeline = 6;
}
```

### 3. BackupCoordinator

```rust
pub struct BackupCoordinator {
    topology: Arc<TopologyStore>,
    agents: Arc<AgentClients>,
    storage: Arc<dyn BackupStorage>,
    raft: Arc<PgClusterRaft>,
    config: BackupConfig,
}

pub struct BackupConfig {
    pub preferred_source: BackupSourcePreference,  // Replica | Primary | Any
    pub compress: bool,
    pub destination: BackupLocation,
}

impl BackupCoordinator {
    pub async fn create_backup(&self, label: &str) -> Result<BackupManifest> {
        // 1. Select source
        let source = self.select_source().await?;
        log::info!("Starting backup '{}' from node {}", label, source);

        // 2. Start backup via vk-agent streaming RPC
        let backup_id = uuid::Uuid::new_v4().to_string();
        let mut stream = self.agents.start_backup(&source, label, self.config.compress).await?;

        // 3. Stream to storage
        let mut hasher = sha2::Sha256::new();
        let mut size_bytes = 0u64;
        let mut start_lsn = 0u64;
        let mut end_lsn = 0u64;
        let mut timeline = 0u32;

        let writer = self.storage.begin_write(&backup_id).await?;
        while let Some(chunk) = stream.message().await? {
            if start_lsn == 0 { start_lsn = chunk.start_lsn; }
            hasher.update(&chunk.data);
            size_bytes += chunk.data.len() as u64;
            writer.write_all(&chunk.data).await?;
            if chunk.is_final {
                end_lsn = chunk.end_lsn;
                timeline = chunk.timeline;
            }
        }
        writer.finish().await?;

        let manifest = BackupManifest {
            backup_id: backup_id.clone(),
            label: label.to_string(),
            source_node: source,
            start_lsn, end_lsn, timeline,
            started_at: /* earlier timestamp */,
            completed_at: unix_now(),
            size_bytes,
            location: self.storage.location(&backup_id),
            sha256: hex::encode(hasher.finalize()),
            postgres_version: "15".into(),
            status: BackupStatus::Completed,
        };

        // 4. Persist manifest via Raft
        self.raft.propose(TopologyCommand::AddBackupManifest(manifest.clone())).await?;
        log::info!("Backup '{}' complete: {} bytes, LSN {:#X}..{:#X}", label, size_bytes, start_lsn, end_lsn);
        Ok(manifest)
    }

    fn select_source(&self) -> impl Future<Output = Result<String>> {
        let topology = self.topology.clone();
        async move {
            match self.config.preferred_source {
                BackupSourcePreference::Replica => {
                    topology.replica_addrs_within_lag(u64::MAX).into_iter().next()
                        .map(Ok)
                        .unwrap_or(Ok(topology.current_primary_addr().unwrap()))
                }
                BackupSourcePreference::Primary => Ok(topology.current_primary_addr().unwrap()),
                BackupSourcePreference::Any => Ok(topology.any_healthy_node()),
            }
        }
    }
}
```

### 4. CLI Extensions

```bash
pgcluster backup create --label "pre-upgrade"    # Create backup
pgcluster backup list                             # List all backups with status
pgcluster backup delete <backup-id>              # Delete backup + manifest
pgcluster backup restore <backup-id> --target pg3 --pitr "2026-06-19 08:00:00"
```

### 5. Integration Points

- `vk_agent` proto is extended with `StartBackup` streaming RPC (Milestone 2).
- `topology_store` stores `BackupManifest` list (added as a field in `ClusterTopology`).
- `raft_consensus` adds `TopologyCommand::AddBackupManifest` and `RemoveBackupManifest`.
- `rest_api` exposes `GET/POST /api/v1/backups` endpoints.
- `cli` adds `pgcluster backup` subcommand group.
- Object storage via the `object_store` crate (supports S3, GCS, Azure, local filesystem uniformly).
