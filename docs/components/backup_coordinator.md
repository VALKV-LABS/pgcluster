# Component: Backup Coordinator (`backup`)

## High-Level Function

The backup coordinator manages automated, policy-driven base backups of the
PostgreSQL cluster.  It runs as a background task on every pgcluster node, but
is active only on the Raft leader to prevent duplicate uploads.

Backups are driven by a **schedule policy** (daily / weekly / monthly with
per-tier retention counts), consistent with how managed PostgreSQL services
(AWS RDS, GCP Cloud SQL, Azure Database for PostgreSQL) handle automated
backups.  An S3-compatible object store is **mandatory** — pgcluster refuses to
start if `[backup]` is configured without a valid `[backup.s3]` section.

---

## Architecture

### Execution Model

```
Raft leader (pgcluster)
  │
  ├── BackupScheduler wakes every hour
  │     └── checks each frequency tier (daily / weekly / monthly)
  │           └── if due: select source node → run pg_basebackup → upload to S3
  │                                           → AddBackupManifest to Raft
  │                                           → prune old manifests (S3 + Raft)
  │
  └── REST API (any node, leader-forwarded)
        POST /api/backups  → manual backup (returns 202 Accepted)
        GET  /api/backups  → list manifests (all nodes read from local Raft state)
        DELETE /api/backups/:id → delete backup (S3 objects + Raft manifest)
```

### Backup Source Selection

To minimise primary write load, pgcluster prefers running `pg_basebackup`
against the replica with the smallest replication lag.  If no healthy replica
is available, or `prefer_replica = false`, the primary is used as a fallback.

```
if prefer_replica:
    pick replica with min(replica_lag_bytes)   → falls back to primary
else:
    primary
```

### Backup Execution

pgcluster invokes `pg_basebackup` as a subprocess, connecting to the selected
node's Postgres address over the standard replication protocol:

```bash
pg_basebackup \
  -h <host> -p <port> -U <repl_user> \
  -F tar -z --wal-method=stream \
  -D <tmpdir> --no-password
```

`PGPASSWORD` is passed via the environment.  Output is:
- `base.tar.gz`   — the PGDATA tar archive (compressed)
- `pg_wal.tar.gz` — WAL segments needed for a consistent restore

Both files are uploaded to S3 under:
```
s3://<bucket>/<prefix>/<frequency>/<backup-uuid>/base.tar.gz
s3://<bucket>/<prefix>/<frequency>/<backup-uuid>/pg_wal.tar.gz
```

> **Requirement**: `pg_basebackup` must be installed on the pgcluster host
> (available in the `postgresql-client` OS package).

### Manifest Tracking

Each completed backup produces a `BackupManifest` written to Raft via
`TopologyCommand::AddBackupManifest`.  Because manifests live in Raft state,
they survive leader re-elections without re-triggering backups.

```rust
pub struct BackupManifest {
    pub backup_id: String,      // UUID v4
    pub label: String,          // "daily-2026-07-04" or operator label
    pub frequency: String,      // "daily" | "weekly" | "monthly" | "manual"
    pub source_node: String,    // node_id that served pg_basebackup
    pub started_at: i64,        // Unix seconds
    pub completed_at: i64,
    pub size_bytes: u64,        // total bytes uploaded
    pub s3_uri: String,         // "s3://bucket/prefix/frequency/backup-id/"
    pub status: BackupStatus,   // Completed | Failed
}
```

### Retention / Pruning

After each successful backup, the scheduler counts completed manifests for that
frequency tier.  If the count exceeds `retain`, the oldest entries are pruned:
S3 objects are deleted first, then the manifest is removed from Raft via
`TopologyCommand::RemoveBackupManifest`.

---

## Configuration

```toml
[backup]
enabled        = true           # set false to disable without removing config
prefer_replica = true           # run pg_basebackup on a replica when possible

[backup.s3]
bucket   = "my-company-pgbackups"
prefix   = "prod-cluster"       # all keys are under this prefix
region   = "us-east-1"          # omit to read AWS_REGION env var
# endpoint = "https://minio.internal"  # MinIO / Ceph / Cloudflare R2
# path_style = true                     # required for MinIO

[[backup.schedule]]
frequency = "daily"
retain    = 7       # keep the 7 most-recent daily backups

[[backup.schedule]]
frequency = "weekly"
retain    = 4

[[backup.schedule]]
frequency = "monthly"
retain    = 3
```

### S3 Credentials

Credentials are **not** stored in the config file.  They are read from the
environment at startup:

| Env var | Purpose |
|---|---|
| `AWS_ACCESS_KEY_ID` | Access key (static credentials) |
| `AWS_SECRET_ACCESS_KEY` | Secret key (static credentials) |
| `AWS_SESSION_TOKEN` | Session token (temporary credentials) |
| IAM instance role / IRSA | No env vars needed |

### Schedule Timing

The scheduler wakes every hour and checks whether each tier is due:

| Frequency | Triggers when last backup was… |
|---|---|
| `daily` | > 23 hours ago (1-hour slack prevents drift) |
| `weekly` | > 6 days 23 hours ago |
| `monthly` | > 28 days ago |

---

## REST API

All endpoints require API key authentication (same as other `/api/*` routes).

### List backups

```http
GET /api/backups
GET /api/backups?frequency=daily
```

Response:
```json
{
  "backups": [
    {
      "backup_id": "550e8400-e29b-41d4-a716-446655440000",
      "label": "daily-2026-07-04",
      "frequency": "daily",
      "source_node": "pg2",
      "started_at": 1751673600,
      "completed_at": 1751673900,
      "size_bytes": 2147483648,
      "s3_uri": "s3://my-bucket/prod-cluster/daily/550e8400-.../",
      "status": "completed"
    }
  ]
}
```

### Trigger a manual backup

```http
POST /api/backups
Content-Type: application/json

{
  "label": "pre-upgrade-2026-07-04",
  "frequency": "manual"
}
```

Returns `202 Accepted` immediately; the backup runs in the background.

```json
{
  "backup_id": "550e8400-...",
  "message": "backup started: frequency=manual, label=pre-upgrade-2026-07-04, source=pg2"
}
```

### Delete a backup

```http
DELETE /api/backups/550e8400-e29b-41d4-a716-446655440000
```

Deletes all S3 objects under the backup's prefix, then removes the manifest
from Raft.  Returns `204 No Content` on success.

---

## S3 Key Layout

```
<bucket>/
  <prefix>/
    daily/
      <backup-uuid>/
        base.tar.gz
        pg_wal.tar.gz
    weekly/
      <backup-uuid>/
        base.tar.gz
        pg_wal.tar.gz
    monthly/
      ...
    manual/
      ...
```

---

## PITR (Point-in-Time Recovery)

PITR restore is a planned future enhancement.  The manifests already carry the
`started_at` / `completed_at` timestamps needed to select the right base backup
for a target recovery time.  WAL archiving (continuous WAL streaming to S3) and
the restore orchestration (`recovery_target_time` config + vk-agent restart) are
not yet implemented.

---

## Module Layout

```
pgcluster/src/
  backup/
    mod.rs         — BackupScheduler, execute_backup(), S3 helpers, select_source()
  api/
    backup.rs      — REST handlers: list, trigger (manual), delete
```

Relevant Raft types:

```
pgcluster/src/raft/
  topology.rs      — BackupManifest, BackupStatus (in ClusterTopology.backups)
  commands.rs      — AddBackupManifest, RemoveBackupManifest
  state_machine.rs — apply_command() handles the two new commands
```
