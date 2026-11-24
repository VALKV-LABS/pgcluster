# Component: Backup Coordinator (`backup`)

## High-Level Function

The backup coordinator gives a cluster scheduled backups, WAL archiving and point-in-time restore. It keeps the schedule, retention and manifests, and WAL-G does the work on each node. It runs on every pgcluster instance, and acts only on the Raft leader. Backups continue through a leader change, because the manifests live in Raft.

## Architecture

```
Raft leader (pgcluster)
  ├── Health-check loop: pushes WAL-G settings to every node, and turns on archiving
  ├── BackupScheduler: when a backup is due
  │     └── BackupPush RPC on the least-lagged standby, falling back to the primary
  │           → AddBackupManifest through Raft
  │           → BackupDelete RPC to apply retention
  └── REST API: list, trigger, delete, verify and restore

Every node (vk-agent)
  ├── wal-g backup-push, delete, wal-verify and backup-fetch
  └── Postgres runs wal-g wal-push through archive_command, whichever node is primary
```

The backup RPCs name no tool. Only `pgcluster/src/backup/walg.rs`, which renders settings, and `vk-agent/src/walg.rs`, which runs commands, know about WAL-G.

## Configuration

```toml
[backup]
interval_seconds   = 86400        # Seconds between backups
delta_steps        = 6            # Deltas between full backups: a weekly full
retain_full        = 4            # Full backups to keep, with what they depend on
prefer_replica     = true
encryption_key_env = "BACKUP_KEY" # A base64 32-byte key. Lose it and the backups go too

[backup.s3]                       # Or [backup.posix] with path = "/backups"
bucket = "my-backups"
prefix = "clusters/one"
region = "us-east-1"
```

Set exactly one of `[backup.s3]` and `[backup.posix]`. Every node needs `archive_mode = on`, which takes a restart, and `wal-g` on its path. The `vk-agent` image includes it. A POSIX path must be shared by all nodes. S3 credentials come from pgcluster's environment or from an instance role.

## Taking backups

A standby can refuse a backup, for example when an idle cluster has written no new WAL since the last one. The coordinator then uses the primary, which can always start a new segment. After a failed backup it waits a minute before trying again.

## Restore

`POST /api/backups/restore` restores onto an agent outside the cluster, as an independent database:

```json
{ "agent_addr": "10.0.0.9:7001", "target_time": "2026-11-01 12:00:00+00" }
```

With a target time, it starts from the newest backup that finished before that time. The restored database drops `archive_command`, so it cannot write into the source's repository, and `synchronous_standby_names`, because it has no standbys. `GET /api/backups/restore` reports how it went.

`POST /api/nodes/:id/seed` with `{"from_backup": true}` restores a cluster node from the latest backup, then has it follow the primary. That spares the primary the load of a base backup. The leader's monitor does the same, unasked, for a node it finds empty when backups are configured and one has completed, and seeds from the primary if the restore fails.

## Verification

`GET /api/backups/verify` runs `wal-g wal-verify` on the primary. In its `integrity` report, `FAILURE` means a segment is lost, and `WARNING` means only that recent segments are still on their way. The `timeline` check reads the control file, so right after a failover it can fail until the first checkpoint.
