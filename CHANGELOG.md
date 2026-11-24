# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- The console covers every admin API route: suspend and resume, delete with or without a final backup and restore from a deleted database, usage, node replacement, retention, custom proxy certificates, S3 repositories and their removal, workspaces and chosen hosts, every operation's steps and logs, the event feed, the audit log, and the admin CA and its rotation.
- `POST /v1/databases/{id}/suspend` and `resume`, and `pgcluster database suspend` and `resume`. A suspension shuts clients out and ends their sessions, while nodes, data and backups stay.
- A deleted database's backups, with a final one taken at delete, stay restorable into a new database for `[lifecycle] deleted_backup_retention_days`, 7 by default, then a host purges them from the repository. `GET /v1/databases?deleted=true` and `pgcluster database list --deleted` list such databases.
- `GET /v1/usage` and `pgcluster database usage`: node, vCPU, memory and storage hours per database over a period, recorded by triggers as nodes are added, resized and removed, and the backup bytes held now.
- A database can opt out of backups with `"backup": {"enabled": false}` or `--no-backup`: no repository, no first backup before access, no WAL archive and no restore window. `PATCH` with a backup policy, or `pgcluster database enable-backups`, turns them on later with no restart. The console does both.
- SIGHUP reads `pgcluster`'s whole `[backup]` section, so one added later starts backups and archiving.
- Named prepared statements work through the proxy across transactions and pooled backends. The proxy prepares a statement again on a backend that lacks it, and `[proxy.pool] max_prepared_statements` caps how many each backend keeps.
- `SECURITY.md` describes the deployment security model: which file holds which secret, who can read it, and what to back up.
- A scheduler that never overcommits a host, keeps memory back for the OS and the page cache, and refuses a request no host can hold at once. `[scheduler]` sets the reserves and the port range.
- Each instance runs in a systemd slice with `CPUQuota=`, `MemoryMax=` and `IOWeight=`, and gets an XFS project quota for disk, set by `disk_quota` in the host config. A host refuses to start an instance whose ports something else holds.
- `POST /v1/hosts/{id}/drain` moves every node off a host with no downtime, and `DELETE` stops it.
- Resizing through `PATCH /v1/databases/{id}`: CPU and storage change online, and memory takes a rolling restart.
- `POST /v1/databases/{id}/upgrade` moves to a newer minor release the hosts have installed, by rolling restart. Hosts report installed releases, and nodes the release they run.
- Cross-region replicas: `POST /v1/databases/{id}/replicas`, listing, deletion, and `promote`, which detaches one into a database of its own.
- `PUT /v1/databases/{id}/certificate` gives the proxy a certificate of its own, through `[tls] proxy_cert` and `proxy_key` in `pgcluster`.
- Connection limits on the admin API, the agent API and the proxy, so a flood cannot stop enrolled hosts or connected clients.
- Named API keys in `[[api.keys]]`, reloaded on SIGHUP, and `GET /v1/audit-log`, which records every change with the key that made it.
- The console drains hosts, resizes, upgrades, and manages replicas.
- [`docs/architecture.md`](docs/architecture.md), and component docs for the scheduler, the console and metrics.
- Three-node databases through `pgcluster-admin`, one node per failure domain, with replication modes that change online, node replacement, switchover and rolling restart as operations, and `GET /v1/events`.
- Backups through `pgcluster-admin`: verified S3 or POSIX repositories, per-database encryption, retention, on-demand backups, the restore window, and point-in-time restore into a new database.
- Host firewalling with nftables: peer ports accept only a database's own nodes, and the proxy port accepts the allowlist. `firewall = "none"` turns it off.
- Rotation under load of a database's CA, its internal passwords, the admin CA and the key that encrypts instance secrets.
- `pgcluster repository`, `pgcluster events`, `pgcluster admin-ca`, and more of `pgcluster database`: `--ha`, `mode`, `retention`, `switchover`, `restart`, `replace`, `nodes`, `backup`, `backups`, `restore-window`, `restore`, `rotate-ca` and `rotate-passwords`. The console covers these too.
- `[cluster] secrets_file` gives `pgcluster` its passwords from a file, so SIGHUP changes them. SIGHUP also reads API keys and retention, and `vk-agent` reads its Postgres password.
- `postgres_config_file` runs Postgres from a config outside the data directory.

### Changed
- **Breaking:** `DELETE /v1/databases/{id}` shuts clients out at once, and a database with backups that has opened to clients takes a final backup before its nodes go. `?final_backup=false`, or `--no-final-backup`, skips it. A deleted database's backup key is kept until its backups are purged.
- A suspension or a delete sends connected clients away, through the agent API's new `Instance.shut_out`: the host sets the proxy's new `[proxy] drop_refused_clients`, which drops them between their messages, and ends their backends in Postgres. An allowlist change, even to empty, still refuses only new clients.
- The agent API's desired state gains `repository_purges`, and `POST /agent/v1/repository-purges/{id}` reports one. The backup task's output gains `completed_at` and `size_bytes`.
- **Breaking:** the agent API's `Instance.backup` may be `null`, and the admin API's `Database.backup` is `null` for a database without backups. Older hosts cannot run such a database.
- The leader seeds an empty node, such as a replacement, from the latest backup when one exists, and from the primary only when there is none or the restore fails. This spares the primary a base backup.
- **Breaking:** the agent API's `Instance` gains `role` and `replicas`, and its secrets may carry a proxy certificate. Reports gain `postgres_versions` and `server_version`. Older hosts ignore what they do not know, but cannot run replicas.
- **Breaking:** `POST /v1/hosts/{id}/drain` answers with the host, not an operation.
- **Breaking:** `POST /v1/databases/{id}/backup`, `restart` and other changes answer `409` on a replica, and a database with replicas refuses `rotate-ca`, `rotate-passwords` and deletion.
- **Breaking:** `POST /v1/databases` needs `backup`, and a database stays closed until its first backup exists, so creating one takes two generations.
- **Breaking:** the agent API's `Instance` gains `address`, `backup` and `tasks`, and `bootstrap` can be `seed` or `restore`.

### Fixed
- A node seeded from a backup that could not follow the primary, such as one that replayed a diverged timeline past the fork, waited out its operation. The leader now waits for it to stream, gives up once its replay has stood still for five minutes without streaming, and seeds it from the primary instead. A node still replaying archived WAL is left to finish, however long that takes.
- A client that disconnected inside a transaction, or with a pipeline not yet synced, had its backend put back in the proxy's pool with the transaction still open, so the next client to get it ran inside that transaction, seeing its uncommitted rows and holding its locks. A session that ended on an error kept its backend's place in the pool, which could run it dry. The proxy now closes a backend still held when a session ends, so Postgres rolls back what it held, and frees its place.
- A new database's first backup and the scheduler's could run at once, and on an idle cluster WAL-G gave both the same name, so the cluster listed one backup twice. `pgcluster-admin` then refused every report from that cluster, so the primary it showed never moved again. Backups now run one at a time, and a report that lists a backup twice is taken.
- A switchover with no target could pick the current primary, when the topology still listed it as a replica, and so either fail or move nothing. It now leaves the primary out by name.
- A switchover task failed when the cluster reached the target on its own between the task's check and its request, with `target is already the primary`. The task now succeeds when the target ends up primary.
- After a failover that dropped the old primary's last commits, as async replication can, every backup failed, because the newest one ended past the new timeline's position and WAL-G cannot build a delta on it. The agent now takes a full backup instead.
- The backup scheduler counted a backup it could not start, before a new cluster had a primary, as a failure, which marked a new database `degraded` until a retry a minute later. It now waits for a primary.
- A node whose data came from another, through `pg_rewind` after a partition or `pg_basebackup`, kept that node's `archive_command`, which names a file it does not have, so it archived nothing once it became primary. A replacement seeded from backup then replayed the old primary's diverged WAL and never streamed. The agent now reports the backup settings applied only while Postgres archives through this node's own config, so the leader configures it again.
- A leader cut off by a partition could report once more after it healed, and overwrite what the current leader had said. Reports carry the leader's Raft term, and an older one is ignored.
- A statement prepared on one client connection could fail with `prepared statement does not exist`, or `already exists`, once other clients had used the same pooled backend.
- `walg.json` and `.pgpass` are created owner-only, where before they were briefly readable by others until a `chmod`.
- An API-triggered failover read the replication password from the environment only, ignoring `[cluster] secrets_file`.
- `vk-agent` built only alongside crates that turned on sqlx's `chrono` feature.
- Failover, switchover and the repair of a stale standby used port 5432 for the primary, whatever its address said.
- A standby counted as already following the primary if the primary's host appeared anywhere in its conninfo.
- The proxy trusted the CA it started with, so a new CA broke its logins to Postgres until a restart.
- Removing a node left its replication slot holding WAL.
- A first node waits for a peer that starts late, instead of giving up its bootstrap after five seconds.
- A failed backup is retried every minute until one succeeds. Before, a failure during a failover waited a day for the next scheduled backup, and kept the database `degraded` until then.
- The systemd runner sends SIGHUP through `systemctl kill`, as units relied on a `/bin/kill` some images lack. A reload that fails is retried, where before a changed allowlist could go unread until the next change.
- `vk-agent host` gives instance processes their own user's home. They kept root's, so libpq could not read its files and seeding a standby failed.

### Changed
- **Breaking:** `[backup]` takes `interval_seconds`, `delta_steps`, `retain_full`, `encryption_key_env` and exactly one of `[backup.s3]` and `[backup.posix]`, replacing `[[backup.schedule]]`. Nodes need `archive_mode = on`.

### Security
- The proxy authenticates clients with SCRAM itself, using verifiers read through a restricted role, and logs in to Postgres over verified TLS. It never sees passwords.
- **Breaking:** `PGCLUSTER_AUTH_PASSWORD` is required, and Postgres must have `ssl = on` with a certificate from the cluster CA, unless `insecure_plaintext` is set. Roles need SCRAM passwords.
- **Breaking:** self-fencing is on by default. `fence_timeout_seconds` defaults to 30, and 0 turns it off. It was previously off unless set, and 0 fenced immediately.
- **Breaking:** `pgcluster server` refuses to start without `api_keys`. `insecure_no_auth = true` opts out, for local development only.
- **Breaking:** Raft, topology and `vk-agent` gRPC require a client certificate from the cluster CA. `pgcluster` and `vk-agent` refuse to start without `ca_cert`, `cert` and `key`. `insecure_plaintext = true` opts out, for local development only.
- **Breaking:** `[tls] auto_generate` and `pgcluster init --gen-certs` are gone, because they gave each node its own CA. Use `pgcluster certs`.

### Fixed
- A cluster whose Raft address is on loopback binds Raft there, not on every interface.
- The proxy caps the idle backends it keeps per database and user. Each login added one, so short-lived clients could exhaust Postgres connections.
- Switchover stops the old primary before promoting the target, then creates slots on the new primary. Before, standbys could not follow the new primary after a switchover.
- Repointing a standby applies with a reload, so standbys follow a new primary after a failover without a restart.
- Health checks time out, so a node that vanishes without a connection reset no longer stalls the monitor and blocks failover.
- A node that diverged and shares a timeline ID with the new primary is seeded again, instead of rejoining with its diverged commits.
- An aborted failover is retried. Before, the first attempt marked the primary offline, which stopped later attempts and left the cluster with no primary.
- Restarting the bootstrap node no longer repeats the bootstrap seed, which moved the primary back to the highest-priority node.
- Failover chooses the candidate from the live position of reachable standbys, not the last position the health checks recorded, and aborts if none answers.
- `pgcluster` now sends the heartbeats `vk-agent` expects. Without them every agent entered safe mode, and with `fence_timeout_seconds` set it stopped Postgres.
- Read routing works: the proxy logs in to replicas for the client. Before, it sent queries on connections with no login, which dropped the client. It is now off by default, because a read after a write may miss it.
- The proxy forwards extended protocol messages unchanged, and waits for a reply only at `Sync`. Before, re-encoding lost format codes and NULLs, so prepared statements with parameters failed.
- `COPY FROM STDIN` no longer hangs through the proxy.
- `make start` works on a fresh checkout. Replicas failed to seed because `docker/init-replica.sh` was not executable and wrote `.pgpass` into `PGDATA` before `pg_basebackup`, which needs an empty directory. The script now exits with an error if seeding fails.

### Changed
- Every release updates the `latest` Docker image tag.
- Every push to `main` publishes `valkvlabs/pgcluster:edge` and `valkvlabs/vk-agent:edge`.
- A release skips CI when it already passed on `main` for the tagged commit.

### Changed
- The `vk-agent` image is built on `postgres:16-bookworm`, so its tools match the server. It shipped Postgres 15 tools before.
- `vk-agent` starts with no Postgres running, and skips self-fencing while there is none to fence.

### Added
- `pgcluster-admin`: one service for a fleet, with an admin API, an agent API, Postgres for metadata, and required `api_keys`. Contracts are in `openapi/`. It creates, changes and deletes one-node databases, as stored operations that resume after a restart.
- `vk-agent host`: a mode of `vk-agent` that enrols a machine with a single-use token, long-polls desired state, and runs instances under systemd or as plain processes. It opens no port and keeps its last state on disk. `vk-agent <config>` is unchanged.
- `pgcluster host` and `pgcluster database` commands. `pgcluster database create` generates the password locally and sends only its verifier.
- `pgcluster console`: a local web page for trying every operation of both planes and checking the result. `make try` starts the full stack and points at it. `make start` now includes `pgcluster-admin`, a host and the console.
- `[proxy] allowed_cidrs`, reloaded on SIGHUP, and `[proxy] require_tls`.
- `pgcluster` and `vk-agent` reload their certificates on SIGHUP, with no restart. Node certificates from `pgcluster-admin` last 90 days and renew by themselves.
- A node image recipe in `images/` and a Terraform template in `deploy/aws/`.
- Docker image `valkvlabs/pgcluster-admin`.
- External standbys: `"external": true` on `POST /api/nodes/add` adds a node that follows the primary but stays outside failover, synchronous commit, backups and rolling restarts. `POST /api/nodes/:id/detach` promotes it into an independent database.
- Backups use WAL-G, run by `vk-agent` from the least-lagged standby, falling back to the primary: full and delta backups, retention, WAL archiving from whichever node is primary, and encryption with libsodium.
- `POST /api/backups/restore` restores to a point in time onto a separate agent, as an independent database. `{"from_backup": true}` on `POST /api/nodes/:id/seed` seeds a node from a backup.
- `GET /api/backups/verify` checks the WAL archive for gaps.
- Rolling restart through `POST /api/restart`: standbys one at a time, then a switchover, which restarts the old primary as a standby.
- An empty node seeds itself from the primary as soon as the leader can reach its agent.
- `vk-agent` starts the Postgres it manages when it starts.
- A diverged old primary rejoins by itself as a standby, with `pg_rewind`, and is seeded again if that fails. `manage_postgres = false` opts an agent out, for when something else owns the Postgres process.
- `POST /api/nodes/:id/seed` seeds a node from the primary through `vk-agent`.
- Raft membership changes through `/api/members`: add a learner, promote it, and remove a member.
- `replication_mode` with `async`, `sync` and `sync_strict`, which replaces `synchronous_standby_names`. The config sets the mode of a new cluster, and `POST /api/replication/mode` changes it online. `GET /api/replication` reports it.
- `sync` falls back to async after `sync_degrade_timeout_seconds` with no standby, recording that through Raft first, and the failover history shows `ran_degraded`. `sync_strict` never falls back, and promotes only if every standby answers.
- `max_slot_wal_keep_size_mb` limits the WAL a slot can hold back. A standby past the limit appears under `needs_reseed` in `GET /api/replication`.
- A proxy that loses contact with the Raft majority stops routing, within `quorum_lease_ms` and before a new primary can be promoted.
- A Raft leader without quorum stops heartbeating agents and stops driving failover.
- Startup validation of partition timing: `quorum_lease_ms` against failover detection, and `agent_heartbeat_timeout_seconds` against Raft elections.
- A partition test in the end-to-end suite.
- The CLI sends the key in `PGCLUSTER_API_KEY` to the REST API.
- `pgcluster certs` generates a cluster CA and certificates.

## [0.1.3] - 2026-10-31

The first published release. Images are `valkvlabs/pgcluster:0.1.3` and `valkvlabs/vk-agent:0.1.3`.

### Added
- Automatic failover and planned switchover, with Raft consensus built in.
- Postgres-protocol proxy with connection pooling and read routing.
- REST API, CLI and Prometheus metrics.
- Scheduled base backups to S3-compatible storage, with `POST /api/backups` for manual backups.
- Maintenance mode through `/api/maintenance/:node_id`.
- Apache 2.0 `LICENSE` and `NOTICE`.
