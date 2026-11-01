# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed
- **Breaking:** `[backup]` takes `interval_seconds`, `delta_steps`, `retain_full`, `encryption_key_env` and exactly one of `[backup.s3]` and `[backup.posix]`, replacing `[[backup.schedule]]`. Nodes need `archive_mode = on`.

### Security
- **Breaking:** self-fencing is on by default. `fence_timeout_seconds` defaults to 30, and 0 turns it off. It was previously off unless set, and 0 fenced immediately.
- **Breaking:** `pgcluster server` refuses to start without `api_keys`. `insecure_no_auth = true` opts out, for local development only.
- **Breaking:** Raft, topology and `vk-agent` gRPC require a client certificate from the cluster CA. `pgcluster` and `vk-agent` refuse to start without `ca_cert`, `cert` and `key`. `insecure_plaintext = true` opts out, for local development only.
- **Breaking:** `[tls] auto_generate` and `pgcluster init --gen-certs` are gone, because they gave each node its own CA. Use `pgcluster certs`.

### Fixed
- The proxy caps the idle backends it keeps per database and user. Each login added one, so short-lived clients could exhaust Postgres connections.
- Switchover stops the old primary before promoting the target, then creates slots on the new primary. Before, standbys could not follow the new primary after a switchover.
- Repointing a standby applies with a reload, so standbys follow a new primary after a failover without a restart.
- Health checks time out, so a node that vanishes without a connection reset no longer stalls the monitor and blocks failover.
- A node that diverged and shares a timeline ID with the new primary is seeded again, instead of rejoining with its diverged commits.
- An aborted failover is retried. Before, the first attempt marked the primary offline, which stopped later attempts and left the cluster with no primary.
- Restarting the bootstrap node no longer repeats the bootstrap seed, which moved the primary back to the highest-priority node.
- Failover chooses the candidate from the live position of reachable standbys, not the last position the health checks recorded, and aborts if none answers.
- `pgcluster` now sends the heartbeats `vk-agent` expects. Without them every agent entered safe mode, and with `fence_timeout_seconds` set it stopped Postgres.
- Logging in through the proxy no longer hangs when a client takes more than 10 ms to answer an authentication challenge, as SCRAM clients do.
- The proxy routes reads to the primary. Sending them to a replica dropped the client's connection.
- `make start` works on a fresh checkout. Replicas failed to seed because `docker/init-replica.sh` was not executable and wrote `.pgpass` into `PGDATA` before `pg_basebackup`, which needs an empty directory. The script now exits with an error if seeding fails.

### Changed
- Every release updates the `latest` Docker image tag.
- Every push to `main` publishes `valkvlabs/pgcluster:edge` and `valkvlabs/vk-agent:edge`.
- A release skips CI when it already passed on `main` for the tagged commit.

### Changed
- The `vk-agent` image is built on `postgres:16-bookworm`, so its tools match the server. It shipped Postgres 15 tools before.
- `vk-agent` starts with no Postgres running, and skips self-fencing while there is none to fence.

### Added
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
- `[proxy] backend_tls` encrypts and verifies the connection to Postgres.

## [0.1.3] - 2026-10-31

The first published release. Images are `valkvlabs/pgcluster:0.1.3` and `valkvlabs/vk-agent:0.1.3`.

### Added
- Automatic failover and planned switchover, with Raft consensus built in.
- Postgres-protocol proxy with connection pooling and read routing.
- REST API, CLI and Prometheus metrics.
- Scheduled base backups to S3-compatible storage, with `POST /api/backups` for manual backups.
- Maintenance mode through `/api/maintenance/:node_id`.
- Apache 2.0 `LICENSE` and `NOTICE`.
