# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Security
- **Breaking:** self-fencing is on by default. `fence_timeout_seconds` defaults to 30, and 0 turns it off. It was previously off unless set, and 0 fenced immediately.
- **Breaking:** `pgcluster server` refuses to start without `api_keys`. `insecure_no_auth = true` opts out, for local development only.
- **Breaking:** Raft, topology and `vk-agent` gRPC require a client certificate from the cluster CA. `pgcluster` and `vk-agent` refuse to start without `ca_cert`, `cert` and `key`. `insecure_plaintext = true` opts out, for local development only.
- **Breaking:** `[tls] auto_generate` and `pgcluster init --gen-certs` are gone, because they gave each node its own CA. Use `pgcluster certs`.

### Fixed
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

### Added
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
