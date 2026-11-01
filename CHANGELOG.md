# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Security
- **Breaking:** `pgcluster server` refuses to start without `api_keys`. `insecure_no_auth = true` opts out, for local development only.
- **Breaking:** Raft, topology and `vk-agent` gRPC require a client certificate from the cluster CA. `pgcluster` and `vk-agent` refuse to start without `ca_cert`, `cert` and `key`. `insecure_plaintext = true` opts out, for local development only.
- **Breaking:** `[tls] auto_generate` and `pgcluster init --gen-certs` are gone, because they gave each node its own CA. Use `pgcluster certs`.

### Fixed
- `pgcluster` now sends the heartbeats `vk-agent` expects. Without them every agent entered safe mode, and with `fence_timeout_seconds` set it stopped Postgres.
- Logging in through the proxy no longer hangs when a client takes more than 10 ms to answer an authentication challenge, as SCRAM clients do.
- The proxy routes reads to the primary. Sending them to a replica dropped the client's connection.
- `make start` works on a fresh checkout. Replicas failed to seed because `docker/init-replica.sh` was not executable and wrote `.pgpass` into `PGDATA` before `pg_basebackup`, which needs an empty directory. The script now exits with an error if seeding fails.

### Changed
- Every release updates the `latest` Docker image tag.
- Every push to `main` publishes `valkvlabs/pgcluster:edge` and `valkvlabs/vk-agent:edge`.
- A release skips CI when it already passed on `main` for the tagged commit.

### Added
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
