# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed
- `make start` works on a fresh checkout. Replicas failed to seed because `docker/init-replica.sh` was not executable and wrote `.pgpass` into `PGDATA` before `pg_basebackup`, which needs an empty directory. The script now exits with an error if seeding fails.

### Changed
- Every release updates the `latest` Docker image tag.

## [0.1.3] - 2026-10-31

The first published release. Images are `valkvlabs/pgcluster:0.1.3` and `valkvlabs/vk-agent:0.1.3`.

### Added
- Automatic failover and planned switchover, with Raft consensus built in.
- Postgres-protocol proxy with connection pooling and read routing.
- REST API, CLI and Prometheus metrics.
- Scheduled base backups to S3-compatible storage, with `POST /api/backups` for manual backups.
- Maintenance mode through `/api/maintenance/:node_id`.
- Apache 2.0 `LICENSE` and `NOTICE`.
