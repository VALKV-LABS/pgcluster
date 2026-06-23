# pgcluster

[![CI](https://github.com/valkv-labs/pgcluster/actions/workflows/ci.yml/badge.svg)](https://github.com/valkv-labs/pgcluster/actions/workflows/ci.yml)
[![Release](https://github.com/valkv-labs/pgcluster/actions/workflows/release.yml/badge.svg)](https://github.com/valkv-labs/pgcluster/actions/workflows/release.yml)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgres-12%2B-336791.svg?logo=postgresql&logoColor=white)](https://www.postgresql.org)
[![Docker](https://img.shields.io/badge/docker-ready-2496ED.svg?logo=docker&logoColor=white)](https://hub.docker.com/r/valkv-labs/pgcluster)

> **Rust-native PostgreSQL HA — no etcd, no Patroni, no HAProxy.**

pgcluster is a single binary that replaces Patroni + etcd + PgBouncer + HAProxy for managing PostgreSQL high-availability clusters. Consensus is embedded via [Raft](https://raft.github.io/); there are no external coordination services to operate.

```
Clients (Postgres wire protocol)
         │
         ▼
┌─────────────────────────────────────────────────────┐
│          pgcluster  (3 instances, any node)          │
│  ┌──────────────┐   ┌────────────────────────────┐  │
│  │  Raft Group  │   │  Proxy + Connection Pool   │  │
│  │  (embedded)  │   │  writes → primary          │  │
│  │              │   │  reads  → replica pool     │  │
│  └──────────────┘   └────────────────────────────┘  │
│  ┌───────────────────────────────────────────────┐   │
│  │  Node Monitor · Failover Engine · Switchover  │   │
│  └───────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
         │ gRPC / TLS
         ▼
┌─────────────────┐
│   vk-agent      │  ← thin sidecar, executes commands
├─────────────────┤
│   PostgreSQL    │
│   (12+, stock)  │
└─────────────────┘
```

---

## Features

- **No external DCS** — Raft consensus is embedded; no etcd, ZooKeeper, or Consul
- **Automatic failover** — detects primary failure in < 5 s, promotes best replica, re-points all standbys
- **Planned switchover** — zero-data-loss handoff with write pause and lag-wait
- **Built-in connection proxy** — Postgres-protocol proxy with per-user connection pooling and lag-aware read routing
- **Thin agent** — `vk-agent` sidecar only executes commands; all decisions live in the Raft leader
- **TLS everywhere** — mutual TLS between pgcluster peers and to vk-agent; auto-generated dev certs
- **Prometheus metrics** — failover count, replication lag, leader status, connection pool saturation
- **REST API** — cluster status, node management, manual failover/switchover
- **Works with stock Postgres 12+** — no patches, no extensions required

---

## Quick Start

### Docker Compose (3-node cluster)

```bash
git clone https://github.com/valkv-labs/pgcluster.git
cd pgcluster
make start        # builds images and starts 3-node stack
make logs         # tail all container logs
make status       # show container health and ports
```

The stack exposes:
| Port | Service |
|------|---------|
| `5432` | Postgres proxy (writes → primary, reads → replicas) |
| `8009` | REST API |
| `9190` | Prometheus metrics |
| `8008` | HTTP health check (for load balancers) |

### Single binary

```bash
cargo build --release
./target/release/pgcluster server --config examples/cluster.toml
```

---

## Installation

### From source

Requires Rust 1.85+ and `protoc` (Protocol Buffers compiler).

```bash
# Install protoc (Debian/Ubuntu)
apt-get install protobuf-compiler

# Build
cargo build --release --bin pgcluster
cargo build --release --bin vk-agent
```

### Docker

```bash
docker pull ghcr.io/valkv-labs/pgcluster:latest
docker pull ghcr.io/valkv-labs/vk-agent:latest
```

---

## Configuration

pgcluster is configured via a TOML file. A minimal 3-node config:

```toml
[cluster]
name     = "prod"
data_dir = "/var/lib/pgcluster/node1"

[raft]
node_id   = 1
bootstrap = true          # set only on the first node, first run

[[raft.peers]]
id = 1; addr = "pgcluster-1:7000"
[[raft.peers]]
id = 2; addr = "pgcluster-2:7000"
[[raft.peers]]
id = 3; addr = "pgcluster-3:7000"

[[nodes.node]]
id            = "pg1"
agent_addr    = "pg1:7001"
postgres_addr = "pg1:5432"
priority      = 100

[replication]
replication_user         = "replicator"
replication_password_env = "PG_REPLICATION_PASSWORD"

[proxy]
listen_addr        = "0.0.0.0:5432"
health_listen_addr = "0.0.0.0:8008"

[api]
listen_addr = "0.0.0.0:8009"
api_keys    = ["change-me"]
```

Full config reference: [`docs/config.md`](docs/config.md) · Example: [`examples/cluster.toml`](examples/cluster.toml)

---

## REST API

All endpoints are served on the `api.listen_addr` port (default `8009`).

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Cluster health (200 = primary known, 503 = no primary) |
| `GET` | `/api/status` | Cluster status, Raft leader, topology version |
| `GET` | `/api/topology` | Full topology snapshot |
| `GET` | `/api/nodes` | List all nodes with roles and LSNs |
| `GET` | `/api/nodes/:id` | Single node info |
| `POST` | `/api/nodes/add` | Register a new node |
| `DELETE` | `/api/nodes/:id` | Remove a node |
| `POST` | `/api/failover` | Trigger manual failover (mark node offline) |
| `POST` | `/api/switchover` | Planned switchover to a target node |
| `GET` | `/api/replication/slots` | Replication slot inventory |

Read endpoints are public when `api.public_read_endpoints = true`; write endpoints always require an `Authorization: Bearer <key>` header.

---

## Development

### Prerequisites

- Rust 1.85+
- Docker + Docker Compose
- `protoc` (Protocol Buffers compiler)

### Common tasks

```bash
make build          # debug build
make build-release  # release build
make fmt            # cargo fmt
make check          # clippy (warnings-as-errors)
make test           # unit tests (no Docker)
make integ-up       # build + run unit + integration tests in Docker (stack stays up)
make test-integ     # same, tears down when done
make start          # start full 3-node e2e stack
make stop           # stop and remove e2e stack
make logs           # tail e2e stack logs
make status         # show container health
```

### Project layout

```
pgcluster/          — main cluster manager binary + library
  src/
    raft/           — Raft consensus, topology state machine, log storage
    api/            — axum REST API handlers
    proxy/          — Postgres-protocol proxy + connection pool
    failover/       — automatic failover engine
    switchover/     — planned switchover engine
    health_check/   — per-node health polling
    agent_clients/  — gRPC client for vk-agent
    config/         — TOML config + validation
  tests/            — integration tests (#[ignore] — need `make integ-up`)

vk-agent/           — thin sidecar binary
  src/
    grpc/           — agent gRPC server (promote, demote, heartbeat, status)
    postgres/       — pg_ctl / psql wrappers
    heartbeat/      — safe-mode watchdog

proto/              — .proto definitions shared by pgcluster and vk-agent
docker/             — Dockerfiles and compose files
  integ-compose.yml — single-node integ test stack
  e2e-compose.yml   — full 3-node e2e stack
examples/           — sample config files
```

### Running tests

```bash
# Unit tests (no Docker)
cargo test --workspace

# Integration tests (needs Docker)
make integ-up
```

Integration tests are all `#[ignore]`; the Docker test-runner passes `--include-ignored` automatically.

---

## Architecture

### Failover flow

1. Raft leader detects primary is unreachable (3 consecutive health-check failures, default 1.5 s)
2. Leader selects best replica (highest `flush_lsn`, then highest `priority`)
3. Leader calls `vk-agent.Promote()` on the chosen replica
4. Replica agent runs `pg_ctl promote` / writes `promote.signal`
5. Leader updates topology via Raft log (`SetPrimary` command)
6. All `pgcluster` instances receive the topology update and re-point their proxy
7. Leader calls `vk-agent.Demote()` on all remaining replicas to re-point to the new primary

Total time: < 5 s under normal conditions (configurable).

### Raft storage

The Raft log is stored in an embedded [`sled`](https://github.com/spacejam/sled) database under `data_dir/raft-log`. No external storage is needed.

---

## Contributing

1. Fork the repository
2. Create a branch: `git checkout -b feat/my-feature`
3. Make your changes and add tests
4. Run `make all` (format + lint + unit tests)
5. Open a pull request

Please open an issue before starting significant work so we can discuss the approach.

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).

---

*pgcluster is part of the [valkv-labs](https://github.com/valkv-labs) project.*
