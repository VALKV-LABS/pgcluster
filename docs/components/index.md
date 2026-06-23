# pgcluster Component Index

## Milestone 1 — Standard Postgres (MVP)

| Component | Doc | Purpose |
|-----------|-----|---------|
| `raft_consensus` | [raft_consensus.md](raft_consensus.md) | Embedded Raft (openraft); leader election; topology log replication |
| `topology_store` | [topology_store.md](topology_store.md) | Replicated cluster state: primary, roles, LSNs, lag, failover history |
| `vk_agent` | [vk_agent.md](vk_agent.md) | Thin sidecar on each Postgres node; executor-only (no decisions) |
| `node_monitor` | [node_monitor.md](node_monitor.md) | Health polling via vk-agent; consecutive failure detection |
| `failover_engine` | [failover_engine.md](failover_engine.md) | Unplanned primary failure: candidate selection, promote, re-point replicas |
| `switchover_engine` | [switchover_engine.md](switchover_engine.md) | Planned zero-data-loss handoff; write pause; zero-lag wait |
| `proxy_layer` | [proxy_layer.md](proxy_layer.md) | Postgres-protocol proxy; write→primary, read→replicas; session routing |
| `connection_pool` | [connection_pool.md](connection_pool.md) | Per-(db, user, backend) connection pools; transaction-mode multiplexing |
| `config_manager` | [config_manager.md](config_manager.md) | `pgcluster.toml` load, validate, hot-reload |
| `tls_manager` | [tls_manager.md](tls_manager.md) | TLS for Raft, agent gRPC, proxy; auto cert generation for dev |
| `rest_api` | [rest_api.md](rest_api.md) | HTTP API: status, switchover, failover, node management |
| `cli` | [cli.md](cli.md) | `pgcluster` binary: server mode + operator CLI subcommands |

## Milestone 5 — Horizontal Sharding (Cluster-of-Clusters)

| Component | Doc | Purpose |
|-----------|-----|---------|
| `coordinator` | [coordinator.md](coordinator.md) | Coordinator mode: global ShardMap in Raft, shard resolution API, connection-hint proxy |

### M5-B (deferred, optional)
SQL-aware router with full SQL parsing and scatter-gather — see `coordinator.md` for rationale on deferral.

---

## Milestone 2 — Production Hardening

| Component | Doc | Purpose |
|-----------|-----|---------|
| `backup_coordinator` | [backup_coordinator.md](backup_coordinator.md) | Base backup via replica; manifest tracking; object storage upload; PITR restore |

## Milestone 3 — pgrust Native

| Component | Doc | Purpose |
|-----------|-----|---------|
| `pgrust_native` | [pgrust_native.md](pgrust_native.md) | Agent-free control via pgrust's native binary port; event push; < 500ms failover |

## Data Flow Summary

```
Client
  └── [proxy_layer] ──read──► replica backends (via connection_pool)
                    ──write─► primary backend  (via connection_pool)

pgcluster Raft leader
  └── [node_monitor] ──polls──► [vk_agent] ──queries──► Postgres
  └── [failover_engine] ─────► [vk_agent] Promote RPC
  └── [switchover_engine] ────► [vk_agent] Promote + Demote RPCs
  └── [raft_consensus] ────────► [topology_store] (replicated to all instances)

[proxy_layer] reads [topology_store] locally (no network hop) for every routing decision
```
