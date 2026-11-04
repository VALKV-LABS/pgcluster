# Component: REST API (`rest_api`)

## High-Level Function

The REST API lets operators, scripts and dashboards inspect a cluster and trigger operations. It runs on every pgcluster instance, on `api.listen_addr` (default port `8009`). The `pgcluster` CLI uses the same endpoints.

The shard endpoints in [coordinator.md](coordinator.md) are a design for coordinator mode, and the server does not serve them yet.

## Authentication

Send `Authorization: Bearer <key>`, with a key from `api_keys`:

```toml
[api]
listen_addr = "0.0.0.0:8009"
api_keys    = ["key1-for-ops", "key2-for-monitoring"]
public_read_endpoints = false   # true lets GET and HEAD through without a key
```

The server refuses to start with no keys. For local development only, `insecure_no_auth = true` runs the API with no credentials, and logs a warning.

The CLI sends the key in the `PGCLUSTER_API_KEY` environment variable.

`/health` is always public, for load balancers and Docker health checks.

## Endpoints

| Method | Path | Success | Purpose |
|---|---|---|---|
| `GET` | `/health` | `200` | Health; `503` when no primary is known |
| `GET` | `/api/status` | `200` | Cluster summary |
| `GET` | `/api/topology` | `200` | Full topology snapshot |
| `GET` | `/api/replication` | `200` | Replication mode, fallback state and standbys that need seeding |
| `POST` | `/api/replication/mode` | `204` | Change the mode online: `{"mode": "async"}`, `"sync"` or `"sync_strict"` |
| `POST` | `/api/nodes/:node_id/seed` | `202` | Seed a node from the primary. `{"force": true}` discards data already on it, and `{"from_backup": true}` uses the latest backup |
| `POST` | `/api/restart` | `202` | Start a rolling restart, on the Raft leader |
| `GET` | `/api/restart` | `200` | Whether a rolling restart is running on this instance, and any error |
| `GET` | `/api/backups/verify` | `200` | Check the WAL archive for gaps |
| `POST` | `/api/nodes/:node_id/detach` | `204` | Turn an external standby into an independent database, on the Raft leader |
| `POST` | `/api/backups/restore` | `202` | Restore onto a separate agent, optionally to a point in time |
| `GET` | `/api/backups/restore` | `200` | Whether a restore is running on this instance, and any error |
| `GET` | `/api/members` | `200` | List Raft voters and learners |
| `POST` | `/api/members` | `201` | Add a learner, `{"id": 4, "addr": "host:7000"}`, and wait for it to catch up |
| `POST` | `/api/members/:id/promote` | `204` | Promote a learner to voter |
| `DELETE` | `/api/members/:id` | `204` | Remove a voter or learner |
| `GET` | `/api/nodes` | `200` | List nodes |
| `GET` | `/api/nodes/:node_id` | `200` | One node |
| `POST` | `/api/nodes/add` | `201` | Register a Postgres node |
| `DELETE` | `/api/nodes/:node_id` | `204` | Remove a node |
| `POST` | `/api/failover` | `202` | Mark a node failed and start failover |
| `GET` | `/api/failover/history` | `200` | Past failovers |
| `POST` | `/api/switchover` | `202` | Planned switchover |
| `POST` | `/api/maintenance/:node_id` | `200` | Put a replica into maintenance |
| `DELETE` | `/api/maintenance/:node_id` | `200` | Return a node to the replica role |
| `GET` | `/api/replication/slots` | `200` | Replication slots |
| `GET` | `/api/config` | `200` | Cluster configuration, without secrets |
| `POST` | `/api/pg-hba/reload` | `200` | Reload Postgres configuration on every node |
| `GET` | `/api/backups` | `200` | List backups; `?frequency=` filters |
| `POST` | `/api/backups` | `202` | Start a manual backup |
| `GET` | `/api/backups/status` | `200` | The label and error of the last backup, if it failed |
| `DELETE` | `/api/backups/:backup_id` | `204` | Delete a backup |

Prometheus metrics are served separately, at `/metrics` on `metrics.listen_addr`.

### Health

```
GET /health

200: { "status": "ok", "primary": "pg1", "raft_leader": true }
```

### Cluster status

```
GET /api/status

200:
{
  "cluster_name": "prod-cluster",
  "primary_node_id": "pg1",
  "topology_version": 14,
  "raft_leader_id": 1,
  "node_count": 3,
  "failover_history_count": 1
}
```

### Nodes

```
GET /api/nodes

200:
{
  "nodes": [
    {
      "node_id": "pg1",
      "role": "primary",
      "postgres_addr": "10.0.0.1:5432",
      "agent_addr": "10.0.0.1:7001",
      "priority": 100,
      "flush_lsn": 77594624,
      "replay_lsn": 77594624,
      "lag_bytes": 0
    }
  ]
}
```

`GET /api/nodes/:node_id` returns one of these objects, or `404`.

```
POST /api/nodes/add
Body: { "node_id": "pg4", "agent_addr": "10.0.0.4:7001", "postgres_addr": "10.0.0.4:5432", "priority": 70 }
```

`priority` is optional and defaults to `100`.

### Switchover

```
POST /api/switchover
Body: { "target_node_id": "pg2", "max_lag_bytes": 65536, "timeout_secs": 60 }

202: { "success": true, "message": "switchover to pg2 initiated" }
```

`max_lag_bytes` and `timeout_secs` are optional. The switchover runs in the background, so poll `/api/status` to confirm it finished.

| Status | Meaning |
|---|---|
| `400` | The target is already the primary, or is not a replica |
| `404` | The target does not exist |
| `409` | A switchover or failover is already in progress |

### Failover

```
POST /api/failover
Body: { "failed_node_id": "pg1" }

202: { "triggered": true, "message": "failover initiated for pg1" }
```

Returns `404` for an unknown node, and `409` if a switchover or failover is already in progress.

`GET /api/failover/history` returns a JSON array of failover events.

### Maintenance

`POST` and `DELETE /api/maintenance/:node_id` return `{ "success": true, "message": "..." }`.

| Status | Meaning |
|---|---|
| `400` | `POST`: the node is the current primary. `DELETE`: the node is not in maintenance |
| `404` | The node does not exist |

### Replication slots

```
GET /api/replication/slots

200: [ { "node_id": "pg2", "slot_name": "pgcluster_pg2" } ]
```

### Configuration

```
GET /api/config

200:
{
  "cluster_name": "prod-cluster",
  "node_id": 1,
  "peers": [ { "id": 1, "addr": "10.0.0.1:7000" } ],
  "proxy_addr": "0.0.0.0:5432",
  "api_addr": "0.0.0.0:8009",
  "metrics_addr": "0.0.0.0:9190"
}
```

### Reload Postgres configuration

```
POST /api/pg-hba/reload

200: { "reloaded": ["pg1", "pg2", "pg3"], "failed": [] }
```

Returns `207` if some nodes failed, and `500` if all did. Each `failed` entry has `node_id` and `error`.

### Backups

See [backup_coordinator.md](backup_coordinator.md).

## Module Layout

```
pgcluster/src/api/
  mod.rs          Router and shared state
  auth.rs         Bearer key middleware
  status.rs       /health, /api/status, /api/topology
  nodes.rs        /api/nodes
  failover.rs     /api/failover
  switchover.rs   /api/switchover
  maintenance.rs  /api/maintenance
  replication.rs  /api/replication/slots
  config.rs       /api/config
  pg_hba.rs       /api/pg-hba/reload
  backup.rs       /api/backups
  metrics.rs      /metrics
```

Member changes go to the Raft leader. Any other instance answers `503` with the leader's address.

To replace a member, start the new instance with `bootstrap = false`, add it, promote it, then remove the old one. That order keeps the cluster at full strength throughout.
