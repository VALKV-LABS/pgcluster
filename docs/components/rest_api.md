# Component: REST API (`rest_api`)

## High-Level Function

The REST API provides an HTTP interface for operators, automation scripts, and dashboards to inspect cluster state and trigger operations. It runs on all pgcluster instances; read-only endpoints are served locally, mutation endpoints are forwarded to the Raft leader.

---

## Endpoints

### Cluster Status

```
GET /api/v1/cluster/status

Response 200:
{
  "cluster_name": "prod-cluster",
  "primary": "pg1",
  "nodes": [
    {
      "id": "pg1", "role": "primary", "addr": "10.0.0.1:5432",
      "flush_lsn": "0/4A00000", "timeline": 3,
      "active_connections": 42, "healthy": true
    },
    {
      "id": "pg2", "role": "replica", "addr": "10.0.0.2:5432",
      "flush_lsn": "0/49FF800", "lag_bytes": 2048, "lag_seconds": 0.12,
      "healthy": true
    },
    {
      "id": "pg3", "role": "offline", "addr": "10.0.0.3:5432",
      "healthy": false, "last_seen_secs_ago": 45
    }
  ],
  "topology_version": 14,
  "last_failover": {
    "old_primary": "pg3", "new_primary": "pg1",
    "at": "2026-06-19T08:15:30Z", "duration_ms": 3240
  }
}
```

### Trigger Switchover

```
POST /api/v1/cluster/switchover
Body: { "target": "pg2" }

Response 200: { "status": "ok", "new_primary": "pg2", "duration_ms": 6120 }
Response 409: { "error": "target pg2 is not streaming or has excessive lag" }
```

### Trigger Failover (manual override)

```
POST /api/v1/cluster/failover
Body: { "target": "pg2" }  // Optional: force specific target

Response 200: { "status": "ok", "new_primary": "pg2", "duration_ms": 3100 }
```

### Node Management

```
POST   /api/v1/nodes              Body: { "id", "agent_addr", "postgres_addr", "priority" }
DELETE /api/v1/nodes/{id}
PUT    /api/v1/nodes/{id}/maintenance   Body: { "enabled": true }
```

### Replication Lag

```
GET /api/v1/replication/lag

Response 200:
{
  "primary_lsn": "0/4A00000",
  "replicas": [
    { "id": "pg2", "flush_lsn": "0/49FF800", "lag_bytes": 2048, "lag_ms": 120 },
    { "id": "pg3", "flush_lsn": "0/0",       "lag_bytes": null, "status": "offline" }
  ]
}
```

### Config

```
GET  /api/v1/config           → current pgcluster.toml values (sanitized, no passwords)
POST /api/v1/config/reload    → reload config from disk (non-destructive fields only)
```

### Metrics (Prometheus)

```
GET /metrics   → Prometheus text format (separate from :8008 health, runs on :9190)
```

---

## Detailed Implementation Plan

### 1. Module Layout

```
src/
  api/
    mod.rs        # Router setup (axum), middleware
    status.rs     # GET /api/v1/cluster/status handler
    switchover.rs # POST /api/v1/cluster/switchover handler
    failover.rs   # POST /api/v1/cluster/failover handler
    nodes.rs      # Node CRUD handlers
    replication.rs # GET /api/v1/replication/lag handler
    config.rs     # GET/POST /api/v1/config handlers
    auth.rs       # API key authentication middleware
    forward.rs    # Forward mutation requests to Raft leader
```

### 2. axum Router

```rust
pub fn build_router(state: Arc<ApiState>) -> Router {
    Router::new()
        .route("/api/v1/cluster/status",       get(status::get_status))
        .route("/api/v1/cluster/switchover",   post(switchover::trigger_switchover))
        .route("/api/v1/cluster/failover",     post(failover::trigger_failover))
        .route("/api/v1/nodes",                post(nodes::add_node))
        .route("/api/v1/nodes/:id",            delete(nodes::remove_node))
        .route("/api/v1/nodes/:id/maintenance",put(nodes::set_maintenance))
        .route("/api/v1/replication/lag",      get(replication::get_lag))
        .route("/api/v1/config",               get(config::get_config))
        .route("/api/v1/config/reload",        post(config::reload_config))
        .route("/metrics",                     get(metrics::prometheus_metrics))
        .layer(middleware::from_fn_with_state(state.clone(), auth::api_key_auth))
        .with_state(state)
}

pub struct ApiState {
    pub topology: Arc<TopologyStore>,
    pub raft: Arc<PgClusterRaft>,
    pub switchover: Arc<SwitchoverEngine>,
    pub failover: Arc<FailoverEngine>,
    pub config: Arc<RwLock<PgClusterConfig>>,
    pub node_id: u64,
}
```

### 3. Leader Forwarding

Mutation endpoints that require Raft consensus must run on the leader:

```rust
async fn ensure_leader_or_forward<B>(
    state: &ApiState,
    req: Request<B>,
) -> Result<Response, ApiError> {
    if state.raft.is_leader().await {
        return Ok(next.run(req).await);
    }
    // Forward to leader
    let leader_id = state.raft.current_leader().await
        .ok_or(ApiError::NoLeader)?;
    let leader_addr = state.topology.get_api_addr(leader_id)?;
    let response = reqwest::Client::new()
        .request(req.method().clone(), format!("http://{}{}", leader_addr, req.uri()))
        .headers(req.headers().clone())
        .body(/* forward body */)
        .send().await?;
    Ok(proxy_response(response).await?)
}
```

### 4. Authentication

API key via `Authorization: Bearer <key>` header. Keys stored in `pgcluster.toml`:

```toml
[api]
api_keys = ["key1-for-ops", "key2-for-monitoring"]
```

Monitoring endpoints (`/api/v1/cluster/status`, `/api/v1/replication/lag`, `/metrics`) can optionally be public (no auth required) via config.

### 5. Coordinator Shard API (M5-A)

These endpoints are added to the coordinator's axum router (same port as the cluster API):

```
GET    /api/v1/shards
Response 200:
{
  "keyspace": "user_id",
  "strategy": "range",
  "shards": [
    {
      "id": "shard-0", "range_low": 0, "range_high": 10000,
      "state": "serving",
      "proxy_addrs": ["10.0.1.1:5432", "10.0.1.2:5432", "10.0.1.3:5432"],
      "health": { "primary": "pg1", "replicas": 2, "lag_bytes": 1024 }
    },
    {
      "id": "shard-1", "range_low": 10000, "range_high": 20000,
      "state": "serving",
      "proxy_addrs": ["10.0.2.1:5432", "10.0.2.2:5432", "10.0.2.3:5432"],
      "health": { "primary": "pg4", "replicas": 2, "lag_bytes": 512 }
    }
  ],
  "shard_map_version": 3
}

GET    /api/v1/shards/resolve?key=12345
Response 200: { "shard_id": "shard-1", "proxy_addr": "10.0.2.1:5432", "range_low": 10000, "range_high": 20000 }
Response 404: { "error": "no shard owns key 12345" }

GET    /api/v1/shards/{id}
Response 200: single ShardDef + live health (proxied from shard's /api/v1/cluster/status)

POST   /api/v1/shards
Body: { "id": "shard-2", "range_low": 20000, "range_high": 30000,
        "pgcluster_proxy_addrs": [...], "pgcluster_api_addrs": [...] }
Response 201: { "shard_id": "shard-2", "shard_map_version": 4 }

DELETE /api/v1/shards/{id}
Response 200: { "status": "removed", "shard_map_version": 5 }
Response 409: { "error": "shard shard-2 state is not Offline — drain first" }

PUT    /api/v1/shards/{id}/state
Body: { "state": "offline" }
Response 200: { "shard_id": "shard-2", "state": "offline" }
```

Module: `src/coordinator/api.rs` — mounted in coordinator mode only; not present on per-shard pgcluster nodes.

### 6. SSE Event Stream (optional, Milestone 2)

```
GET /api/v1/events   → Server-Sent Events stream

data: {"type":"failover_complete","old_primary":"pg3","new_primary":"pg1","duration_ms":3100}
data: {"type":"node_online","node_id":"pg3","role":"replica"}
data: {"type":"replication_lag","node_id":"pg2","lag_bytes":1024}
```

### 6. Integration Points

- Reads `ClusterTopology` from `topology_store` for all status endpoints.
- Calls `switchover_engine::run_switchover()` on switchover request.
- Calls `failover_engine::on_primary_failed()` on manual failover request.
- Proposes `AddNode` / `RemoveNode` to `raft_consensus` for node management.
- Forwards mutation requests to Raft leader if this instance is a follower.
