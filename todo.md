# pgcluster Hardening TODO

## Critical — data loss or extended downtime risk

### 1. Concurrent switchover / failover guard
**Problem:** The switchover API handler spawns a task with no lock. Two simultaneous
`POST /api/switchover` calls (or a switchover racing an automatic failover on a different
code path) can both run `execute_switchover` concurrently, issuing two competing promote
RPCs and two `SetPrimary` writes.  
**Fix:** Hold a single `Arc<Mutex<()>>` in `ApiState`; switchover and trigger_failover
must acquire it before proceeding. Switchover should also check the in-progress flag and
return `409 Conflict` if one is already running.  
**Files:** `pgcluster/src/api/switchover.rs`, `pgcluster/src/failover/mod.rs`

### 2. Fencing the old primary in a network-partition scenario
**Problem:** When all application traffic flows through the pgcluster proxy, the Raft
`SetPrimary` update propagates to all 3 proxy instances within milliseconds, effectively
fencing the old primary from new writes. This is sufficient when the proxy is the
**only** path to postgres.

The remaining gap is a **network partition** (not a crash): if pgcluster nodes lose
contact with pg1 but some application servers can still reach pg1's postgres port
directly (bypassing the proxy), those apps keep writing to pg1 while pg2 is promoted
— split-brain. In a crash scenario (pg1 is actually down) this cannot happen.

**Fix options (in order of effort):**  
- **Firewall direct postgres ports** in production so only pgcluster can reach them (eliminates the gap entirely, low effort).  
- **Watchdog in vk-agent**: if no pgcluster heartbeat is received for N seconds, vk-agent calls `pg_ctl stop` on itself, self-fencing the node.  
- **STONITH** (IPMI / cloud power API): hard-reboot the old primary before promoting the candidate.  
**Files:** `vk-agent/src/heartbeat.rs`, `vk-agent/src/server.rs`, infra/network config

### 3. Automatic repoint after node recovery
**Problem:** When a replica is offline during a failover, it is skipped in the repoint
step (its vk-agent is unreachable). When it comes back, `MarkReplica` is called
(updating Raft role), but its `primary_conninfo` still points at the dead old primary.
It will keep retrying the dead host forever and never rejoin the cluster without manual
intervention.  
**Fix:** In `node_monitor::poll_all_nodes`, after proposing `MarkReplica`, also issue
a `demote` RPC to repoint the node to `topology.primary_node_id`. Only do this when
the node's `primary_conninfo` (visible in `pg_stat_wal_receiver`) does not match the
current primary's hostname — add a `replication_conninfo` field to `StatusResponse`
for this check.  
**Files:** `pgcluster/src/node_monitor/mod.rs`, `vk-agent/src/server.rs`,
`pgcluster/src/raft/network.rs` (proto)

---

## High — security or major reliability gaps

### 4. vk-agent has no authentication
**Problem:** Any process that can reach port 7001 on a vk-agent container can call
`promote`, `demote`, or `stop_postgres`. There is no mTLS or token check.  
**Fix:** Add mTLS: vk-agent generates a self-signed cert on first start; pgcluster
presents a cluster CA-signed client cert. TLS infrastructure (`pgcluster/src/tls/`)
already exists. Wire it into the tonic channel builder in `agent_clients.rs` and into
the tonic server in vk-agent.  
**Files:** `vk-agent/src/main.rs`, `pgcluster/src/agent_clients.rs`, `pgcluster/src/tls/`

### 5. Raft gRPC is plaintext HTTP
**Problem:** `network.rs` builds channels with `http://` — Raft consensus messages
(topology commands, votes, log entries) are sent in the clear between pgcluster nodes.  
**Fix:** Switch to `https://` with TLS using the same cluster CA as item 4. The
`TlsManager` already generates certs; thread it into `PgClusterNetworkFactory`.  
**Files:** `pgcluster/src/raft/network.rs`, `pgcluster/src/lib.rs`

### 6. REST API has no authentication
**Problem:** `POST /api/switchover`, `POST /api/failover`, and all admin endpoints are
unauthenticated. Anyone who can reach port 8009 can trigger a failover.  
**Fix:** Add a shared bearer token (configurable in TOML) validated by an Axum
middleware layer. Long-term, mTLS on the API port is cleaner.  
**Files:** `pgcluster/src/api/mod.rs`

### 7. Stale replication slots block WAL cleanup
**Problem:** pgcluster creates replication slots (`pgcluster_<node_id>`) but never
drops them. If a replica is permanently removed or the slot becomes stale (e.g. after
a failover where the old primary comes back on a different timeline), the slot prevents
the primary from recycling WAL, eventually filling the disk.  
**Fix:** During failover step 1 (pick candidate), call
`SELECT pg_drop_replication_slot(...)` on the old primary for every slot associated
with nodes that will not reconnect. Also add a slot-audit job to node_monitor that
drops orphaned slots (slots with `active = false` and `confirmed_flush_lsn` far behind
`pg_current_wal_lsn()`).  
**Files:** `pgcluster/src/failover/mod.rs`, `pgcluster/src/failover/slots.rs`

### 8. No way to pause automatic failover during maintenance
**Problem:** Routine maintenance (patching postgres, restarting a node) triggers
automatic failover because node_monitor sees the node go offline. Operators have no
way to inhibit it.  
**Fix:** Add `POST /api/maintenance/{node_id}` to put a node in `Maintenance` role
(already exists in `NodeRole` enum) before stopping it. node_monitor must skip
failover for nodes in Maintenance state. Add `DELETE /api/maintenance/{node_id}` to
restore.  
**Files:** `pgcluster/src/api/nodes.rs`, `pgcluster/src/node_monitor/mod.rs`

---

## Medium — operational gaps that will bite in production

### 9. Proxy does not drain connections during switchover
**Problem:** When switchover runs, clients with active transactions through the proxy
get their connections dropped mid-flight when the backend switches. No graceful drain.  
**Fix:** On switchover start, signal the proxy to stop accepting new connections to
the old primary (mark it `draining`), wait up to N seconds for in-flight transactions
to complete, then proceed. Clients waiting for a new connection get the new primary.  
**Files:** `pgcluster/src/proxy/mod.rs`, `pgcluster/src/proxy/pool.rs`,
`pgcluster/src/switchover/mod.rs`

### 10. No post-promotion health check
**Problem:** After `pg_promote()` returns, pgcluster immediately updates Raft and starts
routing writes. If the promotion silently failed (e.g., pg_promote returned OK but
postgres is actually confused), writes go to a non-primary.  
**Fix:** After the promote RPC, issue `GetStatus` on the new primary and verify
`is_in_recovery = false` before proposing `SetPrimary` to Raft. Retry up to 3 times
with 500 ms gaps before aborting the switchover/failover.  
**Files:** `pgcluster/src/switchover/handoff.rs`, `pgcluster/src/failover/mod.rs`

### 11. Failover history grows unbounded in Raft log
**Problem:** Each `RecordFailover` command appends to `failover_history` in the Raft
state machine with no cap. A cluster that experiences many failovers will have a
growing state snapshot.  
**Fix:** Cap `failover_history` at a configurable limit (e.g. 100 entries, oldest
first out) in the state machine `apply` handler.  
**Files:** `pgcluster/src/raft/state_machine.rs`

### 12. Switchover does not validate target is a Replica
**Problem:** Calling `POST /api/switchover {"target_node_id": "pg1"}` when pg1 is
already the primary, or when pg1 is `Offline`, will attempt the switchover anyway
and fail mid-way through (demote succeeds, promote fails), leaving the cluster in a
partial state.  
**Fix:** In `api/switchover.rs`, validate that `target_node_id != current_primary`,
that the target exists in topology, and that its role is `Replica` before spawning
the switchover task. Return `400 Bad Request` with a clear message otherwise.  
**Files:** `pgcluster/src/api/switchover.rs`

### 13. Replication password exposed in `postgresql.auto.conf`
**Problem:** `primary_conninfo` written to each replica's `postgresql.auto.conf` by
vk-agent's demote handler contains the plaintext replication password (e.g.
`password=test`). Anyone who can read PGDATA can see the credential.  
**Fix:** Use a `.pgpass` file owned by postgres (mode 600) instead of embedding the
password in `primary_conninfo`. Alternatively, configure `pg_hba.conf` to use `trust`
or `scram-sha-256` with a passfile, keeping the credential out of auto.conf.  
**Files:** `vk-agent/src/server.rs` (demote handler), `docker/init-replica.sh`

### 14. No end-to-end automated test
**Problem:** Switchover and failover are only validated manually. Regressions (like
the race condition and Unknown-role bugs fixed in this session) go undetected until
manual testing.  
**Fix:** Add an e2e test in `pgcluster/tests/` (or a separate `e2e/` crate) that:
1. Starts the Docker Compose stack
2. Runs `POST /api/switchover` and asserts the new primary is correct and both
   other nodes are standbys
3. Kills the primary container and asserts automatic failover promotes the right
   candidate and all nodes repoint correctly  
**Files:** new `e2e/` crate or `pgcluster/tests/e2e_integ.rs`

### 15. vk-agent PID namespace restart race
**Problem:** When a postgres container restarts, Docker sometimes fails to attach the
corresponding vk-agent to the new PID namespace, leaving vk-agent in a crash loop
until manually `docker start`-ed.  
**Fix:** Add a `depends_on` with `restart: on-failure` and a startup probe in
vk-agent (retry connecting to the local postgres pool for up to 30 s before entering
listen mode). Or decouple vk-agent from the shared PID namespace entirely and use
`pg_ctl stop` with the explicit PID file path instead of the shared namespace trick.  
**Files:** `docker/e2e-compose.yml`, `vk-agent/src/main.rs`

---

## Low — polish and observability

### 16. Prometheus alerting rules not defined
**Problem:** Metrics are exported (`pgcluster_failover_total`, `replica_lag_bytes`,
`raft_is_leader`) but no alerting rules ship with the project. Operators have no
default alerts for "primary has been offline for > 30s" or "replica lag > 100 MB".  
**Fix:** Add a `docker/prometheus-alerts.yml` with rules for: failover triggered,
no Raft leader, replica lag threshold breached, vk-agent unreachable.

### 17. `pg_hba.conf` managed outside postgres
**Problem:** `pg_hba.conf` is mounted as a static read-only file. Adding a new node
or changing auth rules requires touching the Docker volume mount and restarting the
container.  
**Fix:** Add `POST /api/pg-hba/reload` that calls `reload_config` (pg_ctl reload /
SIGHUP) on each vk-agent after writing a new `pg_hba.conf` snippet. This lets
operators update auth rules without a container restart.  
**Files:** `pgcluster/src/api/`, `vk-agent/src/server.rs`

### 18. Failover history timestamp is unix epoch, not human-readable in API
**Problem:** `triggered_at` in failover history is a raw `u64` unix timestamp.
Operators reading the API response have to convert it manually.  
**Fix:** Serialize as RFC 3339 string in the JSON API response.  
**Files:** `pgcluster/src/api/status.rs`, `pgcluster/src/failover/events.rs`

### 19. No `pgcluster` CLI end-to-end test
**Problem:** The CLI commands (`pgcluster switchover`, `pgcluster status`, etc.) in
`pgcluster/src/cli/` are tested only via unit tests. Integration with a running cluster
is not covered.  
**Fix:** Add integration tests in `pgcluster/tests/` that start the API server and
exercise CLI commands against it.

### 20. Raft gRPC Raft election timeout in `pgcluster-N.toml` is default
**Problem:** `election_timeout_ms` defaults to 500 ms but with three pgcluster nodes
all on the same host and `health_check_interval_ms = 1000`, the first health-check
cycle after a Raft leader loss can take up to 1.5 s before a new leader is elected
and failover can begin. Document recommended values and validate them in `validate.rs`.  
**Files:** `pgcluster/src/config/validate.rs`, `docker/configs/pgcluster-*.toml`
