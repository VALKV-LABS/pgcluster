# Architecture

This document describes every process in pgcluster, the ports each one opens, and how data and commands move between them for each operation. The [management plane spec](management-plane.md) gives the reasons for the design, and [`docs/components/`](components/index.md) describes each component in depth.

pgcluster has two layers:

- **The data plane** runs one Postgres cluster: `pgcluster`, `vk-agent` and Postgres on each node. It fails over, proxies clients and takes backups on its own.
- **The management plane** runs many clusters: `pgcluster-admin`, with its metadata database, and `vk-agent host` on each machine. It creates, changes and deletes clusters, and never decides who is primary.

## Processes

| Process | Binary | Runs | Decides | Talks to |
|---|---|---|---|---|
| `pgcluster` | `pgcluster server` | One per node | Raft membership, which node is primary, failover, switchover, backup schedule, client routing | Other `pgcluster` instances over Raft, each node's `vk-agent` over gRPC, Postgres through its proxy |
| `vk-agent` (instance mode) | `vk-agent <config>` | One per node, owning that node's Postgres | Nothing. It carries out what the Raft leader asks | Its Postgres, WAL-G, and other agents' Postgres when it seeds or rewinds |
| Postgres | `postgres` | One per node, started by `vk-agent` | Nothing beyond Postgres itself | Standbys stream from the primary. `archive_command` runs `wal-g wal-push` |
| WAL-G | `wal-g` | Run by `vk-agent` and `archive_command` | Nothing | Object storage or a shared directory |
| `pgcluster-admin` | `pgcluster-admin` | One per fleet | What each database should look like, where its nodes go, and the steps of each operation | Its metadata Postgres. It never calls a host |
| Metadata Postgres | `postgres` | One per fleet | Nothing | Only `pgcluster-admin` |
| `vk-agent host` | `vk-agent host <config>` | One per machine | Nothing about clusters. It makes its machine match desired state | `pgcluster-admin`, by dialling out. Each local instance's REST API on loopback. systemd, nftables and XFS quotas |
| `pgcluster` CLI | `pgcluster <command>` | On an operator's machine | Nothing | The admin API and a cluster's REST API |
| Console | `pgcluster console` | On an operator's machine | Nothing | The admin API and one cluster's REST API, on the operator's behalf |
| Coordinator | `pgcluster coordinator` | Optional, for sharding | Which shard holds a key | Shard clusters' proxies. See [coordinator.md](components/coordinator.md) |

```
 operator: CLI, console or tooling
        │  admin API, HTTPS, bearer key
        ▼
 pgcluster-admin ─────────── metadata Postgres
        ▲
        │  agent API, HTTPS with mutual TLS, opened by the host
        │
 ┌──────┴────────────────────── host ──────────────────────────────┐
 │  vk-agent host      enrols, applies desired state, runs tasks   │
 │     │ systemd units, nftables, XFS quota, files                 │
 │     ▼                                                           │
 │  ┌──────────── instance (one per node, one slice each) ───────┐ │
 │  │  pgcluster   Raft, failover, proxy, REST API, metrics      │ │
 │  │     │ gRPC, mutual TLS                                     │ │
 │  │     ▼                                                      │ │
 │  │  vk-agent ── Postgres ── WAL-G ──────────► object storage  │ │
 │  └────────────────────────────────────────────────────────────┘ │
 └──────────────────────────────────────────────────────────────────┘
        ▲                                   ▲
 clients ┘ TLS to the proxy                  └ peers: Raft, gRPC, replication
```

## Ports

### Management plane

| Process | Port | Default | Reachable from | Protocol and authentication |
|---|---|---|---|---|
| `pgcluster-admin` admin API | `[api] listen_addr` | `8443` | Operators and tooling | HTTPS. Bearer API key, named in the audit log. `GET /health` is open |
| `pgcluster-admin` agent API | `[agent_api] listen_addr` | `8444` | Hosts | HTTPS. A client certificate from the admin CA, except to enrol |
| Metadata Postgres | Its own | `5432` | `pgcluster-admin` only | Postgres |
| `vk-agent host` | None | | | Dials out only |
| Console | `--listen` | `127.0.0.1:8080` | The operator's browser, on loopback names only | HTTP |

### An instance that `pgcluster-admin` runs

Each instance takes a block of ten ports from its `port_base`, allocated by the scheduler from `20000` up:

| Offset | Service | Bound to | Reachable from |
|---|---|---|---|
| 0 | Proxy, for clients | All addresses | The database's allowlist, once it has a backup |
| 1 | Proxy admin | Loopback | The host |
| 2 | Postgres | All addresses with peers, else loopback | The database's own nodes and replicas |
| 3 | Raft | All addresses with peers, else loopback | The database's own nodes |
| 4 | `vk-agent` gRPC | All addresses with peers, else loopback | The database's own nodes, and its source's nodes for a replica |
| 5 | Reserved | | Nobody |
| 6 | Proxy health | Loopback | The host |
| 7 | REST API | Loopback | The host, where `vk-agent host` runs tasks through it |
| 8 | Metrics | Loopback | The host |
| 9 | Reserved | | Nobody |

nftables on each host enforces the last column, one table per instance. Postgres also only accepts its own nodes in `pg_hba.conf`, over TLS, and never sees clients: the proxy does.

### A cluster run by hand

The defaults when `pgcluster.toml` leaves them out, as in `make start`:

| Service | Default |
|---|---|
| Proxy | `0.0.0.0:5432` |
| Proxy admin | `0.0.0.0:5433` |
| Proxy health | `0.0.0.0:8008` |
| REST API | `0.0.0.0:8009` |
| Metrics | `0.0.0.0:9190` |
| Raft | The address in `[raft.peers]` |
| `vk-agent` gRPC | `0.0.0.0:7001` |

## Trust

| Link | How it is secured |
|---|---|
| Operator to admin API | TLS from the admin CA, and an API key of at least 32 characters, stored as a hash. SIGHUP reloads the keys |
| Host to agent API | Mutual TLS. Each host's certificate names its ID, so it can read and report only its own state |
| Enrolment | A single-use token that expires in 15 minutes and ends in the hash of the admin CA, so a new host can check the server with nothing else |
| `pgcluster` to `vk-agent`, Raft | Mutual TLS from the database's own CA |
| Proxy to Postgres | TLS, verified against the database's CA |
| Client to proxy | TLS required. The proxy checks the password against the stored SCRAM verifier |
| Secrets at rest in `pgcluster-admin` | Sealed with the key in `PGCLUSTER_ADMIN_SECRET_KEY` |
| Backups | Encrypted on the node with a key per database, before upload |

Connection limits protect each listener from floods: the admin and agent APIs cap connections per address and in total, and let hosts with a valid certificate out of the caps strangers share. The proxy caps clients that have not logged in yet, per address and in total, with a deadline. See [pgcluster_admin.md](components/pgcluster_admin.md#connection-limits) and [proxy_layer.md](components/proxy_layer.md#connection-limits).

## How the Management Plane Applies a Change

Every change follows the same path:

```
 API request
   → pgcluster-admin stores the new spec, raises the database's generation,
     raises each affected host's generation, and records an operation of steps
   → the host's long poll of GET /agent/v1/desired-state returns the new state
   → vk-agent host writes files, starts, reloads or restarts processes
   → it reports observed_generation through POST /agent/v1/report
   → the reconciler's wait_applied step sees every node at the generation
   → the next step runs, until the operation succeeds
```

The reconciler claims one step at a time with `FOR UPDATE SKIP LOCKED`, inside a transaction that also marks it done, so a crash repeats nothing and several `pgcluster-admin` processes can run. A database runs one operation at a time.

Work only a cluster can do, such as a switchover, is a **task**. `pgcluster-admin` cannot call a host, so the task travels with desired state to every node of the database, and the host whose node is the Raft leader, or the primary, runs it through the instance's REST API on loopback, then posts the result to `POST /agent/v1/tasks/{id}/result`.

The host of the Raft leader also reports what the cluster knows through `POST /agent/v1/cluster`: the primary, sync fallback, failovers and backups. That feeds the API and the event feed. The cluster decides all of it for itself.

## Data Flows: Management Plane

### Enrol a host

1. `POST /v1/hosts` returns a single-use token.
2. `vk-agent host` fetches `GET /agent/v1/ca` without verification, and accepts the CA only if its hash matches the token.
3. It sends the token and a certificate request to `POST /agent/v1/enroll`, and receives a client certificate naming its host ID.
4. It reports its address, capacity and installed Postgres releases, and long-polls desired state.

### Create a database

1. `POST /v1/databases` checks the request, and the scheduler checks the fleet can hold it. See [scheduler.md](components/scheduler.md).
2. `place` chooses hosts, allocates port blocks, generates the database's CA, node certificates, internal passwords and backup key, and publishes the instances.
3. Each host writes the instance's files, applies its slice, quota and firewall, and starts `vk-agent` and `pgcluster`. The first node runs `initdb --data-checksums`. Other nodes start empty, and the Raft leader's monitor seeds them from the primary with `pg_basebackup` through their agents.
4. The first node's host creates the owner role from its SCRAM verifier, the database and the extensions.
5. `first_backup` runs a backup task on the Raft leader, which has a standby's agent run `wal-g backup-push`.
6. `open_access` publishes the allowlist, and the proxy and the firewall let clients in. `activate` marks the database `active`.

### Change settings

| Change | Path |
|---|---|
| Allowlist | New `pgcluster.toml` and SIGHUP to the proxy, and new nftables rules. Connected clients stay |
| Extensions | `CREATE EXTENSION` or `DROP EXTENSION` on the primary as the superuser. A preload extension on three nodes takes a rolling restart task first |
| Owner password | `ALTER ROLE` with the new verifier on the primary |
| Replication mode | A `replication_mode` task on the leader, which records the mode through Raft and sets `synchronous_standby_names` |
| Retention | A new `retain_full` in `pgcluster.toml`, read on SIGHUP |
| Resize | New slice limits and quota, at once. New memory also restarts: on three nodes, a rolling restart task |
| Custom proxy certificate | New files and SIGHUP. Only the proxy presents it |

### Switchover, restart and minor upgrade

- **Switchover**: a task on the leader calls `POST /api/switchover`. The proxies drain, the target catches up, the leader promotes it through its agent and demotes the old primary.
- **Rolling restart**: a task on the leader calls `POST /api/restart`, which restarts each standby through its agent, switches over, then restarts the old primary.
- **Minor upgrade**: install the release on the hosts first. Each host reports its installed releases, `upgrade_check` confirms every host has the target, a rolling restart moves each node onto the new binaries, and `wait_version` waits until every node's server reports the release.

### Replace a node, drain a host

1. A new node is published on the new host as `seed`, and every peer learns of it.
2. A `replace_member` task on the leader adds it as a Raft learner and a cluster node. The monitor seeds it from the primary.
3. Once it streams, the leader promotes it to a voter, switches over if the old node is primary, and removes the old node and its slot.
4. The old node is retracted from its host's desired state, and the host stops and deletes it.

A drain marks a host, and the reconciler replaces its nodes one database at a time onto other hosts, through these same steps.

### Back up and restore

- **Backups**: the leader's scheduler asks a standby's agent to run `wal-g backup-push`, weekly full and daily delta, and records each manifest through Raft. `archive_command` on whichever node is primary runs `wal-g wal-push`. The primary's host reports how far the archive reaches, which ends the restore window.
- **Restore**: `POST .../restore` creates a new database whose first node has `bootstrap: restore`. Its host runs `wal-g backup-fetch` from the source's location, with the source's key, recovers to the target time in a private server with no network, resets internal passwords and names, then starts normally and archives to its own location.

### Cross-region replica

1. `POST .../replicas` creates a database that is a replica of a three-node source. `place_replica` puts its one node on a host outside the source's failure domains. It shares the source's CA and internal passwords, which its data will hold, and gets a backup key of its own.
2. The source's nodes learn the replica's address, so their firewalls admit it to Postgres, and the replica admits the source's nodes to its agent. The replica runs `vk-agent` and Postgres, and no `pgcluster`.
3. An `add_replica` task on the source's leader adds it as an external standby. The monitor seeds it from the primary, and it follows asynchronously, outside Raft and failover.
4. `promote` runs a `detach_replica` task: the leader has the replica's agent promote it, and forgets it. The replica's instance then becomes a one-node database: `pgcluster` starts a Raft group of one on the data, takes a first backup, and opens to clients.

### Rotate secrets

- **A database's CA**: nodes trust both CAs, move to certificates from the new one, then drop the old, each step a generation that hosts apply with SIGHUP.
- **Internal passwords**: nodes learn the next passwords, a task on the primary moves the roles, then the next passwords become current.
- **Node certificates**: renewed by the reconciler 30 days before they expire, as an ordinary change.
- **The admin CA**: hosts trust both, move their client certificates, then drop the old.

### Delete a database

The node rows are retracted from desired state, each host stops its instance, removes its firewall table and quota and deletes its directory, and `finalize` discards the database's secrets, including the key to its backups. A replica first leaves its source's cluster.

## Data Flows: Data Plane

### A client query

```
client ──TLS──► proxy (any node)
                 │ authenticates the client from the stored SCRAM verifier
                 │ logs in to the primary as the client
                 ▼
          writes: the primary, wherever it is
          reads:  replicas, when read routing is on and the session allows
```

The proxy reads the topology from Raft locally, with no network hop. A proxy that loses quorum stops routing writes before the majority could promote a new primary.

### Health and failover

1. The Raft leader's node monitor polls every node's agent over gRPC: role, WAL position, lag, and whether it streams.
2. After enough failed checks of the primary, the failover engine asks each reachable standby for its live position, and picks the most current. With `sync_strict`, it promotes only if every standby answers.
3. It promotes the candidate through its agent, records the new primary through Raft, and repoints the other standbys. Proxies follow the topology at once.
4. An old primary that comes back is rewound with `pg_rewind`, or seeded again if that fails.

A primary that loses its heartbeat from the leader fences itself, so two primaries never take writes.

### Replication modes

| Mode | `synchronous_standby_names` | With every standby down |
|---|---|---|
| `async` | Empty | No effect |
| `sync` | `ANY 1 (...)` | Falls back to async after `sync_degrade_timeout`, recorded through Raft first |
| `sync_strict` | `ANY 1 (...)` | Writes wait |

## Where State Lives

| State | Owner | Where |
|---|---|---|
| What a database should look like, operations, events, audit log | `pgcluster-admin` | The metadata Postgres |
| Instance secrets | `pgcluster-admin` | Sealed in the metadata Postgres, and on each host in files only the instance's user can read |
| What a host runs | `vk-agent host` | Its `state_dir`, including the last desired state, so it carries on while `pgcluster-admin` is away |
| Primary, membership, failover history, backup manifests, replication mode | The cluster's Raft group | Each `pgcluster` instance's Raft log |
| Data and WAL | Postgres | Each node's data directory, and the backup repository |
