# Component: `pgcluster-admin`

## High-Level Function

`pgcluster-admin` holds what each database should look like, and hands that to the `vk-agent host` on each machine. It serves the admin API for operators and the agent API for hosts. [`openapi/`](../../openapi) holds both contracts, and [the management plane spec](../management-plane.md) explains the design.

It never decides who is primary, runs no SQL on user tables, and never sees a user's password. Clusters keep working while it is down.

## Running It

```sh
pgcluster-admin --config /etc/pgcluster-admin/config.toml
```

```toml
[api]
listen_addr = "0.0.0.0:8443"

[[api.keys]]                       # At least one key is required
name   = "ci"                      # Recorded in the audit log with each change it makes
sha256 = "<SHA-256 of the key, in hex>"

[agent_api]
listen_addr = "0.0.0.0:8444"

[tls]
ca_dir       = "/var/lib/pgcluster-admin/ca"   # ca.crt and ca.key, created if missing
server_names = ["admin.internal", "10.0.0.5"]  # The names hosts and clients use
```

It needs two environment variables, whose names `database_url_env` and `secret_key_env` can change:

| Variable | Holds |
|---|---|
| `PGCLUSTER_ADMIN_DATABASE_URL` | The metadata database. Migrations run at startup |
| `PGCLUSTER_ADMIN_SECRET_KEY` | A base64 32-byte key that encrypts instance secrets at rest. Generate it with `openssl rand -base64 32`, and back it up: without it, stored secrets cannot be read |
| `PGCLUSTER_ADMIN_OLD_SECRET_KEY` | Only while rotating that key. See [Rotating Secrets](#rotating-secrets) |

`pgcluster-admin generate-key` prints a new key and its hash, and `pgcluster-admin hash-key` hashes a key from standard input. The config holds only hashes, so a copy of it cannot call the API. Keys must be at least 32 characters: the tools refuse shorter ones, and the API never accepts them, even if a hash is configured.

`[api] api_key_hashes = ["..."]` still works, and names its keys `key-1`, `key-2` and so on, in order. `SIGHUP` reads the keys again, so a key is added or revoked by editing the config, with no restart and no effect on other keys. A config that fails to load leaves the old keys in use.

`[api] insecure_plaintext = true` serves the admin API without TLS, for local development only. `GET /health` is public, and answers `503` if the metadata database is unreachable.

`GET /health` and `POST /agent/v1/enroll` need no credentials, so they share a few slots, and answer `429` with `Retry-After` when those are full. A flood fails fast there, and cannot take the database connections that hosts and operators need.

## Connection Limits

Both listeners cap connections, so a flood can neither crowd out enrolled hosts nor, from one address, everyone else:

| Limit | Default | Applies to |
|---|---|---|
| Total | 4096 | Every connection |
| Handshakes | 256 | TLS handshakes in progress, each with a 10 second deadline |
| Anonymous | 512 | Connections without a client certificate |
| Per address | 32 | Connections without a client certificate, from one address |
| Header deadline | 30 seconds | Time to send a request's headers |

A connection over a limit is closed at once, before its handshake. A host's verified certificate takes its connection out of the anonymous and per-address caps, and a connection a host already holds keeps working whatever arrives after it. Admin API clients have no certificates, so the per-address cap is what keeps one address from taking every slot.

## Audit Log

Every request to the admin API that is not a read is recorded, whether it succeeded or not, with the name of the key that made it, the method, the path, the status it was answered with and the database it named:

```
GET /v1/audit-log?key=ci&database_id=...&page_token=...
```

Entries come oldest first. A failure to record a change is logged, and does not undo the change.

## Hosts and Enrolment

`POST /v1/hosts` returns a token, shown once. It is single-use, lasts 15 minutes, and is stored only as a hash. It has the form `<secret>.<SHA-256 of ca.crt>`, so a new host needs nothing else to trust the server: it fetches `GET /agent/v1/ca` over an unverified connection and accepts the CA only if the hash matches.

The host then sends the secret and a certificate request. `pgcluster-admin` signs a client certificate valid for 90 days, taking only the public key from the request: the identity is always the host's ID. Every later call uses mutual TLS, and handlers read the host from the certificate, so a host can only read or report its own state. Hosts replace their certificates after 30 days. Deleting a host revokes it, because each call checks that it still exists.

`[[bootstrap_hosts]]` creates hosts at startup from token hashes, so that tooling can enrol machines with no API key on them:

```toml
[[bootstrap_hosts]]
name              = "host-1"
token_sha256      = "<SHA-256 of the token's secret>"
token_ttl_minutes = 60
```

## Operations

A change stores the new spec, raises the database's `generation`, and records an operation made of steps. The reconciler claims one step at a time with `FOR UPDATE SKIP LOCKED`, and runs it inside that transaction, which also marks it done. So a crash rolls back a step's work and its claim together, and any number of `pgcluster-admin` processes can run side by side. A database runs one operation at a time.

| Operation | Steps |
|---|---|
| Create | `place`, `wait_applied`, `first_backup`, `open_access`, `wait_applied`, `activate` |
| Change | `wait_applied`, `settle` |
| Change a preload extension on three nodes | `wait_applied`, `rolling_restart`, `new_generation`, `wait_applied`, `settle` |
| Change the replication mode | `wait_applied`, `set_replication_mode`, `settle` |
| Restart, switchover, backup | The task, then `settle` |
| Replace a node | `wait_peers`, `replace_member`, `retract_old`, `wait_old_removed`, `drop_old`, `wait_applied`, `settle` |
| Rotate a database's CA | `ca_trust_both`, `ca_issue` and `ca_trust_new`, each followed by `wait_applied` |
| Rotate internal passwords | `passwords_offer`, `wait_applied`, `passwords_alter`, `passwords_commit`, `wait_applied`, `settle` |
| Resize | `wait_applied`, `resize_restart`, `settle`. The restart is a rolling restart task, and runs only when memory changes on three nodes |
| Minor upgrade | `upgrade_check`, `rolling_restart`, `wait_version`, `settle` |
| Create a replica | `place_replica`, `add_replica`, `wait_applied`, `activate` |
| Promote a replica | `detach_replica`, `promote`, `wait_applied`, `first_backup`, `open_access`, `wait_applied`, `activate` |
| Delete | `remove_replica`, which does nothing unless the database is a replica, then `retract`, `wait_removed`, `finalize`. It replaces any operation still running |

A step that must wait, such as for a host to apply a generation, retries each second. An operation fails after 10 minutes, or 6 hours if it moves data or restarts nodes: create, backup, restart, replace, resize, upgrade, and creating or promoting a replica. A failed create, delete, or replica creation or promotion marks the database `error`, and any other failure leaves it `degraded`, serving as it was. Every few seconds a sweep marks databases `degraded` when a node has failed, its host has stopped reporting, sync has fallen back to async, or backups are failing, and clears that when they recover.

On a three-node database, `wait_applied` skips a host that has gone offline. The database is serving without it, and the host reads its state when it returns.

## Tasks

`pgcluster-admin` never calls a host, and each instance's REST API listens only on loopback. So a step that needs the cluster to do something records a task, which every node's host receives with desired state. A task names where it runs: `leader`, the Raft leader, or `primary`, the Postgres primary. Each host checks that for itself, because either can move while the task waits. The first answer stands, so a task repeated after a node changed hands does no harm.

The host of the Raft leader also reports what the cluster knows: the primary, sync fallback, failovers and backups. That is a record for the API and the event feed. The cluster goes on deciding for itself, and none of it depends on `pgcluster-admin` being up.

## Three-Node Databases

`place` takes three online hosts with room, each in a failure domain of its own, and waits if there are not enough. A host with no domain counts as a domain of its own. Naming hosts in one domain is refused at once. [scheduler.md](scheduler.md) explains how hosts are chosen, how much of each is handed out, and how a host drains. The first node runs `initdb`. The others start empty, and the Raft leader seeds them from the primary.

`endpoint` is the first node on an online host, and `endpoints` lists every node's proxy. Each proxy routes writes to the primary, wherever it is.

`POST .../nodes/{node}/replace` adds the new node, seeds it, promotes it to a voter, and only then lets the old node go, so the cluster never runs short of voters. If the node leaving is the primary, it switches over first. The old host may be gone: nothing waits for it, and if it returns it removes the instance.

## Events

`GET /v1/events?after={cursor}` returns state changes in order, so tooling can react without polling every database:

| Type | When |
|---|---|
| `node_offline`, `node_online` | A node's host stops or resumes reporting |
| `replacement_needed` | A node of a three-node database has been gone for 5 minutes. Sent once |
| `failover_completed` | The cluster promoted a new primary |
| `sync_degraded`, `sync_restored` | `sync` fell back to async, and came back |
| `backup_completed`, `backup_failed` | The leader reported a new backup or a failure |
| `operation_failed` | An operation ended in failure |
| `host_draining`, `host_drained` | A drain began, and the host has no nodes left |

## Backups

`POST /v1/backup-repositories` registers an S3-compatible store or a directory every host can reach. The request waits while an online host writes to it through WAL-G, and fails with the host's reason if it cannot. S3 credentials are optional, because hosts can use an instance role, and are encrypted at rest and never returned. A repository in use cannot be removed.

Every database names a repository, so `backup` is required on create. Each gets its own prefix and its own encryption key, and data is encrypted on the node before upload. `retention_days` becomes a number of weekly full backups: enough to cover the period, and one more, so the window survives the deletion of the oldest.

A database stays `provisioning`, with an empty allowlist, until its first backup exists. A create whose backup fails ends in `error`, and no client has ever reached it.

`restore-window` runs from the end of the oldest backup to the last moment every commit is known to be in the WAL archive. `POST .../restore` accepts only times inside that window, and creates a new database with the source's version, owner, extensions and size. The source is not touched. Its backups stay usable while the source exists: deleting a database discards the key that decrypts them.

## Instance Certificates

Each database has its own CA, valid for 10 years, and each node has a certificate valid for 90 days. When 30 days remain, the reconciler issues a new one from the same CA as an ordinary change, with a new generation and an operation to track it. Hosts that are away are skipped until they return, and a database with an operation running waits its turn.

## Rotating Secrets

Each of these runs under client load, and restarts nothing that clients use.

| Secret | How | When the old one stops working |
|---|---|---|
| Instance internal passwords | `POST .../rotate-passwords`. Every node learns the new values, the primary moves the roles, then the new values become current | At once for the roles, and when the operation ends for the REST API key |
| A database's CA | `POST .../rotate-ca`. Nodes trust both CAs, move to certificates from the new one, then drop the old | When the operation ends. Clients must read `ca_cert_pem` again before then: during the rotation it holds both |
| The admin CA | `POST /v1/admin-ca/rotate`, followed with `GET /v1/admin-ca`. See below | When the rotation ends |
| The key that encrypts secrets | Set the new key in `PGCLUSTER_ADMIN_SECRET_KEY` and the old in `PGCLUSTER_ADMIN_OLD_SECRET_KEY`, and restart. Everything is sealed again in one transaction. Then remove the old key | When you remove it |

The admin CA moves in three phases, each once every enrolled host has caught up. In `trust`, hosts learn the new CA and the agent API accepts certificates from both. In `issue`, this service presents a certificate from the new CA, and hosts ask for new client certificates. In `trim`, nobody trusts the old CA, and `ca.crt` and `ca.key` are replaced. A host that stays offline holds the rotation open, in place of being locked out: delete it to let the rotation finish. API clients need `bundle_pem` from `GET /v1/admin-ca` before `issue`. An enrolment token carries the hash of one CA, so issue new ones after the rotation ends.

## Resizing and Minor Upgrades

`PATCH /v1/databases/{id}` with `resources` resizes every node. [scheduler.md](scheduler.md#resizing) says what applies online and what restarts.

`POST /v1/databases/{id}/upgrade` moves to a newer minor release of the same major. Install it on the database's hosts first, with a package upgrade or a new node image: running servers keep the old binaries until they restart. Each host reports the release each major has installed, and each node reports the release its server runs, which `GET /v1/databases/{id}` shows as `server_version` once every node agrees. With no body, the target is the newest release every one of the database's hosts has. A rolling restart then moves standbys first, switches over, and moves the old primary. A request for a release a host lacks, or for a major upgrade, is refused: a major upgrade restores into a new database.

## Cross-Region Replicas

```
POST   /v1/databases/{id}/replicas                         { name, host_id? }
GET    /v1/databases/{id}/replicas
DELETE /v1/databases/{id}/replicas/{replica_id}
POST   /v1/databases/{id}/replicas/{replica_id}/promote
```

A replica is an external standby of a three-node database, as [failover_engine.md](failover_engine.md#external-standbys) describes, run as a database of its own with one node. It lands on a host outside the source's failure domains, follows asynchronously, is not a Raft voter, and is never a failover target. It runs `vk-agent` and Postgres, and no `pgcluster`: the source's leader drives it. It shares the source's CA and internal passwords, because its data holds the same roles. So while a database has replicas, it refuses `rotate-ca`, `rotate-passwords` and deletion, and a replica refuses every change but promotion and deletion. A one-node source cannot have replicas, because its nodes listen only on loopback: restore it into another region instead.

`promote` detaches the replica: the source's leader has it promote and forgets it, and it becomes a one-node database with a Raft group of its own. Its data holds the source's database under the source's name, so its host renames that to the replica's name before clients are let in. It keeps what it had replayed, so commits in flight on the source may be missing. As with a create, clients reach it only once its first backup exists. It needs the same Postgres major, which it has by construction, and the same CPU architecture, which is the operator's to ensure.

## Custom Proxy Certificate

`PUT /v1/databases/{id}/certificate` with `certificate_pem` and `private_key_pem` has every node's proxy present that certificate, such as one from a public CA for the database's own name. The key must belong to the certificate and the certificate must be in date. Proxies load it on SIGHUP, with no restart. Raft, agents and Postgres keep the database's own CA, and `endpoint.ca_cert_pem` still names that CA, so clients of a custom certificate verify it against its own issuer.
