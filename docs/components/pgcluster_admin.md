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
api_key_hashes = ["<SHA-256 of the key, in hex>"]   # Required. There is no way to run without.

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

`[api] insecure_plaintext = true` serves the admin API without TLS, for local development only. `GET /health` is public, and answers `503` if the metadata database is unreachable.

`GET /health` and `POST /agent/v1/enroll` need no credentials, so they share a few slots, and answer `429` with `Retry-After` when those are full. A flood fails fast there, and cannot take the database connections that hosts and operators need.

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
| Delete | `retract`, `wait_removed`, `finalize`. It replaces any operation still running |

A step that must wait, such as for a host to apply a generation, retries each second. An operation fails after 10 minutes, or 6 hours if it moves data: create, backup, restart and replace. A failed create or delete marks the database `error`, and any other failure leaves it `degraded`, serving as it was. Every few seconds a sweep marks databases `degraded` when a node has failed, its host has stopped reporting, sync has fallen back to async, or backups are failing, and clears that when they recover.

On a three-node database, `wait_applied` skips a host that has gone offline. The database is serving without it, and the host reads its state when it returns.

## Tasks

`pgcluster-admin` never calls a host, and each instance's REST API listens only on loopback. So a step that needs the cluster to do something records a task, which every node's host receives with desired state. A task names where it runs: `leader`, the Raft leader, or `primary`, the Postgres primary. Each host checks that for itself, because either can move while the task waits. The first answer stands, so a task repeated after a node changed hands does no harm.

The host of the Raft leader also reports what the cluster knows: the primary, sync fallback, failovers and backups. That is a record for the API and the event feed. The cluster goes on deciding for itself, and none of it depends on `pgcluster-admin` being up.

## Three-Node Databases

`place` takes three online hosts, each in a failure domain of its own, and waits if there are not enough. A host with no domain counts as a domain of its own. Naming hosts in one domain is refused at once. The first node runs `initdb`. The others start empty, and the Raft leader seeds them from the primary.

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

## Later Stages

Resizing answers `not_supported`. Other later-stage routes in the contract answer `404`.
