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
| Create | `place`, which chooses a host, allocates ports and generates secrets, then `wait_applied`, then `activate` |
| Change | `wait_applied`, then `settle` |
| Delete | `retract`, then `wait_removed`, then `finalize`. It replaces any operation still running |

A step that must wait, such as for a host to apply a generation, retries each second. An operation fails after 10 minutes, and marks the database `error`, or `degraded` if the change was to one already serving. Every few seconds a sweep marks databases `degraded` when a node has failed or its host has stopped reporting, and clears that when they recover.

## Instance Certificates

Each database has its own CA, valid for 10 years, and each node has a certificate valid for 90 days. When 30 days remain, the reconciler issues a new one from the same CA as an ordinary change, with a new generation and an operation to track it. Hosts that are away are skipped until they return, and a database with an operation running waits its turn.

## What Stage 1 Refuses

`ha`, `backup` and a `replication_mode` other than `async` answer `422` until their stages land. Later-stage routes in the contract answer `404`.
