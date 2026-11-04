# Component: Config Manager (`config`)

## High-Level Function

The config manager loads `pgcluster.toml`, checks it as a whole before anything starts, and applies what can change while the server runs. A config that fails validation stops the server at startup with a message naming the setting. `pgcluster config validate` runs the same checks without starting anything.

Restarting `pgcluster` would drop the clients of its proxy, so everything that can change at runtime is read again on `SIGHUP`, which is also what `vk-agent host` sends when it rewrites the file. A file that fails to load on `SIGHUP` leaves the old values in use, and logs the error.

## Sections

| Section | Holds | Changes with no restart |
|---|---|---|
| `[cluster]` | Name, data directory, `secrets_file` | `secrets_file` contents, on SIGHUP |
| `[raft]` | This node's ID, whether it bootstraps the group, peers, timeouts | No. Membership changes through the REST API |
| `[[nodes.node]]` | Each node's agent and Postgres addresses, priority and tags | No. Nodes are added and removed through the REST API, and recorded in Raft |
| `[replication]` | Replication user and password variable, slot prefix, `replication_mode`, `sync_degrade_timeout_seconds`, `max_slot_wal_keep_size_mb` | The mode, through `POST /api/replication/mode`, recorded in Raft |
| `[proxy]` | Listen addresses, pool sizes, read routing, the auth role, `quorum_lease_ms`, `allowed_cidrs`, `require_tls`, connection limits | `allowed_cidrs`, on SIGHUP |
| `[failover]` | Health check interval and threshold, timeouts, self-fencing | No |
| `[tls]` | `ca_cert`, `cert`, `key`, an optional `proxy_cert` and `proxy_key`, and `insecure_plaintext` | Every file and path, on SIGHUP. See [tls_manager.md](tls_manager.md#reloading-certificates) |
| `[metrics]` | Listen address | No |
| `[api]` | Listen address, `api_keys` | `api_keys`, on SIGHUP |
| `[backup]` | Schedule, retention, encryption key variable, repository | `retain_full`, on SIGHUP |

Passwords never sit in the config itself: it names the environment variables that hold them, and `secrets_file` can hold those variables in a file only the instance's user can read.

## Validation

Validation checks the config as a whole, not only each value:

| Check | Why |
|---|---|
| Every address parses | A typo should fail at startup, not at the first failover |
| This node's Raft ID is among the peers, and node IDs are unique | A node must find itself |
| The REST API has at least one key, unless a development flag is set | No unauthenticated control of the cluster |
| Certificates are configured, unless `insecure_plaintext` is set, which cannot be combined with them | mTLS on every gRPC endpoint |
| `quorum_lease_ms` is shorter than failover detection | An isolated proxy stops before a new primary can be promoted |
| Self-fencing fires before failover could complete | A cut-off primary stops taking writes first |
| `require_tls` has certificates to use | |
| Connection limits and the login timeout are above zero | |
| `[backup]` names exactly one repository | |

## Module Layout

```
pgcluster/src/config/
  mod.rs        # PgClusterConfig and its sections, with serde defaults
  validate.rs   # Checks across sections, run at startup and by `config validate`
  runtime.rs    # API keys and retention that SIGHUP replaces
pgcluster/src/proxy/allowlist.rs   # The allowlist that SIGHUP replaces
pgcluster/src/lib.rs               # SIGHUP handlers for settings and certificates
```

## Integration Points

- `rest_api` serves the configuration, without secrets, on `GET /api/config`.
- `vk-agent host` renders the whole file for each instance it runs, and sends SIGHUP when it changes. See [host_agent.md](host_agent.md).
