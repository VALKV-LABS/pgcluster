# Component: Host Agent (`vk-agent host`)

## High-Level Function

`vk-agent host` is a mode of the `vk-agent` binary. One runs on each machine, before any Postgres exists, and makes the machine match what `pgcluster-admin` asks for. It dials out and opens no port, so hosts behind NAT work like any other. `vk-agent <config>` still runs instance mode, unchanged.

It takes no part in failover: each cluster's Raft group decides that.

## Running It

```sh
vk-agent host /etc/vk-agent/host.toml
```

```toml
admin_url             = "https://admin.internal:8444"
enrollment_token_file = "/etc/vk-agent/enrollment-token"   # Only the first start needs it
runner                = "systemd"                          # or "plain"
run_as                = "postgres"                         # When this agent runs as root
```

| Setting | Default | Meaning |
|---|---|---|
| `state_dir` | `/var/lib/vk-agent-host` | This host's identity and last desired state |
| `instances_dir` | `/var/lib/pgcluster/instances` | One directory per instance |
| `advertise_address` | The route to `admin_url` | What clients and peers use to reach this host |
| `pg_bin_template` | `/usr/lib/postgresql/{version}/bin` | Each major's programs |
| `pgcluster_path` | `/usr/local/bin/pgcluster` | The `pgcluster` binary |
| `vk_agent_path` | This executable | The `vk-agent` binary |
| `systemd_unit_dir` | `/etc/systemd/system` | Where units go |
| `report_interval_seconds` | `15` | Time between reports, and the length of a long poll |

`VK_AGENT_ENROLLMENT_TOKEN` can carry the token in place of the file. A refused token stops the agent, because retrying cannot help. An unreachable server is retried.

## What It Does

1. **Enrols**, once, and keeps its certificate in `state_dir`.
2. **Long-polls** desired state, and saves it with mode `600`, because it holds secrets.
3. **Applies** each instance. Every step checks before it acts, so a failed attempt is safe to repeat, and is repeated after the next poll.
4. **Reports** capacity and instance health on a timer of its own, so a slow change does not look like a dead host.

Between changes it checks that instances are running, and starts any that stopped. If `pgcluster-admin` is unreachable, instances keep running, and a restarted agent applies its saved state.

## Applying an Instance

Each instance is two processes: `vk-agent`, which owns Postgres, and `pgcluster`. The ports come from the block in [§9 of the spec](../management-plane.md#ports).

| Change | Effect |
|---|---|
| New instance | Writes its files, runs `initdb --data-checksums`, starts it, then creates the roles, the database and the extensions |
| `allowed_cidrs` | Rewrites `pgcluster.toml` and sends SIGHUP. Connected clients stay connected |
| Extensions with no preload | `CREATE EXTENSION` or `DROP EXTENSION`, as the superuser, so the owner never needs to be one |
| `pg_tle`, `pg_cron`, memory | Rewrites `pgcluster-managed.conf` and restarts the instance |
| Owner verifier | `ALTER ROLE`, only when the verifier in desired state changed, so a later change cannot undo a password the owner set with SQL |
| Gone from desired state | Stops the instance and deletes its directory |

A one-node instance keeps everything but the proxy on loopback. The proxy requires TLS and applies the allowlist, because it is the only process that sees a client's address.

An instance directory records the ID of the host that created it, and deletion acts only on a matching directory. A host that enrols again, and so receives empty desired state, leaves existing data alone and logs a warning.

## Process Runners

| Runner | For | How |
|---|---|---|
| `systemd` | Production | Two units per instance, enabled so they return after a reboot. Postgres shares the agent's control group, and `KillSignal=SIGINT` gives it a fast shutdown |
| `plain` | Development and tests | Detached processes with pid files, and logs in the instance's `logs/` directory. A pid is trusted only if that process still runs the instance's config |

Both let this agent restart without touching instances.
