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
| `firewall` | `nftables` | `nftables` needs `nft` and root. `none` leaves ports open, for development on one machine |
| `walg_path` | `wal-g` | The WAL-G binary |
| `disk_quota` | `auto` | `xfs` caps each instance's disk with an XFS project quota, and refuses to start without one. `auto` does so when `instances_dir` is on XFS mounted with `prjquota`, and logs a warning otherwise. `none` sets no quota, for development |

`VK_AGENT_ENROLLMENT_TOKEN` can carry the token in place of the file. A refused token stops the agent, because retrying cannot help. An unreachable server is retried.

## What It Does

1. **Enrols**, once, and keeps its certificate in `state_dir`.
2. **Long-polls** desired state, and saves it with mode `600`, because it holds secrets.
3. **Applies** each instance. Every step checks before it acts, so a failed attempt is safe to repeat, and is repeated after the next poll.
4. **Reports** capacity, the Postgres release each major has installed, and instance health, with the release each server runs, on a timer of its own, so a slow change does not look like a dead host.
5. **Runs tasks** that reach it with desired state, each on its own, through the local instance's REST API. Each goes to every node, and runs on the node that is the Raft leader or the Postgres primary.
6. **Watches** each cluster: the leader's host reports what the leader knows, and the primary's host reports how far the WAL archive reaches.

Between changes it checks that instances are running, and starts any that stopped. If `pgcluster-admin` is unreachable, instances keep running, and a restarted agent applies its saved state.

## Applying an Instance

Each instance is two processes: `vk-agent`, which owns Postgres, and `pgcluster`. A cross-region replica runs `vk-agent` alone, because its source's cluster drives it. The ports come from the block in [§9 of the spec](../management-plane.md#ports). Before a new instance's first start, the host checks that nothing else listens on any port of its block, and reports the instance as failed, naming the ports, if something does.

| Change | Effect |
|---|---|
| New first node | Writes its files, runs `initdb --data-checksums`, starts it, then creates the roles, the database and the extensions |
| New seeded node | Writes its files and starts its processes with no data. It reports `seeding`, and the host moves on, until the Raft leader has filled it from the primary |
| New restored node | Fills the data directory from the source's repository, finishes recovery in a private server, then starts it. See below |
| `allowed_cidrs` | Rewrites `pgcluster.toml` and sends SIGHUP. Connected clients stay connected |
| Certificates, CAs, passwords, peers | Replaces the files, sends SIGHUP to `vk-agent` and `pgcluster`, and runs `pg_ctl reload`. Nothing restarts, and connected clients stay connected |
| Extensions with no preload | `CREATE EXTENSION` or `DROP EXTENSION`, as the superuser, so the owner never needs to be one |
| `pg_tle`, `pg_cron`, memory | Rewrites `postgresql.conf` and restarts a one-node instance. On three nodes, `pgcluster-admin` runs a rolling restart and then a new generation, which installs the extension |
| vCPU, memory, storage | New limits on the instance's slice, and a new disk quota, at once. See [Resource Limits](#resource-limits) |
| Custom proxy certificate | Writes `tls/proxy.crt` and `tls/proxy.key`, names them in `pgcluster.toml`, and sends SIGHUP |
| Replica promoted | Drops the peers, starts `pgcluster`, which forms a Raft group of one on the data, and closes the peer ports |
| Owner verifier | `ALTER ROLE`, only when the verifier in desired state changed, so a later change cannot undo a password the owner set with SQL |
| Gone from desired state | Stops the instance and deletes its directory |

A one-node instance keeps everything but the proxy on loopback. A node with peers listens for them on Postgres, Raft and `vk-agent`, and the REST API and metrics stay on loopback either way. The proxy requires TLS and applies the allowlist, because it is the only process that sees a client's address.

Postgres runs from `postgresql.conf` and `pg_hba.conf` in the instance directory, not the data directory, because a base backup or `pg_rewind` copies a data directory from another node. The file in the data directory holds what `initdb` chose, and nothing specific to a node.

Only the primary runs `ALTER ROLE` and `CREATE EXTENSION`, and replication carries them to standbys. A standby keeps its old record of what it applied. If it is promoted before the primary finished a change, the health check notices and finishes it.

## Firewall

With `firewall = "nftables"`, each instance has a table of its own, replaced in one transaction, so a change opens no gap. Postgres, Raft and `vk-agent` ports accept only the database's own nodes. The proxy port accepts the allowlist, and stays closed until the first backup exists. Connections already open to the proxy survive an allowlist change. Loopback is always accepted. Deleting an instance deletes its table.

## Resource Limits

So that one busy database cannot starve another on a shared host, each instance runs in a systemd slice of its own, `vk-instance-<id>.slice`, which holds both of its units and Postgres:

| Setting | From |
|---|---|
| `CPUQuota=` | `vcpu × 100%` |
| `MemoryMax=` | `memory_gb`. Postgres gets a quarter of it for `shared_buffers` |
| `IOWeight=` | `vcpu × 100`, so disk bandwidth under contention follows size |

A change rewrites the slice, and `systemctl set-property --runtime` applies it to the running slice at once. CPU, memory and IO limits do nothing for disk space, so each instance's directory also gets an XFS project quota of `storage_gb`, set once on the directory tree and raised in place when storage grows. Deleting an instance lifts its quota.

The `plain` runner sets neither. It is for development, where one machine runs everything.

## Restoring

A backup holds the source's roles, its archiving settings and its name for the database. So recovery runs in a private server with no network, reached through a socket only the instance's user can open, and all of that is put right before anything else can connect:

1. `backup-fetch` the newest backup that finished before the target, from the source's repository with the source's key.
2. Recover to the target with archiving off, so nothing lands in the source's archive.
3. Set every internal password and the owner's verifier, rename the database, drop the source's slots, and clear the settings the backup carried.
4. Start normally. `pgcluster` then points archiving at this database's own repository.

Postgres stops with an error if WAL ends before a commit later than the target. The API admits only times the archive covers, so in that case every commit in the archive came before the target, and the end of the archive is the state asked for. The agent recovers to there.

An instance directory records the ID of the host that created it, and deletion acts only on a matching directory. A host that enrols again, and so receives empty desired state, leaves existing data alone and logs a warning.

## Process Runners

| Runner | For | How |
|---|---|---|
| `systemd` | Production | Two units per instance in a slice with its limits, enabled so they return after a reboot. Postgres shares the agent's control group, and `KillSignal=SIGINT` gives it a fast shutdown |
| `plain` | Development and tests | Detached processes with pid files, and logs in the instance's `logs/` directory. A pid is trusted only if that process still runs the instance's config |

Both let this agent restart without touching instances.
