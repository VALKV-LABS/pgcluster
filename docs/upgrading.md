# Upgrading

This guide covers moving a deployment from `v0.2.0` to `v1.0.0`. Read the [changelog](../CHANGELOG.md) for everything that changed. Try the upgrade on a copy of your deployment first: `make try` brings up the full stack locally.

`v0.2.0` holds the data plane: `pgcluster` and `vk-agent` running one cluster. `v1.0.0` adds the management plane, `pgcluster-admin` and `vk-agent host`, which is new and needs no upgrade. A cluster run by hand keeps running by hand: nothing in `v1.0.0` requires the management plane.

## Before You Start

- Take a backup, and check that `GET /api/backups/verify` reports no gaps.
- Check that every node is healthy: `pgcluster status` shows a primary and every standby streaming.
- Keep the `v0.2.0` binaries, so you can roll a node back.

## A Cluster Run by Hand

The upgrade replaces `pgcluster` and `vk-agent` on one node at a time, so the cluster keeps quorum and clients keep a primary throughout.

### Config

Every new setting has a default, so a `v0.2.0` config loads unchanged. Run `pgcluster config validate <config>` with the new binary on each node's config before you start. New settings you may want:

| Setting | Default | What it does |
|---|---|---|
| `[proxy] max_client_connections` | 10000 | Caps client connections |
| `[proxy] max_pending_logins` | 512 | Caps connections that have not logged in yet |
| `[proxy] max_pending_logins_per_address` | 64 | The same, from one address. Raise it if many clients log in at once from one address, such as a large application server behind NAT |
| `[proxy] login_timeout_ms` | 10000 | Time a client has to negotiate TLS and log in |
| `[tls] proxy_cert`, `proxy_key` | None | A certificate the proxy presents in place of the node's |

### Order

1. For each standby, one at a time:
   1. Put it in maintenance, so failover leaves it alone: `POST /api/maintenance/<node>` on the REST API.
   2. Stop its `pgcluster` and its `vk-agent`, which stops its Postgres. Replace both binaries, and start them again.
   3. Wait until it streams again, then take it out of maintenance: `DELETE /api/maintenance/<node>`.
2. Switch over to an upgraded standby: `pgcluster switchover <node>`. Clients of the proxies see one brief pause.
3. Upgrade the old primary, now a standby, the same way.

A `pgcluster` instance that restarts drops the clients of its own proxy, so restart one node's proxy at a time, and point clients at more than one node.

### Rolling Back

Repeat the same order with the `v0.2.0` binaries. Do it before you use anything new in `v1.0.0`, such as `proxy_cert`, which `v0.2.0` would refuse.

## The Management Plane

There is nothing to upgrade the first time. To bring it to an existing fleet, set up `pgcluster-admin` as in [pgcluster_admin.md](components/pgcluster_admin.md), enrol each host with `vk-agent host`, and create new databases through the API. Clusters run by hand cannot be adopted by `pgcluster-admin`: restore their backups into new databases, or replicate logically, then move clients.

### Later Upgrades of the Management Plane

`pgcluster-admin` supports `vk-agent host` from its own minor release and the one before, so hosts can follow one at a time:

1. **`pgcluster-admin` first.** Stop it, replace the binary, and start it. Migrations run at startup. Clusters keep working while it is down, and hosts keep their last desired state.
2. **Each host's `vk-agent host`.** Replace the binary and restart the service. Instances keep running: their units belong to systemd, not to the host agent.
3. **Each instance's `pgcluster` and `vk-agent`.** They keep running the old binaries until restarted, so restart them host by host:
   1. On a three-node database, if the node on this host is primary, switch it over first with `POST /v1/databases/{id}/switchover`.
   2. Restart `vk-instance-<id>-agent.service`, which stops and starts its Postgres, then `vk-instance-<id>-pgcluster.service`.
   3. Wait until the database is `active` again before the next host.

   A one-node database is unavailable while its instance restarts.
4. **Postgres minor releases** come with the node image or the distribution's packages. Install them on a database's hosts, then call `POST /v1/databases/{id}/upgrade`, which restarts each node onto the new release with no more than one switchover.

Changes to the agent API that a host from the previous minor release does not understand are listed in the changelog under **Breaking**. In `v1.0.0`, an older host ignores the new `role` and `replicas` fields, so it must be upgraded before it can run a cross-region replica.
