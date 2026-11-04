# Component: Scheduler and Shared Hosts (`pgcluster-admin`)

## High-Level Function

The scheduler decides which host each node lands on. It lets several databases share a host without one starving another, and it keeps a three-node database spread across failure domains. It lives in `pgcluster-admin/src/scheduler.rs`, and is used when a database is created, when a node is replaced, when a host drains, when a database is resized, and when a cross-region replica is placed.

The scheduler only chooses. Each host enforces the result itself, with a systemd slice per instance for CPU, memory and IO, and an XFS project quota for disk. See [host_agent.md](host_agent.md#resource-limits).

## What a Host Can Give

Each host reports its CPUs, memory and disk. The scheduler holds part of that back before handing anything out:

| Resource | Handed out |
|---|---|
| vCPU | All of it. CPU is never overcommitted |
| Memory | `(memory − os_reserved_memory_gb) × (1 − page_cache_share)`. The rest is for the OS, the agents, and the page cache all instances share |
| Storage | `storage × (1 − storage_reserve_share)`. The rest is for logs and the OS |
| Ports | One block of ten for each instance, from `port_base_start` to `port_base_end` |

Each node of a database takes the database's `resources`. A node on its way out, during a replacement or a drain, still counts until its host has removed it, because it still runs.

`GET /v1/hosts` shows `capacity`, which the host reported, and `free_capacity`, which is what is left to hand out after reserves and every instance. It also shows `instances`, `draining` and `drain_error`.

```toml
# pgcluster-admin config. These are the defaults.
[scheduler]
os_reserved_memory_gb = 1.0
page_cache_share      = 0.2
storage_reserve_share = 0.1
port_base_start       = 20000
port_base_end         = 32760
```

## Placement

A one-node database goes to the online host with the most free memory that has room, which spreads databases out. A three-node database takes three such hosts in separate failure domains. A host with no failure domain counts as a domain of its own. Draining hosts and offline hosts are never chosen. A request that names `hosts` is placed only on those.

A create is checked twice:

1. **When it is asked for.** Against every host that has reported its size:
   - A node larger than any host, even an empty one, is refused with `422`, naming the largest host's size after reserves.
   - Enough hosts and domains exist, but too few have room: refused with `422`, naming the most any host has free.
   - Too few hosts or domains: accepted. The create waits for more hosts to enrol.
2. **When the `place` step runs.** Against online hosts, with the hosts locked, so two creates cannot both take the last of one host. One that no longer fits fails with the same message, and the database goes to `error`.

## Ports

Each instance takes ten consecutive ports from its `port_base`. The block's layout is in [§9 of the spec](../management-plane.md#ports). Blocks are allocated per host, and the metadata database holds a unique `(host_id, port_base)`, so two instances on one host never collide. A host whose blocks are all taken counts as full.

Something outside pgcluster may already listen on a port in the range. A host checks every port of a new instance's block before its first start, and reports the instance as failed, naming the ports in use, rather than starting it.

## Draining a Host

```
POST   /v1/hosts/{id}/drain    stop placing here, and move every node off
DELETE /v1/hosts/{id}/drain    let nodes land here again
```

A drain marks the host so nothing new lands on it. Every few seconds the reconciler then picks one node on it, of a database with no operation running, and starts a `replace` operation for it on the host with the most room that keeps the database's nodes in separate failure domains. A replacement brings the new node in before the old one goes, and switches over first if the old one is the primary, so clients see no downtime. Each database moves one node at a time, and different databases move in parallel.

| Host status | Meaning |
|---|---|
| `draining` | Nodes remain. `drain_error` says why one cannot move yet, such as no host with room in another failure domain. The drain retries on its own once a host appears |
| `drained` | Nothing is left. `host_drained` goes to the event feed, and `DELETE /v1/hosts/{id}` can remove the host |

A one-node database cannot move without downtime, so a drain of a host that runs one is refused with `409`, naming the databases. Restore each into a new database on another host, or delete it, and drain again.

## Resizing

`PATCH /v1/databases/{id}` with `resources` checks each node's host for room to grow, counting the database's current size as already taken. Storage only grows, because a quota cannot take back what data already uses. A resize runs as a `resize` operation:

| Change | How it applies |
|---|---|
| vCPU | The instance's slice gets a new `CPUQuota=`, at once and with no restart |
| Storage | The instance's disk quota rises, at once and with no restart |
| Memory | The slice gets a new `MemoryMax=`, and `shared_buffers`, a quarter of memory, needs a restart. Three nodes take a rolling restart: standbys first, then a switchover, then the old primary. One node restarts when its host applies the change |

## Related

- [host_agent.md](host_agent.md): slices, quotas and the port check on each host.
- [pgcluster_admin.md](pgcluster_admin.md): operations, replacement and the event feed.
