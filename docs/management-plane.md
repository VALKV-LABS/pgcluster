# Management Plane Specification

**Status:** proposed. Nothing in this document is built yet.
**Audience:** contributors, and operators who run their own Postgres fleet.

## 1. Summary

pgcluster manages one Postgres cluster well. Running many of them is still manual: someone writes TOML for each node, starts the processes, wires up backups and tracks what runs where.

The management plane adds fleet management on top, using two new components:

- **`pgcluster-admin`:** one service and one API for every cluster in a fleet.
- **`vk-agent host`:** a new mode of the existing `vk-agent` binary. One runs on every host and does what `pgcluster-admin` asks.

An operator enrols their hosts, calls `POST /v1/databases`, and gets a running Postgres endpoint with failover and point-in-time restore. Both components are open source and run on their own. They need one Postgres database, and no cloud account.

## 2. Principles

1. **The data plane runs without the management plane.** Failover, proxying and backups keep working if `pgcluster-admin` is down. It never decides who is primary; each cluster's Raft group does.
2. **Agents dial out.** `vk-agent host` opens every connection, so hosts behind NAT or a firewall work the same as any other. There is no SSH and no inbound access.
3. **One path to every host.** A VM, a bare-metal server and a cloud instance are all enrolled and managed the same way.
4. **No access to user data.** `pgcluster-admin` runs no SQL against user tables and never holds a user's Postgres password.
5. **Hosts come from you.** `pgcluster-admin` makes no cloud API calls. You bring the machines, by hand, with your own tooling, or with the optional AWS template (§12), which runs with your own credentials.
6. **Stable contracts.** The admin API and the agent API are versioned, so other tools can drive `pgcluster-admin` or replace it.

## 3. Goals and non-goals

**Goals**

- Create, resize, back up, restore and delete databases through an API.
- One-node databases, and three-node databases with automatic failover.
- Async, sync and strict sync replication.
- Point-in-time restore.
- Several databases on one host, with resource limits.
- Replacing a failed node without downtime.
- Trying the whole system with one command, locally or on AWS.

**Non-goals**

- Multi-tenancy, billing or a portal. An optional opaque `workspace` label lets a layer above group databases, and `pgcluster-admin` attaches no meaning to it.
- Provisioning machines inside `pgcluster-admin`. The AWS template (§12) sits alongside it as a separate example.
- Active-active or multi-master replication.
- Automatic cross-region failover.
- Sharding, which stays in pgcluster's coordinator mode.

## 4. Architecture

```
 CLI / your tooling
        │  admin API, bearer key
        ▼
 pgcluster-admin ──── Postgres (metadata)
        ▲
        │  agent API, HTTPS with mTLS, opened by the host
        │
 ┌──────┴──────────── host ─────────────────────────┐
 │  vk-agent host   enrol, apply state, report      │
 │  ┌───────────── instance (one per node) ───────┐  │
 │  │  pgcluster   Raft, failover, proxy, pool    │  │
 │  │  vk-agent    executes pgcluster's commands  │  │
 │  │  PostgreSQL + pgBackRest                    │  │
 │  └─────────────────────────────────────────────┘  │
 └───────────────────────────────────────────────────┘
        ▲                              │
 clients ┘ TLS                         └──► object storage (backups, WAL)
```

| Component | New? | Responsibility |
|---|---|---|
| `pgcluster-admin` | New | Admin API, agent API, desired state, reconciler, scheduler, operations |
| `vk-agent host` | New mode | Enrolment, capacity reports, config rendering, process and storage management, certificate rotation |
| `pgcluster`, `vk-agent` | Extended | See §10 |
| Node image | New | Packer recipe with Postgres majors, extensions, pgBackRest and all binaries, pinned. Published as a public AMI with each release |
| AWS template | New | Terraform that launches a test fleet in your own account (§12) |

**One binary, two modes**

`vk-agent` ships as one binary and runs as two kinds of process:

| | `vk-agent` (instance mode) | `vk-agent host` (host mode) |
|---|---|---|
| Runs | One per Postgres instance | One per machine |
| Starts | After its instance has been set up | Before any Postgres exists |
| Network | Listens on a port for its cluster's nodes | Opens no port; dials out only |
| Privileges | The `postgres` user | Enough to manage services, cgroups, disks and the firewall |
| Takes orders from | The cluster's Raft leader | `pgcluster-admin` |

They stay separate processes for three reasons:

- Something has to start the first instance, and an instance's own agent cannot do that.
- A process with host-level privileges should not sit behind a port that peers can reach.
- On a shared host, capacity, ports and disk quotas belong to the machine, not to any one instance.

Instance mode keeps its existing rule: it only executes commands and makes no decisions. The two modes share TLS, config and Postgres code, and release together.

**Who owns what**

| Fact | Owner |
|---|---|
| What a database should look like | `pgcluster-admin` |
| What is running on a host right now | `vk-agent host`, reported to `pgcluster-admin` |
| Which node is primary, cluster membership | That cluster's Raft group |

**Repository layout**

```
pgcluster-admin/   service: admin API, agent API, reconciler
vk-agent/          instance mode and host mode
openapi/           admin API and agent API contracts
images/            node image recipe
deploy/aws/        Terraform template for a test fleet
```

`pgcluster-admin` is written in Rust, in this workspace, and uses Postgres as its only dependency.

## 5. Concepts

| Term | Meaning |
|---|---|
| **Host** | A machine running `vk-agent host` |
| **Database** | A Postgres cluster of one or three nodes |
| **Node** | One Postgres instance of a database, on one host |
| **Instance** | The `pgcluster`, `vk-agent` and Postgres processes for one node |
| **Failure domain** | A label on a host, such as a rack or availability zone. HA nodes are placed in different domains |
| **Operation** | A long-running change, with stored steps and logs |
| **Generation** | A number that increases each time a database's desired state changes |

## 6. Admin API

Conventions:

- Authentication is `Authorization: Bearer <key>`. `pgcluster-admin` refuses to start with no keys configured.
- Errors are `{ "error": { "code": "...", "message": "..." } }`.
- Lists are `{ "items": [...], "next_page_token": "..." }`, with `limit` and `page_token`.
- Changes that take time return `202` and an operation.

### Hosts

```
POST   /v1/hosts                  { name, failure_domain? } → enrollment_token, shown once
GET    /v1/hosts                  status, reported capacity, free capacity
GET    /v1/hosts/{id}
POST   /v1/hosts/{id}/drain
DELETE /v1/hosts/{id}
```

A host reports its own address and capacity after enrolling.

### Backup repositories

```
POST   /v1/backup-repositories    { name, type: s3 | posix, ... }
GET    /v1/backup-repositories
DELETE /v1/backup-repositories/{id}
```

### Databases

```
POST   /v1/databases
GET    /v1/databases
GET    /v1/databases/{id}
PATCH  /v1/databases/{id}                       resources, replication_mode, retention_days
DELETE /v1/databases/{id}

POST   /v1/databases/{id}/restart
POST   /v1/databases/{id}/switchover
POST   /v1/databases/{id}/backup
POST   /v1/databases/{id}/restore               { name, target_time } → a new database
POST   /v1/databases/{id}/owner-password        { scram_verifier }

GET|PUT /v1/databases/{id}/allowlist
GET|PUT /v1/databases/{id}/extensions
PUT     /v1/databases/{id}/certificate
GET     /v1/databases/{id}/nodes
POST    /v1/databases/{id}/nodes/{node}/replace { host_id }
GET     /v1/databases/{id}/backups
GET     /v1/databases/{id}/restore-window

POST|GET /v1/databases/{id}/replicas
DELETE   /v1/databases/{id}/replicas/{replica_id}
POST     /v1/databases/{id}/replicas/{replica_id}/promote
```

**Create request**

```json
{
  "name": "orders",
  "workspace": "team-payments",
  "version": "18",
  "ha": true,
  "hosts": ["host-a", "host-b", "host-c"],
  "resources": { "vcpu": 2, "memory_gb": 8, "storage_gb": 100 },
  "replication_mode": "sync",
  "owner": { "username": "app_owner", "scram_verifier": "SCRAM-SHA-256$..." },
  "allowed_cidrs": ["198.51.100.0/24"],
  "extensions": ["vector", "pg_trgm"],
  "backup": { "repository_id": "repo-1", "retention_days": 7 }
}
```

`hosts` is optional; the scheduler chooses when it is omitted. The CLI generates the password locally and sends only the SCRAM verifier.

**Status** is one of `provisioning`, `active`, `degraded`, `deleting`, `deleted` or `error`, with a `status_reason`. `degraded` means the database is serving with a problem, such as a node being down, sync having fallen back to async, or backups failing.

### Operations and events

```
GET /v1/databases/{id}/operations
GET /v1/databases/{id}/operations/{op_id}       steps and logs
GET /v1/events?after={cursor}
```

The event feed reports state changes such as `node_offline`, `failover_completed`, `backup_failed` and `replacement_needed`. It lets tooling react without polling every database. For example, a provisioning tool can respond to `replacement_needed` by bringing up a new host.

## 7. Agent API

`vk-agent host` calls these endpoints; `pgcluster-admin` never calls a host.

```
POST /agent/v1/enroll                  single-use token and a key pair → client certificate
GET  /agent/v1/desired-state           ?generation=N, long-polled
POST /agent/v1/report                  observed_generation, capacity, instance health
POST /agent/v1/operations/{id}/log     step output
```

- Enrolment tokens are single-use and expire after 15 minutes.
- Every later call uses mutual TLS, and a certificate can only read or report its own host's state.
- `pgcluster-admin` is the certificate authority for host certificates, and rotates them.

**Desired state** lists the instances a host should run:

```json
{
  "generation": 42,
  "instances": [
    {
      "database_id": "orders",
      "node": "n1",
      "postgres_version": "18",
      "resources": { "vcpu": 2, "memory_gb": 8, "storage_gb": 100 },
      "port_base": 20000,
      "peers": [ { "node": "n2", "address": "10.0.0.2", "port_base": 20000 } ],
      "bootstrap": "initdb",
      "replication_mode": "sync",
      "allowed_cidrs": ["198.51.100.0/24"],
      "extensions": ["vector", "pg_trgm"]
    }
  ]
}
```

The agent API is a public contract. Anything that serves it can drive `vk-agent host`.

## 8. How changes are applied

Each database has a `spec` and a `generation`. `vk-agent host` long-polls for desired state, applies it, and reports `observed_generation` with health. An operation finishes when every node reports the new generation.

- Applying the same state twice has no effect, so failures are retried safely.
- Operations are stored steps, claimed with `FOR UPDATE SKIP LOCKED`. A restarted `pgcluster-admin` resumes from the last completed step.
- `vk-agent host` keeps its last desired state on disk and keeps instances running if `pgcluster-admin` is unreachable.

**Creating a database**

1. Validate the request, choose hosts and allocate ports.
2. Each `vk-agent host` picks up its instance from desired state.
3. Node 1 runs `initdb --data-checksums` and `pgbackrest stanza-create`. Other nodes seed from it.
4. pgcluster forms the Raft group.
5. Apply the replication mode, install extensions, create the owner role.
6. Take the first full backup.
7. Apply the allowlist and mark the database `active`.

Clients get no access until step 7, so a reachable database always has a restorable backup.

**Other operations**

| Operation | How |
|---|---|
| Replace a node | Add the new node as a Raft learner, seed it from backup, promote it to voter, remove the old member |
| Grow storage | Grow the volume and filesystem online on each node |
| Change resources | One standby at a time, then switch over, then the old primary |
| Add HA to one node | Add two nodes the same way as a replacement |
| Minor upgrade | Rolling across standbys, then switch over, then the old primary |
| Major upgrade | Not in scope yet. Restore or logical migration into a new database |

## 9. Database behaviour

### Replication

| | `async` | `sync` | `sync_strict` |
|---|---|---|---|
| `synchronous_standby_names` | empty | `ANY 1 (n2, n3)` | `ANY 1 (n2, n3)` |
| Commit returns after | Local flush | Flush on one standby | Flush on one standby |
| Both standbys down | No effect | Stalls for `sync_degrade_timeout`, then falls back to async and goes `degraded` | Writes block until a standby returns |
| Primary lost | Promote the best reachable standby | Promote the best reachable standby | Promote only if both standbys answer; otherwise stay down and alert |

With `ANY 1` of 2, only one standby is guaranteed to hold the last commit, and only asking both reveals which. That is why `sync_strict` needs both to answer.

Even strict settings cannot promise literal zero loss. A transaction can become visible locally before a standby has it, for example when a client cancels the wait, or across a primary crash and restart.

`replication_mode` needs `ha: true`, and can change online.

### Availability

- **One node:** no failover. If the host is lost, the database is restored from backup.
- **Three nodes:** one node per failure domain. pgcluster needs three Raft voters for quorum, so two-node databases are not offered.
- **Cross-region replicas:** an external standby that follows asynchronously, is not a Raft voter, and is never an automatic failover target. `promote` detaches it into an independent database. It needs the same major version and CPU architecture as the source.

### Backups and restore

Every database has point-in-time restore.

| Setting | Value |
|---|---|
| Tool | pgBackRest 2.60.0 or later |
| WAL | `archive_command` with `archive-async=y`; `archive_timeout` 60 s |
| Base backups | Weekly full, daily differential, `backup-standby=prefer` |
| Storage | S3-compatible or POSIX, with `repo-bundle=y` and `repo-block=y` |
| Encryption | `repo-cipher-type=aes-256-cbc`, one passphrase per database, encrypted at rest in `pgcluster-admin` |

Restore always creates a new database, so the source stays up and a failed restore harms nothing. `restore-window` returns the earliest and latest restorable times.

Scheduling stays on each cluster's Raft leader, so backups continue during a management-plane outage.

### Network

- Clients reach the proxy only. Postgres, Raft and agent ports accept the database's own nodes.
- The allowlist is a whole-list `PUT`, applied to `pg_hba.conf` and the host firewall. Empty means no access. Public access is the explicit entry `0.0.0.0/0`.
- TLS is required at the proxy. `pgcluster-admin` issues a certificate by default, and `PUT .../certificate` replaces it.
- The owner role gets `CREATEDB` and `CREATEROLE`, not superuser.

### Ports

Each instance uses a block of ten consecutive ports from `port_base`:

| Offset | Service | Reachable from |
|---|---|---|
| 0 | Proxy, clients | Allowlist |
| 1 | Proxy admin | Local |
| 2 | Postgres | Peers |
| 3 | Raft | Peers |
| 4 | vk-agent | Peers |
| 5 | pgBackRest TLS server | Peers |
| 6 | Proxy health | Local |
| 7 | REST API | Local |
| 8 | Metrics | Local |
| 9 | Reserved | None |

### Extensions

The node image ships a fixed list, and `pgcluster-admin` installs them as a privileged local operation, so owners do not need superuser.

| Extension | `CREATE EXTENSION` name | Needs preload |
|---|---|---|
| uuid-ossp | `uuid-ossp` | No |
| pgcrypto | `pgcrypto` | No |
| pg_trgm | `pg_trgm` | No |
| unaccent | `unaccent` | No |
| pgvector | `vector` | No |
| pg_stat_statements | `pg_stat_statements` | Yes, always on |
| pg_tle | `pg_tle` | Yes |
| PostGIS | `postgis` | No |
| pg_cron | `pg_cron` | Yes |

Enabling a preload extension needs a rolling restart.

### Several databases on one host

- `vk-agent host` runs each instance as systemd units with cgroup v2 limits: `CPUQuota=`, `MemoryMax=` and `IOWeight=`. A plain process runner exists for development and tests.
- Each instance gets a disk quota. Disk is the likeliest noisy neighbour, and CPU and memory limits do nothing about it.
- The scheduler refuses overcommit, reserves memory for page cache and the OS, and places HA nodes on different hosts and failure domains.
- A shared host is not a security boundary between mutually untrusting users. Only share hosts between databases with the same owner.

## 10. Requirements on pgcluster and vk-agent

The management plane depends on the data plane meeting these requirements:

| Area | Requirement |
|---|---|
| Replication | Apply `synchronous_standby_names` from the replication mode, and record a fallback to async through Raft before clearing it, so a later failover knows no standby is guaranteed current |
| Failover | Choose the candidate from live status of reachable standbys, and honour the rules in §9 for `sync_strict` |
| Partitions | A proxy without quorum stops routing writes before a new primary can be promoted. Self-fencing is on by default |
| Authentication | Every gRPC endpoint requires a client certificate from the cluster CA. The REST API refuses to run without credentials. Proxy-to-Postgres traffic uses TLS |
| Membership | Raft learners, voter promotion and member removal, exposed through the REST API |
| Node lifecycle | Seed a node from backup or from the primary through vk-agent, and rejoin an old primary with `pg_rewind` |
| Backups | A pgBackRest engine in the backup coordinator, with WAL archiving |
| Restarts | Rolling restart across standbys, then switchover, then the old primary |
| Replicas | An external standby role that is excluded from failover and from Raft |
| Slots | `max_slot_wal_keep_size` set, so a dead standby cannot fill the primary's disk |

## 11. Compatibility

- Admin and agent APIs are versioned under `/v1`. Breaking changes need `/v2`.
- `pgcluster-admin` supports `vk-agent host` versions from the current and previous minor release, so hosts can upgrade one at a time.
- Postgres 16, 17 and 18 are supported at first.

## 12. Trying it

| Path | Needs | What you get |
|---|---|---|
| Local | Docker | The full stack on one machine, with `make start` |
| AWS | An AWS account and Terraform | A test fleet in your own account |

### AWS

```bash
cd deploy/aws
terraform init
terraform apply -var region=us-east-1 -var allowed_cidr=203.0.113.4/32
```

This creates:

- A VPC.
- One `pgcluster-admin` VM, with Postgres on the same VM.
- Three hosts from the public node image, already enrolled.
- An S3 bucket for backups.

Terraform prints the admin API URL and a generated API key. `terraform destroy` removes everything.

- **Your credentials stay in Terraform.** They never reach `pgcluster-admin` or the hosts. Hosts use an instance role limited to the backup bucket.
- **`allowed_cidr` has no default.** Nothing opens to the internet unless you ask, and the admin API and databases accept only that range.
- **Enrolment needs no admin key on the hosts.** Terraform generates one single-use token per host and loads their hashes into `pgcluster-admin` at boot.
- **Terraform state holds the generated API key.** Treat the state file as a secret.
- **It is for testing, not production.** `pgcluster-admin` is a single VM, and the default size is `t4g.small`.
- **It costs money while it runs.** Four on-demand `t4g.small` VMs in `us-east-1` cost about $0.07 per hour, plus storage.

## 13. Stages

| Stage | Scope | Done when |
|---|---|---|
| **1. One node** | `pgcluster-admin`, `vk-agent host`, agent API, node image and public AMI, CLI, AWS template, one-node databases | A host enrols and a one-node database is created through the API, locally and after one `terraform apply` on AWS |
| **2. High availability** | Three nodes, replication modes, node replacement, the §10 requirements except backups | A three-node database survives primary loss, a network partition and node replacement in all three modes, by automated test |
| **3. Backups** | pgBackRest, repositories, restore | A point-in-time restore is verified by automated test |
| **4. Shared hosts** | Scheduler, isolation, disk quotas, ports | Several databases share a host under load without one starving another |
| **5. Day-two** | Rolling upgrades, resizing, cross-region replicas | Each operation passes under client load |
