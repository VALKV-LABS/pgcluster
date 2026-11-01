# pgcluster Roadmap

Work in order, top to bottom. Each step lists what must be true for it to be done. Steps in Parts 1 to 5 implement [§10 of the management plane spec](docs/management-plane.md), and Parts 6 to 10 implement its stages.

## Part 1: Release and project basics

- [x] **1.1 Publish `v0.1.3`.** Add the Docker Hub secrets, then push the tag. *Done when:* `valkvlabs/pgcluster:0.1.3` and `valkvlabs/vk-agent:0.1.3` pull, and the GitHub release exists.
- [x] **1.2 Publish `latest` from releases.** The workflow now sets it explicitly on every release. *Done when:* a release updates `latest`.
- [x] **1.3 Add `CONTRIBUTING.md`, `SECURITY.md` and `CHANGELOG.md`, and choose a CLA or DCO.** The project uses the DCO. *Done when:* contributors have a documented path and security reports have a private route.
- [x] **1.4 Fix documentation drift.** *Done when:* every README link resolves and the API doc matches the routes.
- [x] **1.5 Run the Docker integration and end-to-end tests in CI.** *Done when:* they run on every PR.
- [ ] **1.6 Build release images natively.** Each architecture builds on its own runner, with dependencies in a cached layer, and pushes to `main` keep the cache warm. *Done when:* a push to `main` publishes `edge` for both architectures, and a release takes under 15 minutes. **Pending verification:** `edge` is published for both architectures. A release no longer repeats CI that passed on `main`, but the 15-minute target can only be measured by the next tag.

## Part 2: Secure defaults

- [x] **2.1 Require credentials on the REST API.** *Done when:* the server refuses to start without API keys, unless an explicit development flag is set.
- [ ] **2.2 Require client certificates on every gRPC endpoint.** Covers `vk-agent` and Raft, signed by the cluster CA. *Done when:* a connection without a valid client certificate is rejected, by automated test.
- [ ] **2.3 Use TLS between the proxy and Postgres.** *Done when:* the connection is encrypted and certificates are verified.
- [ ] **2.4 Update docs.** *Done when:* the README and `docs/components/tls_manager.md` describe what 2.1 to 2.3 deliver.

## Part 3: Safe failover

- [ ] **3.1 Choose the failover candidate from live status.** Query reachable standbys at failover time. *Done when:* a test with a stale cached position promotes the most current standby.
- [ ] **3.2 Stop writes on a proxy without quorum.** It must stop before a new primary can be promoted. *Done when:* a partition test shows no writes accepted on both sides.
- [ ] **3.3 Turn self-fencing on by default.** Align its timeout with 3.2 and with failover timing, and validate the settings at startup. *Done when:* an invalid timing combination fails config validation.
- [ ] **3.4 Add a partition test to the end-to-end suite.** *Done when:* it runs in CI and passes for 3.2 and 3.3.

## Part 4: Replication modes

- [ ] **4.1 Add `replication_mode` to the config.** Values are `async`, `sync` and `sync_strict`, replacing the unused `synchronous_standby_names` setting and its sample comment. *Done when:* the setting is validated, and changing it applies without a restart.
- [ ] **4.2 Apply `synchronous_standby_names` from the mode.** Use `ANY 1 (...)`, and update it when membership changes. *Done when:* `sync` commits wait for a standby, by test.
- [ ] **4.3 Implement the fallback for `sync`.** Record the fallback through Raft before clearing the setting. *Done when:* writes resume after `sync_degrade_timeout` with both standbys down, and a later failover knows no standby is guaranteed current.
- [ ] **4.4 Implement `sync_strict`.** Writes block with no standby, and promotion needs both standbys to answer. *Done when:* each case is covered by test.
- [ ] **4.5 Set `max_slot_wal_keep_size`.** *Done when:* a dead standby cannot fill the primary's disk, and a standby past the limit is flagged for re-seeding.

## Part 5: Node lifecycle and backups

- [ ] **5.1 Add Raft membership changes.** Learners, voter promotion and member removal, through the REST API. *Done when:* a member can be replaced with no loss of quorum.
- [ ] **5.2 Move node seeding into `vk-agent`.** It lives in `docker/init-replica.sh` today. *Done when:* a new node seeds through an RPC, from the primary.
- [ ] **5.3 Rejoin an old primary with `pg_rewind`.** Re-seed if rewind fails. *Done when:* a diverged old primary rejoins automatically, by test.
- [ ] **5.4 Add rolling restart.** Standbys, then switchover, then the old primary. *Done when:* it completes under client load with no failed commits beyond the switchover.
- [ ] **5.5 Add a pgBackRest engine to the backup coordinator.** Use 2.60.0 or later, with data checksums. *Done when:* scheduled backups run from a standby and survive a leader change.
- [ ] **5.6 Add WAL archiving.** *Done when:* `archive_command` works from whichever node is primary, including after failover.
- [ ] **5.7 Add restore.** Seed from backup, and restore to a point in time. *Done when:* a point-in-time restore is verified by automated test, and 5.2 can seed from backup.
- [ ] **5.8 Test one-node recovery.** *Done when:* a one-node cluster recovers after its VM is replaced and its volume reattached.
- [ ] **5.9 Add an external standby role.** Excluded from failover and from Raft. *Done when:* it follows the primary and can be promoted into an independent cluster.
- [ ] **5.10 Release `v0.2.0`.** *Done when:* the full end-to-end suite passes and images are published.

## Part 6: Management plane, one node (Stage 1)

- [ ] **6.1 Define the contracts.** Write the admin and agent APIs in `openapi/`. *Done when:* both validate in CI.
- [ ] **6.2 Scaffold `pgcluster-admin`.** Service, Postgres schema and migrations, and required API keys. *Done when:* it starts, migrates and serves health.
- [ ] **6.3 Add hosts and enrolment.** Single-use tokens, the internal CA and client certificates. *Done when:* a host enrols, and a reused or expired token is rejected.
- [ ] **6.4 Add `vk-agent host`.** New mode in the existing binary, with `vk-agent <config>` unchanged. It enrols, reports capacity and long-polls desired state. *Done when:* it runs as its own process, opens no port, and keeps its last state on disk.
- [ ] **6.5 Add config rendering.** Generate each instance's `pgcluster` and `vk-agent` config from desired state. *Done when:* output passes `pgcluster` validation.
- [ ] **6.6 Add process runners.** systemd for production, plain processes for development and tests. *Done when:* the host agent starts, stops and restarts an instance with both.
- [ ] **6.7 Add the reconciler and operations.** Stored steps with `FOR UPDATE SKIP LOCKED`, `generation` and `observed_generation`. *Done when:* a restarted `pgcluster-admin` resumes an operation with no duplicate work.
- [ ] **6.8 Add the database API for one node.** Create, read, delete, the owner role with a SCRAM verifier, and the allowlist. *Done when:* a database is created through the API and reachable over TLS.
- [ ] **6.9 Add extensions.** *Done when:* each listed extension installs through the API without superuser.
- [ ] **6.10 Add CLI commands.** *Done when:* the CLI covers enrolment and creating a database, and generates the password locally.
- [ ] **6.11 Build the node image.** Packer recipe in `images/`, published as a public AMI with each release. *Done when:* a host from the AMI enrols.
- [ ] **6.12 Add local quick start.** *Done when:* `make start` runs the full stack, including `pgcluster-admin` and the host agent.
- [ ] **6.13 Add the AWS template.** Terraform in `deploy/aws/`, with a required `allowed_cidr` and per-host single-use tokens. *Done when:* one `terraform apply` produces a working database, and `terraform destroy` removes everything.

## Part 7: Management plane, high availability (Stage 2)

- [ ] **7.1 Create three-node databases.** *Done when:* nodes land on separate hosts and failure domains.
- [ ] **7.2 Expose replication modes through the API.** *Done when:* `PATCH` changes the mode online.
- [ ] **7.3 Add node replacement.** Replace a node through the API, using 5.1 and 5.2. *Done when:* replacement completes under client load.
- [ ] **7.4 Add the event feed.** `GET /v1/events`, including `replacement_needed`. *Done when:* tooling can react without polling every database.
- [ ] **7.5 Add switchover and restart.** *Done when:* both run as operations with logs.
- [ ] **7.6 Add the HA test suite.** *Done when:* primary loss, a network partition and node replacement pass in all three modes.

## Part 8: Management plane, backups (Stage 3)

- [ ] **8.1 Add backup repositories.** S3-compatible and POSIX, with per-database encryption. *Done when:* a repository is verified when registered.
- [ ] **8.2 Add the backup API.** Retention, on-demand backups, listing and the restore window. *Done when:* the window matches what can actually be restored.
- [ ] **8.3 Add restore as a new database.** *Done when:* a restore to a chosen time is verified by automated test and the source stays up.
- [ ] **8.4 Require a first backup before access.** *Done when:* no database becomes `active` without a restorable backup.

## Part 9: Management plane, shared hosts (Stage 4)

- [ ] **9.1 Add the scheduler.** Capacity tracking, no overcommit, memory reserved for page cache, and placement across failure domains. *Done when:* an impossible request fails with a clear error.
- [ ] **9.2 Add port blocks.** *Done when:* several instances run on one host with no collisions.
- [ ] **9.3 Add isolation.** cgroup v2 limits and per-instance disk quotas. *Done when:* one busy database cannot starve another, by load test.
- [ ] **9.4 Add host drain.** *Done when:* draining moves every instance off a host with no downtime.

## Part 10: Management plane, day two (Stage 5)

- [ ] **10.1 Add resizing.** Resources and storage. *Done when:* each passes under client load.
- [ ] **10.2 Add minor upgrades.** *Done when:* a rolling upgrade passes under client load.
- [ ] **10.3 Add cross-region replicas.** Uses 5.9. *Done when:* a replica promotes into an independent database through the API.
- [ ] **10.4 Release `v1.0.0`.** *Done when:* every stage in the spec meets its completion check, and the upgrade path from `v0.2.0` is documented.

## Done

- Concurrent switchover and failover guard
- Repoint of a recovered replica
- Slot cleanup and orphan audit
- Maintenance mode
- Connection drain during switchover
- Post-promotion health check
- Failover history cap
- Switchover target validation
- `.pgpass` for replication credentials
- `vk-agent` startup retry
- Prometheus alert rules
- Config reload endpoint
- RFC 3339 failover timestamps
- End-to-end and CLI integration tests
- TLS on Raft and `vk-agent` connections
- Apache 2.0 licence, Clippy on `main`, and README corrections
- Replica seeding in `make start`
