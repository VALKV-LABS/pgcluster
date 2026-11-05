# Security policy

## Reporting a vulnerability

Please do not open a public issue. Use GitHub's private vulnerability reporting on this repository ("Security" tab, "Report a vulnerability").

We aim to acknowledge a report within 3 working days and to give a first assessment within 10. We will keep you informed until a fix is released, and credit you unless you prefer otherwise.

## Supported versions

The latest minor release receives security fixes.

## Scope

In scope: `pgcluster`, `vk-agent` in both modes, `pgcluster-admin`, the CLI and console, the node image recipe, the AWS template and the published Docker images.

## Deployment security model

This section lists every file that holds a key, password or token, who can read it, and what to back up. Paths are the defaults; the setting that changes each one is named where it can change.

### Principles

- **Secrets are generated where they are used.** Hosts generate their own private keys and send only a certificate request. The CLI generates a database owner's password locally and sends only its SCRAM verifier.
- **`pgcluster-admin` seals what it keeps.** Instance secrets, backup keys and repository credentials are encrypted in the metadata database with XChaCha20-Poly1305 under the secret key, and bound to the row they belong to. API keys and enrolment tokens are kept only as SHA-256 hashes.
- **A host receives only the secrets of the instances it runs.** It writes them to files created with mode `0600`, never readable by others even briefly, and owned by the user instances run as (`run_as`).
- **Backups are encrypted on the node before upload**, with a libsodium key per database. The repository never sees it.
- **Private keys never leave the machine that uses them**, with one exception: each database's CA key lives sealed in `pgcluster-admin`, which issues node certificates.

### `pgcluster-admin`

| What | Where | Readable by | Notes |
|---|---|---|---|
| API key hashes, bootstrap enrolment token hashes | `/etc/pgcluster-admin/config.toml` (`--config`) | root and the `pgcluster-admin` group (`0640`) | Hashes only: a copy of the file cannot call the API |
| Metadata database URL, with its password | `PGCLUSTER_ADMIN_DATABASE_URL`, from `/etc/pgcluster-admin/admin.env` | root and the `pgcluster-admin` group (`0640`) | |
| Secret key | `PGCLUSTER_ADMIN_SECRET_KEY`, from the same `admin.env` | as above | Decrypts every sealed secret. See [what to back up](#what-to-back-up) |
| Admin CA certificate and key | `ca.crt`, `ca.key` in `[tls] ca_dir` | `ca.key` is `0600`, owned by `pgcluster-admin` | Signs host client certificates and the admin server's certificate. If either file is missing at startup, `pgcluster-admin` creates a new CA, and enrolled hosts can no longer connect |
| Sealed instance secrets | The metadata database: `databases.secrets`, `nodes.secrets`, `backup_repositories.credentials` | Whoever can read the metadata database | Each database's CA key, internal passwords, REST API key, backup key and custom proxy certificate; each node's certificate and key; S3 credentials. Unreadable without the secret key |
| Owner verifiers, enrolment token hashes | The metadata database: `databases.spec`, `enrollment_tokens` | as above | A SCRAM verifier cannot be used to log in, but allows an offline guess at the password, so protect database dumps |

The admin server's own TLS certificate is issued in memory at each start and never written to disk.

### Each host (`vk-agent host`)

The host agent runs as root. Instances run as `run_as`.

| What | Where | Readable by | Notes |
|---|---|---|---|
| Enrolment token | `VK_AGENT_ENROLLMENT_TOKEN`, or `enrollment_token_file` | root | Single use and short lived. Delete the file after the host enrols |
| Host identity: `ca.crt`, `host.crt`, `host.key`, `host_id` | `/var/lib/vk-agent-host` (`state_dir`) | root (`0600`) | The key never leaves the host. The certificate renews itself before it expires |
| Last desired state | `state_dir/desired.json` | root (`0600`) | Every secret for every instance on the host, including backup keys and repository credentials, so the host keeps running while `pgcluster-admin` is away |
| Node certificate and key, database CA, custom proxy certificate | `/var/lib/pgcluster/instances/<id>/tls/` (`instances_dir`) | `run_as` (`0600`, in a `0700` directory) | |
| pgcluster REST API keys | `<id>/pgcluster.toml` | `run_as` (`0600`) | Kept in plain text, unlike the admin API's hashes |
| Superuser password | `<id>/vk-agent.toml` | `run_as` (`0600`) | |
| Proxy, replication and backup secrets | `<id>/instance.env` | `run_as` (`0600`) | Also loaded into the `pgcluster` process's environment by its systemd unit |
| Replication password | `<id>/pgdata/.pgpass` | `run_as` (`0600`) | |
| WAL-G settings: backup key, repository credentials | `<id>/walg.json` | `run_as` (`0600`) | During a restore from another database, `walg-restore.json` holds that database's equivalents until the restore ends |

Root on a host can read every secret of every instance on it, and a user who can act as `run_as` can read those of all instances on the host. Give `run_as` no login, and treat host root as access to every database the host runs. Encrypt host disks.

### A cluster without the management plane (`pgcluster server`)

| What | Where | Notes |
|---|---|---|
| REST API keys | `[api] api_keys` in `pgcluster.toml` | Plain text. Make the file readable only by the user `pgcluster` runs as |
| Replication, proxy and backup passwords | Environment variables named by `replication_password_env`, `proxy.auth_password_env` and `backup.encryption_key_env`, or `[cluster] secrets_file` | Prefer `secrets_file` at `0600`: SIGHUP reloads it, and it keeps the values out of the process environment |
| Cluster CA key | Wherever `pgcluster certs` wrote it, `0600` | Needed only to issue certificates. Keep it off the nodes |
| Node certificates and keys | `[tls]` paths | `pgcluster certs` writes keys at `0600` |

The proxy holds no client passwords: it reads SCRAM verifiers from Postgres through a role that can call one function, and keeps them in memory.

### The CLI and console

They hold nothing on disk. Keys come from `PGCLUSTER_ADMIN_KEY` and `PGCLUSTER_API_KEY`. A database owner's password is printed once, when it is created, and is not stored anywhere.

### The AWS template

Terraform generates the admin CA, the secret key, the metadata database password, the API key and each host's enrolment token, so **Terraform state holds all of them**: store it encrypted, with restricted access. The admin VM's user data holds the CA key, secret key and metadata database password, and each host's user data holds its enrolment token. User data stays readable from the VM through the instance metadata service (IMDSv2 only) and in cloud-init's copy on disk. Root volumes are encrypted.

### What to back up

| Back up | Why | If it is lost |
|---|---|---|
| The metadata database **and** the secret key, stored apart | Together they hold every database's spec, CA, passwords and backup key | Without the metadata database or the key, sealed secrets cannot be read, and backups cannot be decrypted except with a copy of the backup key still on a host |
| `ca_dir` | The admin CA that every host trusts | Every host must be enrolled again |
| Each host's `state_dir`, optionally | The host's identity | The host must be enrolled again under a new name. Its instances keep running |

Everything under `instances_dir` except Postgres data is regenerated from desired state, and Postgres data is covered by the backup repository. Store backups of the secret key and `ca_dir` encrypted, and apart from database backups: anyone with both the metadata database and the secret key holds every database's secrets.

To replace a secret that may have leaked, see [Rotating Secrets](docs/components/pgcluster_admin.md#rotating-secrets).
