# Component: Console (`pgcluster console`)

## High-Level Function

`pgcluster console` serves a local web page for trying every operation of the management plane and of one cluster, and checking the result with SQL. It is a tool for one operator on their own machine, not a portal for users. `make try` runs it with the full Docker stack on `http://localhost:18080`.

```sh
pgcluster console --admin-url https://localhost:18443 --admin-ca admin-ca.crt \
  --cluster-url http://localhost:8009
```

It reads the admin API key from `PGCLUSTER_ADMIN_KEY` and the cluster's REST API key from `PGCLUSTER_API_KEY`, and listens on `127.0.0.1:8080` unless `--listen` says otherwise.

## Why a Server

The page cannot call the APIs itself: they use a private CA, need keys a page should not hold, and allow no other origin. So the console is a small server that holds the keys and passes requests on:

| Route | Goes to |
|---|---|
| `/admin/v1/*` | The admin API of `pgcluster-admin`, with its key |
| `/cluster/api/*` | The REST API of one pgcluster instance, with its key |
| `/console/overview` | Whether each is up, and the cluster's primary |
| `/console/password` | A new password and its SCRAM verifier, generated here, so only the verifier reaches `pgcluster-admin` |
| `/console/sql` | A query run as a database's owner over verified TLS, or through the cluster's proxy, capped at 200 rows and 15 seconds |

Passwords stay in the page and are gone on reload.

## Safety

Any website can make a browser call `localhost`, and DNS rebinding can make that look same-origin. So the console answers only when the `Host` header is `localhost`, `127.0.0.1` or `[::1]`, and refuses a write without its own `x-pgcluster-console` header, which another site's page cannot add without a CORS preflight the console never answers.

## Tabs

| Tab | Try |
|---|---|
| Databases | Create one or three nodes, with or without backups, turn backups on later, change the allowlist and extensions, resize, switch over, change the replication mode, restart, upgrade to the hosts' minor release, back up, restore to a point in time, rotate the CA, internal passwords and the owner's password, add, promote and delete cross-region replicas, and query as the owner |
| Hosts | Add a host and get its enrolment token, see capacity, free capacity and installed Postgres releases, drain a host and stop draining it, and remove it |
| Cluster | The `make start` cluster: nodes, maintenance, switchover, replication mode, rolling restart, manual failover and its history, and SQL through the proxy |
| Backups | The cluster's backups: take one, check the WAL archive, delete one, and restore onto a spare agent |

Each action shows its operation's steps and logs as they run.
