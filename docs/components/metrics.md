# Component: Metrics and Alerts

## High-Level Function

Each pgcluster instance serves Prometheus metrics on `[metrics] listen_addr`, `0.0.0.0:9190` unless set, at `/metrics`. Instances that `pgcluster-admin` runs keep this port on loopback, at offset 8 of their block. [`docker/prometheus-alerts.yml`](../../docker/prometheus-alerts.yml) holds alert rules for them.

`pgcluster-admin` and `vk-agent` serve no metrics. The admin API's event feed, `GET /v1/events`, is the management plane's signal for tooling.

## Metrics

| Metric | Type | Meaning |
|---|---|---|
| `pgcluster_failovers_total` | Counter | Automatic failovers this instance ran |
| `pgcluster_failover_duration_seconds` | Histogram | From detection to a new primary |
| `pgcluster_switchovers_total` | Counter | Planned switchovers |
| `pgcluster_primary_changes_total` | Counter | Failovers and switchovers together |
| `pgcluster_primary_info{node_id}` | Gauge | 1 for the node this instance believes is primary |
| `pgcluster_replica_lag_bytes{node_id}` | Gauge | Each standby's lag behind the primary |
| `pgcluster_health_check_failures_total{node_id}` | Counter | Failed health checks of each node |
| `pgcluster_raft_is_leader` | Gauge | 1 if this instance leads Raft |
| `pgcluster_topology_version` | Gauge | The version of the replicated topology |
| `pgcluster_proxy_connections_total` | Counter | Client connections the proxy accepted |
| `pgcluster_proxy_connections_refused_total` | Counter | Client connections closed at once, over the proxy's connection limits |
| `pgcluster_connected_clients` | Gauge | Clients connected to the proxy now |

Leader-only metrics, such as failovers and lag, come from whichever instance leads Raft, so sum or max across instances.

## Alerts

| Alert | Fires when |
|---|---|
| `PgClusterFailoverTriggered` | A failover happened in the last 5 minutes |
| `PgClusterFrequentFailovers` | Three or more failovers in an hour |
| `PgClusterNoRaftLeader` | No instance leads Raft for 30 seconds |
| `PgClusterMultipleRaftLeaders` | More than one instance claims to lead |
| `PgClusterReplicaLagHigh` | A standby is over 100 MiB behind |
| `PgClusterReplicaLagCritical` | A standby is over 1 GiB behind |
| `PgClusterNodeUnreachable` | More than three failed health checks of a node in a minute |
| `PgClusterNoPrimary` | No primary is known |
| `PgClusterSwitchoverSlow` | A failover took over 30 seconds |

A rising `pgcluster_proxy_connections_refused_total` means the proxy is turning clients away at its limits. See [proxy_layer.md](proxy_layer.md#connection-limits).
