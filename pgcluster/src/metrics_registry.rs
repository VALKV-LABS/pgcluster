use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntGauge, Opts, Registry, TextEncoder,
};
use std::sync::Arc;

pub struct Metrics {
    pub registry: Registry,
    pub failover_total: IntCounter,
    pub failover_duration_seconds: Histogram,
    pub switchover_total: IntCounter,
    pub primary_changes_total: IntCounter,
    pub proxy_connections_total: IntCounter,
    pub connected_clients: IntGauge,
    pub primary_node_id: prometheus::GaugeVec,
    pub replica_lag_bytes: prometheus::GaugeVec,
    pub health_check_failures: prometheus::IntCounterVec,
    pub raft_leader: IntGauge,
    pub topology_version: IntGauge,
}

impl Metrics {
    pub fn new() -> anyhow::Result<Arc<Self>> {
        let registry = Registry::new();

        let failover_total = IntCounter::with_opts(Opts::new(
            "pgcluster_failovers_total",
            "Total automatic failovers triggered",
        ))?;
        let failover_duration_seconds = Histogram::with_opts(HistogramOpts::new(
            "pgcluster_failover_duration_seconds",
            "Duration of each automatic failover from detection to new primary",
        ))?;
        let proxy_connections_total = IntCounter::with_opts(Opts::new(
            "pgcluster_proxy_connections_total",
            "Total client connections accepted by the proxy",
        ))?;
        let switchover_total = IntCounter::with_opts(Opts::new(
            "pgcluster_switchovers_total",
            "Total planned switchovers",
        ))?;
        let primary_changes_total = IntCounter::with_opts(Opts::new(
            "pgcluster_primary_changes_total",
            "Total primary changes (failover + switchover)",
        ))?;
        let connected_clients = IntGauge::with_opts(Opts::new(
            "pgcluster_connected_clients",
            "Number of clients connected to the proxy",
        ))?;
        let primary_node_id = prometheus::GaugeVec::new(
            Opts::new(
                "pgcluster_primary_info",
                "Current primary node info (label: node_id)",
            ),
            &["node_id"],
        )?;
        let replica_lag_bytes = prometheus::GaugeVec::new(
            Opts::new(
                "pgcluster_replica_lag_bytes",
                "Replication lag in bytes per replica",
            ),
            &["node_id"],
        )?;
        let health_check_failures = prometheus::IntCounterVec::new(
            Opts::new(
                "pgcluster_health_check_failures_total",
                "Health check failures per node",
            ),
            &["node_id"],
        )?;
        let raft_leader = IntGauge::with_opts(Opts::new(
            "pgcluster_raft_is_leader",
            "1 if this node is the Raft leader",
        ))?;
        let topology_version = IntGauge::with_opts(Opts::new(
            "pgcluster_topology_version",
            "Current topology version from Raft state machine",
        ))?;

        registry.register(Box::new(failover_total.clone()))?;
        registry.register(Box::new(failover_duration_seconds.clone()))?;
        registry.register(Box::new(proxy_connections_total.clone()))?;
        registry.register(Box::new(switchover_total.clone()))?;
        registry.register(Box::new(primary_changes_total.clone()))?;
        registry.register(Box::new(connected_clients.clone()))?;
        registry.register(Box::new(primary_node_id.clone()))?;
        registry.register(Box::new(replica_lag_bytes.clone()))?;
        registry.register(Box::new(health_check_failures.clone()))?;
        registry.register(Box::new(raft_leader.clone()))?;
        registry.register(Box::new(topology_version.clone()))?;

        Ok(Arc::new(Self {
            registry,
            failover_total,
            failover_duration_seconds,
            switchover_total,
            primary_changes_total,
            proxy_connections_total,
            connected_clients,
            primary_node_id,
            replica_lag_bytes,
            health_check_failures,
            raft_leader,
            topology_version,
        }))
    }

    pub fn render(&self) -> anyhow::Result<String> {
        let encoder = TextEncoder::new();
        let families = self.registry.gather();
        let mut buf = Vec::new();
        encoder.encode(&families, &mut buf)?;
        Ok(String::from_utf8(buf)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failover_counter_increments() {
        let m = Metrics::new().unwrap();
        assert_eq!(m.failover_total.get(), 0);
        m.failover_total.inc();
        m.failover_total.inc();
        assert_eq!(m.failover_total.get(), 2);

        let text = m.render().unwrap();
        assert!(
            text.contains("pgcluster_failovers_total 2"),
            "counter not found in rendered output: {text}"
        );
    }

    #[test]
    fn replication_lag_gauge_updates_per_node() {
        let m = Metrics::new().unwrap();
        m.replica_lag_bytes.with_label_values(&["pg2"]).set(1024.0);
        m.replica_lag_bytes.with_label_values(&["pg3"]).set(512.0);

        let text = m.render().unwrap();
        assert!(
            text.contains(r#"pgcluster_replica_lag_bytes{node_id="pg2"}"#),
            "pg2 lag missing: {text}"
        );
        assert!(
            text.contains(r#"pgcluster_replica_lag_bytes{node_id="pg3"}"#),
            "pg3 lag missing: {text}"
        );
    }

    #[test]
    fn failover_duration_histogram_observes() {
        let m = Metrics::new().unwrap();
        m.failover_duration_seconds.observe(1.5);
        m.failover_duration_seconds.observe(3.2);

        let text = m.render().unwrap();
        assert!(
            text.contains("pgcluster_failover_duration_seconds"),
            "histogram missing: {text}"
        );
    }

    #[test]
    fn proxy_connections_total_increments() {
        let m = Metrics::new().unwrap();
        m.proxy_connections_total.inc_by(10);
        assert_eq!(m.proxy_connections_total.get(), 10);
    }
}
