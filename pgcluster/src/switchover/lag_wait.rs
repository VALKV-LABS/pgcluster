use anyhow::Result;
use std::time::{Duration, Instant};
use tracing::info;

use crate::raft::TopologyWatch;

// ── wait_for_replica_sync ─────────────────────────────────────────────────────

/// Block until `target_node_id` is within `max_lag_bytes` of the primary's
/// flush LSN, or until `timeout` elapses.
///
/// The topology is re-read every 200 ms via the watch channel; the monitor
/// loop keeps it up to date on the leader.
pub async fn wait_for_replica_sync(
    topology_rx: &TopologyWatch,
    target_node_id: &str,
    max_lag_bytes: u64,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;

    loop {
        if Instant::now() >= deadline {
            anyhow::bail!(
                "replica {} did not sync within {:?}",
                target_node_id,
                timeout
            );
        }

        let topology = topology_rx.current();
        let primary_lsn = topology
            .last_flush_lsns
            .get(&topology.primary_node_id)
            .copied()
            .unwrap_or(0);
        let replica_lsn = topology
            .last_flush_lsns
            .get(target_node_id)
            .copied()
            .unwrap_or(0);
        let lag = primary_lsn.saturating_sub(replica_lsn);

        if lag <= max_lag_bytes {
            info!(
                target_node_id,
                lag, "replica is in sync, proceeding with switchover"
            );
            return Ok(());
        }

        info!(
            target_node_id,
            lag, max_lag_bytes, "waiting for replica to sync"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::topology::ClusterTopology;
    use crate::raft::TopologyWatch;

    fn watch_from(t: ClusterTopology) -> TopologyWatch {
        let (_, rx) = TopologyWatch::new(t);
        rx
    }

    #[tokio::test]
    async fn synced_replica_returns_immediately() {
        let mut t = ClusterTopology {
            primary_node_id: "pg1".into(),
            ..Default::default()
        };
        t.last_flush_lsns.insert("pg1".into(), 1000);
        t.last_flush_lsns.insert("pg2".into(), 999); // 1-byte lag — within 10

        let rx = watch_from(t);
        let result = wait_for_replica_sync(&rx, "pg2", 10, Duration::from_secs(1)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn times_out_when_replica_behind() {
        let mut t = ClusterTopology {
            primary_node_id: "pg1".into(),
            ..Default::default()
        };
        t.last_flush_lsns.insert("pg1".into(), 1000);
        t.last_flush_lsns.insert("pg2".into(), 0); // far behind

        let rx = watch_from(t);
        let result = wait_for_replica_sync(&rx, "pg2", 10, Duration::from_millis(50)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn lag_zero_returns_immediately() {
        let mut t = ClusterTopology {
            primary_node_id: "pg1".into(),
            ..Default::default()
        };
        t.last_flush_lsns.insert("pg1".into(), 500);
        t.last_flush_lsns.insert("pg2".into(), 500); // zero lag

        let rx = watch_from(t);
        let result = wait_for_replica_sync(&rx, "pg2", 0, Duration::from_secs(1)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn lag_nonzero_waits_until_caught_up() {
        let mut t = ClusterTopology {
            primary_node_id: "pg1".into(),
            ..Default::default()
        };
        t.last_flush_lsns.insert("pg1".into(), 100);
        t.last_flush_lsns.insert("pg2".into(), 95); // 5 bytes lag

        let rx = watch_from(t);
        // max_lag_bytes=10 so 5-byte lag is within threshold
        let result = wait_for_replica_sync(&rx, "pg2", 10, Duration::from_secs(1)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn lag_wait_times_out() {
        let mut t = ClusterTopology {
            primary_node_id: "pg1".into(),
            ..Default::default()
        };
        t.last_flush_lsns.insert("pg1".into(), 10_000);
        t.last_flush_lsns.insert("pg2".into(), 0); // very far behind

        let rx = watch_from(t);
        // strict 0-lag threshold → will never satisfy
        let result = wait_for_replica_sync(&rx, "pg2", 0, Duration::from_millis(30)).await;
        assert!(result.is_err());
    }
}
