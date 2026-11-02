//! Quorum lease: lets a proxy prove it can still reach a majority of voters.
//!
//! A proxy cut off from the majority must stop routing before the other side
//! can promote a new primary. openraft reports quorum contact only on the
//! leader, and a proxy runs on every instance, so each instance probes the
//! voters itself. The lease is stamped with the time a probe round started,
//! so it can only expire early, and it expires by the clock even while a probe
//! is hanging on a dead network.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use tonic::transport::{Channel, ClientTlsConfig};
use tracing::{info, warn};

use super::raft_proto::topology_service_client::TopologyServiceClient;
use super::raft_proto::GetLeaderRequest;
use super::types::{NodeId, RaftClient};

// ── QuorumLease ───────────────────────────────────────────────────────────────

/// Whether this instance reached a majority of voters within the lease.
pub struct QuorumLease {
    origin: Instant,
    /// Milliseconds from `origin` to the last renewal, plus one. Zero means
    /// never renewed.
    renewed: AtomicU64,
    duration: Duration,
}

impl QuorumLease {
    pub fn new(duration: Duration) -> Self {
        Self {
            origin: Instant::now(),
            renewed: AtomicU64::new(0),
            duration,
        }
    }

    /// Extend the lease from `at`. An older renewal never shortens a newer one.
    pub fn renew(&self, at: Instant) {
        let renewed = at.duration_since(self.origin).as_millis() as u64 + 1;
        self.renewed.fetch_max(renewed, Ordering::AcqRel);
    }

    /// Returns `true` if a majority was reached within the lease.
    pub fn is_held(&self) -> bool {
        let renewed = self.renewed.load(Ordering::Acquire);
        if renewed == 0 {
            return false;
        }
        let now = self.origin.elapsed().as_millis() as u64 + 1;
        now.saturating_sub(renewed) < self.duration.as_millis() as u64
    }
}

/// Returns `true` if `reached` voters, counting this one, are a majority.
fn is_majority(reached: usize, voters: usize) -> bool {
    reached > voters / 2
}

// ── Renewal ───────────────────────────────────────────────────────────────────

/// Probe the voters four times per lease, renewing it on each majority.
pub fn spawn_renewal(lease: Arc<QuorumLease>, raft: RaftClient, tls: Option<ClientTlsConfig>) {
    tokio::spawn(async move {
        let period = lease.duration / 4;
        let mut tick = tokio::time::interval(period);
        let mut channels: HashMap<NodeId, Channel> = HashMap::new();
        let mut held = false;

        loop {
            tick.tick().await;
            let started = Instant::now();

            let metrics = raft.metrics().borrow().clone();
            let membership = metrics.membership_config.membership().clone();
            let voters: Vec<NodeId> = membership.voter_ids().collect();

            if voters.contains(&metrics.id) {
                let probes: Vec<_> = voters
                    .iter()
                    .filter(|id| **id != metrics.id)
                    .filter_map(|id| {
                        let addr = &membership.get_node(id)?.addr;
                        let channel = match channels.get(id) {
                            Some(channel) => channel.clone(),
                            None => {
                                let channel = lazy_channel(addr, tls.as_ref())?;
                                channels.insert(*id, channel.clone());
                                channel
                            }
                        };
                        Some(probe(channel, period))
                    })
                    .collect();
                let reached = 1 + join_all(probes).await.iter().filter(|ok| **ok).count();
                if is_majority(reached, voters.len()) {
                    lease.renew(started);
                }
            }

            match (held, lease.is_held()) {
                (true, false) => warn!("lost contact with the Raft majority: proxy stops routing"),
                (false, true) => info!("reached the Raft majority: proxy routes"),
                _ => {}
            }
            held = lease.is_held();
        }
    });
}

/// A channel that connects on first use and reconnects after failures.
fn lazy_channel(addr: &str, tls: Option<&ClientTlsConfig>) -> Option<Channel> {
    let scheme = if tls.is_some() { "https" } else { "http" };
    let mut endpoint = Channel::from_shared(format!("{scheme}://{addr}")).ok()?;
    if let Some(tls) = tls {
        endpoint = endpoint.tls_config(tls.clone()).ok()?;
    }
    Some(endpoint.connect_lazy())
}

async fn probe(channel: Channel, timeout: Duration) -> bool {
    let mut client = TopologyServiceClient::new(channel);
    matches!(
        tokio::time::timeout(timeout, client.get_leader(GetLeaderRequest {})).await,
        Ok(Ok(_))
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_starts_unheld() {
        assert!(!QuorumLease::new(Duration::from_secs(1)).is_held());
    }

    #[test]
    fn renewed_lease_is_held() {
        let lease = QuorumLease::new(Duration::from_secs(1));
        lease.renew(Instant::now());
        assert!(lease.is_held());
    }

    #[test]
    fn lease_expires_by_the_clock() {
        let lease = QuorumLease::new(Duration::from_millis(50));
        lease.renew(Instant::now());
        std::thread::sleep(Duration::from_millis(80));
        assert!(!lease.is_held());
    }

    #[test]
    fn older_renewal_does_not_shorten_the_lease() {
        let lease = QuorumLease::new(Duration::from_millis(100));
        let older = Instant::now();
        std::thread::sleep(Duration::from_millis(60));
        lease.renew(Instant::now());
        lease.renew(older);
        std::thread::sleep(Duration::from_millis(60));
        assert!(lease.is_held());
    }

    #[test]
    fn majority_counts() {
        assert!(is_majority(1, 1));
        assert!(!is_majority(1, 2));
        assert!(!is_majority(1, 3));
        assert!(is_majority(2, 3));
        assert!(!is_majority(2, 5));
        assert!(is_majority(3, 5));
    }
}
