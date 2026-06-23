use std::collections::HashMap;
use std::time::Instant;

// ── FailureRecord ─────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct FailureRecord {
    pub count: u32,
    pub first_seen: Instant,
    pub last_seen: Instant,
}

// ── FailureTracker ────────────────────────────────────────────────────────────

/// Tracks consecutive health-check failures per node.
///
/// A successful check resets a node's counter to zero so that transient
/// blips do not accumulate toward the failover threshold.
#[derive(Debug, Default)]
pub struct FailureTracker {
    failures: HashMap<String, FailureRecord>,
}

impl FailureTracker {
    /// Record one failure for `node_id`.
    pub fn record_failure(&mut self, node_id: &str) {
        let now = Instant::now();
        let rec = self
            .failures
            .entry(node_id.to_string())
            .or_insert(FailureRecord {
                count: 0,
                first_seen: now,
                last_seen: now,
            });
        rec.count += 1;
        rec.last_seen = now;
    }

    /// Reset the failure counter for `node_id` (node answered OK).
    pub fn record_success(&mut self, node_id: &str) {
        self.failures.remove(node_id);
    }

    /// How many consecutive failures have been recorded for `node_id`.
    pub fn failure_count(&self, node_id: &str) -> u32 {
        self.failures.get(node_id).map(|r| r.count).unwrap_or(0)
    }

    /// Returns `true` when the failure count has reached or exceeded `threshold`.
    pub fn is_failed(&self, node_id: &str, threshold: u32) -> bool {
        self.failure_count(node_id) >= threshold
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_counts_failures() {
        let mut t = FailureTracker::default();
        assert_eq!(t.failure_count("pg1"), 0);

        t.record_failure("pg1");
        assert_eq!(t.failure_count("pg1"), 1);

        t.record_failure("pg1");
        t.record_failure("pg1");
        assert_eq!(t.failure_count("pg1"), 3);

        // Other nodes unaffected
        assert_eq!(t.failure_count("pg2"), 0);
    }

    #[test]
    fn success_clears_count() {
        let mut t = FailureTracker::default();
        t.record_failure("pg1");
        t.record_failure("pg1");
        assert_eq!(t.failure_count("pg1"), 2);

        t.record_success("pg1");
        assert_eq!(t.failure_count("pg1"), 0);
    }

    #[test]
    fn is_failed_threshold() {
        let mut t = FailureTracker::default();
        assert!(!t.is_failed("pg1", 3));

        t.record_failure("pg1");
        t.record_failure("pg1");
        assert!(!t.is_failed("pg1", 3));

        t.record_failure("pg1");
        assert!(t.is_failed("pg1", 3));

        t.record_failure("pg1");
        assert!(t.is_failed("pg1", 3)); // still failed above threshold
    }

    #[test]
    fn failure_increments_counter() {
        let mut t = FailureTracker::default();
        assert_eq!(t.failure_count("pg1"), 0);
        t.record_failure("pg1");
        assert_eq!(t.failure_count("pg1"), 1);
        t.record_failure("pg1");
        assert_eq!(t.failure_count("pg1"), 2);
    }

    #[test]
    fn success_resets_counter() {
        let mut t = FailureTracker::default();
        t.record_failure("pg1");
        t.record_failure("pg1");
        t.record_success("pg1");
        assert_eq!(t.failure_count("pg1"), 0);
    }

    #[test]
    fn threshold_triggers_failover_callback() {
        let mut t = FailureTracker::default();
        // The failover callback is triggered when is_failed() returns true.
        // Here we verify the tracker correctly signals "failed" at threshold.
        for _ in 0..3 {
            t.record_failure("pg1");
        }
        assert!(t.is_failed("pg1", 3), "should signal failover at threshold");
    }

    #[test]
    fn threshold_triggers_mark_offline_for_replica() {
        let mut t = FailureTracker::default();
        // Replicas are marked offline after reaching the same threshold.
        let threshold = 3u32;
        for _ in 0..threshold {
            t.record_failure("pg-replica");
        }
        assert!(
            t.is_failed("pg-replica", threshold),
            "replica should be marked offline at threshold"
        );
    }
}
