use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::raft::topology::FailoverEvent;

/// Build a `FailoverEvent` from the pieces collected during a failover run.
///
/// `started_at` is the `Instant` captured at the very beginning of the
/// failover sequence; `duration_ms` is computed from it automatically.
pub fn build_failover_event(
    old_primary: String,
    new_primary: String,
    started_at: Instant,
    reason: String,
) -> FailoverEvent {
    let now_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    FailoverEvent {
        old_primary,
        new_primary,
        triggered_at: now_unix,
        duration_ms: started_at.elapsed().as_millis() as u64,
        reason,
    }
}
