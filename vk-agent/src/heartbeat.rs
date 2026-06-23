use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

/// Tracks heartbeats from the pgcluster Raft leader.
///
/// If no heartbeat is received within `timeout`, the tracker enters **safe mode**
/// and the agent will refuse Promote RPCs until the heartbeat resumes.
/// `safe_mode_since` records when safe mode was entered so callers can enforce
/// a hard fence after a configurable additional delay.
#[derive(Debug)]
pub struct HeartbeatTracker {
    last_seen: Arc<Mutex<Instant>>,
    timeout: Duration,
    in_safe_mode: Arc<AtomicBool>,
    safe_mode_since: Arc<Mutex<Option<Instant>>>,
}

impl HeartbeatTracker {
    /// Create a new tracker. Safe mode starts as **inactive** — the agent
    /// does not refuse promotes at startup before the first heartbeat window
    /// expires. The watchdog task will flip the flag after `timeout` elapses.
    pub fn new(timeout: Duration) -> Arc<Self> {
        Arc::new(Self {
            last_seen: Arc::new(Mutex::new(Instant::now())),
            timeout,
            in_safe_mode: Arc::new(AtomicBool::new(false)),
            safe_mode_since: Arc::new(Mutex::new(None)),
        })
    }

    /// Record that a heartbeat was just received. Clears safe mode.
    pub fn touch(&self) {
        *self.last_seen.lock().unwrap() = Instant::now();
        if self.in_safe_mode.swap(false, Ordering::SeqCst) {
            *self.safe_mode_since.lock().unwrap() = None;
            tracing::info!("pgcluster heartbeat restored — leaving safe mode");
        }
    }

    /// Returns `true` when the agent is in safe mode (heartbeat lost).
    pub fn is_safe_mode(&self) -> bool {
        self.in_safe_mode.load(Ordering::SeqCst)
    }

    /// Returns how long the agent has been continuously in safe mode.
    /// Returns `None` if the agent is not currently in safe mode.
    pub fn time_in_safe_mode(&self) -> Option<Duration> {
        self.safe_mode_since.lock().unwrap().map(|t| t.elapsed())
    }

    /// Spawn a background tokio task that checks every second whether the
    /// heartbeat deadline has been exceeded and flips `in_safe_mode` accordingly.
    pub fn spawn_watchdog(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                // Check before sleeping so the first poll after a clock
                // advance (via tokio::time::advance in tests, or real time in
                // production) immediately evaluates the timeout condition.
                let elapsed = self.last_seen.lock().unwrap().elapsed();
                if elapsed > self.timeout && !self.in_safe_mode.load(Ordering::SeqCst) {
                    self.in_safe_mode.store(true, Ordering::SeqCst);
                    *self.safe_mode_since.lock().unwrap() = Some(Instant::now());
                    tracing::warn!(
                        elapsed_secs = elapsed.as_secs_f64(),
                        timeout_secs = self.timeout.as_secs_f64(),
                        "pgcluster heartbeat lost — entering safe mode"
                    );
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn starts_not_in_safe_mode() {
        let tracker = HeartbeatTracker::new(Duration::from_secs(10));
        assert!(!tracker.is_safe_mode());
        assert!(tracker.time_in_safe_mode().is_none());
    }

    #[tokio::test]
    async fn touch_clears_safe_mode() {
        let tracker = HeartbeatTracker::new(Duration::from_secs(10));
        // Force safe mode by setting the flag directly.
        tracker.in_safe_mode.store(true, Ordering::SeqCst);
        assert!(tracker.is_safe_mode());
        tracker.touch();
        assert!(!tracker.is_safe_mode());
        assert!(tracker.time_in_safe_mode().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn watchdog_sets_safe_mode_after_timeout() {
        let tracker = HeartbeatTracker::new(Duration::from_secs(5));
        assert!(!tracker.is_safe_mode());

        let t = tracker.clone();
        t.spawn_watchdog();

        // Advance time by more than the timeout.
        tokio::time::advance(Duration::from_secs(6)).await;
        // Yield so the watchdog task can run.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert!(tracker.is_safe_mode());
        assert!(tracker.time_in_safe_mode().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn watchdog_clears_safe_mode_on_touch() {
        let tracker = HeartbeatTracker::new(Duration::from_secs(5));
        let t = tracker.clone();
        t.spawn_watchdog();

        // Expire the heartbeat.
        tokio::time::advance(Duration::from_secs(6)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(tracker.is_safe_mode());
        assert!(tracker.time_in_safe_mode().is_some());

        // A new heartbeat should clear safe mode immediately.
        tracker.touch();
        assert!(!tracker.is_safe_mode());
        assert!(tracker.time_in_safe_mode().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn safe_mode_triggers_after_timeout() {
        let tracker = HeartbeatTracker::new(Duration::from_secs(5));
        assert!(!tracker.is_safe_mode());
        let t = tracker.clone();
        t.spawn_watchdog();
        tokio::time::advance(Duration::from_secs(6)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(tracker.is_safe_mode());
    }

    #[tokio::test]
    async fn safe_mode_blocks_promote() {
        let tracker = HeartbeatTracker::new(Duration::from_secs(10));
        // Simulate safe mode by directly setting the flag.
        tracker.in_safe_mode.store(true, Ordering::SeqCst);
        // When safe mode is on, is_safe_mode() returns true — the server
        // checks this flag before executing Promote RPCs.
        assert!(tracker.is_safe_mode(), "safe mode should block promote");
        // A heartbeat clears safe mode and re-enables promote.
        tracker.touch();
        assert!(!tracker.is_safe_mode());
    }

    #[tokio::test(start_paused = true)]
    async fn time_in_safe_mode_tracks_duration() {
        let tracker = HeartbeatTracker::new(Duration::from_secs(5));
        let t = tracker.clone();
        t.spawn_watchdog();

        // No safe mode yet.
        assert!(tracker.time_in_safe_mode().is_none());

        // Expire the heartbeat — watchdog enters safe mode.
        tokio::time::advance(Duration::from_secs(6)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(tracker.time_in_safe_mode().is_some());

        // Advance further — time_in_safe_mode grows.
        tokio::time::advance(Duration::from_secs(20)).await;
        tokio::task::yield_now().await;
        let elapsed = tracker.time_in_safe_mode().expect("should be in safe mode");
        assert!(
            elapsed >= Duration::from_secs(19),
            "elapsed should be at least 19s, got {:?}",
            elapsed
        );
    }
}
