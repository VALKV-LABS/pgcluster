//! Settings that change with no restart. SIGHUP reads them again, because
//! restarting `pgcluster` would drop the clients of its proxy.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

use arc_swap::ArcSwap;

use super::PgClusterConfig;

pub struct Runtime {
    api_keys: ArcSwap<Vec<String>>,
    /// 0 when no `[backup]` is configured.
    retain_full: AtomicU32,
    /// The label and error of the last backup, if it failed.
    backup_failure: Mutex<Option<(String, String)>>,
}

impl Runtime {
    pub fn new(config: &PgClusterConfig) -> Self {
        let runtime = Self {
            api_keys: ArcSwap::from_pointee(Vec::new()),
            retain_full: AtomicU32::new(0),
            backup_failure: Mutex::new(None),
        };
        runtime.apply(config);
        runtime
    }

    pub fn apply(&self, config: &PgClusterConfig) {
        self.api_keys.store(config.api.api_keys.clone().into());
        let retain_full = config.backup.as_ref().map_or(0, |b| b.retain_full);
        self.retain_full.store(retain_full, Ordering::SeqCst);
    }

    pub fn api_keys(&self) -> Vec<String> {
        self.api_keys.load().to_vec()
    }

    /// The current retention, or `configured` if there is none yet.
    pub fn retain_full(&self, configured: u32) -> u32 {
        match self.retain_full.load(Ordering::SeqCst) {
            0 => configured,
            current => current,
        }
    }

    pub fn record_backup(&self, failure: Option<(String, String)>) {
        *self.backup_failure.lock().expect("backup failure lock") = failure;
    }

    pub fn last_backup_failure(&self) -> Option<(String, String)> {
        self.backup_failure
            .lock()
            .expect("backup failure lock")
            .clone()
    }
}
