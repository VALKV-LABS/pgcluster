//! Settings that change with no restart. SIGHUP reads them again, because
//! restarting `pgcluster` would drop the clients of its proxy.

use std::sync::Mutex;

use arc_swap::{ArcSwap, ArcSwapOption};

use super::{BackupConfig, PgClusterConfig};

pub struct Runtime {
    api_keys: ArcSwap<Vec<String>>,
    /// `[backup]`, so a section added later starts backups with no restart.
    backup: ArcSwapOption<BackupConfig>,
    /// The label and error of the last backup, if it failed.
    backup_failure: Mutex<Option<(String, String)>>,
}

impl Runtime {
    pub fn new(config: &PgClusterConfig) -> Self {
        let runtime = Self {
            api_keys: ArcSwap::from_pointee(Vec::new()),
            backup: ArcSwapOption::empty(),
            backup_failure: Mutex::new(None),
        };
        runtime.apply(config);
        runtime
    }

    pub fn apply(&self, config: &PgClusterConfig) {
        self.api_keys.store(config.api.api_keys.clone().into());
        self.backup.store(config.backup.clone().map(Into::into));
    }

    pub fn api_keys(&self) -> Vec<String> {
        self.api_keys.load().to_vec()
    }

    /// The current `[backup]` section, if backups are on.
    pub fn backup(&self) -> Option<BackupConfig> {
        self.backup
            .load_full()
            .map(|b| (*b).clone())
            .filter(|b| b.enabled)
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
