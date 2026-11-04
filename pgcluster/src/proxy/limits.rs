//! Caps on client connections, so a flood cannot crowd out clients that are
//! connected, or that connect from elsewhere.
//!
//! A connection counts as pending from accept until it has logged in, and
//! pending connections are capped in total and per client address, with a
//! deadline. Logged-in clients count only toward the total.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::ProxyConfig;

pub struct ConnectionLimits {
    total: Arc<Semaphore>,
    pending: Arc<Semaphore>,
    per_address: Arc<Mutex<HashMap<IpAddr, usize>>>,
    per_address_max: usize,
    pub login_timeout: Duration,
}

/// Held for as long as a client is connected.
pub struct Admitted {
    pub total: OwnedSemaphorePermit,
    pub login: LoginGuard,
}

/// Held until a client has logged in, or gone.
pub struct LoginGuard {
    permit: Option<OwnedSemaphorePermit>,
    address: IpAddr,
    per_address: Arc<Mutex<HashMap<IpAddr, usize>>>,
    done: Arc<AtomicBool>,
}

impl LoginGuard {
    /// Whether the client has logged in. A watchdog reads this.
    pub fn done_flag(&self) -> Arc<AtomicBool> {
        self.done.clone()
    }

    /// The client has logged in, and stops counting as pending.
    pub fn finish(&mut self) {
        if self.permit.take().is_some() {
            release(&self.per_address, self.address);
            self.done.store(true, Ordering::SeqCst);
        }
    }
}

impl Drop for LoginGuard {
    fn drop(&mut self) {
        self.finish();
    }
}

fn release(per_address: &Mutex<HashMap<IpAddr, usize>>, address: IpAddr) {
    let mut counts = per_address.lock().expect("limits lock");
    if let Some(count) = counts.get_mut(&address) {
        *count -= 1;
        if *count == 0 {
            counts.remove(&address);
        }
    }
}

impl ConnectionLimits {
    pub fn new(config: &ProxyConfig) -> Self {
        Self::with(
            config.max_client_connections,
            config.max_pending_logins,
            config.max_pending_logins_per_address,
            Duration::from_millis(config.login_timeout_ms),
        )
    }

    pub fn with(total: usize, pending: usize, per_address: usize, login_timeout: Duration) -> Self {
        Self {
            total: Arc::new(Semaphore::new(total)),
            pending: Arc::new(Semaphore::new(pending)),
            per_address: Arc::default(),
            per_address_max: per_address,
            login_timeout,
        }
    }

    /// Admit a new connection from `address`, or refuse it.
    pub fn admit(&self, address: IpAddr) -> Option<Admitted> {
        let total = self.total.clone().try_acquire_owned().ok()?;
        {
            let mut counts = self.per_address.lock().expect("limits lock");
            let count = counts.entry(address).or_default();
            if *count >= self.per_address_max {
                return None;
            }
            *count += 1;
        }
        let Ok(permit) = self.pending.clone().try_acquire_owned() else {
            release(&self.per_address, address);
            return None;
        };
        Some(Admitted {
            total,
            login: LoginGuard {
                permit: Some(permit),
                address,
                per_address: self.per_address.clone(),
                done: Arc::default(),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(last: u8) -> IpAddr {
        IpAddr::from([10, 0, 0, last])
    }

    #[test]
    fn one_address_cannot_take_every_login_slot() {
        let limits = ConnectionLimits::with(100, 10, 2, Duration::from_secs(1));
        let _a = limits.admit(ip(1)).unwrap();
        let _b = limits.admit(ip(1)).unwrap();
        assert!(limits.admit(ip(1)).is_none());
        assert!(limits.admit(ip(2)).is_some());
    }

    #[test]
    fn a_logged_in_client_frees_its_login_slot() {
        let limits = ConnectionLimits::with(100, 1, 1, Duration::from_secs(1));
        let mut first = limits.admit(ip(1)).unwrap();
        assert!(limits.admit(ip(2)).is_none(), "the one login slot is taken");
        first.login.finish();
        assert!(first.login.done_flag().load(Ordering::SeqCst));
        let _second = limits.admit(ip(1)).unwrap();
    }

    #[test]
    fn the_total_counts_logged_in_clients() {
        let limits = ConnectionLimits::with(1, 10, 10, Duration::from_secs(1));
        let mut first = limits.admit(ip(1)).unwrap();
        first.login.finish();
        assert!(limits.admit(ip(2)).is_none());
        drop(first);
        assert!(limits.admit(ip(2)).is_some());
    }

    #[test]
    fn a_refused_connection_holds_nothing() {
        let limits = ConnectionLimits::with(10, 1, 5, Duration::from_secs(1));
        let _held = limits.admit(ip(1)).unwrap();
        assert!(limits.admit(ip(2)).is_none());
        assert!(limits.per_address.lock().unwrap().get(&ip(2)).is_none());
    }
}
