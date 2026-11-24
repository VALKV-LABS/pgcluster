//! Which client addresses the proxy accepts.
//!
//! The proxy is the only place that sees a client's address: Postgres sees the
//! proxy, so `pg_hba.conf` cannot filter clients. SIGHUP reloads the list from
//! the config file. Connected clients stay, unless `drop_refused_clients` is
//! set, as for a suspended database: then those the list no longer accepts
//! are sent away once they finish what they are doing.

use std::net::IpAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use ipnet::IpNet;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;

/// The current policy, and a signal at each reload, so connected clients can
/// be checked against it again.
pub type SharedAllowlist = Arc<watch::Sender<Arc<Policy>>>;

/// The list, and whether connected clients it refuses must go.
#[derive(Debug, PartialEq, Eq)]
pub struct Policy {
    pub list: Allowlist,
    pub drop_refused: bool,
}

impl Policy {
    fn from_config(proxy: &crate::config::ProxyConfig) -> Result<Self> {
        Ok(Self {
            list: Allowlist::parse(proxy.allowed_cidrs.as_deref())?,
            drop_refused: proxy.drop_refused_clients,
        })
    }

    /// Whether a client already connected from `addr` must go.
    pub fn evicts(&self, addr: IpAddr) -> bool {
        self.drop_refused && !self.list.allows(addr)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Allowlist {
    /// `allowed_cidrs` is not set.
    Open,
    Only(Vec<IpNet>),
}

impl Allowlist {
    pub fn parse(cidrs: Option<&[String]>) -> Result<Self> {
        let Some(cidrs) = cidrs else {
            return Ok(Self::Open);
        };
        let nets = cidrs
            .iter()
            .map(|c| c.parse().with_context(|| format!("invalid CIDR {c:?}")))
            .collect::<Result<_>>()?;
        Ok(Self::Only(nets))
    }

    pub fn allows(&self, addr: IpAddr) -> bool {
        match self {
            Self::Open => true,
            Self::Only(nets) => {
                // A dual-stack listener reports IPv4 clients as ::ffff:a.b.c.d.
                let addr = addr.to_canonical();
                nets.iter().any(|net| net.contains(&addr))
            }
        }
    }
}

pub fn shared(proxy: &crate::config::ProxyConfig) -> Result<SharedAllowlist> {
    let (sender, _) = watch::channel(Arc::new(Policy::from_config(proxy)?));
    Ok(Arc::new(sender))
}

/// Resolves once the policy sends away a client connected from `addr`: at
/// once if it already does, or at the reload that makes it.
pub async fn evicted(allowlist: &SharedAllowlist, addr: IpAddr) {
    let mut changes = allowlist.subscribe();
    loop {
        if changes.borrow_and_update().evicts(addr) {
            return;
        }
        if changes.changed().await.is_err() {
            return std::future::pending().await;
        }
    }
}

/// Reload the list on SIGHUP. A config that no longer parses keeps the old list.
pub fn spawn_reload(config_path: String, allowlist: SharedAllowlist) -> Result<()> {
    let mut hangup = signal(SignalKind::hangup()).context("listen for SIGHUP")?;
    tokio::spawn(async move {
        while hangup.recv().await.is_some() {
            let reloaded = crate::config::PgClusterConfig::from_file(&config_path)
                .and_then(|cfg| Policy::from_config(&cfg.proxy));
            match reloaded {
                Ok(policy) => {
                    allowlist.send_replace(Arc::new(policy));
                    tracing::info!("reloaded allowed_cidrs");
                }
                Err(e) => tracing::error!(err = %e, "kept the old allowed_cidrs"),
            }
        }
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list(cidrs: &[&str]) -> Allowlist {
        let cidrs: Vec<String> = cidrs.iter().map(|c| c.to_string()).collect();
        Allowlist::parse(Some(&cidrs)).unwrap()
    }

    #[test]
    fn unset_accepts_everyone() {
        let open = Allowlist::parse(None).unwrap();
        assert!(open.allows("203.0.113.9".parse().unwrap()));
    }

    #[test]
    fn empty_refuses_everyone() {
        assert!(!list(&[]).allows("127.0.0.1".parse().unwrap()));
    }

    #[test]
    fn matches_blocks_and_mapped_ipv4() {
        let cidrs = list(&["198.51.100.0/24", "2001:db8::/32"]);
        assert!(cidrs.allows("198.51.100.7".parse().unwrap()));
        assert!(cidrs.allows("::ffff:198.51.100.7".parse().unwrap()));
        assert!(cidrs.allows("2001:db8::1".parse().unwrap()));
        assert!(!cidrs.allows("198.51.101.7".parse().unwrap()));
    }

    #[test]
    fn only_a_policy_that_drops_refused_clients_evicts() {
        let addr = "198.51.100.7".parse().unwrap();
        let keep = Policy {
            list: list(&[]),
            drop_refused: false,
        };
        assert!(
            !keep.evicts(addr),
            "an empty list alone keeps connected clients"
        );
        let shut = Policy {
            list: list(&[]),
            drop_refused: true,
        };
        assert!(shut.evicts(addr));
        let allowed = Policy {
            list: list(&["198.51.100.0/24"]),
            drop_refused: true,
        };
        assert!(!allowed.evicts(addr), "a client the list accepts stays");
    }

    #[tokio::test]
    async fn a_reload_that_refuses_a_client_evicts_it() {
        let addr: IpAddr = "198.51.100.7".parse().unwrap();
        let (sender, _) = watch::channel(Arc::new(Policy {
            list: list(&["198.51.100.0/24"]),
            drop_refused: true,
        }));
        let shared = Arc::new(sender);
        let waiting = tokio::spawn({
            let shared = shared.clone();
            async move { evicted(&shared, addr).await }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        shared.send_replace(Arc::new(Policy {
            list: list(&[]),
            drop_refused: true,
        }));
        tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .expect("the client is evicted")
            .unwrap();
    }

    #[test]
    fn a_bad_block_is_an_error() {
        let bad = ["10.0.0.0/33".to_string()];
        assert!(Allowlist::parse(Some(&bad)).is_err());
    }
}
