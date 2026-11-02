//! Which client addresses the proxy accepts.
//!
//! The proxy is the only place that sees a client's address: Postgres sees the
//! proxy, so `pg_hba.conf` cannot filter clients. SIGHUP reloads the list from
//! the config file, without dropping connected clients.

use std::net::IpAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use ipnet::IpNet;
use tokio::signal::unix::{signal, SignalKind};

pub type SharedAllowlist = Arc<ArcSwap<Allowlist>>;

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

pub fn shared(cidrs: Option<&[String]>) -> Result<SharedAllowlist> {
    Ok(Arc::new(ArcSwap::from_pointee(Allowlist::parse(cidrs)?)))
}

/// Reload the list on SIGHUP. A config that no longer parses keeps the old list.
pub fn spawn_reload(config_path: String, allowlist: SharedAllowlist) -> Result<()> {
    let mut hangup = signal(SignalKind::hangup()).context("listen for SIGHUP")?;
    tokio::spawn(async move {
        while hangup.recv().await.is_some() {
            let reloaded = crate::config::PgClusterConfig::from_file(&config_path)
                .and_then(|cfg| Allowlist::parse(cfg.proxy.allowed_cidrs.as_deref()));
            match reloaded {
                Ok(list) => {
                    allowlist.store(Arc::new(list));
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
    fn a_bad_block_is_an_error() {
        let bad = ["10.0.0.0/33".to_string()];
        assert!(Allowlist::parse(Some(&bad)).is_err());
    }
}
