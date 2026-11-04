use std::sync::Arc;

use arc_swap::ArcSwap;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;

use crate::ca::Ca;
use crate::secrets::SecretBox;
use crate::tls::SharedAcceptor;

/// Shorter keys could be guessed from a stolen hash, so they never authenticate.
pub const MIN_API_KEY_LEN: usize = 32;

/// Requests that may use the metadata database before anyone has been
/// authenticated. It leaves most of the pool to hosts and operators.
const OPEN_ROUTE_LIMIT: usize = 6;

/// What a rotation of the admin CA changes while the service runs.
pub struct Listeners {
    pub agent: SharedAcceptor,
    pub api: SharedAcceptor,
    pub server_names: Vec<String>,
    pub ca_dir: String,
}

/// What API handlers and the reconciler share.
#[derive(Clone)]
pub struct AdminState {
    pub pool: PgPool,
    ca: Arc<ArcSwap<Ca>>,
    pub secrets: SecretBox,
    /// `None` in tests that do not serve TLS.
    pub listeners: Option<Arc<Listeners>>,
    /// Held for the length of each request to a route open to anyone.
    pub open_routes: Arc<Semaphore>,
    pub scheduler: Arc<crate::scheduler::SchedulerConfig>,
    /// Accepted keys by name. SIGHUP replaces them.
    api_keys: Arc<ArcSwap<Vec<NamedHash>>>,
}

/// A key's name, and the SHA-256 of the key.
type NamedHash = (String, [u8; 32]);

/// The name of the key that authenticated a request.
#[derive(Debug, Clone)]
pub struct KeyName(pub String);

fn parse_keys(keys: &[(String, String)]) -> anyhow::Result<Vec<NamedHash>> {
    keys.iter()
        .map(|(name, hash)| {
            let bytes = hex::decode(hash)?;
            let hash = <[u8; 32]>::try_from(bytes).map_err(|_| anyhow::anyhow!("not a SHA-256"))?;
            Ok((name.clone(), hash))
        })
        .collect()
}

impl AdminState {
    /// `api_key_hashes` are in hex, as in the config, and named `key-1` on.
    pub fn new(
        pool: PgPool,
        ca: Arc<Ca>,
        secrets: SecretBox,
        api_key_hashes: &[String],
    ) -> anyhow::Result<Self> {
        let named: Vec<(String, String)> = api_key_hashes
            .iter()
            .enumerate()
            .map(|(i, hash)| (format!("key-{}", i + 1), hash.clone()))
            .collect();
        Self::with_keys(pool, ca, secrets, &named)
    }

    /// `keys` are names and hex hashes.
    pub fn with_keys(
        pool: PgPool,
        ca: Arc<Ca>,
        secrets: SecretBox,
        keys: &[(String, String)],
    ) -> anyhow::Result<Self> {
        let hashes = parse_keys(keys)?;
        Ok(Self {
            pool,
            ca: Arc::new(ArcSwap::new(ca)),
            secrets,
            listeners: None,
            open_routes: Arc::new(Semaphore::new(OPEN_ROUTE_LIMIT)),
            scheduler: Arc::default(),
            api_keys: Arc::new(ArcSwap::from_pointee(hashes)),
        })
    }

    /// The CA that signs host and server certificates now.
    pub fn ca(&self) -> Arc<Ca> {
        self.ca.load_full()
    }

    pub fn set_ca(&self, ca: Arc<Ca>) {
        self.ca.store(ca);
    }

    pub fn accepts_key(&self, key: &str) -> bool {
        self.identify(key).is_some()
    }

    /// The name of the key, if it is accepted. Hashing first gives both sides
    /// the same length, so the comparison leaks nothing about a key's size, and
    /// every hash is compared, so timing leaks nothing about which matched.
    pub fn identify(&self, key: &str) -> Option<String> {
        if key.len() < MIN_API_KEY_LEN {
            return None;
        }
        let candidate = sha256(key);
        let keys = self.api_keys.load();
        let mut found = None;
        for (name, hash) in keys.iter() {
            if bool::from(hash.ct_eq(&candidate)) {
                found = Some(name.clone());
            }
        }
        found
    }

    /// Replace the accepted keys, as SIGHUP does. Requests in flight finish.
    pub fn set_keys(&self, keys: &[(String, String)]) -> anyhow::Result<()> {
        self.api_keys.store(Arc::new(parse_keys(keys)?));
        Ok(())
    }
}

fn sha256(text: &str) -> [u8; 32] {
    Sha256::digest(text.as_bytes()).into()
}

/// The hash the config holds for `key`, or an error if the key is too short.
pub fn hash_api_key(key: &str) -> anyhow::Result<String> {
    if key.len() < MIN_API_KEY_LEN {
        anyhow::bail!("an API key must be at least {MIN_API_KEY_LEN} characters");
    }
    Ok(hex::encode(sha256(key)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secrets::random_token;

    #[test]
    fn a_short_key_cannot_be_hashed() {
        assert!(hash_api_key(&"k".repeat(MIN_API_KEY_LEN - 1)).is_err());
        assert!(hash_api_key(&"k".repeat(MIN_API_KEY_LEN)).is_ok());
    }

    #[tokio::test]
    async fn one_key_is_revoked_alone() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://nobody@127.0.0.1:1/none")
            .unwrap();
        let (a, b) = (random_token(), random_token());
        let keys = |list: &[(&str, &str)]| -> Vec<(String, String)> {
            list.iter()
                .map(|(n, k)| (n.to_string(), hash_api_key(k).unwrap()))
                .collect()
        };
        let state = AdminState::with_keys(
            pool,
            Arc::new(Ca::generate("test").unwrap()),
            SecretBox::from_base64("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=").unwrap(),
            &keys(&[("ci", &a), ("ops", &b)]),
        )
        .unwrap();
        assert_eq!(state.identify(&a).as_deref(), Some("ci"));
        assert_eq!(state.identify(&b).as_deref(), Some("ops"));
        state.set_keys(&keys(&[("ops", &b)])).unwrap();
        assert_eq!(state.identify(&a), None);
        assert_eq!(state.identify(&b).as_deref(), Some("ops"));
    }

    #[test]
    fn generated_keys_are_long_enough() {
        assert!(hash_api_key(&random_token()).is_ok());
    }
}
