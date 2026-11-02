use std::sync::Arc;

use sha2::{Digest, Sha256};
use sqlx::PgPool;
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;

use crate::ca::Ca;
use crate::secrets::SecretBox;

/// Shorter keys could be guessed from a stolen hash, so they never authenticate.
pub const MIN_API_KEY_LEN: usize = 32;

/// Requests that may use the metadata database before anyone has been
/// authenticated. It leaves most of the pool to hosts and operators.
const OPEN_ROUTE_LIMIT: usize = 6;

/// What API handlers and the reconciler share.
#[derive(Clone)]
pub struct AdminState {
    pub pool: PgPool,
    pub ca: Arc<Ca>,
    pub secrets: SecretBox,
    /// Held for the length of each request to a route open to anyone.
    pub open_routes: Arc<Semaphore>,
    api_key_hashes: Arc<Vec<[u8; 32]>>,
}

impl AdminState {
    /// `api_key_hashes` are in hex, as in the config.
    pub fn new(
        pool: PgPool,
        ca: Arc<Ca>,
        secrets: SecretBox,
        api_key_hashes: &[String],
    ) -> anyhow::Result<Self> {
        let hashes = api_key_hashes
            .iter()
            .map(|hash| {
                let bytes = hex::decode(hash)?;
                <[u8; 32]>::try_from(bytes).map_err(|_| anyhow::anyhow!("not a SHA-256"))
            })
            .collect::<anyhow::Result<_>>()?;
        Ok(Self {
            pool,
            ca,
            secrets,
            open_routes: Arc::new(Semaphore::new(OPEN_ROUTE_LIMIT)),
            api_key_hashes: Arc::new(hashes),
        })
    }

    /// Hashing first gives both sides the same length, so the comparison leaks
    /// nothing about a key's size either.
    pub fn accepts_key(&self, key: &str) -> bool {
        if key.len() < MIN_API_KEY_LEN {
            return false;
        }
        let candidate = sha256(key);
        self.api_key_hashes
            .iter()
            .fold(false, |found, k| found | bool::from(k.ct_eq(&candidate)))
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

    #[test]
    fn generated_keys_are_long_enough() {
        assert!(hash_api_key(&random_token()).is_ok());
    }
}
