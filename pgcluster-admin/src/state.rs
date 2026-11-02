use std::sync::Arc;

use sha2::{Digest, Sha256};
use sqlx::PgPool;
use subtle::ConstantTimeEq;

use crate::ca::Ca;
use crate::secrets::SecretBox;

/// What API handlers and the reconciler share.
#[derive(Clone)]
pub struct AdminState {
    pub pool: PgPool,
    pub ca: Arc<Ca>,
    pub secrets: SecretBox,
    api_key_hashes: Arc<Vec<[u8; 32]>>,
}

impl AdminState {
    pub fn new(pool: PgPool, ca: Arc<Ca>, secrets: SecretBox, api_keys: &[String]) -> Self {
        Self {
            pool,
            ca,
            secrets,
            api_key_hashes: Arc::new(api_keys.iter().map(|k| sha256(k)).collect()),
        }
    }

    /// Hashing first gives both sides the same length, so the comparison leaks
    /// nothing about a key's size either.
    pub fn accepts_key(&self, key: &str) -> bool {
        let candidate = sha256(key);
        self.api_key_hashes
            .iter()
            .fold(false, |found, k| found | bool::from(k.ct_eq(&candidate)))
    }
}

fn sha256(text: &str) -> [u8; 32] {
    Sha256::digest(text.as_bytes()).into()
}
