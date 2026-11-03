//! Encrypts instance secrets at rest, so a copy of the metadata database gives
//! up no database's certificates or internal passwords.

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use rand::RngCore;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

const NONCE_LEN: usize = 24;

/// Per-database material, shared by every node.
#[derive(Clone, Serialize, Deserialize)]
pub struct DatabaseSecrets {
    pub ca_cert_pem: String,
    pub ca_key_pem: String,
    pub superuser_password: String,
    pub replication_password: String,
    pub auth_password: String,
    pub api_key: String,
}

/// Per-node material.
#[derive(Clone, Serialize, Deserialize)]
pub struct NodeSecrets {
    pub cert_pem: String,
    pub key_pem: String,
}

/// A random value that is safe in a config file, a URL or a shell.
pub fn random_token() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

#[derive(Clone)]
pub struct SecretBox {
    cipher: XChaCha20Poly1305,
}

impl SecretBox {
    pub fn from_base64(key: &str) -> Result<Self> {
        let key = BASE64.decode(key.trim()).context("decode the secret key")?;
        if key.len() != 32 {
            bail!("the secret key must be 32 bytes, base64 encoded");
        }
        Ok(Self {
            cipher: XChaCha20Poly1305::new(Key::from_slice(&key)),
        })
    }

    /// Encrypt `value`, bound to `context`, so a row's ciphertext cannot be
    /// moved to another row.
    pub fn seal<T: Serialize>(&self, context: &str, value: &T) -> Result<Vec<u8>> {
        let plain = serde_json::to_vec(value)?;
        let mut nonce = [0u8; NONCE_LEN];
        rand::thread_rng().fill_bytes(&mut nonce);
        let sealed = self
            .cipher
            .encrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: &plain,
                    aad: context.as_bytes(),
                },
            )
            .map_err(|_| anyhow::anyhow!("encrypt secrets"))?;
        Ok([nonce.as_slice(), &sealed].concat())
    }

    pub fn open<T: DeserializeOwned>(&self, context: &str, sealed: &[u8]) -> Result<T> {
        if sealed.len() < NONCE_LEN {
            bail!("sealed secrets are too short");
        }
        let (nonce, body) = sealed.split_at(NONCE_LEN);
        let plain = self
            .cipher
            .decrypt(
                XNonce::from_slice(nonce),
                Payload {
                    msg: body,
                    aad: context.as_bytes(),
                },
            )
            .map_err(|_| anyhow::anyhow!("decrypt secrets: wrong key or altered data"))?;
        Ok(serde_json::from_slice(&plain)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secret_box() -> SecretBox {
        SecretBox::from_base64(&BASE64.encode([7u8; 32])).unwrap()
    }

    #[test]
    fn a_value_round_trips() {
        let b = secret_box();
        let sealed = b.seal("db-1", &"hello".to_string()).unwrap();
        assert_eq!(b.open::<String>("db-1", &sealed).unwrap(), "hello");
    }

    #[test]
    fn ciphertext_does_not_move_between_rows() {
        let b = secret_box();
        let sealed = b.seal("db-1", &"hello".to_string()).unwrap();
        assert!(b.open::<String>("db-2", &sealed).is_err());
    }

    #[test]
    fn another_key_cannot_open_it() {
        let sealed = secret_box().seal("db-1", &"hello".to_string()).unwrap();
        let other = SecretBox::from_base64(&BASE64.encode([8u8; 32])).unwrap();
        assert!(other.open::<String>("db-1", &sealed).is_err());
    }

    #[test]
    fn a_short_key_is_refused() {
        assert!(SecretBox::from_base64(&BASE64.encode([7u8; 16])).is_err());
    }
}
