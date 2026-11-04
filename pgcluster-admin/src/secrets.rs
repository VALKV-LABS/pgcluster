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
    /// Encrypts this database's backups on the node before upload.
    #[serde(default)]
    pub backup_key: String,
    /// The key of the backups this database was restored from.
    #[serde(default)]
    pub source_backup_key: Option<String>,
    /// The CA being replaced, trusted alongside the new one during a rotation.
    #[serde(default)]
    pub previous_ca_cert_pem: Option<String>,
    /// Passwords about to replace these, during a rotation.
    #[serde(default)]
    pub next: Option<NextPasswords>,
    /// A certificate the proxy presents in place of the one from this CA.
    #[serde(default)]
    pub proxy_certificate: Option<ProxyCertificate>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ProxyCertificate {
    pub cert_pem: String,
    pub key_pem: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct NextPasswords {
    pub superuser_password: String,
    pub replication_password: String,
    pub auth_password: String,
    pub api_key: String,
}

impl NextPasswords {
    pub fn generate() -> Self {
        Self {
            superuser_password: random_token(),
            replication_password: random_token(),
            auth_password: random_token(),
            api_key: random_token(),
        }
    }
}

impl DatabaseSecrets {
    /// Every CA a node should trust: during a rotation, the old one as well.
    pub fn ca_bundle(&self) -> String {
        match &self.previous_ca_cert_pem {
            Some(previous) => format!("{}\n{previous}", self.ca_cert_pem),
            None => self.ca_cert_pem.clone(),
        }
    }
}

/// Credentials for a backup repository.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct RepositoryCredentials {
    #[serde(default)]
    pub access_key_id: Option<String>,
    #[serde(default)]
    pub secret_access_key: Option<String>,
}

/// Per-node material.
#[derive(Clone, Serialize, Deserialize)]
pub struct NodeSecrets {
    pub cert_pem: String,
    pub key_pem: String,
}

/// A key for libsodium, in base64.
pub fn random_backup_key() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    BASE64.encode(bytes)
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
    /// A key being rotated out, which can still open what it sealed.
    previous: Option<XChaCha20Poly1305>,
}

impl SecretBox {
    pub fn from_base64(key: &str) -> Result<Self> {
        let key = BASE64.decode(key.trim()).context("decode the secret key")?;
        if key.len() != 32 {
            bail!("the secret key must be 32 bytes, base64 encoded");
        }
        Ok(Self {
            cipher: XChaCha20Poly1305::new(Key::from_slice(&key)),
            previous: None,
        })
    }

    /// Keep opening what `key` sealed, while it is rotated out.
    pub fn with_previous(mut self, key: &str) -> Result<Self> {
        self.previous = Some(Self::from_base64(key)?.cipher);
        Ok(self)
    }

    pub fn has_previous(&self) -> bool {
        self.previous.is_some()
    }

    /// Seal `sealed` again with the current key.
    pub fn reseal(&self, context: &str, sealed: &[u8]) -> Result<Vec<u8>> {
        let value: serde_json::Value = self.open(context, sealed)?;
        self.seal(context, &value)
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
        let open_with = |cipher: &XChaCha20Poly1305| {
            cipher.decrypt(
                XNonce::from_slice(nonce),
                Payload {
                    msg: body,
                    aad: context.as_bytes(),
                },
            )
        };
        let plain = open_with(&self.cipher)
            .or_else(|e| self.previous.as_ref().map_or(Err(e), open_with))
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
    fn a_rotated_key_opens_old_values_and_seals_new_ones() {
        let old = secret_box();
        let sealed = old.seal("db-1", &"hello".to_string()).unwrap();
        let new_key = BASE64.encode([8u8; 32]);
        let rotating = SecretBox::from_base64(&new_key)
            .unwrap()
            .with_previous(&BASE64.encode([7u8; 32]))
            .unwrap();
        let resealed = rotating.reseal("db-1", &sealed).unwrap();

        let new = SecretBox::from_base64(&new_key).unwrap();
        assert_eq!(new.open::<String>("db-1", &resealed).unwrap(), "hello");
        assert!(old.open::<String>("db-1", &resealed).is_err());
        assert!(new.open::<String>("db-1", &sealed).is_err());
    }

    #[test]
    fn a_short_key_is_refused() {
        assert!(SecretBox::from_base64(&BASE64.encode([7u8; 16])).is_err());
    }
}
