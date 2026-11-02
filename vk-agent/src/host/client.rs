//! Talks to the agent API. Every connection is opened from here.

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use anyhow::{bail, Context, Result};
use rcgen::{CertificateParams, KeyPair, PKCS_ECDSA_P256_SHA256};
use reqwest::{Certificate, Client, Identity, StatusCode};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

use super::fsutil::write_private;
use super::model::{DesiredState, Report};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Certificates last 90 days, so this leaves two months of retries.
const ROTATE_AFTER: Duration = Duration::from_secs(30 * 86_400);

/// This host's credentials on disk.
struct IdentityFiles {
    ca: PathBuf,
    cert: PathBuf,
    key: PathBuf,
    host_id: PathBuf,
}

impl IdentityFiles {
    fn new(state_dir: &str) -> Self {
        let dir = Path::new(state_dir);
        Self {
            ca: dir.join("ca.crt"),
            cert: dir.join("host.crt"),
            key: dir.join("host.key"),
            host_id: dir.join("host_id"),
        }
    }

    fn exist(&self) -> bool {
        [&self.ca, &self.cert, &self.key, &self.host_id]
            .iter()
            .all(|p| p.exists())
    }
}

/// A token is `<secret>.<SHA-256 of the CA's PEM>`.
pub fn split_token(token: &str) -> Result<(&str, &str)> {
    let (secret, fingerprint) = token
        .trim()
        .rsplit_once('.')
        .context("the enrolment token has no CA hash")?;
    if secret.is_empty() || fingerprint.len() != 64 {
        bail!("the enrolment token is malformed");
    }
    Ok((secret, fingerprint))
}

pub fn fingerprint(pem: &str) -> String {
    hex::encode(Sha256::digest(pem.as_bytes()))
}

fn new_key_and_csr() -> Result<(String, String)> {
    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).context("generate key")?;
    let csr = CertificateParams::new(Vec::<String>::new())
        .context("request params")?
        .serialize_request(&key)
        .context("build request")?
        .pem()
        .context("encode request")?;
    Ok((key.serialize_pem(), csr))
}

fn build_client(ca_pem: &[u8], identity: Option<(&str, &str)>) -> Result<Client> {
    let mut builder = Client::builder()
        .use_rustls_tls()
        .tls_built_in_root_certs(false)
        .add_root_certificate(Certificate::from_pem(ca_pem).context("parse CA")?)
        .connect_timeout(CONNECT_TIMEOUT);
    if let Some((cert, key)) = identity {
        let pem = format!("{cert}\n{key}");
        builder = builder.identity(Identity::from_pem(pem.as_bytes()).context("parse identity")?);
    }
    builder.build().context("build HTTP client")
}

#[derive(Deserialize)]
struct EnrollResponse {
    host_id: String,
    certificate_pem: String,
}

#[derive(Deserialize)]
struct RotateResponse {
    certificate_pem: String,
}

/// The result of one poll for desired state.
pub enum Poll {
    Changed(DesiredState),
    Unchanged,
}

#[derive(Clone)]
pub struct AdminClient {
    base: String,
    files: Arc<IdentityFiles>,
    client: Arc<RwLock<Client>>,
    pub host_id: String,
}

impl AdminClient {
    /// Load this host's identity, enrolling with `token` on the first start.
    pub async fn connect(
        admin_url: &str,
        state_dir: &str,
        token: impl FnOnce() -> Result<String>,
    ) -> Result<Self> {
        let base = admin_url.trim_end_matches('/').to_string();
        let files = IdentityFiles::new(state_dir);
        if !files.exist() {
            enroll(&base, &files, &token()?).await?;
        }
        let client = client_from_files(&files)?;
        let host_id = std::fs::read_to_string(&files.host_id)
            .context("read host_id")?
            .trim()
            .to_string();
        Ok(Self {
            base,
            files: Arc::new(files),
            client: Arc::new(RwLock::new(client)),
            host_id,
        })
    }

    fn http(&self) -> Client {
        self.client.read().expect("client lock").clone()
    }

    /// Wait up to `wait` for state newer than `generation`.
    pub async fn poll(&self, generation: i64, wait: Duration) -> Result<Poll> {
        let response = self
            .http()
            .get(format!("{}/agent/v1/desired-state", self.base))
            .query(&[
                ("generation", generation.to_string()),
                ("wait_seconds", wait.as_secs().to_string()),
            ])
            .timeout(wait + REQUEST_TIMEOUT)
            .send()
            .await
            .context("poll desired state")?;
        match response.status() {
            StatusCode::NOT_MODIFIED => Ok(Poll::Unchanged),
            StatusCode::OK => Ok(Poll::Changed(
                response.json().await.context("parse desired state")?,
            )),
            status => bail!("desired state returned {status}"),
        }
    }

    pub async fn report(&self, report: &Report) -> Result<()> {
        self.http()
            .post(format!("{}/agent/v1/report", self.base))
            .json(report)
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await
            .context("send report")?
            .error_for_status()
            .context("report refused")?;
        Ok(())
    }

    /// Append to an operation's log. Output is a convenience, so a failure here
    /// must not fail the step.
    pub async fn log(&self, operation_id: Option<&str>, message: &str) {
        let Some(id) = operation_id else {
            return;
        };
        let result = self
            .http()
            .post(format!("{}/agent/v1/operations/{id}/log", self.base))
            .json(&json!({ "message": message }))
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await;
        if let Err(e) = result {
            tracing::debug!(err = %e, "could not send an operation log");
        }
    }

    /// Replace the client certificate once it is old enough.
    pub async fn rotate_if_due(&self) -> Result<()> {
        let issued = std::fs::metadata(&self.files.cert)
            .and_then(|m| m.modified())
            .context("read certificate age")?;
        let age = SystemTime::now().duration_since(issued).unwrap_or_default();
        if age < ROTATE_AFTER {
            return Ok(());
        }
        let (key, csr) = new_key_and_csr()?;
        let response: RotateResponse = self
            .http()
            .post(format!("{}/agent/v1/certificate", self.base))
            .json(&json!({ "csr_pem": csr }))
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await
            .context("request a new certificate")?
            .error_for_status()
            .context("certificate refused")?
            .json()
            .await
            .context("parse the new certificate")?;
        let ca = std::fs::read(&self.files.ca).context("read CA")?;
        let client = build_client(&ca, Some((&response.certificate_pem, &key)))?;
        write_private(&self.files.key, key.as_bytes())?;
        write_private(&self.files.cert, response.certificate_pem.as_bytes())?;
        *self.client.write().expect("client lock") = client;
        tracing::info!("rotated the client certificate");
        Ok(())
    }
}

fn client_from_files(files: &IdentityFiles) -> Result<Client> {
    let ca = std::fs::read(&files.ca).context("read CA")?;
    let cert = std::fs::read_to_string(&files.cert).context("read certificate")?;
    let key = std::fs::read_to_string(&files.key).context("read key")?;
    build_client(&ca, Some((&cert, &key)))
}

/// Trust comes from the token: the CA is fetched over a connection nothing
/// vouches for yet, then accepted only if it matches the hash the token carries.
async fn enroll(base: &str, files: &IdentityFiles, token: &str) -> Result<()> {
    let (secret, expected) = split_token(token)?;
    let unverified = Client::builder()
        .use_rustls_tls()
        .danger_accept_invalid_certs(true)
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .build()
        .context("build HTTP client")?;
    let ca_pem = unverified
        .get(format!("{base}/agent/v1/ca"))
        .send()
        .await
        .context("fetch the CA")?
        .error_for_status()
        .context("fetch the CA")?
        .text()
        .await
        .context("read the CA")?;
    if fingerprint(&ca_pem) != expected.to_lowercase() {
        bail!("the server's CA does not match the enrolment token");
    }

    let (key, csr) = new_key_and_csr()?;
    let response = build_client(ca_pem.as_bytes(), None)?
        .post(format!("{base}/agent/v1/enroll"))
        .json(&json!({ "token": secret, "csr_pem": csr }))
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
        .context("enrol")?;
    if response.status() == StatusCode::UNAUTHORIZED {
        bail!("the enrolment token is invalid, expired or already used");
    }
    let enrolled: EnrollResponse = response
        .error_for_status()
        .context("enrol")?
        .json()
        .await
        .context("parse the enrolment response")?;

    // host_id goes last: it marks the identity as complete.
    write_private(&files.ca, ca_pem.as_bytes())?;
    write_private(&files.key, key.as_bytes())?;
    write_private(&files.cert, enrolled.certificate_pem.as_bytes())?;
    write_private(&files.host_id, enrolled.host_id.as_bytes())?;
    tracing::info!(host_id = enrolled.host_id, "enrolled");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_token_splits_at_the_last_dot() {
        let hash = "a".repeat(64);
        let token = format!("secret.{hash}\n");
        assert_eq!(split_token(&token).unwrap(), ("secret", hash.as_str()));
    }

    #[test]
    fn a_token_without_a_hash_is_refused() {
        assert!(split_token("secret").is_err());
        assert!(split_token("secret.short").is_err());
        assert!(split_token(&format!(".{}", "a".repeat(64))).is_err());
    }

    #[test]
    fn a_new_key_comes_with_a_request_for_it() {
        let (key, csr) = new_key_and_csr().unwrap();
        assert!(key.contains("PRIVATE KEY"));
        assert!(csr.contains("CERTIFICATE REQUEST"));
    }
}
