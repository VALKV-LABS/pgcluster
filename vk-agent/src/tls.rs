//! TLS configuration for vk-agent's gRPC server.

use anyhow::{Context, Result};
use tonic::transport::{Certificate, Identity, ServerTlsConfig};

use crate::config::AgentTlsConfig;

/// Build a server TLS config from the configured PEM files. Every client must
/// present a certificate signed by the cluster CA.
pub fn server_tls_config(cfg: &AgentTlsConfig) -> Result<ServerTlsConfig> {
    let (Some(ca_path), Some(cert_path), Some(key_path)) = (&cfg.ca_cert, &cfg.cert, &cfg.key)
    else {
        anyhow::bail!("[tls] ca_cert, cert and key are all required");
    };

    let ca = std::fs::read(ca_path).with_context(|| format!("read CA cert file {ca_path}"))?;
    let cert = std::fs::read(cert_path).with_context(|| format!("read cert file {cert_path}"))?;
    let key = std::fs::read(key_path).with_context(|| format!("read key file {key_path}"))?;

    Ok(ServerTlsConfig::new()
        .identity(Identity::from_pem(cert, key))
        .client_ca_root(Certificate::from_pem(ca)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_paths_return_an_error() {
        assert!(server_tls_config(&AgentTlsConfig::default()).is_err());
    }

    #[test]
    fn unreadable_file_names_the_path() {
        let cfg = AgentTlsConfig {
            ca_cert: Some("/nonexistent/ca.crt".into()),
            cert: Some("/nonexistent/node.crt".into()),
            key: Some("/nonexistent/node.key".into()),
            insecure_plaintext: false,
        };
        let err = server_tls_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("/nonexistent/ca.crt"), "{err}");
    }
}
