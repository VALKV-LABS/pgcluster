//! Fetches a role's password verifier from Postgres, through the restricted
//! role `vk-agent` sets up. That role can call one `SECURITY DEFINER` function
//! and nothing else, the same arrangement as Supavisor's `auth_query`.

use std::time::Duration;

use anyhow::{Context, Result};
use dashmap::DashMap;
use rand::RngCore;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use sqlx::PgPool;

use super::scram::Verifier;

const AUTH_DATABASE: &str = "postgres";
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);

/// The role that may read verifiers.
#[derive(Clone)]
pub struct AuthRole {
    pub user: String,
    pub password: String,
    /// The cluster CA, or `None` for plaintext in development.
    pub ca_pem: Option<Vec<u8>>,
}

pub struct AuthQuery {
    role: AuthRole,
    /// One small pool per primary address, because the primary can move.
    pools: DashMap<String, PgPool>,
    /// Seeds mock verifiers, so unknown roles look alike only to this process.
    mock_secret: [u8; 32],
}

impl AuthQuery {
    pub fn new(role: AuthRole) -> Self {
        let mut mock_secret = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut mock_secret);
        Self {
            role,
            pools: DashMap::new(),
            mock_secret,
        }
    }

    fn pool(&self, addr: &str) -> Result<PgPool> {
        if let Some(pool) = self.pools.get(addr) {
            return Ok(pool.clone());
        }
        let (host, port) = addr
            .rsplit_once(':')
            .with_context(|| format!("address {addr} has no port"))?;
        let mut options = PgConnectOptions::new()
            .host(host)
            .port(port.parse().context("parse port")?)
            .username(&self.role.user)
            .password(&self.role.password)
            .database(AUTH_DATABASE);
        options = match &self.role.ca_pem {
            Some(ca) => options
                .ssl_mode(PgSslMode::VerifyFull)
                .ssl_root_cert_from_pem(ca.clone()),
            None => options.ssl_mode(PgSslMode::Disable),
        };
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .idle_timeout(Duration::from_secs(60))
            .acquire_timeout(ACQUIRE_TIMEOUT)
            .connect_lazy_with(options);
        self.pools.insert(addr.to_string(), pool.clone());
        Ok(pool)
    }

    /// The verifier for `role` from the primary at `addr`. A role that cannot
    /// log in gets a mock, so it fails exactly as a wrong password does.
    pub async fn verifier(&self, addr: &str, role: &str) -> Result<Verifier> {
        let stored: Option<String> = sqlx::query_scalar("SELECT pgcluster.get_auth($1)")
            .bind(role)
            .fetch_one(&self.pool(addr)?)
            .await
            .context("query the password verifier")?;
        Ok(stored
            .as_deref()
            .and_then(Verifier::parse)
            .unwrap_or_else(|| Verifier::mock(role, &self.mock_secret)))
    }
}
