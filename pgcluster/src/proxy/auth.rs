//! Fetches a role's password verifier from Postgres, through the restricted
//! role `vk-agent` sets up. That role can call one `SECURITY DEFINER` function
//! and nothing else, the same arrangement as Supavisor's `auth_query`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use dashmap::DashMap;
use rand::RngCore;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use sqlx::PgPool;

use super::scram::Verifier;
use crate::credentials;
use crate::tls::SharedCa;

const AUTH_DATABASE: &str = "postgres";
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);

/// The role that may read verifiers.
#[derive(Clone)]
pub struct AuthRole {
    pub user: String,
    /// The name its password goes by, for `credentials`.
    pub password_env: String,
    /// The cluster CA, or `None` for plaintext in development.
    pub ca_pem: Option<SharedCa>,
}

const INVALID_PASSWORD: &str = "28P01";

pub struct AuthQuery {
    role: AuthRole,
    /// One small pool per primary address and password, because the primary can
    /// move and the password can rotate.
    pools: DashMap<(String, String), PgPool>,
    /// The `credentials` generation `pools` was built from.
    built_from: AtomicU64,
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
            built_from: AtomicU64::new(credentials::generation()),
            mock_secret,
        }
    }

    fn pool(&self, addr: &str, password: &str) -> Result<PgPool> {
        let generation = credentials::generation();
        if self.built_from.swap(generation, Ordering::SeqCst) != generation {
            self.pools.clear();
        }
        let key = (addr.to_string(), password.to_string());
        if let Some(pool) = self.pools.get(&key) {
            return Ok(pool.clone());
        }
        let (host, port) = addr
            .rsplit_once(':')
            .with_context(|| format!("address {addr} has no port"))?;
        let mut options = PgConnectOptions::new()
            .host(host)
            .port(port.parse().context("parse port")?)
            .username(&self.role.user)
            .password(password)
            .database(AUTH_DATABASE);
        options = match &self.role.ca_pem {
            Some(ca) => options
                .ssl_mode(PgSslMode::VerifyFull)
                .ssl_root_cert_from_pem(ca.load().to_vec()),
            None => options.ssl_mode(PgSslMode::Disable),
        };
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .idle_timeout(Duration::from_secs(60))
            .acquire_timeout(ACQUIRE_TIMEOUT)
            .connect_lazy_with(options);
        self.pools.insert(key, pool.clone());
        Ok(pool)
    }

    /// The verifier for `role` from the primary at `addr`. A role that cannot
    /// log in gets a mock, so it fails exactly as a wrong password does.
    pub async fn verifier(&self, addr: &str, role: &str) -> Result<Verifier> {
        let password = credentials::get(&self.role.password_env);
        let stored = match self.lookup(addr, &password, role).await {
            // The role's password may have just moved on, ahead of this node.
            Err(e) if refused_password(&e) => match credentials::next(&self.role.password_env) {
                Some(next) => self.lookup(addr, &next, role).await,
                None => Err(e),
            },
            result => result,
        }
        .context("query the password verifier")?;
        Ok(stored
            .as_deref()
            .and_then(Verifier::parse)
            .unwrap_or_else(|| Verifier::mock(role, &self.mock_secret)))
    }

    async fn lookup(
        &self,
        addr: &str,
        password: &str,
        role: &str,
    ) -> Result<Option<String>, sqlx::Error> {
        let pool = self
            .pool(addr, password)
            .map_err(|e| sqlx::Error::Configuration(e.into()))?;
        sqlx::query_scalar("SELECT pgcluster.get_auth($1)")
            .bind(role)
            .fetch_one(&pool)
            .await
    }
}

fn refused_password(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(|e| e.code().as_deref() == Some(INVALID_PASSWORD))
}
