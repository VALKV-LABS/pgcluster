//! Replacing the two secrets `pgcluster-admin` itself holds.
//!
//! The admin CA moves in three phases, so that at no point does a host and this
//! service disagree about whom to trust. Each ends when every enrolled host has
//! caught up, which means a host that stays away holds a rotation open rather
//! than being locked out.
//!
//! - `trust`: hosts learn the new CA, and the agent API accepts client
//!   certificates from both.
//! - `issue`: this service presents a certificate from the new CA, signs with it,
//!   and hosts ask for new client certificates.
//! - `trim`: the old CA is trusted by nobody, on either side.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool};

use crate::ca::{Ca, ADMIN_CA_NAME};
use crate::error::ApiError;
use crate::model::AdminCa;
use crate::secrets::{random_token, SecretBox};
use crate::state::AdminState;
use crate::tls;

const NEW_KEY_CONTEXT: &str = "admin-ca-rotation";

#[derive(Debug, Clone, FromRow)]
struct Rotation {
    phase: String,
    old_cert_pem: String,
    new_cert_pem: String,
    new_key: Vec<u8>,
}

impl Rotation {
    fn bundle(&self) -> String {
        match self.phase.as_str() {
            "trim" => self.new_cert_pem.clone(),
            _ => format!("{}\n{}", self.new_cert_pem, self.old_cert_pem),
        }
    }

    fn new_ca(&self, state: &AdminState) -> Result<Ca> {
        let key: String = state.secrets.open(NEW_KEY_CONTEXT, &self.new_key)?;
        Ca::from_pem(&self.new_cert_pem, &key)
    }
}

async fn current(pool: &PgPool) -> Result<Option<Rotation>, sqlx::Error> {
    sqlx::query_as(
        "SELECT phase, old_cert_pem, new_cert_pem, new_key FROM admin_ca_rotation WHERE id = 1",
    )
    .fetch_optional(pool)
    .await
}

fn hash(pem: &str) -> String {
    hex::encode(Sha256::digest(pem.as_bytes()))
}

async fn bump_all_hosts(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE hosts SET generation = generation + 1 WHERE enrolled_at IS NOT NULL")
        .execute(pool)
        .await?;
    Ok(())
}

/// What desired state says about CAs, during a rotation.
pub async fn for_hosts(state: &AdminState) -> Result<Option<AdminCa>, ApiError> {
    Ok(current(&state.pool).await?.map(|rotation| AdminCa {
        bundle_pem: rotation.bundle(),
        renew_client_certificate: rotation.phase == "issue",
    }))
}

/// Make the listeners match a phase. Doing this again changes nothing.
fn apply(state: &AdminState, rotation: &Rotation) -> Result<()> {
    let Some(listeners) = &state.listeners else {
        if rotation.phase != "trust" {
            state.set_ca(Arc::new(rotation.new_ca(state)?));
        }
        return Ok(());
    };
    let new_ca = Arc::new(rotation.new_ca(state)?);
    let signer = match rotation.phase.as_str() {
        "trust" => state.ca(),
        _ => new_ca.clone(),
    };
    state.set_ca(signer.clone());
    let issued = signer.issue_server(&listeners.server_names)?;
    let trusted: Vec<&str> = match rotation.phase.as_str() {
        "trim" => vec![&rotation.new_cert_pem],
        _ => vec![&rotation.old_cert_pem, &rotation.new_cert_pem],
    };
    listeners
        .agent
        .store(Arc::new(tls::agent_tls(&trusted, &issued)?));
    listeners.api.store(Arc::new(tls::server_tls(&issued)?));
    if rotation.phase == "trim" {
        new_ca.store(Path::new(&listeners.ca_dir))?;
    }
    Ok(())
}

/// After a restart, pick up where the listeners were.
pub async fn resume(state: &AdminState) -> Result<()> {
    if let Some(rotation) = current(&state.pool).await? {
        apply(state, &rotation)?;
    }
    Ok(())
}

#[derive(Debug, Serialize)]
pub struct Status {
    /// `trust`, `issue` or `trim`, or nothing when no rotation is running.
    pub phase: Option<String>,
    /// What to trust: both CAs during a rotation.
    pub bundle_pem: String,
    /// Hosts the current phase is waiting for.
    pub waiting_for: Vec<String>,
}

async fn waiting_for(pool: &PgPool, rotation: &Rotation) -> Result<Vec<String>, sqlx::Error> {
    let (column, wanted) = match rotation.phase.as_str() {
        "issue" => ("certificate_ca", hash(&rotation.new_cert_pem)),
        _ => ("ca_fingerprint", hash(&rotation.bundle())),
    };
    sqlx::query_scalar(&format!(
        "SELECT name FROM hosts WHERE enrolled_at IS NOT NULL \
         AND {column} IS DISTINCT FROM $1 ORDER BY name"
    ))
    .bind(wanted)
    .fetch_all(pool)
    .await
}

pub async fn status(state: &AdminState) -> Result<Status, ApiError> {
    Ok(match current(&state.pool).await? {
        Some(rotation) => Status {
            waiting_for: waiting_for(&state.pool, &rotation).await?,
            phase: Some(rotation.phase.clone()),
            bundle_pem: rotation.bundle(),
        },
        None => Status {
            phase: None,
            bundle_pem: state.ca().cert_pem().to_string(),
            waiting_for: vec![],
        },
    })
}

/// Start a rotation. New enrolment tokens fail until the host trusts the bundle,
/// because a token carries the hash of one CA, so issue them after it ends.
pub async fn start(state: &AdminState) -> Result<Status, ApiError> {
    if current(&state.pool).await?.is_some() {
        return Err(ApiError::conflict("a rotation is already running"));
    }
    // Distinct from the old one's, so that nothing mistakes one for the other.
    let new_ca = Ca::generate(&format!("{ADMIN_CA_NAME}-{}", &random_token()[..8]))?;
    let rotation = Rotation {
        phase: "trust".to_string(),
        old_cert_pem: state.ca().cert_pem().to_string(),
        new_cert_pem: new_ca.cert_pem().to_string(),
        new_key: state.secrets.seal(NEW_KEY_CONTEXT, &new_ca.key_pem())?,
    };
    let inserted = sqlx::query(
        "INSERT INTO admin_ca_rotation (phase, old_cert_pem, new_cert_pem, new_key) \
         VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING",
    )
    .bind(&rotation.phase)
    .bind(&rotation.old_cert_pem)
    .bind(&rotation.new_cert_pem)
    .bind(&rotation.new_key)
    .execute(&state.pool)
    .await?;
    if inserted.rows_affected() == 0 {
        return Err(ApiError::conflict("a rotation is already running"));
    }
    apply(state, &rotation)?;
    bump_all_hosts(&state.pool).await?;
    status(state).await
}

/// Move a rotation on once every host has caught up.
pub async fn advance(state: &AdminState) -> Result<()> {
    let Some(mut rotation) = current(&state.pool).await? else {
        return Ok(());
    };
    if !waiting_for(&state.pool, &rotation).await?.is_empty() {
        return Ok(());
    }
    let next = match rotation.phase.as_str() {
        "trust" => "issue",
        "issue" => "trim",
        _ => {
            sqlx::query("DELETE FROM admin_ca_rotation WHERE id = 1")
                .execute(&state.pool)
                .await?;
            bump_all_hosts(&state.pool).await?;
            tracing::info!("the admin CA has been replaced");
            return Ok(());
        }
    };
    rotation.phase = next.to_string();
    apply(state, &rotation)?;
    sqlx::query("UPDATE admin_ca_rotation SET phase = $1 WHERE id = 1")
        .bind(next)
        .execute(&state.pool)
        .await?;
    bump_all_hosts(&state.pool).await?;
    tracing::info!(phase = next, "admin CA rotation moved on");
    Ok(())
}

/// Seal everything again with the current key, in one transaction, so that a
/// failure leaves all of it readable with the old one. Returns how many values.
pub async fn reseal_all(pool: &PgPool, secrets: &SecretBox) -> Result<usize> {
    let mut tx = pool.begin().await?;
    let mut count = 0;

    let databases: Vec<(String, Vec<u8>)> =
        sqlx::query_as("SELECT id, secrets FROM databases WHERE secrets IS NOT NULL FOR UPDATE")
            .fetch_all(&mut *tx)
            .await?;
    for (id, sealed) in databases {
        let resealed = secrets.reseal(&id, &sealed).context("reseal a database")?;
        sqlx::query("UPDATE databases SET secrets = $2 WHERE id = $1")
            .bind(&id)
            .bind(resealed)
            .execute(&mut *tx)
            .await?;
        count += 1;
    }

    let nodes: Vec<(String, String, Vec<u8>)> =
        sqlx::query_as("SELECT database_id, node, secrets FROM nodes FOR UPDATE")
            .fetch_all(&mut *tx)
            .await?;
    for (database_id, node, sealed) in nodes {
        let context = format!("{database_id}/{node}");
        let resealed = secrets.reseal(&context, &sealed).context("reseal a node")?;
        sqlx::query("UPDATE nodes SET secrets = $3 WHERE database_id = $1 AND node = $2")
            .bind(&database_id)
            .bind(&node)
            .bind(resealed)
            .execute(&mut *tx)
            .await?;
        count += 1;
    }

    let repositories: Vec<(String, Vec<u8>)> = sqlx::query_as(
        "SELECT id, credentials FROM backup_repositories WHERE credentials IS NOT NULL FOR UPDATE",
    )
    .fetch_all(&mut *tx)
    .await?;
    for (id, sealed) in repositories {
        let context = format!("repository/{id}");
        let resealed = secrets
            .reseal(&context, &sealed)
            .context("reseal a repository")?;
        sqlx::query("UPDATE backup_repositories SET credentials = $2 WHERE id = $1")
            .bind(&id)
            .bind(resealed)
            .execute(&mut *tx)
            .await?;
        count += 1;
    }

    let key: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT new_key FROM admin_ca_rotation WHERE id = 1 FOR UPDATE")
            .fetch_optional(&mut *tx)
            .await?;
    if let Some(sealed) = key {
        let resealed = secrets
            .reseal(NEW_KEY_CONTEXT, &sealed)
            .context("reseal the rotation")?;
        sqlx::query("UPDATE admin_ca_rotation SET new_key = $1 WHERE id = 1")
            .bind(resealed)
            .execute(&mut *tx)
            .await?;
        count += 1;
    }

    tx.commit().await?;
    Ok(count)
}
