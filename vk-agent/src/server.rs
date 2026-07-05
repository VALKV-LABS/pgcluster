pub mod agent {
    tonic::include_proto!("pgcluster.agent");
}

use std::path::Path;
use std::sync::Arc;

use tonic::{Request, Response, Status};

use agent::agent_service_server::AgentService;
use agent::{
    DemoteRequest, DemoteResponse, HeartbeatRequest, HeartbeatResponse, PromoteRequest,
    PromoteResponse, ReloadRequest, ReloadResponse, StatusRequest, StatusResponse, StopRequest,
    StopResponse,
};

use crate::config::AgentConfig;
use crate::files::write_signal_file;
use crate::heartbeat::HeartbeatTracker;
use crate::postgres::LocalPostgres;
use crate::process::PgCtl;

pub struct AgentServiceImpl {
    config: Arc<AgentConfig>,
    heartbeat: Arc<HeartbeatTracker>,
    pg: Arc<LocalPostgres>,
    pg_ctl: Arc<PgCtl>,
}

impl AgentServiceImpl {
    pub fn new(
        config: Arc<AgentConfig>,
        heartbeat: Arc<HeartbeatTracker>,
        pg: Arc<LocalPostgres>,
        pg_ctl: Arc<PgCtl>,
    ) -> Self {
        Self {
            config,
            heartbeat,
            pg,
            pg_ctl,
        }
    }
}

#[tonic::async_trait]
impl AgentService for AgentServiceImpl {
    /// Promote this standby to primary via `SELECT pg_promote()`.
    ///
    /// Using the SQL function is more reliable than writing `promote.signal`
    /// because it works over an existing authenticated connection and does not
    /// require the agent to have write access to PGDATA.
    async fn promote(
        &self,
        _request: Request<PromoteRequest>,
    ) -> Result<Response<PromoteResponse>, Status> {
        // Refuse promotion when the heartbeat watchdog has entered safe mode.
        // This prevents a stale pgcluster leader from causing split-brain.
        if self.heartbeat.is_safe_mode() {
            tracing::warn!("promote RPC rejected: agent is in safe mode (heartbeat lost)");
            return Ok(Response::new(PromoteResponse {
                success: false,
                error: "safe mode: pgcluster heartbeat lost — promote rejected to prevent split-brain".into(),
                promoted_at_lsn: 0,
                new_timeline: 0,
            }));
        }
        // pg_promote(wait, wait_seconds) — wait up to 30 s for promotion to complete.
        match sqlx::query("SELECT pg_promote(true, 30)")
            .execute(self.pg.pool())
            .await
        {
            Ok(_) => Ok(Response::new(PromoteResponse {
                success: true,
                error: String::new(),
                promoted_at_lsn: 0,
                new_timeline: 0,
            })),
            Err(e) => Ok(Response::new(PromoteResponse {
                success: false,
                error: e.to_string(),
                promoted_at_lsn: 0,
                new_timeline: 0,
            })),
        }
    }

    /// Demote this node to a standby by writing `standby.signal`,
    /// updating `postgresql.auto.conf`, and removing `promote.signal`.
    ///
    /// If `new_primary_conninfo` contains `password=<value>`, the password is
    /// extracted, written to `PGDATA/.pgpass` (mode 0600), and replaced with
    /// `passfile=<path>` in the stored conninfo so that credentials are never
    /// stored in plaintext inside the config file.
    async fn demote(
        &self,
        request: Request<DemoteRequest>,
    ) -> Result<Response<DemoteResponse>, Status> {
        let req = request.into_inner();
        let data_dir = Path::new(&self.config.data_dir);

        if let Err(e) = write_signal_file(data_dir, "standby.signal").await {
            return Ok(Response::new(DemoteResponse {
                success: false,
                error: e.to_string(),
            }));
        }

        // Write new primary_conninfo and slot_name to postgresql.auto.conf.
        if !req.new_primary_conninfo.is_empty() {
            let (conninfo, maybe_password) =
                extract_conninfo_password(&req.new_primary_conninfo);

            // If the conninfo carried a password, persist it to .pgpass and use
            // passfile= so the password is never stored in the config file.
            let final_conninfo = if let Some(password) = maybe_password {
                let host = conninfo_value(&req.new_primary_conninfo, "host")
                    .unwrap_or_else(|| "*".to_string());
                let port = conninfo_value(&req.new_primary_conninfo, "port")
                    .unwrap_or_else(|| "5432".to_string());
                let user = conninfo_value(&req.new_primary_conninfo, "user")
                    .unwrap_or_else(|| "*".to_string());

                if let Err(e) =
                    crate::files::write_pgpass(data_dir, &host, &port, "replication", &user, &password)
                        .await
                {
                    return Ok(Response::new(DemoteResponse {
                        success: false,
                        error: format!("write_pgpass failed: {e}"),
                    }));
                }

                let passfile = data_dir.join(".pgpass");
                format!("{} passfile={}", conninfo, passfile.display())
            } else {
                conninfo
            };

            if let Err(e) =
                crate::files::update_auto_conf(data_dir, "primary_conninfo", &final_conninfo).await
            {
                return Ok(Response::new(DemoteResponse {
                    success: false,
                    error: e.to_string(),
                }));
            }
        }

        if !req.slot_name.is_empty() {
            if let Err(e) =
                crate::files::update_auto_conf(data_dir, "primary_slot_name", &req.slot_name).await
            {
                return Ok(Response::new(DemoteResponse {
                    success: false,
                    error: e.to_string(),
                }));
            }
        }

        // Remove promote.signal if present; ignore errors.
        let _ = tokio::fs::remove_file(data_dir.join("promote.signal")).await;

        Ok(Response::new(DemoteResponse {
            success: true,
            error: String::new(),
        }))
    }

    /// Return current node status: role, LSNs, timeline, version, connection
    /// count, and replication info.
    async fn get_status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let safe_mode = self.heartbeat.is_safe_mode();

        // If Postgres is not reachable, return a minimal status rather than an
        // RPC error so callers can still observe the node state.
        if !self.pg.is_running().await {
            return Ok(Response::new(StatusResponse {
                is_in_recovery: false,
                received_lsn: 0,
                replayed_lsn: 0,
                sent_lsn: 0,
                timeline: 0,
                postgres_version: String::new(),
                active_connections: 0,
                postgres_running: false,
                replicas: vec![],
                replication_conninfo: String::new(),
            }));
        }

        // Gather status; on any query error fall back to postgres_running=false.
        let is_recovery = match self.pg.is_in_recovery().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("is_in_recovery query failed: {e}");
                return Ok(Response::new(StatusResponse {
                    is_in_recovery: false,
                    received_lsn: 0,
                    replayed_lsn: 0,
                    sent_lsn: 0,
                    timeline: 0,
                    postgres_version: String::new(),
                    active_connections: 0,
                    postgres_running: false,
                    replicas: vec![],
                    replication_conninfo: String::new(),
                }));
            }
        };

        let (received_lsn, replayed_lsn, sent_lsn) = if is_recovery {
            let recv = self.pg.get_receive_lsn().await.unwrap_or(0);
            let repl = self.pg.get_replay_lsn().await.unwrap_or(0);
            (recv, repl, 0u64)
        } else {
            let cur = self.pg.get_current_lsn().await.unwrap_or(0);
            (0u64, 0u64, cur)
        };

        let timeline = self.pg.get_timeline().await.unwrap_or(0);
        let version = self.pg.get_postgres_version().await.unwrap_or_default();
        let conns = self.pg.get_active_connections().await.unwrap_or(0);

        let replicas = if is_recovery {
            vec![]
        } else {
            self.pg.get_stat_replication().await.unwrap_or_default()
        };

        let replication_conninfo = if is_recovery {
            self.pg
                .get_wal_receiver_conninfo()
                .await
                .unwrap_or_default()
        } else {
            String::new()
        };

        let _ = safe_mode; // exposed indirectly through heartbeat; not in StatusResponse proto

        Ok(Response::new(StatusResponse {
            is_in_recovery: is_recovery,
            received_lsn,
            replayed_lsn,
            sent_lsn,
            timeline,
            postgres_version: version,
            active_connections: conns,
            postgres_running: true,
            replicas,
            replication_conninfo,
        }))
    }

    /// Record a heartbeat from the pgcluster leader.
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let seq = request.into_inner().seq;
        self.heartbeat.touch();
        Ok(Response::new(HeartbeatResponse {
            in_safe_mode: self.heartbeat.is_safe_mode(),
            seq,
        }))
    }

    /// Stop the local Postgres instance using `pg_ctl stop -m fast`.
    async fn stop_postgres(
        &self,
        _request: Request<StopRequest>,
    ) -> Result<Response<StopResponse>, Status> {
        match self.pg_ctl.stop_fast().await {
            Ok(()) => Ok(Response::new(StopResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(StopResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    /// Reload the Postgres configuration using `pg_ctl reload` (SIGHUP).
    async fn reload_config(
        &self,
        _request: Request<ReloadRequest>,
    ) -> Result<Response<ReloadResponse>, Status> {
        match self.pg_ctl.reload().await {
            Ok(()) => Ok(Response::new(ReloadResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(ReloadResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }
}

/// Split `password=<value>` out of a libpq keyword/value conninfo string.
///
/// Returns `(conninfo_without_password, Some(password))` when a `password`
/// keyword is present, or `(original, None)` otherwise.
///
/// This handles the plain-token format we generate internally
/// (`key=value` pairs separated by whitespace). Values may be surrounded by
/// single quotes; the returned password has those quotes stripped.
fn extract_conninfo_password(conninfo: &str) -> (String, Option<String>) {
    let mut password: Option<String> = None;
    let filtered: Vec<&str> = conninfo
        .split_whitespace()
        .filter(|token| {
            if let Some(raw) = token.strip_prefix("password=") {
                let pw = raw.trim_matches('\'').to_string();
                password = Some(pw);
                false
            } else {
                true
            }
        })
        .collect();
    (filtered.join(" "), password)
}

/// Extract a single keyword value from a libpq conninfo string.
///
/// Returns `Some(value)` for the first `key=value` or `key='value'` token
/// that matches `key`, stripping surrounding single quotes from the value.
fn conninfo_value(conninfo: &str, key: &str) -> Option<String> {
    let prefix = format!("{key}=");
    conninfo.split_whitespace().find_map(|token| {
        token.strip_prefix(&prefix).map(|v| v.trim_matches('\'').to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_password_removes_token_and_returns_value() {
        let conninfo = "host=pg-primary port=5432 user=replicator password=s3cr3t";
        let (new, pw) = extract_conninfo_password(conninfo);
        assert_eq!(pw, Some("s3cr3t".to_string()));
        assert!(!new.contains("password="), "password must be stripped");
        assert!(new.contains("host=pg-primary"));
        assert!(new.contains("port=5432"));
        assert!(new.contains("user=replicator"));
    }

    #[test]
    fn extract_password_no_password_returns_none() {
        let conninfo = "host=pg-primary port=5432 user=replicator";
        let (new, pw) = extract_conninfo_password(conninfo);
        assert_eq!(pw, None);
        assert_eq!(new, conninfo);
    }

    #[test]
    fn extract_password_strips_surrounding_quotes() {
        // Single-quoted unspaced value (e.g. password='p@ss!').
        let conninfo = "host=pg-primary password='p@ss!'";
        let (_, pw) = extract_conninfo_password(conninfo);
        assert_eq!(pw, Some("p@ss!".to_string()));
    }

    #[test]
    fn conninfo_value_finds_host() {
        let conninfo = "host=pg-primary port=5432 user=replicator";
        assert_eq!(
            conninfo_value(conninfo, "host"),
            Some("pg-primary".to_string())
        );
    }

    #[test]
    fn conninfo_value_missing_key_returns_none() {
        let conninfo = "host=pg-primary port=5432";
        assert_eq!(conninfo_value(conninfo, "user"), None);
    }
}
