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
            if let Err(e) = crate::files::update_auto_conf(
                data_dir,
                "primary_conninfo",
                &req.new_primary_conninfo,
            )
            .await
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
