use super::ApiState;
use crate::failover::seed::SeedPlan;
use crate::raft::topology::{ClusterTopology, EXTERNAL_ROLE};
use crate::raft::{commands::TopologyCommand, topology::NodeConfig};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;

#[derive(Serialize)]
pub struct NodeListResponse {
    pub nodes: Vec<NodeInfo>,
}

#[derive(Serialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub role: String,
    pub postgres_addr: String,
    pub agent_addr: String,
    pub priority: u32,
    pub flush_lsn: u64,
    pub replay_lsn: u64,
    pub lag_bytes: u64,
}

pub async fn list_nodes(State(s): State<ApiState>) -> Json<NodeListResponse> {
    let t = s.topology.borrow().clone();
    let nodes = t
        .node_configs
        .values()
        .map(|cfg| NodeInfo {
            node_id: cfg.node_id.clone(),
            role: t
                .node_roles
                .get(&cfg.node_id)
                .map(|r| r.to_string())
                .unwrap_or_else(|| "unknown".into()),
            postgres_addr: cfg.postgres_addr.clone(),
            agent_addr: cfg.agent_addr.clone(),
            priority: cfg.priority,
            flush_lsn: t.last_flush_lsns.get(&cfg.node_id).copied().unwrap_or(0),
            replay_lsn: t.last_replay_lsns.get(&cfg.node_id).copied().unwrap_or(0),
            lag_bytes: t.replica_lag_bytes.get(&cfg.node_id).copied().unwrap_or(0),
        })
        .collect();
    Json(NodeListResponse { nodes })
}

pub async fn get_node(
    State(s): State<ApiState>,
    Path(node_id): Path<String>,
) -> Result<Json<NodeInfo>, StatusCode> {
    let t = s.topology.borrow().clone();
    let cfg = t
        .node_configs
        .get(&node_id)
        .ok_or(StatusCode::NOT_FOUND)?
        .clone();
    Ok(Json(NodeInfo {
        node_id: cfg.node_id.clone(),
        role: t
            .node_roles
            .get(&cfg.node_id)
            .map(|r| r.to_string())
            .unwrap_or_default(),
        postgres_addr: cfg.postgres_addr.clone(),
        agent_addr: cfg.agent_addr.clone(),
        priority: cfg.priority,
        flush_lsn: t.last_flush_lsns.get(&cfg.node_id).copied().unwrap_or(0),
        replay_lsn: t.last_replay_lsns.get(&cfg.node_id).copied().unwrap_or(0),
        lag_bytes: t.replica_lag_bytes.get(&cfg.node_id).copied().unwrap_or(0),
    }))
}

#[derive(Deserialize)]
pub struct AddNodeRequest {
    pub node_id: String,
    pub agent_addr: String,
    pub postgres_addr: String,
    pub priority: Option<u32>,
    /// Add an external standby, which follows the primary but stays outside
    /// failover, synchronous commit and backups.
    #[serde(default)]
    pub external: bool,
}

pub async fn add_node(
    State(s): State<ApiState>,
    Json(req): Json<AddNodeRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let cmd = TopologyCommand::AddNode(NodeConfig {
        node_id: req.node_id,
        agent_addr: req.agent_addr,
        postgres_addr: req.postgres_addr,
        priority: req.priority.unwrap_or(100),
        tags: req
            .external
            .then(|| ("role".to_string(), EXTERNAL_ROLE.to_string()))
            .into_iter()
            .collect(),
    });
    s.raft
        .raft
        .client_write(cmd)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::CREATED)
}

pub async fn remove_node(
    State(s): State<ApiState>,
    Path(node_id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    // Evict the cached gRPC channel before the Raft write so that any
    // concurrent task that calls get_or_connect after this point opens a
    // fresh connection.  If the node is later re-added with the same ID but
    // a different agent_addr, the pool won't return the stale channel.
    s.pool.remove(&node_id);
    let cmd = TopologyCommand::RemoveNode { node_id };
    s.raft
        .raft
        .client_write(cmd)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Default, Deserialize)]
pub struct SeedNodeRequest {
    /// Discard data already on the node.
    #[serde(default)]
    pub force: bool,
    /// Seed from the latest backup rather than from the primary.
    #[serde(default)]
    pub from_backup: bool,
}

/// Start seeding a node from the primary. The backup can take hours, so this
/// returns at once, and the node appears as `replica` when it finishes.
pub async fn seed_node(
    State(s): State<ApiState>,
    Path(node_id): Path<String>,
    Json(req): Json<SeedNodeRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let topology = s.topology.borrow();
    let plan = SeedPlan::new(&topology, &node_id, &s.config.replication)
        .map_err(|e| (StatusCode::CONFLICT, e.to_string()))?;
    let settings = match (req.from_backup, &s.config.backup) {
        (true, Some(backup)) => Some(crate::backup::walg::settings(backup)),
        (true, None) => {
            return Err((StatusCode::NOT_FOUND, "backup not configured".to_string()));
        }
        (false, _) => None,
    };
    let pool = s.pool.clone();
    tokio::spawn(async move {
        let result = match settings {
            Some(settings) => plan.run_from_backup(&pool, settings, req.force).await,
            None => plan.run(&pool, req.force).await,
        };
        match result {
            Ok(()) => tracing::info!(node_id, "node seeded from the primary"),
            Err(e) => tracing::error!(node_id, err = %e, "seeding failed"),
        }
    });
    Ok(StatusCode::ACCEPTED)
}

/// Turn an external standby into an independent primary, and drop it from this
/// cluster. Held under `op_in_progress`, so the monitor does not mistake the
/// promoted node for a stale primary and rewind it.
pub async fn detach_node(
    State(s): State<ApiState>,
    Path(node_id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let topology = s.topology.borrow();
    let node = topology
        .node_configs
        .get(&node_id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("node {node_id} not found")))?;
    if !node.is_external() {
        return Err((
            StatusCode::CONFLICT,
            format!("{node_id} is not an external standby"),
        ));
    }
    // Check first: once the node has detached, the cluster must be able to
    // forget it.
    let metrics = s.raft.raft.metrics().borrow().clone();
    if metrics.current_leader != Some(metrics.id) {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "this instance is not the Raft leader".to_string(),
        ));
    }
    if s.op_in_progress
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return Err((
            StatusCode::CONFLICT,
            "a switchover, failover or restart is already in progress".to_string(),
        ));
    }

    let result = detach(&s, &topology, &node_id, &node.agent_addr).await;
    s.op_in_progress.store(false, Ordering::SeqCst);
    result.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn detach(
    s: &ApiState,
    topology: &ClusterTopology,
    node_id: &str,
    agent_addr: &str,
) -> anyhow::Result<()> {
    let mut client = s.pool.get_or_connect(node_id, agent_addr).await?;
    let response = client.detach().await?;
    if !response.success {
        anyhow::bail!("{}", response.error);
    }

    s.raft
        .raft
        .client_write(TopologyCommand::RemoveNode {
            node_id: node_id.to_string(),
        })
        .await
        .map_err(|e| anyhow::anyhow!("remove node: {e}"))?;
    s.pool.remove(node_id);

    // A leftover slot would hold WAL on the primary, up to the slot limit.
    if let Some(addr) = topology.primary_postgres_addr() {
        let replication = &s.config.replication;
        let password = std::env::var(&replication.replication_password_env).unwrap_or_default();
        let url = format!(
            "postgres://{}:{password}@{addr}/postgres",
            replication.replication_user
        );
        let slot = format!("{}{node_id}", replication.slot_prefix);
        if let Err(e) = crate::failover::slots::drop_slot(&url, &slot).await {
            tracing::warn!(slot, err = %e, "could not drop the detached node's slot");
        }
    }
    Ok(())
}
