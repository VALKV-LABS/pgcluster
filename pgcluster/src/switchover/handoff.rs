use anyhow::Result;
use std::sync::Arc;
use tracing::info;

use crate::agent_clients::AgentClientPool;
use crate::raft::commands::TopologyCommand;
use crate::raft::topology::ClusterTopology;
use crate::raft::RaftNode;

// ── execute_switchover ────────────────────────────────────────────────────────

/// Execute a planned (operator-initiated) switchover from the current primary
/// to `new_primary_id`.
///
/// The caller is responsible for ensuring the replica is sufficiently in sync
/// before calling this function (see `lag_wait::wait_for_replica_sync`).
///
/// Steps:
/// 1. Demote the current primary (writes `standby.signal` + new conninfo).
/// 2. Promote the new primary.
/// 3. Propose `SetPrimary` to Raft.
/// 4. Repoint the old primary as a replica of the new primary.
pub async fn execute_switchover(
    raft: &Arc<RaftNode>,
    topology: &ClusterTopology,
    new_primary_id: &str,
    pool: &Arc<AgentClientPool>,
) -> Result<()> {
    let old_primary_id = &topology.primary_node_id;

    let old_cfg = topology
        .node_configs
        .get(old_primary_id)
        .ok_or_else(|| anyhow::anyhow!("old primary '{}' not found in topology", old_primary_id))?
        .clone();
    let new_cfg = topology
        .node_configs
        .get(new_primary_id)
        .ok_or_else(|| anyhow::anyhow!("new primary '{}' not found in topology", new_primary_id))?
        .clone();

    info!(
        old = old_primary_id,
        new = new_primary_id,
        "starting planned switchover"
    );

    // ── Step 1: Demote current primary ───────────────────────────────────────
    {
        let new_host = new_cfg
            .postgres_addr
            .split(':')
            .next()
            .unwrap_or(&new_cfg.postgres_addr);
        let conninfo = format!("host={} port=5432 user=replicator", new_host);
        let slot_name = format!("pgcluster_{}", old_primary_id);

        let mut client = pool
            .get_or_connect(old_primary_id, &old_cfg.agent_addr)
            .await?;
        let resp = client.demote(&conninfo, &slot_name).await?;
        if !resp.success {
            anyhow::bail!(
                "demote RPC failed for old primary {}: {}",
                old_primary_id,
                resp.error
            );
        }
        info!(node_id = old_primary_id, "old primary demoted");
    }

    // ── Step 2: Promote new primary ──────────────────────────────────────────
    {
        let mut client = pool
            .get_or_connect(new_primary_id, &new_cfg.agent_addr)
            .await?;
        let resp = client.promote().await?;
        if !resp.success {
            anyhow::bail!(
                "promote RPC failed for new primary {}: {}",
                new_primary_id,
                resp.error
            );
        }
        info!(node_id = new_primary_id, "new primary promoted");
    }

    // ── Step 3: Propose SetPrimary to Raft ───────────────────────────────────
    let at_lsn = topology
        .last_flush_lsns
        .get(new_primary_id)
        .copied()
        .unwrap_or(0);
    raft.raft
        .client_write(TopologyCommand::SetPrimary {
            node_id: new_primary_id.to_string(),
            at_lsn,
            new_timeline: 0, // updated by the next health-check poll
        })
        .await
        .map_err(|e| anyhow::anyhow!("Raft propose SetPrimary: {e}"))?;

    // ── Step 4: Repoint old primary to follow new primary ────────────────────
    let new_host = new_cfg
        .postgres_addr
        .split(':')
        .next()
        .unwrap_or(&new_cfg.postgres_addr);
    let conninfo = format!("host={} port=5432 user=replicator", new_host);
    let errs = crate::failover::repoint::repoint_replicas(
        &[(old_primary_id.clone(), old_cfg.agent_addr.clone())],
        &conninfo,
        "pgcluster_",
        pool,
    )
    .await;
    for (id, e) in errs {
        // Log but don't fail — the old primary can be repointed manually.
        tracing::warn!(node_id = id, err = %e, "failed to repoint old primary after switchover");
    }

    info!(
        old = old_primary_id,
        new = new_primary_id,
        "switchover complete"
    );
    Ok(())
}
