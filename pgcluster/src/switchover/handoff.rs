use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
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
/// 1. Demote the current primary (write `standby.signal` + correct conninfo).
/// 2. Promote the new primary via `pg_promote()`.
/// 3. Propose `SetPrimary` to Raft BEFORE stopping the old primary — this is
///    critical: it ensures node_monitor sees the old primary as a failing
///    *replica*, not a failing *primary*, preventing spurious automatic failover.
/// 4. Stop the old primary so Docker restarts it in standby mode.
/// 5. Repoint ALL other nodes to the new primary and restart them.
pub async fn execute_switchover(
    raft: &Arc<RaftNode>,
    topology: &ClusterTopology,
    new_primary_id: &str,
    pool: &Arc<AgentClientPool>,
    replication_user: &str,
    replication_password: &str,
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

    let new_host = new_cfg
        .postgres_addr
        .split(':')
        .next()
        .unwrap_or(&new_cfg.postgres_addr);
    let conninfo_to_new = format!(
        "host={} port=5432 user={} password={}",
        new_host, replication_user, replication_password
    );
    let slot_prefix = "pgcluster_";

    // ── Step 1: Demote current primary ───────────────────────────────────────
    // Writes standby.signal + primary_conninfo to PGDATA.  The running postgres
    // ignores these until it is restarted (step 2 below).
    {
        let slot_name = format!("{}{}", slot_prefix, old_primary_id);
        let mut client = pool
            .get_or_connect(old_primary_id, &old_cfg.agent_addr)
            .await?;
        let resp = client.demote(&conninfo_to_new, &slot_name).await?;
        if !resp.success {
            anyhow::bail!(
                "demote RPC failed for old primary {}: {}",
                old_primary_id,
                resp.error
            );
        }
        info!(
            node_id = old_primary_id,
            "old primary demoted (signal written)"
        );
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
        // Give postgres a moment to finish the promotion before we update Raft.
        tokio::time::sleep(Duration::from_millis(600)).await;
    }

    // ── Step 3: Propose SetPrimary to Raft ───────────────────────────────────
    // Done BEFORE stopping the old primary so that if node_monitor sees the
    // old primary go offline, it treats it as a failing replica (not failing
    // primary) and does not trigger automatic failover.
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
    info!(new = new_primary_id, "Raft topology updated");

    // ── Step 4: Stop old primary so Docker restarts it as a standby ──────────
    // Raft already records the new primary (step 3), so node_monitor will see
    // the old primary as an offline replica and will NOT trigger failover.
    {
        let mut client = pool
            .get_or_connect(old_primary_id, &old_cfg.agent_addr)
            .await?;
        match client.stop_postgres("fast").await {
            Ok(resp) if resp.success => {
                info!(
                    node_id = old_primary_id,
                    "old primary stopped — Docker will restart it as standby"
                );
            }
            Ok(resp) => {
                tracing::warn!(
                    node_id = old_primary_id,
                    err = %resp.error,
                    "stop_postgres RPC returned failure; old primary may remain as primary until manually restarted"
                );
            }
            Err(e) => {
                tracing::warn!(
                    node_id = old_primary_id,
                    err = %e,
                    "stop_postgres RPC failed; old primary may remain as primary until manually restarted"
                );
            }
        }
    }

    // ── Step 5: Repoint ALL other nodes to the new primary ───────────────────
    // Collect every node that is not the new primary (old primary was already
    // handled in steps 1–4, but we still need to stop other standbys so they
    // pick up the updated primary_conninfo after Docker restarts them).
    let other_nodes: Vec<(String, String)> = topology
        .node_configs
        .values()
        .filter(|cfg| cfg.node_id != new_primary_id)
        .map(|cfg| (cfg.node_id.clone(), cfg.agent_addr.clone()))
        .collect();

    let errs = crate::failover::repoint::repoint_replicas(
        &other_nodes,
        &conninfo_to_new,
        slot_prefix,
        pool,
    )
    .await;
    for (id, e) in &errs {
        tracing::warn!(node_id = id, err = %e, "failed to repoint node after switchover");
    }

    // Stop each successfully-repointed node so Docker restarts it with the new
    // primary_conninfo.  primary_conninfo takes effect only after a restart.
    for (node_id, agent_addr) in &other_nodes {
        if errs.iter().any(|(id, _)| id == node_id) {
            continue; // skip nodes whose repoint already failed
        }
        if node_id == old_primary_id {
            continue; // already stopped in step 4
        }
        let mut client = match pool.get_or_connect(node_id, agent_addr).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(node_id, err = %e, "could not connect to stop node after repoint");
                continue;
            }
        };
        match client.stop_postgres("fast").await {
            Ok(resp) if resp.success => {
                info!(
                    node_id,
                    "node stopped — Docker will restart it pointing to new primary"
                );
            }
            Ok(resp) => {
                tracing::warn!(node_id, err = %resp.error, "stop_postgres failed after repoint");
            }
            Err(e) => {
                tracing::warn!(node_id, err = %e, "stop_postgres RPC error after repoint");
            }
        }
    }

    info!(
        old = old_primary_id,
        new = new_primary_id,
        "switchover complete"
    );
    Ok(())
}
