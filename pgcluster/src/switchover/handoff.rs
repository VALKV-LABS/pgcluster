use anyhow::Result;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::info;

use crate::agent_clients::AgentClientPool;
use crate::proxy::Router as ProxyRouter;
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
/// 2. Stop it. A fast shutdown waits until standbys have all its WAL, including
///    its shutdown record, so everything it wrote is before the new timeline.
/// 3. Wait for the target's position to settle, then promote it. Promoting
///    first would fork the timeline before that last WAL, and no other node
///    could then follow the new primary.
/// 4. Propose `SetPrimary` to Raft, and end the drain.
/// 5. Create the other nodes' slots on the new primary, and repoint them.
///
/// The health checks see the old primary go down during this, but they do not
/// start a failover while the caller holds `op_in_progress`.
#[allow(clippy::too_many_arguments)]
pub async fn execute_switchover(
    raft: &Arc<RaftNode>,
    topology: &ClusterTopology,
    new_primary_id: &str,
    pool: &Arc<AgentClientPool>,
    replication_user: &str,
    replication_password: &str,
    proxy_drain: Option<&Arc<ProxyRouter>>,
    drain_timeout: Duration,
    slot_prefix: &str,
) -> Result<()> {
    let started = Instant::now();
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

    // ── Step 0: Drain proxy connections ─────────────────────────────────────
    // Signal the proxy to refuse new connections to the primary, then wait for
    // in-flight transactions to complete before touching Postgres.
    if let Some(r) = proxy_drain {
        r.begin_drain();
        info!(
            secs = drain_timeout.as_secs_f32(),
            "waiting for in-flight transactions to drain"
        );
        tokio::time::sleep(drain_timeout).await;
    }

    let conninfo_to_new = new_cfg.conninfo(replication_user, replication_password);

    // ── Step 1: Demote current primary ───────────────────────────────────────
    // Writes standby.signal + primary_conninfo to PGDATA.  The running postgres
    // ignores these until it is restarted (step 2 below).
    {
        let slot_name = format!("{}{}", slot_prefix, old_primary_id);
        let mut client = pool
            .get_or_connect(old_primary_id, &old_cfg.agent_addr)
            .await?;
        let resp = client
            .demote(&conninfo_to_new, &slot_name, old_primary_id)
            .await?;
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

    // ── Step 2: Stop the old primary ─────────────────────────────────────────
    // It comes back as a standby. The RPC can fail if the agent shares a fate
    // with Postgres, as a container sidecar does, so step 3 confirms by position.
    {
        let mut client = pool
            .get_or_connect(old_primary_id, &old_cfg.agent_addr)
            .await?;
        match client.stop_postgres("fast").await {
            Ok(resp) if resp.success => {
                info!(node_id = old_primary_id, "old primary stopped");
            }
            Ok(resp) => {
                tracing::warn!(node_id = old_primary_id, err = %resp.error, "stop_postgres failed");
            }
            Err(e) => {
                pool.remove(old_primary_id);
                tracing::warn!(node_id = old_primary_id, err = %e, "stop_postgres RPC failed");
            }
        }
    }

    // ── Step 3: Promote the new primary ──────────────────────────────────────
    wait_for_position_to_settle(new_primary_id, &new_cfg.agent_addr, pool).await?;
    {
        let promoted = async {
            let mut client = pool
                .get_or_connect(new_primary_id, &new_cfg.agent_addr)
                .await?;
            let resp = client.promote().await?;
            if !resp.success {
                anyhow::bail!("promote RPC failed for {}: {}", new_primary_id, resp.error);
            }
            // Poll GetStatus until is_in_recovery == false (up to 3 times, 500 ms apart).
            crate::failover::promote::verify_promotion(
                new_primary_id,
                &new_cfg.agent_addr,
                pool,
                3,
                Duration::from_millis(500),
            )
            .await
        }
        .await;

        if let Err(e) = promoted {
            // The old primary is a standby now, so the cluster has no primary
            // until it is promoted back.
            tracing::error!(err = %e, "promotion failed: restoring the old primary");
            let restored = async {
                let mut client = pool
                    .get_or_connect(old_primary_id, &old_cfg.agent_addr)
                    .await?;
                client.promote().await
            }
            .await;
            if let Err(restore) = restored {
                tracing::error!(err = %restore, "could not restore the old primary");
            }
            if let Some(r) = proxy_drain {
                r.end_drain();
            }
            anyhow::bail!("switchover aborted: {e}");
        }
        info!(node_id = new_primary_id, "new primary promoted");
    }

    // ── Step 4: Propose SetPrimary to Raft ───────────────────────────────────
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

    // End drain after the topology update: new connections now route to the
    // new primary. This must happen AFTER the Raft write so that the router's
    // `primary_addr()` already returns the new primary's address.
    if let Some(r) = proxy_drain {
        r.end_drain();
    }

    // ── Step 5: Repoint every other node to the new primary ──────────────────
    let other_nodes: Vec<(String, String)> = topology
        .node_configs
        .values()
        .filter(|cfg| cfg.node_id != new_primary_id)
        .map(|cfg| (cfg.node_id.clone(), cfg.agent_addr.clone()))
        .collect();

    let primary_url = format!(
        "postgres://{}:{}@{}/postgres",
        replication_user, replication_password, new_cfg.postgres_addr
    );
    let other_ids: Vec<String> = other_nodes.iter().map(|(id, _)| id.clone()).collect();
    crate::failover::slots::ensure_slots_for_replicas(&primary_url, &other_ids, slot_prefix).await;

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

    // Record the switchover in Raft failover history so /api/failover/history
    // shows operator-initiated role changes alongside automatic failovers.
    let duration_ms = started.elapsed().as_millis() as u64;
    let triggered_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let _ = raft
        .raft
        .client_write(TopologyCommand::RecordFailover {
            old_primary: old_primary_id.clone(),
            new_primary: new_primary_id.to_string(),
            triggered_at,
            duration_ms,
            reason: "planned_switchover".to_string(),
            ran_degraded: false,
        })
        .await;

    info!(
        old = old_primary_id,
        new = new_primary_id,
        duration_ms,
        "switchover complete"
    );
    Ok(())
}

/// Wait until the target stops receiving WAL, so that it holds everything the
/// old primary wrote, and return an error if it stops answering.
async fn wait_for_position_to_settle(
    node_id: &str,
    agent_addr: &str,
    pool: &Arc<AgentClientPool>,
) -> Result<()> {
    const STEADY_READS: u32 = 3;
    const MAX_READS: u32 = 50;

    let mut last = None;
    let mut steady = 0;
    for _ in 0..MAX_READS {
        let mut client = pool.get_or_connect(node_id, agent_addr).await?;
        let position = client.get_status().await?.received_lsn;
        if last == Some(position) {
            steady += 1;
            if steady == STEADY_READS {
                return Ok(());
            }
        } else {
            steady = 0;
        }
        last = Some(position);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    anyhow::bail!("{node_id} was still receiving WAL when the wait ended")
}
