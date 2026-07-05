pub mod candidate;
pub mod events;
pub mod promote;
pub mod repoint;
pub mod slots;

use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

use crate::agent_clients::AgentClientPool;
use crate::metrics_registry::Metrics;
use crate::raft::commands::TopologyCommand;
use crate::raft::topology::{ClusterTopology, NodeRole};
use crate::raft::RaftNode;

// ── trigger_failover ──────────────────────────────────────────────────────────

/// Entry point called by `NodeMonitor` when the primary has failed enough
/// consecutive health checks.
///
/// Steps:
/// 1. Pick the best replica candidate (highest LSN, then priority).
/// 2. Promote the candidate via the Promote RPC.
/// 3. Propose `SetPrimary` to Raft so all cluster members update their view.
/// 4. Record the failover event in Raft history.
/// 5. Send Demote to all remaining replicas so they repoint to the new primary.
pub async fn trigger_failover(
    raft: &Arc<RaftNode>,
    topology: &ClusterTopology,
    failed_primary: &str,
    pool: &Arc<AgentClientPool>,
    metrics: &Arc<Metrics>,
    repl_user: &str,
    repl_password: &str,
    slot_prefix: &str,
) {
    let started = Instant::now();
    info!(failed_primary, "starting automatic failover");

    // ── 1. Pick candidate ────────────────────────────────────────────────────
    let candidate = match candidate::pick_candidate(topology, failed_primary) {
        Some(c) => c,
        None => {
            error!(
                failed_primary,
                "failover aborted: no eligible replica found"
            );
            return;
        }
    };

    let candidate_cfg = match topology.node_configs.get(&candidate) {
        Some(c) => c.clone(),
        None => {
            error!(
                candidate,
                "failover aborted: candidate config not found in topology"
            );
            return;
        }
    };

    // ── 2. Promote candidate ─────────────────────────────────────────────────
    if let Err(e) = promote::promote_node(&candidate, &candidate_cfg.agent_addr, pool).await {
        error!(
            candidate,
            err = %e,
            "promote RPC failed — aborting failover"
        );
        return;
    }

    // ── 2b. Verify promotion succeeded ───────────────────────────────────────
    // Poll GetStatus until is_in_recovery == false (up to 3 times, 500 ms apart).
    if let Err(e) = promote::verify_promotion(
        &candidate,
        &candidate_cfg.agent_addr,
        pool,
        3,
        Duration::from_millis(500),
    )
    .await
    {
        error!(
            candidate,
            err = %e,
            "post-promotion health check failed — aborting failover"
        );
        return;
    }

    // ── 3. Propose SetPrimary ────────────────────────────────────────────────
    let at_lsn = topology
        .last_flush_lsns
        .get(&candidate)
        .copied()
        .unwrap_or(0);
    let new_timeline = 1u32; // will be refreshed by the next health-check poll
    if let Err(e) = raft
        .raft
        .client_write(TopologyCommand::SetPrimary {
            node_id: candidate.clone(),
            at_lsn,
            new_timeline,
        })
        .await
    {
        error!(err = %e, "failed to propose SetPrimary after failover");
        // Continue anyway — the promote already happened; at least record the
        // event so operators can see what occurred.
    }

    // ── 4. Record failover event ─────────────────────────────────────────────
    let event = events::build_failover_event(
        failed_primary.to_string(),
        candidate.clone(),
        started,
        "health_check_failure".to_string(),
    );
    let _ = raft
        .raft
        .client_write(TopologyCommand::RecordFailover {
            old_primary: event.old_primary.clone(),
            new_primary: event.new_primary.clone(),
            triggered_at: event.triggered_at,
            duration_ms: event.duration_ms,
            reason: event.reason.clone(),
        })
        .await;

    // ── 5. Repoint remaining replicas ────────────────────────────────────────
    // Include Replica AND Unknown nodes: a node may be Unknown right after
    // bootstrap or after a Docker restart cycle. If the agent is reachable,
    // demote will succeed; if not, repoint_replicas logs a warning and moves on.
    let replicas: Vec<(String, String)> = topology
        .node_configs
        .iter()
        .filter(|(id, _)| *id != failed_primary && **id != candidate)
        .filter(|(id, _)| {
            matches!(
                topology.node_roles.get(*id),
                Some(&NodeRole::Replica) | Some(&NodeRole::Unknown) | None
            )
        })
        .map(|(id, cfg)| (id.clone(), cfg.agent_addr.clone()))
        .collect();

    if !replicas.is_empty() {
        let new_host = candidate_cfg
            .postgres_addr
            .split(':')
            .next()
            .unwrap_or(&candidate_cfg.postgres_addr);
        let conninfo = format!(
            "host={} port=5432 user={} password={}",
            new_host, repl_user, repl_password
        );

        // Ensure the new primary has a WAL slot for every replica that will
        // connect to it.  Stale slots from the old primary are unreachable;
        // they'll be cleaned up by the node_monitor orphan-slot audit.
        let replica_ids: Vec<String> = replicas.iter().map(|(id, _)| id.clone()).collect();
        let primary_url = format!(
            "postgres://{}:{}@{}/postgres",
            repl_user, repl_password, candidate_cfg.postgres_addr
        );
        slots::ensure_slots_for_replicas(&primary_url, &replica_ids, slot_prefix).await;

        let errs = repoint::repoint_replicas(&replicas, &conninfo, slot_prefix, pool).await;
        for (id, e) in errs {
            warn!(node_id = id, err = %e, "failed to repoint replica after failover");
        }
    }

    let elapsed = started.elapsed();
    metrics.failover_total.inc();
    metrics.primary_changes_total.inc();
    metrics
        .failover_duration_seconds
        .observe(elapsed.as_secs_f64());

    info!(
        old_primary  = failed_primary,
        new_primary  = %candidate,
        duration_ms  = elapsed.as_millis(),
        "failover complete"
    );
}
