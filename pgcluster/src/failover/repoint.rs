use tracing::info;

use crate::agent_clients::AgentClientPool;

/// Send the Demote RPC to each replica so it repoints to the new primary.
///
/// `replicas` is a list of `(node_id, agent_addr)` pairs.
/// `new_primary_conninfo` is the `primary_conninfo` string to write into
/// `postgresql.auto.conf` on each standby.
/// `slot_prefix` is used to derive the `primary_slot_name` for each replica
/// (e.g. `"pgcluster_"` → slot `"pgcluster_<node_id>"`).
///
/// Returns a list of `(node_id, error)` for any replicas that could not be
/// repointed.  Callers should log the errors but continue — a subset of
/// replicas failing to repoint is recoverable.
pub async fn repoint_replicas(
    replicas: &[(String, String)],
    new_primary_conninfo: &str,
    slot_prefix: &str,
    pool: &AgentClientPool,
) -> Vec<(String, anyhow::Error)> {
    let mut errors = Vec::new();

    for (node_id, agent_addr) in replicas {
        let mut client = match pool.get_or_connect(node_id, agent_addr).await {
            Ok(c) => c,
            Err(e) => {
                pool.remove(node_id);
                errors.push((node_id.clone(), e));
                continue;
            }
        };

        let slot_name = format!("{}{}", slot_prefix, node_id);
        match client.demote(new_primary_conninfo, &slot_name).await {
            Ok(resp) if resp.success => {
                info!(node_id, "replica repointed to new primary");
            }
            Ok(resp) => {
                errors.push((
                    node_id.clone(),
                    anyhow::anyhow!("demote RPC failed: {}", resp.error),
                ));
            }
            Err(e) => {
                // Evict the broken channel so the next caller doesn't reuse it.
                pool.remove(node_id);
                errors.push((node_id.clone(), e));
            }
        }
    }

    errors
}
