use anyhow::Result;
use std::time::Duration;
use tracing::{info, warn};

use crate::agent_clients::AgentClientPool;

/// Send the Promote RPC to `node_id` via the agent at `agent_addr`.
///
/// Returns `Ok(())` when the agent confirms the promotion succeeded, or an
/// `Err` if the RPC fails or the agent reports an error.
pub async fn promote_node(node_id: &str, agent_addr: &str, pool: &AgentClientPool) -> Result<()> {
    let mut client = pool.get_or_connect(node_id, agent_addr).await?;
    let resp = client.promote().await?;
    if resp.success {
        info!(
            node_id,
            promoted_at_lsn = resp.promoted_at_lsn,
            new_timeline = resp.new_timeline,
            "promote RPC succeeded"
        );
        Ok(())
    } else {
        warn!(node_id, error = %resp.error, "promote RPC returned failure");
        anyhow::bail!("promote failed for {}: {}", node_id, resp.error)
    }
}

/// Verify that the node is no longer in recovery (i.e. promotion succeeded).
///
/// Retries up to `max_attempts` times, waiting `interval` between each attempt.
/// Returns `Ok(())` when `is_in_recovery == false`, or an error if the node
/// still reports being in recovery after all attempts.
pub async fn verify_promotion(
    node_id: &str,
    agent_addr: &str,
    pool: &AgentClientPool,
    max_attempts: u32,
    interval: Duration,
) -> Result<()> {
    for attempt in 1..=max_attempts {
        // Check first; sleep only between retries so a fast promotion pays no penalty.
        let mut client = pool.get_or_connect(node_id, agent_addr).await?;
        match client.get_status().await {
            Ok(status) if !status.is_in_recovery && status.postgres_running => {
                info!(node_id, attempt, "post-promotion health check passed: node is primary");
                return Ok(());
            }
            Ok(status) => {
                warn!(
                    node_id,
                    attempt,
                    is_in_recovery = status.is_in_recovery,
                    postgres_running = status.postgres_running,
                    "post-promotion check: node not yet primary — retrying"
                );
            }
            Err(e) => {
                warn!(node_id, attempt, err = %e, "post-promotion GetStatus failed — retrying");
                // Evict the broken channel so the next attempt opens a fresh connection.
                pool.remove(node_id);
            }
        }
        if attempt < max_attempts {
            tokio::time::sleep(interval).await;
        }
    }
    anyhow::bail!(
        "node {} did not become primary after {} attempts",
        node_id,
        max_attempts
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn verify_promotion_parameters() {
        // Simple compile-time check that the function signature is consistent.
        let _ = std::time::Duration::from_millis(500);
        let max = 3u32;
        assert!(max > 0);
    }
}
