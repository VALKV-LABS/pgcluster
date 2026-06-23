use anyhow::Result;
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
