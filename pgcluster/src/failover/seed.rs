use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use tracing::warn;

use super::slots;
use crate::agent_clients::agent_proto::{RestoreRequest, SeedRequest};
use crate::agent_clients::AgentClientPool;
use crate::config::ReplicationConfig;
use crate::raft::topology::ClusterTopology;

/// What it takes to seed one node from the current primary.
#[derive(Debug)]
pub struct SeedPlan {
    node_id: String,
    agent_addr: String,
    primary_node_id: String,
    primary_agent_addr: String,
    primary_host: String,
    primary_port: u32,
    primary_addr: String,
    user: String,
    password: String,
    slot_name: String,
}

impl SeedPlan {
    pub fn new(
        topology: &ClusterTopology,
        node_id: &str,
        replication: &ReplicationConfig,
    ) -> Result<Self> {
        if node_id == topology.primary_node_id {
            bail!("{node_id} is the primary");
        }
        let node = topology
            .node_configs
            .get(node_id)
            .ok_or_else(|| anyhow!("node {node_id} not found"))?;
        let primary_addr = topology
            .primary_postgres_addr()
            .ok_or_else(|| anyhow!("the cluster has no primary"))?;
        let primary_agent_addr = topology
            .node_configs
            .get(&topology.primary_node_id)
            .map(|p| p.agent_addr.clone())
            .ok_or_else(|| anyhow!("the primary has no agent address"))?;
        let (host, port) = primary_addr
            .rsplit_once(':')
            .ok_or_else(|| anyhow!("primary address {primary_addr} has no port"))?;

        Ok(Self {
            node_id: node_id.to_string(),
            agent_addr: node.agent_addr.clone(),
            primary_node_id: topology.primary_node_id.clone(),
            primary_agent_addr,
            primary_host: host.to_string(),
            primary_port: port.parse()?,
            primary_addr: primary_addr.to_string(),
            user: replication.replication_user.clone(),
            password: crate::credentials::get(&replication.replication_password_env),
            slot_name: format!("{}{}", replication.slot_prefix, node_id),
        })
    }

    /// Create the node's slot on the primary, then have its agent seed. Waits
    /// for the whole backup.
    pub async fn run(&self, pool: &AgentClientPool, force: bool) -> Result<()> {
        self.ensure_slot().await?;
        let mut client = pool.get_or_connect(&self.node_id, &self.agent_addr).await?;
        let response = client.seed(self.request(force)).await?;
        if !response.success {
            bail!("{}", response.error);
        }
        Ok(())
    }

    /// Create the node's slot on the primary, then have its agent restore the
    /// latest backup and follow the primary. This spares the primary the load
    /// of a base backup.
    pub async fn run_from_backup(
        &self,
        pool: &AgentClientPool,
        settings: HashMap<String, String>,
        force: bool,
    ) -> Result<()> {
        self.ensure_slot().await?;
        let mut client = pool.get_or_connect(&self.node_id, &self.agent_addr).await?;
        let response = client
            .restore(RestoreRequest {
                backup_name: String::new(),
                target_time: String::new(),
                force,
                standby: Some(self.request(false)),
                settings,
            })
            .await?;
        if !response.success {
            bail!("{}", response.error);
        }
        Ok(())
    }

    /// Wait until a node restored from a backup streams from the primary.
    /// Before it does, it may replay archived WAL for hours, so this gives up
    /// only once its replay has stood still for `stall` without it streaming.
    /// One that replayed a diverged timeline past the fork, such as WAL an old
    /// primary archived after a partition, stands still for good. Returns
    /// whether it streams.
    pub async fn wait_streaming(
        &self,
        pool: &AgentClientPool,
        stall: Duration,
        poll: Duration,
    ) -> bool {
        let mut replayed = 0;
        let mut moved = Instant::now();
        loop {
            if self.streams(pool).await {
                return true;
            }
            let now = self.replayed(pool).await;
            if now > replayed {
                replayed = now;
                moved = Instant::now();
            } else if moved.elapsed() >= stall {
                warn!(
                    node_id = self.node_id,
                    replayed, "replay has stood still and the node does not stream"
                );
                return false;
            }
            tokio::time::sleep(poll).await;
        }
    }

    /// Whether the primary lists this node as streaming.
    async fn streams(&self, pool: &AgentClientPool) -> bool {
        let Ok(mut primary) = pool
            .get_or_connect(&self.primary_node_id, &self.primary_agent_addr)
            .await
        else {
            return false;
        };
        primary.get_status().await.is_ok_and(|status| {
            status
                .replicas
                .iter()
                .any(|r| r.application_name == self.node_id && r.state == "streaming")
        })
    }

    /// How far the node has replayed, or 0 if it cannot say.
    async fn replayed(&self, pool: &AgentClientPool) -> u64 {
        match pool.get_or_connect(&self.node_id, &self.agent_addr).await {
            Ok(mut node) => node.get_status().await.map_or(0, |s| s.replayed_lsn),
            Err(_) => 0,
        }
    }

    /// Bring a diverged old primary back as a standby: rewind it, and seed it
    /// again if that fails.
    pub async fn rejoin(&self, pool: &AgentClientPool) -> Result<()> {
        self.ensure_slot().await?;
        let mut client = pool.get_or_connect(&self.node_id, &self.agent_addr).await?;
        let response = client.rewind(self.request(false)).await?;
        if response.success {
            return Ok(());
        }
        warn!(
            node_id = self.node_id,
            err = %response.error,
            "rewind failed, seeding from the primary"
        );
        self.run(pool, true).await
    }

    async fn ensure_slot(&self) -> Result<()> {
        let primary_url = format!(
            "postgres://{}:{}@{}/postgres",
            self.user, self.password, self.primary_addr
        );
        slots::ensure_slot(&primary_url, &self.slot_name).await
    }

    fn request(&self, force: bool) -> SeedRequest {
        SeedRequest {
            primary_host: self.primary_host.clone(),
            primary_port: self.primary_port,
            user: self.user.clone(),
            password: self.password.clone(),
            slot_name: self.slot_name.clone(),
            application_name: self.node_id.clone(),
            force,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::topology::NodeConfig;

    fn replication() -> ReplicationConfig {
        toml::from_str("replication_user = \"replicator\"").unwrap()
    }

    fn topology() -> ClusterTopology {
        let mut t = ClusterTopology {
            primary_node_id: "pg1".into(),
            ..Default::default()
        };
        for id in ["pg1", "pg2"] {
            t.node_configs.insert(
                id.into(),
                NodeConfig {
                    node_id: id.into(),
                    agent_addr: format!("{id}-agent:7001"),
                    postgres_addr: format!("{id}-db:5432"),
                    priority: 100,
                    tags: Default::default(),
                },
            );
        }
        t
    }

    #[test]
    fn plan_points_at_the_primary_and_the_nodes_slot() {
        let plan = SeedPlan::new(&topology(), "pg2", &replication()).unwrap();
        assert_eq!(plan.primary_host, "pg1-db");
        assert_eq!(plan.primary_port, 5432);
        assert_eq!(plan.agent_addr, "pg2-agent:7001");
        assert_eq!(plan.slot_name, "pgcluster_pg2");
    }

    #[test]
    fn the_primary_cannot_be_seeded() {
        assert!(SeedPlan::new(&topology(), "pg1", &replication()).is_err());
    }

    #[test]
    fn unknown_node_is_refused() {
        assert!(SeedPlan::new(&topology(), "pg9", &replication()).is_err());
    }

    #[test]
    fn a_cluster_without_a_primary_cannot_seed() {
        let mut t = topology();
        t.primary_node_id.clear();
        assert!(SeedPlan::new(&t, "pg2", &replication()).is_err());
    }
}
