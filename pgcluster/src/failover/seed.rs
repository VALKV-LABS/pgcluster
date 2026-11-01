use anyhow::{anyhow, bail, Result};

use super::slots;
use crate::agent_clients::agent_proto::SeedRequest;
use crate::agent_clients::AgentClientPool;
use crate::config::ReplicationConfig;
use crate::raft::topology::ClusterTopology;

/// What it takes to seed one node from the current primary.
#[derive(Debug)]
pub struct SeedPlan {
    node_id: String,
    agent_addr: String,
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
        let (host, port) = primary_addr
            .rsplit_once(':')
            .ok_or_else(|| anyhow!("primary address {primary_addr} has no port"))?;

        Ok(Self {
            node_id: node_id.to_string(),
            agent_addr: node.agent_addr.clone(),
            primary_host: host.to_string(),
            primary_port: port.parse()?,
            primary_addr: primary_addr.to_string(),
            user: replication.replication_user.clone(),
            password: std::env::var(&replication.replication_password_env).unwrap_or_default(),
            slot_name: format!("{}{}", replication.slot_prefix, node_id),
        })
    }

    /// Create the node's slot on the primary, then have its agent seed. Waits
    /// for the whole backup.
    pub async fn run(self, pool: &AgentClientPool, force: bool) -> Result<()> {
        let primary_url = format!(
            "postgres://{}:{}@{}/postgres",
            self.user, self.password, self.primary_addr
        );
        slots::ensure_slot(&primary_url, &self.slot_name).await?;

        let mut client = pool.get_or_connect(&self.node_id, &self.agent_addr).await?;
        let response = client
            .seed(SeedRequest {
                primary_host: self.primary_host,
                primary_port: self.primary_port,
                user: self.user,
                password: self.password,
                slot_name: self.slot_name,
                application_name: self.node_id,
                force,
            })
            .await?;
        if !response.success {
            bail!("{}", response.error);
        }
        Ok(())
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
