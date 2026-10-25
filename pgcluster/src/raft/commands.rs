use super::topology::{BackupManifest, NodeConfig};
use serde::{Deserialize, Serialize};

/// Every topology change is a Raft log entry with one of these variants.
/// The Raft leader proposes a command; all instances apply it to their local
/// ClusterTopology copy via TopologyStateMachine::apply().
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TopologyCommand {
    /// Declare a node as the current primary (called after successful promotion)
    SetPrimary {
        node_id: String,
        at_lsn: u64,
        new_timeline: u32,
    },
    /// Mark a node as offline (failed health checks, or operator request)
    MarkOffline { node_id: String },
    /// Mark a node back as a streaming replica
    MarkReplica { node_id: String, flush_lsn: u64 },
    /// Put a node into maintenance mode (excluded from failover candidates)
    MarkMaintenance { node_id: String },
    /// Update the primary_conninfo string for a replica
    UpdatePrimaryConninfo { node_id: String, conninfo: String },
    /// Update flush/replay LSN from a health check poll
    UpdateFlushLsn {
        node_id: String,
        flush_lsn: u64,
        replay_lsn: u64,
    },
    /// Register a new node in the cluster
    AddNode(NodeConfig),
    /// Remove a node from the cluster
    RemoveNode { node_id: String },
    /// Record a completed failover event in history
    RecordFailover {
        old_primary: String,
        new_primary: String,
        triggered_at: i64,
        duration_ms: u64,
        reason: String,
    },
    /// Update the replication slot name for a replica
    SetReplicationSlot { node_id: String, slot_name: String },
    /// Record a completed (or failed) backup in cluster state
    AddBackupManifest(BackupManifest),
    /// Remove a backup manifest (after pruning or operator delete)
    RemoveBackupManifest { backup_id: String },
}

impl TopologyCommand {
    /// Returns true if this command changes who the primary is (used for watch notification).
    pub fn changes_primary(&self) -> bool {
        matches!(self, TopologyCommand::SetPrimary { .. })
    }
}
