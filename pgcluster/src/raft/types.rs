use openraft::BasicNode;
use serde::{Deserialize, Serialize};

// ── NodeId ────────────────────────────────────────────────────────────────────

/// Raft node identifier — maps 1-to-1 to `RaftPeer.id` in the config.
pub type NodeId = u64;

// ── TopologyResponse ──────────────────────────────────────────────────────────

/// Returned by the state machine after each applied log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyResponse {
    /// The `ClusterTopology::version` after this entry was applied.
    pub applied_version: u64,
}

// ── RaftTypeConfig ────────────────────────────────────────────────────────────

openraft::declare_raft_types!(
    /// openraft type parameters for pgcluster.
    pub RaftTypeConfig:
        D            = crate::raft::commands::TopologyCommand,
        R            = TopologyResponse,
        NodeId       = NodeId,
        Node         = BasicNode,
        Entry        = openraft::Entry<RaftTypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

/// The live openraft handle — use this to propose commands and query leadership.
pub type RaftClient = openraft::Raft<RaftTypeConfig>;
