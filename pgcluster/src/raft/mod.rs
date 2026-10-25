//! Raft consensus module.
//!
//! # Module layout
//!
//! | Sub-module       | Responsibility                                        |
//! |------------------|-------------------------------------------------------|
//! | `commands`       | `TopologyCommand` — the Raft log entry payload        |
//! | `topology`       | `ClusterTopology` — the replicated state              |
//! | `types`          | openraft type config + `NodeId` + `RaftClient`        |
//! | `state_machine`  | Applies commands to `ClusterTopology`                 |
//! | `storage`        | Sled-backed Raft log storage + combined `PgClusterStorage` |
//! | `network`        | gRPC transport between Raft peers                     |

pub mod commands;
pub mod network;
pub mod state_machine;
pub mod storage;
pub mod topology;
pub mod types;

pub use topology::{ClusterTopology, NodeRole};

use std::sync::Arc;
use tokio::sync::watch;

// ── TopologyWatch / TopologySender ────────────────────────────────────────────

/// A cheap, cloneable handle for reading the current cluster topology.
///
/// Backed by a `tokio::sync::watch` channel so readers always see the latest
/// snapshot without contention.
#[derive(Clone)]
pub struct TopologyWatch {
    rx: watch::Receiver<Arc<ClusterTopology>>,
}

impl TopologyWatch {
    /// Create a watch pair seeded with `initial`.
    pub fn new(initial: ClusterTopology) -> (TopologySender, Self) {
        let (tx, rx) = watch::channel(Arc::new(initial));
        (TopologySender { tx }, TopologyWatch { rx })
    }

    /// Return a snapshot of the current topology (cheap `Arc` clone).
    pub fn current(&self) -> Arc<ClusterTopology> {
        Arc::clone(&*self.rx.borrow())
    }

    /// Return a clone of the current topology.
    ///
    /// Named `borrow` to match the `watch::Receiver::borrow().clone()` pattern
    /// used by API handlers.  Double-derefs through the `Ref` and the `Arc` so
    /// callers receive an owned `ClusterTopology` directly.
    pub fn borrow(&self) -> ClusterTopology {
        (**self.rx.borrow()).clone()
    }

    /// Wait until the topology changes, then return the new snapshot.
    pub async fn changed(&mut self) -> Arc<ClusterTopology> {
        let _ = self.rx.changed().await;
        self.current()
    }
}

/// The write half of the topology watch — held by the Raft state machine.
pub struct TopologySender {
    tx: watch::Sender<Arc<ClusterTopology>>,
}

impl TopologySender {
    /// Publish a new topology snapshot to all [`TopologyWatch`] receivers.
    pub fn publish(&self, topology: ClusterTopology) {
        let _ = self.tx.send(Arc::new(topology));
    }
}

// ── RaftNode ──────────────────────────────────────────────────────────────────

use anyhow::Result;
use openraft::{BasicNode, Config as OpenRaftConfig};

use crate::config::PgClusterConfig;
use network::PgClusterNetworkFactory;
use state_machine::TopologyStateMachine;
use storage::{PgClusterStorage, SledLogStorage};
use types::{NodeId, RaftClient, RaftTypeConfig};

/// The running Raft consensus node.
pub struct RaftNode {
    /// The openraft handle — propose commands, query leadership, etc.
    pub raft: RaftClient,
    /// Subscribe to topology changes.  Cheap to clone.
    pub topology_rx: TopologyWatch,
}

impl RaftNode {
    /// Start the Raft node: open log store, build state machine, wire network,
    /// optionally bootstrap.
    ///
    /// Pass a PEM-encoded CA certificate in `raft_ca_pem` to encrypt peer
    /// gRPC connections with TLS. `None` → plaintext (dev/test default).
    pub async fn start(cfg: &PgClusterConfig, raft_ca_pem: Option<Vec<u8>>) -> Result<Self> {
        let raft_config = Arc::new(
            OpenRaftConfig {
                heartbeat_interval: cfg.raft.heartbeat_interval_ms,
                election_timeout_min: cfg.raft.election_timeout_ms,
                election_timeout_max: cfg.raft.election_timeout_ms * 2,
                ..Default::default()
            }
            .validate()?,
        );

        let log_store_path = std::path::Path::new(&cfg.cluster.data_dir).join("raft-log");
        let log_store = SledLogStorage::open(&log_store_path)?;

        let (sm, inner_rx) = TopologyStateMachine::new();
        let initial_topo = sm.current_topology();

        // Wire the state machine's internal watch to the outer TopologyWatch
        // before `sm` is moved into PgClusterStorage.
        let (outer_tx, outer_rx) = watch::channel(initial_topo);
        {
            let mut fwd_rx = inner_rx;
            let fwd_tx = outer_tx;
            tokio::spawn(async move {
                loop {
                    if fwd_rx.changed().await.is_err() {
                        break;
                    }
                    let snap = Arc::clone(&*fwd_rx.borrow());
                    if fwd_tx.send(snap).is_err() {
                        break;
                    }
                }
            });
        }
        let topology_rx = TopologyWatch { rx: outer_rx };

        // Wrap log + state machine into the combined v1 RaftStorage, then split
        // via Adaptor into the sealed v2 (RaftLogStorage, RaftStateMachine) pair
        // that Raft::new requires.
        let combined = PgClusterStorage::new(log_store, sm);
        let (log_adaptor, sm_adaptor) = openraft::storage::Adaptor::new(combined);

        let raft = openraft::Raft::new(
            cfg.raft.node_id,
            raft_config,
            PgClusterNetworkFactory::new(raft_ca_pem),
            log_adaptor,
            sm_adaptor,
        )
        .await?;

        if cfg.raft.bootstrap {
            let members: std::collections::BTreeMap<NodeId, BasicNode> = cfg
                .raft
                .peers
                .iter()
                .map(|p| {
                    (
                        p.id,
                        BasicNode {
                            addr: p.addr.clone(),
                        },
                    )
                })
                .collect();
            let _ = raft.initialize(members).await;
        }

        Ok(Self { raft, topology_rx })
    }
}

// ── Raft gRPC server ──────────────────────────────────────────────────────────

mod raft_proto {
    tonic::include_proto!("pgcluster.raft");
}

use raft_proto::{
    raft_service_server::RaftService, topology_service_server::TopologyService,
    AppendEntriesRequest as ProtoAEReq, AppendEntriesResponse as ProtoAEResp, GetLeaderRequest,
    GetLeaderResponse, ProposeRequest, ProposeResponse, SnapshotRequest as ProtoSnapReq,
    SnapshotResponse as ProtoSnapResp, VoteRequest as ProtoVoteReq, VoteResponse as ProtoVoteResp,
};

/// gRPC service: receives Raft RPCs from peers, forwards to local Raft handle.
pub struct RaftGrpcServer {
    raft: RaftClient,
}

impl RaftGrpcServer {
    pub fn new(raft: RaftClient) -> Self {
        Self { raft }
    }

    pub fn into_services(
        self,
    ) -> (
        raft_proto::raft_service_server::RaftServiceServer<RaftGrpcServer>,
        raft_proto::topology_service_server::TopologyServiceServer<RaftGrpcServer>,
    ) {
        use raft_proto::{
            raft_service_server::RaftServiceServer, topology_service_server::TopologyServiceServer,
        };
        let raft2 = self.raft.clone();
        (
            RaftServiceServer::new(self),
            TopologyServiceServer::new(RaftGrpcServer { raft: raft2 }),
        )
    }
}

#[async_trait::async_trait]
impl RaftService for RaftGrpcServer {
    async fn append_entries(
        &self,
        request: tonic::Request<ProtoAEReq>,
    ) -> Result<tonic::Response<ProtoAEResp>, tonic::Status> {
        let payload = request.into_inner().payload;
        let req: openraft::raft::AppendEntriesRequest<RaftTypeConfig> =
            serde_json::from_slice(&payload)
                .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        let resp = self
            .raft
            .append_entries(req)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        let bytes =
            serde_json::to_vec(&resp).map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(tonic::Response::new(ProtoAEResp { payload: bytes }))
    }

    async fn request_vote(
        &self,
        request: tonic::Request<ProtoVoteReq>,
    ) -> Result<tonic::Response<ProtoVoteResp>, tonic::Status> {
        let payload = request.into_inner().payload;
        let req: openraft::raft::VoteRequest<NodeId> = serde_json::from_slice(&payload)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        let resp = self
            .raft
            .vote(req)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        let bytes =
            serde_json::to_vec(&resp).map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(tonic::Response::new(ProtoVoteResp { payload: bytes }))
    }

    async fn install_snapshot(
        &self,
        request: tonic::Request<ProtoSnapReq>,
    ) -> Result<tonic::Response<ProtoSnapResp>, tonic::Status> {
        let payload = request.into_inner().payload;
        let req: openraft::raft::InstallSnapshotRequest<RaftTypeConfig> =
            serde_json::from_slice(&payload)
                .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        let resp = self
            .raft
            .install_snapshot(req)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        let bytes =
            serde_json::to_vec(&resp).map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(tonic::Response::new(ProtoSnapResp { payload: bytes }))
    }
}

#[async_trait::async_trait]
impl TopologyService for RaftGrpcServer {
    async fn propose(
        &self,
        request: tonic::Request<ProposeRequest>,
    ) -> Result<tonic::Response<ProposeResponse>, tonic::Status> {
        let cmd_json = request.into_inner().command_json;
        let cmd: commands::TopologyCommand = serde_json::from_slice(&cmd_json)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        match self.raft.client_write(cmd).await {
            Ok(_) => Ok(tonic::Response::new(ProposeResponse {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(tonic::Response::new(ProposeResponse {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn get_leader(
        &self,
        _request: tonic::Request<GetLeaderRequest>,
    ) -> Result<tonic::Response<GetLeaderResponse>, tonic::Status> {
        let metrics = self.raft.metrics().borrow().clone();
        match metrics.current_leader {
            Some(leader_id) => {
                let leader_addr = metrics
                    .membership_config
                    .membership()
                    .get_node(&leader_id)
                    .map(|n| n.addr.clone())
                    .unwrap_or_default();
                Ok(tonic::Response::new(GetLeaderResponse {
                    leader_id,
                    leader_addr,
                    has_leader: true,
                }))
            }
            None => Ok(tonic::Response::new(GetLeaderResponse {
                leader_id: 0,
                leader_addr: String::new(),
                has_leader: false,
            })),
        }
    }
}
