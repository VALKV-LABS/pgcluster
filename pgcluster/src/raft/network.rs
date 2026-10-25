use openraft::{
    error::{InstallSnapshotError, NetworkError, RPCError, RaftError},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};

use crate::raft::types::{NodeId, RaftTypeConfig};

// ── Generated proto module ────────────────────────────────────────────────────

mod raft_proto {
    tonic::include_proto!("pgcluster.raft");
}

// ── PgClusterNetworkFactory ───────────────────────────────────────────────────

/// Creates per-peer gRPC connections on demand.
///
/// Named `PgClusterNetworkFactory` (not `RaftNetworkFactory`) to avoid a name
/// collision with the `RaftNetworkFactory` trait imported from openraft.
///
/// Pass a PEM-encoded CA certificate to enable TLS for Raft peer connections.
pub struct PgClusterNetworkFactory {
    /// PEM-encoded CA cert for verifying peer TLS certificates.
    /// `None` → plaintext HTTP/2 (default for dev/test).
    pub ca_pem: Option<Vec<u8>>,
}

impl PgClusterNetworkFactory {
    pub fn new(ca_pem: Option<Vec<u8>>) -> Self {
        Self { ca_pem }
    }
}

// openraft 0.9 uses native async traits; do NOT annotate with #[async_trait].
impl RaftNetworkFactory<RaftTypeConfig> for PgClusterNetworkFactory {
    type Network = RaftNetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &openraft::BasicNode) -> Self::Network {
        RaftNetworkConnection {
            target,
            target_addr: node.addr.clone(),
            ca_pem: self.ca_pem.clone(),
        }
    }
}

// ── RaftNetworkConnection ─────────────────────────────────────────────────────

/// A logical connection to a single peer node.
pub struct RaftNetworkConnection {
    #[allow(dead_code)]
    target: NodeId,
    target_addr: String,
    ca_pem: Option<Vec<u8>>,
}

impl RaftNetworkConnection {
    /// Build a tonic channel to the peer.
    async fn make_channel(
        &self,
    ) -> Result<tonic::transport::Channel, RPCError<NodeId, openraft::BasicNode, RaftError<NodeId>>>
    {
        let net_err = |msg: &str| {
            RPCError::Network(NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                msg.to_string(),
            )))
        };

        let (scheme, tls_cfg) = if let Some(pem) = &self.ca_pem {
            let ca = tonic::transport::Certificate::from_pem(pem);
            let cfg = tonic::transport::ClientTlsConfig::new().ca_certificate(ca);
            ("https", Some(cfg))
        } else {
            ("http", None)
        };

        let uri = format!("{scheme}://{}", self.target_addr)
            .parse::<tonic::transport::Uri>()
            .map_err(|e| net_err(&format!("invalid peer URI: {e}")))?;

        let mut builder = tonic::transport::Channel::builder(uri);
        if let Some(cfg) = tls_cfg {
            builder = builder
                .tls_config(cfg)
                .map_err(|e| net_err(&format!("TLS config: {e}")))?;
        }

        builder
            .connect()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }
}

// ── RaftNetwork ───────────────────────────────────────────────────────────────

// openraft 0.9 uses native async traits; do NOT annotate with #[async_trait].
impl RaftNetwork<RaftTypeConfig> for RaftNetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<RaftTypeConfig>,
        _opt: RPCOption,
    ) -> Result<
        AppendEntriesResponse<NodeId>,
        RPCError<NodeId, openraft::BasicNode, RaftError<NodeId>>,
    > {
        let payload =
            serde_json::to_vec(&rpc).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let channel = self.make_channel().await?;
        let mut client = raft_proto::raft_service_client::RaftServiceClient::new(channel);

        let resp = client
            .append_entries(raft_proto::AppendEntriesRequest { payload })
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
            .into_inner();

        serde_json::from_slice::<AppendEntriesResponse<NodeId>>(&resp.payload)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _opt: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, openraft::BasicNode, RaftError<NodeId>>>
    {
        let payload =
            serde_json::to_vec(&rpc).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let channel = self.make_channel().await?;
        let mut client = raft_proto::raft_service_client::RaftServiceClient::new(channel);

        let resp = client
            .request_vote(raft_proto::VoteRequest { payload })
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
            .into_inner();

        serde_json::from_slice::<VoteResponse<NodeId>>(&resp.payload)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }

    // In openraft 0.9, InstallSnapshotError takes 0 generic arguments.
    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<RaftTypeConfig>,
        _opt: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, openraft::BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let payload =
            serde_json::to_vec(&rpc).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        // make_channel errors are Network-typed; re-box to satisfy the richer
        // error type required by install_snapshot.
        let channel = self.make_channel().await.map_err(|e| {
            RPCError::Network(NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                format!("{e:?}"),
            )))
        })?;

        let mut client = raft_proto::raft_service_client::RaftServiceClient::new(channel);

        let resp = client
            .install_snapshot(raft_proto::SnapshotRequest { payload })
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
            .into_inner();

        serde_json::from_slice::<InstallSnapshotResponse<NodeId>>(&resp.payload)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }
}
