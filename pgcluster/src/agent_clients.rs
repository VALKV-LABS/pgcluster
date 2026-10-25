mod agent_proto {
    tonic::include_proto!("pgcluster.agent");
}

pub use agent_proto::{HeartbeatResponse, StatusResponse};

use anyhow::{Context, Result};
use dashmap::DashMap;

// ── AgentClient ───────────────────────────────────────────────────────────────

/// A gRPC client for a single vk-agent instance.
///
/// Wraps the proto-generated stub and tracks the heartbeat sequence number.
/// Cheaply constructed from a cached [`tonic::transport::Channel`] via
/// [`AgentClientPool::get_or_connect`], or created directly with
/// [`AgentClient::connect`] for one-shot use.
pub struct AgentClient {
    addr: String,
    stub: agent_proto::agent_service_client::AgentServiceClient<tonic::transport::Channel>,
    seq: u64,
}

impl AgentClient {
    /// Open a direct gRPC connection (bypasses the pool).
    pub async fn connect(_node_id: &str, addr: &str) -> Result<Self> {
        let channel = make_channel(addr, None).await?;
        Ok(Self::from_channel(addr, channel))
    }

    /// Open a direct TLS connection, verifying the server cert against `ca_pem`.
    pub async fn connect_tls(_node_id: &str, addr: &str, ca_pem: &[u8]) -> Result<Self> {
        let channel = make_channel(addr, Some(ca_pem)).await?;
        Ok(Self::from_channel(addr, channel))
    }

    fn from_channel(addr: &str, channel: tonic::transport::Channel) -> Self {
        Self {
            addr: addr.to_string(),
            stub: agent_proto::agent_service_client::AgentServiceClient::new(channel),
            seq: 0,
        }
    }

    /// Query current node status (role, LSNs, Postgres state).
    pub async fn get_status(&mut self) -> Result<StatusResponse> {
        self.stub
            .get_status(tonic::Request::new(agent_proto::StatusRequest {}))
            .await
            .with_context(|| format!("GetStatus from {}", self.addr))
            .map(|r| r.into_inner())
    }

    /// Send a keep-alive heartbeat to the agent.
    ///
    /// The agent enters safe mode if it does not receive a heartbeat within its
    /// configured timeout.  The sequence number is monotonically incremented.
    pub async fn heartbeat(&mut self) -> Result<HeartbeatResponse> {
        self.seq += 1;
        self.stub
            .heartbeat(tonic::Request::new(agent_proto::HeartbeatRequest {
                seq: self.seq,
            }))
            .await
            .with_context(|| format!("Heartbeat to {}", self.addr))
            .map(|r| r.into_inner())
    }

    /// Promote the standby managed by this agent to primary.
    pub async fn promote(&mut self) -> Result<agent_proto::PromoteResponse> {
        self.stub
            .promote(tonic::Request::new(agent_proto::PromoteRequest {
                expected_lsn: 0,
            }))
            .await
            .with_context(|| format!("Promote at {}", self.addr))
            .map(|r| r.into_inner())
    }

    /// Demote this node to a standby that replicates from a new primary.
    pub async fn demote(
        &mut self,
        new_primary_conninfo: &str,
        slot_name: &str,
    ) -> Result<agent_proto::DemoteResponse> {
        self.stub
            .demote(tonic::Request::new(agent_proto::DemoteRequest {
                new_primary_conninfo: new_primary_conninfo.to_string(),
                slot_name: slot_name.to_string(),
            }))
            .await
            .with_context(|| format!("Demote at {}", self.addr))
            .map(|r| r.into_inner())
    }

    /// Gracefully stop Postgres on this node.
    pub async fn stop_postgres(&mut self, mode: &str) -> Result<agent_proto::StopResponse> {
        self.stub
            .stop_postgres(tonic::Request::new(agent_proto::StopRequest {
                mode: mode.to_string(),
            }))
            .await
            .with_context(|| format!("StopPostgres at {}", self.addr))
            .map(|r| r.into_inner())
    }

    /// Reload the Postgres configuration on this node (SIGHUP / pg_ctl reload).
    pub async fn reload_config(&mut self) -> Result<agent_proto::ReloadResponse> {
        self.stub
            .reload_config(tonic::Request::new(agent_proto::ReloadRequest {}))
            .await
            .with_context(|| format!("ReloadConfig at {}", self.addr))
            .map(|r| r.into_inner())
    }
}

// ── AgentClientPool ───────────────────────────────────────────────────────────

/// Connection pool for vk-agent gRPC endpoints, keyed by node ID.
///
/// Caches the underlying [`tonic::transport::Channel`] (which multiplexes HTTP/2
/// streams) rather than full client stubs.  Each call to [`get_or_connect`]
/// returns a fresh [`AgentClient`] wrapping the cached channel — cheap because
/// channel clone is reference-counted.
///
/// Interior mutability via [`dashmap::DashMap`] makes the pool `Sync` so it can
/// be shared across tasks behind an `Arc`.
///
/// Optionally constructed with a PEM-encoded CA certificate to verify agent
/// server certificates (TLS). When no CA is provided, connections are plaintext.
pub struct AgentClientPool {
    channels: DashMap<String, tonic::transport::Channel>,
    /// CA certificate PEM used to verify agent TLS certs.
    /// `None` → plaintext connections (default).
    ca_pem: Option<Vec<u8>>,
}

impl AgentClientPool {
    /// Create a pool that connects to agents over plaintext (default).
    pub fn new() -> Self {
        Self {
            channels: DashMap::new(),
            ca_pem: None,
        }
    }

    /// Create a pool that uses TLS, verifying agent certs against `ca_pem`.
    pub fn new_with_ca(ca_pem: Vec<u8>) -> Self {
        Self {
            channels: DashMap::new(),
            ca_pem: Some(ca_pem),
        }
    }

    /// Return a client for `node_id`, connecting to `addr` on first use.
    ///
    /// Subsequent calls for the same `node_id` reuse the cached channel.
    /// If the channel was evicted (e.g., after an RPC error), a new one is
    /// created.
    pub async fn get_or_connect(&self, node_id: &str, addr: &str) -> Result<AgentClient> {
        if let Some(ch) = self.channels.get(node_id) {
            return Ok(AgentClient::from_channel(addr, ch.value().clone()));
        }
        let channel = make_channel(addr, self.ca_pem.as_deref()).await?;
        self.channels.insert(node_id.to_string(), channel.clone());
        Ok(AgentClient::from_channel(addr, channel))
    }

    /// Evict a cached channel so the next call reconnects from scratch.
    ///
    /// Call this after any RPC error to avoid reusing a broken channel.
    pub fn remove(&self, node_id: &str) {
        self.channels.remove(node_id);
    }
}

impl Default for AgentClientPool {
    fn default() -> Self {
        Self::new()
    }
}

// ── Channel builder ───────────────────────────────────────────────────────────

async fn make_channel(addr: &str, ca_pem: Option<&[u8]>) -> Result<tonic::transport::Channel> {
    let (scheme, tls_cfg) = if let Some(pem) = ca_pem {
        let ca = tonic::transport::Certificate::from_pem(pem);
        let cfg = tonic::transport::ClientTlsConfig::new().ca_certificate(ca);
        ("https", Some(cfg))
    } else {
        ("http", None)
    };

    let url = format!("{scheme}://{addr}");
    let mut endpoint = tonic::transport::Endpoint::from_shared(url)
        .with_context(|| format!("invalid agent addr: {addr}"))?;

    if let Some(cfg) = tls_cfg {
        endpoint = endpoint
            .tls_config(cfg)
            .with_context(|| format!("set TLS config for {addr}"))?;
    }

    endpoint
        .connect()
        .await
        .with_context(|| format!("connect to vk-agent at {addr}"))
}
