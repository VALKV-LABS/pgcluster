//! Raft and topology gRPC must reject any caller without a client certificate
//! from the cluster CA. Runs a real single-node Raft server.

use std::net::{SocketAddr, TcpListener};
use std::time::Duration;

use pgcluster::config::{PgClusterConfig, TlsConfig};
use pgcluster::raft::raft_proto::topology_service_client::TopologyServiceClient;
use pgcluster::raft::raft_proto::GetLeaderRequest;
use pgcluster::raft::{serve_grpc, RaftNode};
use pgcluster::tls::{certs, shared_client_tls, TlsManager};
use tempfile::TempDir;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};

fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn tls_config(dir: &TempDir, name: &str) -> TlsConfig {
    let file = |f: String| Some(dir.path().join(f).to_str().unwrap().to_string());
    TlsConfig {
        ca_cert: file("ca.crt".into()),
        cert: file(format!("{name}.crt")),
        key: file(format!("{name}.key")),
        insecure_plaintext: false,
    }
}

fn config(data_dir: &TempDir, addr: SocketAddr, tls: TlsConfig) -> PgClusterConfig {
    let toml = format!(
        r#"
[cluster]
name     = "mtls"
data_dir = "{}"

[raft]
node_id   = 1
bootstrap = true
peers     = [{{ id = 1, addr = "{addr}" }}]

[replication]
replication_user = "replicator"

[proxy]
listen_addr = "127.0.0.1:0"

[api]
api_keys = ["test-key"]
"#,
        data_dir.path().display()
    );
    let mut cfg: PgClusterConfig = toml::from_str(&toml).unwrap();
    cfg.tls = tls;
    cfg
}

struct Server {
    addr: SocketAddr,
    certs: TempDir,
    _data: TempDir,
    _raft: RaftNode,
    reload: tokio::sync::watch::Sender<tonic::transport::ServerTlsConfig>,
}

async fn start_server() -> Server {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let certs_dir = tempfile::tempdir().unwrap();
    certs::generate(certs_dir.path(), &["localhost".into(), "client".into()]).unwrap();
    let data = tempfile::tempdir().unwrap();
    let addr = free_addr();

    let tls = tls_config(&certs_dir, "localhost");
    let manager = TlsManager::new(&tls).unwrap();
    let cfg = config(&data, addr, tls);
    let raft = RaftNode::start(
        &cfg,
        Some(shared_client_tls(manager.tonic_client_tls_config())),
    )
    .await
    .unwrap();

    let (reload, rx) = tokio::sync::watch::channel(manager.tonic_server_tls_config());
    tokio::spawn(serve_grpc(raft.raft.clone(), addr, Some(rx)));
    tokio::time::sleep(Duration::from_millis(200)).await;

    Server {
        addr,
        certs: certs_dir,
        _data: data,
        _raft: raft,
        reload,
    }
}

/// Connect and call `GetLeader`. TLS 1.3 clients finish the handshake before
/// the server checks their certificate, so only an RPC proves rejection.
async fn get_leader(addr: SocketAddr, tls: Option<ClientTlsConfig>) -> anyhow::Result<()> {
    let scheme = if tls.is_some() { "https" } else { "http" };
    let mut endpoint = Channel::from_shared(format!("{scheme}://localhost:{}", addr.port()))?;
    if let Some(tls) = tls {
        endpoint = endpoint.tls_config(tls)?;
    }
    let channel = endpoint.connect().await?;
    TopologyServiceClient::new(channel)
        .get_leader(GetLeaderRequest {})
        .await?;
    Ok(())
}

#[tokio::test]
async fn certificate_from_the_cluster_ca_is_accepted() {
    let server = start_server().await;
    let client = TlsManager::new(&tls_config(&server.certs, "client")).unwrap();
    get_leader(server.addr, Some(client.tonic_client_tls_config()))
        .await
        .expect("a valid client certificate must be accepted");
}

#[tokio::test]
async fn connection_without_a_client_certificate_is_rejected() {
    let server = start_server().await;
    let ca = std::fs::read(server.certs.path().join("ca.crt")).unwrap();
    let tls = ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca));
    assert!(get_leader(server.addr, Some(tls)).await.is_err());
}

#[tokio::test]
async fn certificate_from_another_ca_is_rejected() {
    let server = start_server().await;
    let other = tempfile::tempdir().unwrap();
    certs::generate(other.path(), &["client".into()]).unwrap();
    let identity = TlsManager::new(&tls_config(&other, "client"))
        .unwrap()
        .tonic_client_tls_config();
    // Trust the real CA for the server, but present the foreign identity.
    let ca = std::fs::read(server.certs.path().join("ca.crt")).unwrap();
    let tls = identity.ca_certificate(Certificate::from_pem(ca));
    assert!(get_leader(server.addr, Some(tls)).await.is_err());
}

/// After a reload, new connections meet the new certificate, on the same port.
/// A second CA makes the change visible: only one side of it can connect.
#[tokio::test]
async fn a_reload_changes_the_certificate_without_closing_the_port() {
    let server = start_server().await;
    let old = TlsManager::new(&tls_config(&server.certs, "client")).unwrap();
    get_leader(server.addr, Some(old.tonic_client_tls_config()))
        .await
        .unwrap();

    // A connection in use across the reload. The call matters: with TLS 1.3 the
    // client finishes first, so until a call returns the server may still be
    // handshaking, and a new generation drops handshakes in flight. Clients
    // reconnect from that on their own, so it costs a retry, not a client.
    let port = server.addr.port();
    let endpoint = Channel::from_shared(format!("https://localhost:{port}"))
        .unwrap()
        .tls_config(old.tonic_client_tls_config())
        .unwrap();
    let mut open = TopologyServiceClient::new(endpoint.connect().await.unwrap());
    open.get_leader(GetLeaderRequest {}).await.unwrap();

    let renewed = tempfile::tempdir().unwrap();
    certs::generate(renewed.path(), &["localhost".into(), "client".into()]).unwrap();
    let manager = TlsManager::new(&tls_config(&renewed, "localhost")).unwrap();
    server
        .reload
        .send_replace(manager.tonic_server_tls_config());

    let new = TlsManager::new(&tls_config(&renewed, "client")).unwrap();
    let mut accepted = false;
    for _ in 0..20 {
        // The old generation accepts until its next wake, so allow it a moment.
        accepted = get_leader(server.addr, Some(new.tonic_client_tls_config()))
            .await
            .is_ok();
        if accepted {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(accepted, "the new certificate must be served");
    assert!(
        get_leader(server.addr, Some(old.tonic_client_tls_config()))
            .await
            .is_err(),
        "the old CA must no longer verify the server"
    );
    open.get_leader(GetLeaderRequest {})
        .await
        .expect("a connection from before the reload stays up");
}

#[tokio::test]
async fn plaintext_connection_is_rejected() {
    let server = start_server().await;
    assert!(get_leader(server.addr, None).await.is_err());
}

#[tokio::test]
async fn certificate_issued_by_a_later_run_of_the_generator_is_accepted() {
    let server = start_server().await;
    certs::generate(server.certs.path(), &["late".into()]).unwrap();
    let client = TlsManager::new(&tls_config(&server.certs, "late")).unwrap();
    get_leader(server.addr, Some(client.tonic_client_tls_config()))
        .await
        .expect("a certificate from a reused CA must chain to ca.crt");
}
