//! The agent's gRPC endpoint must reject any caller without a client
//! certificate from the cluster CA. Runs the real service and the real
//! pgcluster client, with no Postgres, because heartbeats never touch it.

use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use pgcluster::agent_clients::AgentClient;
use pgcluster::tls::certs;
use tempfile::TempDir;
use tonic::transport::{Certificate, ClientTlsConfig, Identity};
use vk_agent::config::{AgentConfig, AgentTlsConfig};
use vk_agent::heartbeat::HeartbeatTracker;
use vk_agent::postgres::LocalPostgres;
use vk_agent::process::PgCtl;
use vk_agent::server::{serve, AgentServiceImpl};

fn read(dir: &TempDir, file: &str) -> Vec<u8> {
    std::fs::read(dir.path().join(file)).unwrap()
}

fn path(dir: &TempDir, file: &str) -> Option<String> {
    Some(dir.path().join(file).to_str().unwrap().to_string())
}

fn client_tls(ca: &TempDir, identity: Option<&TempDir>) -> ClientTlsConfig {
    let tls = ClientTlsConfig::new().ca_certificate(Certificate::from_pem(read(ca, "ca.crt")));
    match identity {
        Some(dir) => tls.identity(Identity::from_pem(
            read(dir, "client.crt"),
            read(dir, "client.key"),
        )),
        None => tls,
    }
}

/// Start an agent on a free port and return its address and its CA directory.
async fn start_agent() -> (SocketAddr, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    certs::generate(dir.path(), &["localhost".into(), "client".into()]).unwrap();

    let tls = AgentTlsConfig {
        ca_cert: path(&dir, "ca.crt"),
        cert: path(&dir, "localhost.crt"),
        key: path(&dir, "localhost.key"),
        insecure_plaintext: false,
    };
    let toml = "data_dir = \"/pgdata\"\n";
    let config: AgentConfig = toml::from_str(toml).unwrap();
    let svc = AgentServiceImpl::new(
        Arc::new(config),
        HeartbeatTracker::new(Duration::from_secs(10)),
        Arc::new(LocalPostgres::connect_lazy("postgres://postgres@localhost:1/postgres").unwrap()),
        Arc::new(PgCtl::new("pg_ctl", "/pgdata")),
    );

    let addr = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let server_tls = vk_agent::tls::server_tls_config(&tls).unwrap();
    tokio::spawn(serve(svc, addr, Some(server_tls)));
    tokio::time::sleep(Duration::from_millis(200)).await;
    (addr, dir)
}

/// Connect and send a heartbeat. TLS 1.3 clients finish the handshake before
/// the server checks their certificate, so only an RPC proves rejection.
async fn heartbeat(addr: SocketAddr, tls: ClientTlsConfig) -> anyhow::Result<()> {
    let target = format!("localhost:{}", addr.port());
    AgentClient::connect_tls("pg1", &target, tls)
        .await?
        .heartbeat()
        .await?;
    Ok(())
}

#[tokio::test]
async fn certificate_from_the_cluster_ca_is_accepted() {
    let (addr, ca) = start_agent().await;
    heartbeat(addr, client_tls(&ca, Some(&ca)))
        .await
        .expect("a valid client certificate must be accepted");
}

#[tokio::test]
async fn connection_without_a_client_certificate_is_rejected() {
    let (addr, ca) = start_agent().await;
    assert!(heartbeat(addr, client_tls(&ca, None)).await.is_err());
}

#[tokio::test]
async fn certificate_from_another_ca_is_rejected() {
    let (addr, ca) = start_agent().await;
    let other = tempfile::tempdir().unwrap();
    certs::generate(other.path(), &["client".into()]).unwrap();
    assert!(heartbeat(addr, client_tls(&ca, Some(&other)))
        .await
        .is_err());
}

#[tokio::test]
async fn plaintext_connection_is_rejected() {
    let (addr, _ca) = start_agent().await;
    let target = format!("127.0.0.1:{}", addr.port());
    let result = match AgentClient::connect("pg1", &target).await {
        Ok(mut client) => client.heartbeat().await.map(|_| ()),
        Err(e) => Err(e),
    };
    assert!(result.is_err());
}
