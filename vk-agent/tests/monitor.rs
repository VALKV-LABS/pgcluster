//! The Raft leader's heartbeats must keep agents out of safe mode, or they
//! self-fence. Runs the real service and the real pgcluster heartbeat.

use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use pgcluster::agent_clients::AgentClientPool;
use pgcluster::node_monitor::health_check::send_heartbeat;
use vk_agent::config::AgentConfig;
use vk_agent::heartbeat::HeartbeatTracker;
use vk_agent::postgres::LocalPostgres;
use vk_agent::process::PgCtl;
use vk_agent::server::{serve, AgentServiceImpl};

#[tokio::test]
async fn heartbeats_keep_the_agent_out_of_safe_mode() {
    let heartbeat = HeartbeatTracker::new(Duration::from_millis(300));
    heartbeat.clone().spawn_watchdog();

    let config: AgentConfig = toml::from_str("data_dir = \"/pgdata\"\n").unwrap();
    let svc = AgentServiceImpl::new(
        Arc::new(config),
        heartbeat.clone(),
        Arc::new(LocalPostgres::connect_lazy("postgres://postgres@localhost:1/postgres").unwrap()),
        Arc::new(PgCtl::new("pg_ctl", "/pgdata")),
    );
    let addr = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    tokio::spawn(serve(svc, addr, None));
    tokio::time::sleep(Duration::from_millis(100)).await;

    let pool = AgentClientPool::new();
    for _ in 0..10 {
        send_heartbeat("pg1", &addr.to_string(), &pool).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        !heartbeat.is_safe_mode(),
        "agent entered safe mode while receiving heartbeats"
    );
}
