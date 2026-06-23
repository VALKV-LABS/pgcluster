//! Integration tests for agent client connectivity.
//! Requires a live vk-agent. Address is taken from AGENT_TEST_ADDR env var
//! (default: 127.0.0.1:7001 for local dev; set to vk-agent:7001 in Docker).

fn agent_addr() -> String {
    std::env::var("AGENT_TEST_ADDR").unwrap_or_else(|_| "127.0.0.1:7001".into())
}

#[tokio::test]
#[ignore = "requires a running vk-agent (AGENT_TEST_ADDR)"]
async fn agent_client_heartbeat() {
    let addr = agent_addr();
    let mut client = pgcluster::agent_clients::AgentClient::connect("pg1", &addr)
        .await
        .expect("connect");
    client.heartbeat().await.expect("heartbeat");
}

#[tokio::test]
#[ignore = "requires a running vk-agent (AGENT_TEST_ADDR)"]
async fn agent_client_get_status() {
    let addr = agent_addr();
    let mut client = pgcluster::agent_clients::AgentClient::connect("pg1", &addr)
        .await
        .expect("connect");
    let status = client.get_status().await.expect("get_status");
    assert!(status.postgres_running, "postgres should be running");
}

#[tokio::test]
#[ignore = "requires Docker integ stack (make integ-up)"]
async fn get_status_returns_primary_info() {
    let addr = agent_addr();
    let mut client = pgcluster::agent_clients::AgentClient::connect("pg1", &addr)
        .await
        .expect("connect to vk-agent");
    let status = client.get_status().await.expect("get_status");
    assert!(status.postgres_running, "postgres should be running");
    assert!(!status.is_in_recovery, "primary should not be in recovery");
    assert!(
        !status.postgres_version.is_empty(),
        "postgres_version should be set"
    );
}

#[tokio::test]
#[ignore = "requires Docker integ stack (make integ-up)"]
async fn heartbeat_keeps_agent_active() {
    let addr = agent_addr();
    let mut client = pgcluster::agent_clients::AgentClient::connect("pg1", &addr)
        .await
        .expect("connect");
    // Send heartbeats every 500ms for 3 seconds — agent should stay active
    for _ in 0..6u32 {
        client.heartbeat().await.expect("heartbeat");
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    // Agent is still alive (no panic / connection error)
    client
        .get_status()
        .await
        .expect("agent still active after heartbeats");
}

#[tokio::test]
#[ignore = "requires Docker integ stack with heartbeat_timeout_seconds=5 (make integ-up)"]
async fn heartbeat_timeout_enters_safe_mode() {
    let addr = agent_addr();
    let mut client = pgcluster::agent_clients::AgentClient::connect("pg1", &addr)
        .await
        .expect("connect");
    // Send one heartbeat to start, then go quiet
    client.heartbeat().await.expect("initial heartbeat");
    // Wait longer than the watchdog timeout (integ compose sets it to a small value)
    tokio::time::sleep(std::time::Duration::from_secs(12)).await;
    // Agent should still be reachable (watchdog only triggers safe-mode, not shutdown)
    let _status = client.get_status().await.expect("get_status after timeout");
    // Note: verifying safe_mode field requires adding it to GetStatusResponse proto.
    // For now this test confirms the agent doesn't crash when the watchdog fires.
    eprintln!("safe_mode watchdog test: agent still reachable after timeout period");
}

#[tokio::test]
#[ignore = "requires Docker integ stack with a standby Postgres (make integ-up)"]
async fn promote_on_standby_succeeds() {
    // This test requires a replica Postgres, which is not present in the single-node
    // integ stack. It will be enabled once the integ compose adds a replica service.
    let standby_addr = match std::env::var("AGENT_STANDBY_ADDR") {
        Ok(a) => a,
        Err(_) => {
            eprintln!("skipping: AGENT_STANDBY_ADDR not set — no standby in integ stack");
            return;
        }
    };
    let mut client = pgcluster::agent_clients::AgentClient::connect("pg-replica", &standby_addr)
        .await
        .expect("connect to standby agent");
    // Call promote and poll until is_in_recovery=false
    client.promote().await.expect("promote");
    for _ in 0..10u32 {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let status = client.get_status().await.expect("get_status");
        if !status.is_in_recovery {
            return; // promoted successfully
        }
    }
    panic!("standby did not promote within 5 seconds");
}
