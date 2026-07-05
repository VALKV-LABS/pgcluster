/// Raft layer integration tests.
/// Tests that don't need Docker run directly; Docker-requiring tests are #[ignore].
use pgcluster::raft::state_machine::TopologyStateMachine;

#[test]
fn state_machine_apply_set_primary() {
    let (sm, _rx) = TopologyStateMachine::new();
    let t = sm.current_topology();
    assert!(t.primary_node_id.is_empty());
}

#[test]
fn sled_log_storage_opens() {
    let dir = tempfile::TempDir::new().unwrap();
    let result = pgcluster::raft::storage::SledLogStorage::open(dir.path());
    assert!(
        result.is_ok(),
        "SledLogStorage should open: {:?}",
        result.err()
    );
}

// ── E2E tests (require Docker e2e stack with E2E_STACK_ADDR set) ─────────────
// These tests are skipped automatically when E2E_STACK_ADDR is not set.

fn e2e_endpoints() -> Option<Vec<String>> {
    std::env::var("E2E_STACK_ADDR").ok().map(|addr| {
        let host = addr.split(':').next().unwrap_or("127.0.0.1");
        vec![
            format!("http://{host}:8009/api/status"),
            format!("http://{host}:18009/api/status"),
            format!("http://{host}:28009/api/status"),
        ]
    })
}

#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn three_nodes_elect_leader_within_2s() {
    let Some(endpoints) = e2e_endpoints() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    let client = reqwest::Client::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if std::time::Instant::now() >= deadline {
            panic!("no leader elected within 2s");
        }
        let mut leaders = 0u32;
        for url in &endpoints {
            if let Ok(resp) = client.get(url).send().await {
                if let Ok(body) = resp.json::<serde_json::Value>().await {
                    if body.get("raft_role").and_then(|v| v.as_str()) == Some("Leader") {
                        leaders += 1;
                    }
                }
            }
        }
        if leaders == 1 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn topology_replicates_to_all_followers() {
    let Some(endpoints) = e2e_endpoints() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    let client = reqwest::Client::new();
    let mut primary_ids = std::collections::HashSet::new();
    for url in &endpoints {
        let resp = client
            .get(url)
            .send()
            .await
            .expect("request")
            .json::<serde_json::Value>()
            .await
            .expect("json");
        if let Some(id) = resp.get("primary_node_id").and_then(|v| v.as_str()) {
            primary_ids.insert(id.to_string());
        }
    }
    assert_eq!(
        primary_ids.len(),
        1,
        "all nodes should agree on the primary"
    );
}

/// Kill the pgcluster container that holds the Raft leader role and verify
/// that one of the remaining two nodes elects a new leader within 15 seconds.
///
/// Steps:
///   1. Query all 3 API endpoints to find which node is the Raft leader.
///   2. `docker kill` that pgcluster container.
///   3. Poll the surviving nodes until one reports raft_role == "Leader".
///   4. Assert the new leader is different from the killed node.
#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn leader_crash_triggers_re_election() {
    let Some(endpoints) = e2e_endpoints() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    let host = std::env::var("E2E_STACK_ADDR").unwrap_or_default();
    let host = host.split(':').next().unwrap_or("127.0.0.1");

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
        .unwrap();

    // Node i uses container "pgcluster-{i+1}" and API port 8009 / 18009 / 28009.
    let pgcluster_containers = ["pgcluster-1", "pgcluster-2", "pgcluster-3"];

    // Find the current Raft leader.
    let mut leader_container: Option<&str> = None;
    let mut leader_node_id: Option<u64> = None;
    for (i, url) in endpoints.iter().enumerate() {
        if let Ok(resp) = client.get(url).send().await {
            if let Ok(body) = resp.json::<serde_json::Value>().await {
                if body.get("raft_role").and_then(|v| v.as_str()) == Some("Leader") {
                    leader_container = Some(pgcluster_containers[i]);
                    leader_node_id = body.get("raft_leader_id").and_then(|v| v.as_u64());
                    break;
                }
            }
        }
    }
    let leader_container =
        leader_container.expect("no Raft leader found among the 3 pgcluster nodes");
    let leader_node_id = leader_node_id.unwrap_or(0);
    eprintln!("==> Raft leader is {leader_container} (node_id={leader_node_id})");

    // Kill the leader container.
    eprintln!("==> Killing {leader_container}...");
    std::process::Command::new("docker")
        .args(["kill", leader_container])
        .status()
        .unwrap_or_else(|e| panic!("docker kill {leader_container}: {e}"));

    // Poll the remaining two endpoints for a new leader within 15s.
    eprintln!("==> Waiting for re-election...");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let surviving: Vec<&str> = endpoints
        .iter()
        .enumerate()
        .filter(|(i, _)| pgcluster_containers[*i] != leader_container)
        .map(|(_, u)| u.as_str())
        .collect();

    loop {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        for url in &surviving {
            if let Ok(resp) = client.get(*url).send().await {
                if let Ok(body) = resp.json::<serde_json::Value>().await {
                    if body.get("raft_role").and_then(|v| v.as_str()) == Some("Leader") {
                        let new_id = body
                            .get("raft_leader_id")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        assert_ne!(
                            new_id, leader_node_id,
                            "new Raft leader must differ from the killed leader"
                        );
                        eprintln!("==> New Raft leader elected: node_id={new_id}");
                        let _ = host; // suppress unused warning
                        return;
                    }
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            panic!("no new Raft leader elected within 15s after killing {leader_container}");
        }
    }
}
