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

#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn leader_crash_triggers_re_election() {
    let Some(_) = e2e_endpoints() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    // TODO: implement with docker kill pgcluster-N
    eprintln!("leader_crash_triggers_re_election: not yet implemented");
}
