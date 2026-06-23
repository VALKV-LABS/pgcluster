/// Failover engine unit tests (no Docker needed for candidate selection).
/// End-to-end failover tests are #[ignore].
use pgcluster::failover::candidate::pick_candidate;
use pgcluster::raft::topology::{ClusterTopology, NodeConfig, NodeRole};

fn node(id: &str, priority: u32) -> NodeConfig {
    NodeConfig {
        node_id: id.into(),
        agent_addr: format!("127.0.0.1:700{}", id.len()),
        postgres_addr: format!("127.0.0.1:543{}", id.len()),
        priority,
        tags: Default::default(),
    }
}

#[test]
fn pick_candidate_no_replicas_returns_none() {
    let mut t = ClusterTopology::default();
    t.node_configs.insert("pg1".into(), node("pg1", 100));
    t.node_roles.insert("pg1".into(), NodeRole::Primary);
    assert_eq!(pick_candidate(&t, "pg1"), None);
}

#[test]
fn pick_candidate_excludes_offline_nodes() {
    let mut t = ClusterTopology::default();
    t.node_configs.insert("pg1".into(), node("pg1", 100));
    t.node_configs.insert("pg2".into(), node("pg2", 90));
    t.node_roles.insert("pg1".into(), NodeRole::Primary);
    t.node_roles.insert("pg2".into(), NodeRole::Offline); // not eligible
    assert_eq!(pick_candidate(&t, "pg1"), None);
}

#[test]
fn pick_candidate_picks_highest_lsn() {
    let mut t = ClusterTopology::default();
    for id in ["pg1", "pg2", "pg3"] {
        t.node_configs.insert(id.into(), node(id, 100));
    }
    t.node_roles.insert("pg1".into(), NodeRole::Primary);
    t.node_roles.insert("pg2".into(), NodeRole::Replica);
    t.node_roles.insert("pg3".into(), NodeRole::Replica);
    t.last_flush_lsns.insert("pg2".into(), 100);
    t.last_flush_lsns.insert("pg3".into(), 999); // winner
    assert_eq!(pick_candidate(&t, "pg1").as_deref(), Some("pg3"));
}

// ── E2E tests (require `make start` + E2E_STACK_ADDR) ────────────────────────
// These tests are skipped automatically when E2E_STACK_ADDR is not set.

fn e2e_api_base() -> Option<String> {
    std::env::var("E2E_STACK_ADDR")
        .ok()
        .map(|addr| format!("http://{}", addr))
}

#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn primary_crash_triggers_automatic_failover() {
    let Some(_base) = e2e_api_base() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    // TODO: implement with docker API or docker CLI subprocess
    // 1. Wait for cluster to stabilize: GET /api/status until primary_node_id is set
    // 2. Kill pg-primary container
    // 3. Poll GET /api/status until primary_node_id changes (< 10s)
    // 4. Verify new primary is writable (INSERT succeeds through proxy)
    eprintln!("primary_crash_triggers_automatic_failover: not yet implemented");
}

#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn switchover_is_zero_data_loss() {
    let Some(_base) = e2e_api_base() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    // TODO: implement when proxy integration is ready
    // 1. INSERT 100 rows to primary through pgcluster proxy
    // 2. POST /api/switchover { "target": "pg2" }
    // 3. Poll /api/status until primary_node_id == "pg2"
    // 4. Verify all 100 rows exist on new primary
    eprintln!("switchover_is_zero_data_loss: not yet implemented");
}

#[tokio::test]
#[ignore = "requires Docker e2e stack (make start) with E2E_STACK_ADDR set"]
async fn failover_picks_most_caught_up_replica() {
    let Some(_base) = e2e_api_base() else {
        eprintln!("skipping: E2E_STACK_ADDR not set");
        return;
    };
    // TODO: implement with pg_wal_replay_pause on pg3
    // 1. Pause replication to pg3 (lag it behind pg2)
    // 2. Kill primary container
    // 3. Verify pg2 was promoted (it had higher LSN)
    eprintln!("failover_picks_most_caught_up_replica: not yet implemented");
}
