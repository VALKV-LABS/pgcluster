//! CLI integration tests (item 19).
//!
//! Each test spins up an in-process Axum mock server on a random port and
//! calls the CLI `run()` functions directly so they exercise the real HTTP
//! client path without requiring a running cluster or Docker.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{
    routing::{delete, get, post},
    Json, Router,
};
use tokio::net::TcpListener;

// ── Mock server helpers ───────────────────────────────────────────────────────

/// Bind an Axum router to an OS-assigned port and return the local address.
///
/// The server runs in a background task for the lifetime of the caller's
/// async test function.  No explicit shutdown is needed — the task is dropped
/// when the test ends.
async fn spawn_mock(router: Router) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    // Give the server a moment to be ready.
    tokio::time::sleep(Duration::from_millis(10)).await;
    addr
}

/// Minimal JSON payloads returned by the mock.
fn status_json() -> serde_json::Value {
    serde_json::json!({
        "cluster_name": "test-cluster",
        "primary_node_id": "pg1",
        "topology_version": 7,
        "raft_leader_id": 1,
        "node_count": 3,
        "failover_history_count": 2
    })
}

fn nodes_json() -> serde_json::Value {
    serde_json::json!({
        "nodes": [
            {"node_id": "pg1", "role": "primary",
             "postgres_addr": "127.0.0.1:5432", "agent_addr": "127.0.0.1:7001", "lag_bytes": 0},
            {"node_id": "pg2", "role": "replica",
             "postgres_addr": "127.0.0.1:5433", "agent_addr": "127.0.0.1:7002", "lag_bytes": 1024},
        ]
    })
}

fn node_json() -> serde_json::Value {
    serde_json::json!({
        "node_id": "pg1",
        "role": "primary",
        "postgres_addr": "127.0.0.1:5432",
        "agent_addr": "127.0.0.1:7001"
    })
}

fn ok_msg_json() -> serde_json::Value {
    serde_json::json!({"message": "ok"})
}

// ── Shared request recorder ───────────────────────────────────────────────────

#[derive(Clone, Default)]
struct Recorder {
    bodies: Arc<Mutex<Vec<serde_json::Value>>>,
}

impl Recorder {
    fn push(&self, v: serde_json::Value) {
        self.bodies.lock().unwrap().push(v);
    }
    fn last(&self) -> serde_json::Value {
        self.bodies
            .lock()
            .unwrap()
            .last()
            .cloned()
            .unwrap_or_default()
    }
}

// ── status ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn cli_status_returns_ok_with_mock_server() {
    let router = Router::new().route("/api/status", get(|| async { Json(status_json()) }));
    let addr = spawn_mock(router).await;

    let args = pgcluster::cli::status::StatusArgs {
        api: addr,
        json: false,
    };
    pgcluster::cli::status::run(args)
        .await
        .expect("status should succeed with mock server");
}

#[tokio::test]
async fn cli_status_json_mode_returns_ok() {
    let router = Router::new().route("/api/status", get(|| async { Json(status_json()) }));
    let addr = spawn_mock(router).await;

    let args = pgcluster::cli::status::StatusArgs {
        api: addr,
        json: true,
    };
    pgcluster::cli::status::run(args)
        .await
        .expect("status --json should succeed");
}

#[tokio::test]
async fn cli_status_fails_when_server_unreachable() {
    // Port 19998 has nothing listening in a typical test environment.
    let args = pgcluster::cli::status::StatusArgs {
        api: "127.0.0.1:19998".into(),
        json: false,
    };
    assert!(
        pgcluster::cli::status::run(args).await.is_err(),
        "should return Err when server is unreachable"
    );
}

// ── switchover ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn cli_switchover_posts_correct_payload() {
    let recorder = Recorder::default();
    let rec = recorder.clone();

    let router = Router::new()
        .route(
            "/api/switchover",
            post(move |Json(body): Json<serde_json::Value>| {
                let r = rec.clone();
                async move {
                    r.push(body);
                    Json(ok_msg_json())
                }
            }),
        )
        .with_state(());
    let addr = spawn_mock(router).await;

    let args = pgcluster::cli::switchover::SwitchoverArgs {
        target: "pg2".into(),
        api: addr,
        max_lag: 2_097_152,
        timeout: 45,
    };
    pgcluster::cli::switchover::run(args)
        .await
        .expect("switchover should succeed");

    let body = recorder.last();
    assert_eq!(body["target_node_id"], "pg2", "wrong target_node_id");
    assert_eq!(body["max_lag_bytes"], 2_097_152u64, "wrong max_lag_bytes");
    assert_eq!(body["timeout_secs"], 45u64, "wrong timeout_secs");
}

// ── failover ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn cli_failover_posts_failed_node_id() {
    let recorder = Recorder::default();
    let rec = recorder.clone();

    let router = Router::new().route(
        "/api/failover",
        post(move |Json(body): Json<serde_json::Value>| {
            let r = rec.clone();
            async move {
                r.push(body);
                Json(ok_msg_json())
            }
        }),
    );
    let addr = spawn_mock(router).await;

    let args = pgcluster::cli::failover::FailoverArgs {
        failed_node: "pg1".into(),
        api: addr,
    };
    pgcluster::cli::failover::run(args)
        .await
        .expect("failover should succeed");

    let body = recorder.last();
    assert_eq!(body["failed_node_id"], "pg1", "wrong failed_node_id");
}

// ── node list ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn cli_node_list_returns_ok() {
    let router = Router::new().route("/api/nodes", get(|| async { Json(nodes_json()) }));
    let addr = spawn_mock(router).await;

    let args = pgcluster::cli::node::NodeArgs {
        command: pgcluster::cli::node::NodeCommands::List {
            api: addr,
            json: false,
        },
    };
    pgcluster::cli::node::run(args)
        .await
        .expect("node list should succeed");
}

#[tokio::test]
async fn cli_node_list_json_mode_returns_ok() {
    let router = Router::new().route("/api/nodes", get(|| async { Json(nodes_json()) }));
    let addr = spawn_mock(router).await;

    let args = pgcluster::cli::node::NodeArgs {
        command: pgcluster::cli::node::NodeCommands::List {
            api: addr,
            json: true,
        },
    };
    pgcluster::cli::node::run(args)
        .await
        .expect("node list --json should succeed");
}

// ── node get ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn cli_node_get_returns_ok() {
    let router = Router::new().route("/api/nodes/:id", get(|| async { Json(node_json()) }));
    let addr = spawn_mock(router).await;

    let args = pgcluster::cli::node::NodeArgs {
        command: pgcluster::cli::node::NodeCommands::Get {
            node_id: "pg1".into(),
            api: addr,
            json: false,
        },
    };
    pgcluster::cli::node::run(args)
        .await
        .expect("node get should succeed");
}

// ── node add / remove ─────────────────────────────────────────────────────────

#[tokio::test]
async fn cli_node_add_posts_correct_payload() {
    let recorder = Recorder::default();
    let rec = recorder.clone();

    let router = Router::new().route(
        "/api/nodes/add",
        post(move |Json(body): Json<serde_json::Value>| {
            let r = rec.clone();
            async move {
                r.push(body);
                Json(ok_msg_json())
            }
        }),
    );
    let addr = spawn_mock(router).await;

    let args = pgcluster::cli::node::NodeArgs {
        command: pgcluster::cli::node::NodeCommands::Add {
            node_id: "pg3".into(),
            agent_addr: "127.0.0.1:7003".into(),
            postgres_addr: "127.0.0.1:5435".into(),
            priority: 80,
            api: addr,
        },
    };
    pgcluster::cli::node::run(args)
        .await
        .expect("node add should succeed");

    let body = recorder.last();
    assert_eq!(body["node_id"], "pg3");
    assert_eq!(body["priority"], 80u32);
}

#[tokio::test]
async fn cli_node_remove_sends_delete() {
    let router = Router::new().route(
        "/api/nodes/:id",
        delete(|| async { axum::http::StatusCode::NO_CONTENT }),
    );
    let addr = spawn_mock(router).await;

    let args = pgcluster::cli::node::NodeArgs {
        command: pgcluster::cli::node::NodeCommands::Remove {
            node_id: "pg3".into(),
            api: addr,
        },
    };
    pgcluster::cli::node::run(args)
        .await
        .expect("node remove should succeed");
}

// ── status field validation ───────────────────────────────────────────────────

#[tokio::test]
async fn cli_status_parses_all_fields_from_response() {
    let router = Router::new().route("/api/status", get(|| async { Json(status_json()) }));
    let addr = spawn_mock(router).await;

    // Use the ApiClient directly to verify field parsing.
    let client = pgcluster::cli::client::ApiClient::new(&addr);
    let status: serde_json::Value = client.get("/api/status").await.expect("GET /api/status");

    assert_eq!(status["cluster_name"], "test-cluster");
    assert_eq!(status["primary_node_id"], "pg1");
    assert_eq!(status["topology_version"], 7);
    assert_eq!(status["raft_leader_id"], 1);
    assert_eq!(status["node_count"], 3);
}

// ── server-side error propagation ─────────────────────────────────────────────

#[tokio::test]
async fn cli_switchover_propagates_server_error() {
    use axum::http::StatusCode;
    let router = Router::new().route("/api/switchover", post(|| async { StatusCode::CONFLICT }));
    let addr = spawn_mock(router).await;

    let args = pgcluster::cli::switchover::SwitchoverArgs {
        target: "pg2".into(),
        api: addr,
        max_lag: 1_048_576,
        timeout: 30,
    };
    assert!(
        pgcluster::cli::switchover::run(args).await.is_err(),
        "should propagate 409 Conflict as Err"
    );
}
