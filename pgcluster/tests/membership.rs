//! Raft membership changes on a real in-process cluster over gRPC.

use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pgcluster::config::PgClusterConfig;
use pgcluster::raft::commands::TopologyCommand;
use pgcluster::raft::membership::{self, MembershipError};
use pgcluster::raft::types::RaftClient;
use pgcluster::raft::{serve_grpc, RaftNode};
use tempfile::TempDir;
use tokio::task::JoinHandle;

fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

struct Node {
    id: u64,
    addr: SocketAddr,
    raft: RaftClient,
    server: JoinHandle<anyhow::Result<()>>,
    _node: RaftNode,
    _data: TempDir,
}

async fn start_node(
    id: u64,
    addr: SocketAddr,
    peers: &[(u64, SocketAddr)],
    bootstrap: bool,
) -> Node {
    let data = tempfile::tempdir().unwrap();
    let peers: Vec<String> = peers
        .iter()
        .map(|(id, addr)| format!("{{ id = {id}, addr = \"{addr}\" }}"))
        .collect();
    let toml = format!(
        r#"
[cluster]
name     = "membership"
data_dir = "{}"

[raft]
node_id   = {id}
bootstrap = {bootstrap}
peers     = [{}]

[replication]
replication_user = "replicator"

[proxy]
listen_addr = "127.0.0.1:0"

[tls]
insecure_plaintext = true

[api]
insecure_no_auth = true
"#,
        data.path().display(),
        peers.join(", ")
    );
    let cfg: PgClusterConfig = toml::from_str(&toml).unwrap();
    let node = RaftNode::start(&cfg, None).await.unwrap();
    let raft = node.raft.clone();
    let server = tokio::spawn(serve_grpc(raft.clone(), addr, None));
    Node {
        id,
        addr,
        raft,
        server,
        _node: node,
        _data: data,
    }
}

async fn start_cluster() -> Vec<Node> {
    let addrs: Vec<(u64, SocketAddr)> = (1..=3).map(|id| (id, free_addr())).collect();
    let mut nodes = Vec::new();
    for (id, addr) in addrs.iter().rev() {
        nodes.push(start_node(*id, *addr, &addrs, *id == 1).await);
    }
    leader_of(&nodes).await;
    nodes
}

async fn leader_of(nodes: &[Node]) -> RaftClient {
    for _ in 0..100 {
        for node in nodes {
            let metrics = node.raft.metrics().borrow().clone();
            if metrics.current_leader == Some(node.id) {
                return node.raft.clone();
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no leader elected");
}

fn write(id: u64) -> TopologyCommand {
    TopologyCommand::MarkMaintenance {
        node_id: format!("n{id}"),
    }
}

#[tokio::test]
async fn a_dead_member_is_replaced_with_no_loss_of_quorum() {
    let mut nodes = start_cluster().await;
    let leader = leader_of(&nodes).await;
    let leader_id = leader.metrics().borrow().id;

    // Take a follower down.
    let dead = nodes.iter().position(|n| n.id != leader_id).unwrap();
    let dead = nodes.remove(dead);
    dead.server.abort();
    dead.raft.shutdown().await.unwrap();

    // Commit continuously through the whole replacement.
    let stop = Arc::new(AtomicBool::new(false));
    let committed = Arc::new(AtomicU64::new(0));
    let writer = {
        let (leader, stop, committed) = (leader.clone(), stop.clone(), committed.clone());
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let id = committed.load(Ordering::Relaxed);
                tokio::time::timeout(Duration::from_secs(2), leader.client_write(write(id)))
                    .await
                    .expect("a commit stalled, so quorum was lost")
                    .expect("a commit failed");
                committed.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    };

    let addr = free_addr();
    let new_node = start_node(4, addr, &[(4, addr)], false).await;
    membership::add_learner(&leader, 4, addr.to_string())
        .await
        .unwrap();
    let learner = membership::list(&leader);
    assert!(learner.iter().any(|m| m.id == 4 && !m.voter));

    membership::promote(&leader, 4).await.unwrap();
    membership::remove(&leader, dead.id).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    stop.store(true, Ordering::Relaxed);
    writer.await.unwrap();
    assert!(committed.load(Ordering::Relaxed) > 0);

    let members = membership::list(&leader);
    assert_eq!(members.iter().filter(|m| m.voter).count(), 3);
    assert!(members.iter().any(|m| m.id == 4 && m.voter));
    assert!(!members.iter().any(|m| m.id == dead.id));

    // The new voter carries the log.
    let target = leader.metrics().borrow().last_log_index;
    for _ in 0..100 {
        if new_node.raft.metrics().borrow().last_log_index >= target {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the new voter did not catch up");
}

#[tokio::test]
async fn a_learner_can_be_removed() {
    let nodes = start_cluster().await;
    let leader = leader_of(&nodes).await;
    let addr = free_addr();
    let _learner = start_node(4, addr, &[(4, addr)], false).await;

    membership::add_learner(&leader, 4, addr.to_string())
        .await
        .unwrap();
    membership::remove(&leader, 4).await.unwrap();
    assert!(!membership::list(&leader).iter().any(|m| m.id == 4));
}

#[tokio::test]
async fn invalid_changes_are_refused() {
    let nodes = start_cluster().await;
    let leader = leader_of(&nodes).await;

    assert!(matches!(
        membership::add_learner(&leader, 1, "127.0.0.1:1".into()).await,
        Err(MembershipError::Conflict(_))
    ));
    assert!(matches!(
        membership::promote(&leader, 1).await,
        Err(MembershipError::Conflict(_))
    ));
    assert!(matches!(
        membership::promote(&leader, 99).await,
        Err(MembershipError::NotFound(99))
    ));
    assert!(matches!(
        membership::remove(&leader, 99).await,
        Err(MembershipError::NotFound(99))
    ));
}

#[tokio::test]
async fn a_follower_points_at_the_leader() {
    let nodes = start_cluster().await;
    let leader = leader_of(&nodes).await;
    let leader_id = leader.metrics().borrow().id;
    let follower = nodes.iter().find(|n| n.id != leader_id).unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;
    let err = membership::remove(&follower.raft, leader_id)
        .await
        .unwrap_err();
    let leader_addr = nodes.iter().find(|n| n.id == leader_id).unwrap().addr;
    assert!(
        matches!(&err, MembershipError::NotLeader(addr) if *addr == leader_addr.to_string()),
        "{err}"
    );
}

#[tokio::test]
async fn the_last_voter_cannot_be_removed() {
    let addr = free_addr();
    let node = start_node(1, addr, &[(1, addr)], true).await;
    let nodes = [node];
    let leader = leader_of(&nodes).await;
    assert!(matches!(
        membership::remove(&leader, 1).await,
        Err(MembershipError::Conflict(_))
    ));
}
