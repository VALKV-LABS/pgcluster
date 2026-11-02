//! Raft membership: add a learner, promote it to voter, remove a member.
//!
//! To replace a member, add the new one as a learner and promote it before
//! removing the old one, so the cluster never runs with fewer voters.

use std::collections::BTreeSet;

use openraft::error::{ChangeMembershipError, ClientWriteError, RaftError};
use openraft::{BasicNode, ChangeMembers};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::types::{NodeId, RaftClient};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Member {
    pub id: NodeId,
    pub addr: String,
    pub voter: bool,
}

#[derive(Debug, Error)]
pub enum MembershipError {
    #[error("member {0} not found")]
    NotFound(NodeId),
    #[error("{0}")]
    Conflict(String),
    #[error("this instance is not the Raft leader; send this to {0}")]
    NotLeader(String),
    #[error("{0}")]
    Failed(String),
}

impl MembershipError {
    /// Classify an openraft failure, pointing a caller at the leader.
    fn from_write(e: RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>) -> Self {
        if let Some(forward) = e.forward_to_leader::<BasicNode>() {
            let leader = forward.leader_node.as_ref().map_or("unknown", |n| &n.addr);
            return Self::NotLeader(leader.to_string());
        }
        match e {
            RaftError::APIError(ClientWriteError::ChangeMembershipError(
                ChangeMembershipError::InProgress(_),
            )) => Self::Conflict("a membership change is already in progress".into()),
            other => Self::Failed(other.to_string()),
        }
    }
}

/// Every voter and learner, ordered by ID.
pub fn list(raft: &RaftClient) -> Vec<Member> {
    let metrics = raft.metrics().borrow().clone();
    let membership = metrics.membership_config.membership();
    let voters: BTreeSet<NodeId> = membership.voter_ids().collect();
    let mut members: Vec<Member> = membership
        .nodes()
        .map(|(id, node)| Member {
            id: *id,
            addr: node.addr.clone(),
            voter: voters.contains(id),
        })
        .collect();
    members.sort_by_key(|m| m.id);
    members
}

/// Add a learner and wait until it has caught up with the leader's log.
pub async fn add_learner(
    raft: &RaftClient,
    id: NodeId,
    addr: String,
) -> Result<(), MembershipError> {
    if list(raft).iter().any(|m| m.id == id) {
        return Err(MembershipError::Conflict(format!(
            "member {id} already exists"
        )));
    }
    raft.add_learner(id, BasicNode { addr }, true)
        .await
        .map(|_| ())
        .map_err(MembershipError::from_write)
}

/// Promote a learner to voter.
pub async fn promote(raft: &RaftClient, id: NodeId) -> Result<(), MembershipError> {
    let member = find(raft, id)?;
    if member.voter {
        return Err(MembershipError::Conflict(format!(
            "member {id} is already a voter"
        )));
    }
    change(raft, ChangeMembers::AddVoterIds(BTreeSet::from([id]))).await
}

/// Remove a voter or a learner. The last voter cannot be removed.
pub async fn remove(raft: &RaftClient, id: NodeId) -> Result<(), MembershipError> {
    let member = find(raft, id)?;
    let ids = BTreeSet::from([id]);
    if !member.voter {
        return change(raft, ChangeMembers::RemoveNodes(ids)).await;
    }
    if list(raft).iter().filter(|m| m.voter).count() == 1 {
        return Err(MembershipError::Conflict(
            "cannot remove the last voter".into(),
        ));
    }
    change(raft, ChangeMembers::RemoveVoters(ids)).await
}

fn find(raft: &RaftClient, id: NodeId) -> Result<Member, MembershipError> {
    list(raft)
        .into_iter()
        .find(|m| m.id == id)
        .ok_or(MembershipError::NotFound(id))
}

async fn change(
    raft: &RaftClient,
    change: ChangeMembers<NodeId, BasicNode>,
) -> Result<(), MembershipError> {
    raft.change_membership(change, false)
        .await
        .map(|_| ())
        .map_err(MembershipError::from_write)
}
