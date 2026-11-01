use super::ApiState;
use crate::raft::membership::{self, Member, MembershipError};
use crate::raft::types::NodeId;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AddMemberRequest {
    pub id: NodeId,
    pub addr: String,
}

type ApiResult = Result<StatusCode, (StatusCode, String)>;

fn status(e: MembershipError) -> (StatusCode, String) {
    let code = match e {
        MembershipError::NotFound(_) => StatusCode::NOT_FOUND,
        MembershipError::Conflict(_) => StatusCode::CONFLICT,
        MembershipError::NotLeader(_) => StatusCode::SERVICE_UNAVAILABLE,
        MembershipError::Failed(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (code, e.to_string())
}

pub async fn list_members(State(s): State<ApiState>) -> Json<Vec<Member>> {
    Json(membership::list(&s.raft.raft))
}

/// Add a learner. Returns once it has caught up.
pub async fn add_member(State(s): State<ApiState>, Json(req): Json<AddMemberRequest>) -> ApiResult {
    membership::add_learner(&s.raft.raft, req.id, req.addr)
        .await
        .map_err(status)?;
    Ok(StatusCode::CREATED)
}

pub async fn promote_member(State(s): State<ApiState>, Path(id): Path<NodeId>) -> ApiResult {
    membership::promote(&s.raft.raft, id)
        .await
        .map_err(status)?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn remove_member(State(s): State<ApiState>, Path(id): Path<NodeId>) -> ApiResult {
    membership::remove(&s.raft.raft, id).await.map_err(status)?;
    Ok(StatusCode::NO_CONTENT)
}
