use super::ApiState;
use axum::{extract::State, Json};
use serde::Serialize;

#[derive(Serialize)]
pub struct SlotInfo {
    pub node_id: String,
    pub slot_name: String,
}

pub async fn list_slots(State(s): State<ApiState>) -> Json<Vec<SlotInfo>> {
    let t = s.topology.borrow();
    let slots: Vec<SlotInfo> = t
        .replica_slots
        .iter()
        .map(|(node_id, slot_name)| SlotInfo {
            node_id: node_id.clone(),
            slot_name: slot_name.clone(),
        })
        .collect();
    Json(slots)
}
