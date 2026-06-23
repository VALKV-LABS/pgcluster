use super::ApiState;
use axum::{extract::State, http::StatusCode, response::IntoResponse};

pub async fn prometheus_metrics(State(s): State<ApiState>) -> impl IntoResponse {
    match s.metrics.render() {
        Ok(body) => (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4")],
            body,
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
