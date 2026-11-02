//! The admin API.

pub mod databases;
pub mod hosts;

use axum::async_trait;
use axum::body::Body;
use axum::extract::rejection::QueryRejection;
use axum::extract::{FromRequest, FromRequestParts, Query, State};
use axum::http::request::Parts;
use axum::http::{header, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::error::ApiError;
use crate::state::AdminState;

const DEFAULT_LIMIT: i64 = 100;
const MAX_LIMIT: i64 = 500;

pub fn router(state: AdminState) -> Router {
    let protected = Router::new()
        .route("/v1/hosts", post(hosts::create).get(hosts::list))
        .route("/v1/hosts/:id", get(hosts::get).delete(hosts::delete))
        .route(
            "/v1/databases",
            post(databases::create).get(databases::list),
        )
        .route(
            "/v1/databases/:id",
            get(databases::get).delete(databases::delete),
        )
        .route(
            "/v1/databases/:id/owner-password",
            post(databases::set_owner_password),
        )
        .route(
            "/v1/databases/:id/allowlist",
            put(databases::put_allowlist).get(databases::get_allowlist),
        )
        .route(
            "/v1/databases/:id/extensions",
            put(databases::put_extensions).get(databases::get_extensions),
        )
        .route("/v1/databases/:id/nodes", get(databases::list_nodes))
        .route(
            "/v1/databases/:id/operations",
            get(databases::list_operations),
        )
        .route(
            "/v1/databases/:id/operations/:op_id",
            get(databases::get_operation),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_api_key,
        ));
    Router::new()
        .route(
            "/health",
            get(health).route_layer(middleware::from_fn_with_state(
                state.clone(),
                crate::agent::limit_open_route,
            )),
        )
        .merge(protected)
        .with_state(state)
}

async fn require_api_key(
    State(state): State<AdminState>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, ApiError> {
    let authorized = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|key| state.accepts_key(key.trim()));
    if authorized {
        Ok(next.run(request).await)
    } else {
        Err(ApiError::unauthorized("a valid API key is required"))
    }
}

async fn health(State(state): State<AdminState>) -> Result<Json<serde_json::Value>, ApiError> {
    sqlx::query("SELECT 1")
        .execute(&state.pool)
        .await
        .map_err(|e| {
            tracing::error!(err = %e, "health check failed");
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                "the metadata database is unreachable",
            )
        })?;
    Ok(Json(json!({ "status": "ok" })))
}

#[derive(FromRequest)]
#[from_request(via(axum::Json), rejection(ApiError))]
pub struct JsonBody<T>(pub T);

#[derive(Debug, Deserialize)]
pub struct Page {
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    page_token: Option<String>,
}

#[async_trait]
impl FromRequestParts<AdminState> for Page {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AdminState,
    ) -> Result<Self, Self::Rejection> {
        let Query(page) = Query::<Page>::from_request_parts(parts, state)
            .await
            .map_err(|e: QueryRejection| ApiError::from(e))?;
        Ok(page)
    }
}

impl Page {
    /// Rows to fetch: one more than a page, to learn whether another follows.
    pub fn fetch_limit(&self) -> i64 {
        self.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT) + 1
    }

    /// Lists are ordered by ID, and the token is the last ID of the page before.
    pub fn after(&self) -> &str {
        self.page_token.as_deref().unwrap_or("")
    }
}

#[derive(Debug, Serialize)]
pub struct List<T> {
    pub items: Vec<T>,
    pub next_page_token: Option<String>,
}

impl<T> List<T> {
    /// Build a page from `fetch_limit` rows.
    pub fn from_rows(mut rows: Vec<T>, page: &Page, id: impl Fn(&T) -> String) -> Self {
        let size = (page.fetch_limit() - 1) as usize;
        let more = rows.len() > size;
        rows.truncate(size);
        Self {
            next_page_token: rows.last().filter(|_| more).map(id),
            items: rows,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ca::Ca;
    use crate::secrets::SecretBox;
    use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
    use std::sync::Arc;
    use tower::ServiceExt;

    const KEY: &str = "a-test-key-that-is-long-enough-to-pass";

    fn state() -> AdminState {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://nobody@127.0.0.1:1/none")
            .unwrap();
        AdminState::new(
            pool,
            Arc::new(Ca::generate("test").unwrap()),
            SecretBox::from_base64(&BASE64.encode([1u8; 32])).unwrap(),
            &[crate::state::hash_api_key(KEY).unwrap()],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn a_flood_of_open_requests_is_turned_away() {
        let state = state();
        let _held = state
            .open_routes
            .clone()
            .acquire_many_owned(6)
            .await
            .unwrap();
        let request = Request::get("/health").body(Body::empty()).unwrap();
        let response = router(state).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()["retry-after"], "1");
    }

    #[tokio::test]
    async fn a_short_key_never_authenticates_even_with_its_hash_configured() {
        let short = "short";
        let hash = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(short));
        let state = AdminState::new(
            state().pool,
            Arc::new(Ca::generate("test").unwrap()),
            SecretBox::from_base64(&BASE64.encode([1u8; 32])).unwrap(),
            &[hash],
        )
        .unwrap();
        assert!(!state.accepts_key(short));
    }

    #[tokio::test]
    async fn only_the_right_key_is_accepted() {
        assert!(state().accepts_key(KEY));
        assert!(!state().accepts_key("another-key-that-is-long-enough-to-pass"));
    }

    /// Every operation of a delivered stage exists, and sits behind the key;
    /// later stages do not exist yet. This keeps the code and contract in step.
    #[tokio::test]
    async fn routes_match_the_contract() {
        let spec: serde_yaml::Value =
            serde_yaml::from_str(include_str!("../../../openapi/admin.yaml")).unwrap();
        let mut checked = 0;
        for (path, item) in spec["paths"].as_mapping().unwrap() {
            let path = path.as_str().unwrap();
            for (method, operation) in item.as_mapping().unwrap() {
                let method = method.as_str().unwrap();
                if method == "parameters" || path == "/health" {
                    continue;
                }
                let stage = operation["x-stage"].as_u64().unwrap();
                let uri = path.replace("{id}", "x").replace("{op_id}", "y");
                let uri = uri.replace("{node}", "n").replace("{replica_id}", "r");
                let request = |authorized: bool| {
                    let mut request = Request::builder()
                        .method(method.to_uppercase().as_str())
                        .uri(&uri);
                    if authorized {
                        request = request.header(header::AUTHORIZATION, format!("Bearer {KEY}"));
                    }
                    request.body(Body::empty()).unwrap()
                };
                if stage == 1 {
                    let response = router(state()).oneshot(request(false)).await.unwrap();
                    let status = response.status();
                    assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {path}");
                } else {
                    let response = router(state()).oneshot(request(true)).await.unwrap();
                    let status = response.status();
                    assert!(
                        status == StatusCode::NOT_FOUND || status == StatusCode::METHOD_NOT_ALLOWED,
                        "{method} {path} is stage {stage}, but answered {status}"
                    );
                }
                checked += 1;
            }
        }
        assert!(checked > 30);
    }

    #[test]
    fn a_full_page_names_the_next() {
        let page = Page {
            limit: Some(2),
            page_token: None,
        };
        let list = List::from_rows(vec![1, 2, 3], &page, |n: &i32| n.to_string());
        assert_eq!(list.items, vec![1, 2]);
        assert_eq!(list.next_page_token.as_deref(), Some("2"));
    }

    #[test]
    fn the_last_page_names_nothing() {
        let page = Page {
            limit: Some(2),
            page_token: None,
        };
        let list = List::from_rows(vec![1, 2], &page, |n: &i32| n.to_string());
        assert_eq!(list.next_page_token, None);
    }
}
