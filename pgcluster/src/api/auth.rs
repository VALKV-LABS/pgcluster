use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, Method, Request, StatusCode},
    middleware::Next,
    response::Response,
};

use super::ApiState;
use crate::config::ApiConfig;

/// Axum middleware that validates `Authorization: Bearer <token>` against the
/// configured `api_keys` list.
pub async fn require_auth(
    State(s): State<ApiState>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    if is_authorized(&s.config.api, request.method(), request.headers()) {
        Ok(next.run(request).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

/// Decide whether a request may proceed.
///
/// Rules:
/// - If `insecure_no_auth = true`: all requests are allowed.
/// - If `public_read_endpoints = true`: GET and HEAD requests bypass auth.
/// - Otherwise: a valid `Authorization: Bearer <key>` header is required, so
///   an empty `api_keys` list rejects everything.
fn is_authorized(api: &ApiConfig, method: &Method, headers: &HeaderMap) -> bool {
    if api.insecure_no_auth {
        return true;
    }

    // Public GET/HEAD endpoints bypass auth when configured.
    if api.public_read_endpoints && (method == Method::GET || method == Method::HEAD) {
        return true;
    }

    // Extract the Bearer token from the Authorization header.
    let token = headers
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(str::trim);

    match token {
        Some(t) => api.api_keys.iter().any(|k| constant_time_eq(k, t)),
        None => false,
    }
}

/// Compare two strings in constant time to prevent timing side-channel attacks.
///
/// Returns `true` only when both slices have identical length and identical
/// byte content, without short-circuiting on the first differing byte.
fn constant_time_eq(a: &str, b: &str) -> bool {
    let a = a.as_bytes();
    let b = b.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (&x, &y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify the strip_prefix logic used to extract the token.
    #[test]
    fn bearer_extraction() {
        let header = "Bearer my-secret-token";
        let token = header.strip_prefix("Bearer ").map(str::trim);
        assert_eq!(token, Some("my-secret-token"));
    }

    #[test]
    fn bearer_missing_prefix_returns_none() {
        let header = "Basic abc123";
        let token = header.strip_prefix("Bearer ").map(str::trim);
        assert_eq!(token, None);
    }

    #[test]
    fn bearer_whitespace_trimmed() {
        let header = "Bearer   token-with-space  ";
        let token = header.strip_prefix("Bearer ").map(str::trim);
        assert_eq!(token, Some("token-with-space"));
    }

    fn api(keys: &[&str]) -> ApiConfig {
        ApiConfig {
            api_keys: keys.iter().map(|k| k.to_string()).collect(),
            ..Default::default()
        }
    }

    fn bearer(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", format!("Bearer {token}").parse().unwrap());
        headers
    }

    #[test]
    fn empty_api_keys_rejects_everything() {
        assert!(!is_authorized(&api(&[]), &Method::GET, &HeaderMap::new()));
        assert!(!is_authorized(&api(&[]), &Method::GET, &bearer("anything")));
    }

    #[test]
    fn missing_or_wrong_token_is_rejected() {
        let cfg = api(&["secret"]);
        assert!(!is_authorized(&cfg, &Method::POST, &HeaderMap::new()));
        assert!(!is_authorized(&cfg, &Method::POST, &bearer("wrong")));
    }

    #[test]
    fn valid_token_is_accepted() {
        assert!(is_authorized(
            &api(&["secret"]),
            &Method::POST,
            &bearer("secret")
        ));
    }

    #[test]
    fn public_reads_do_not_open_writes() {
        let cfg = ApiConfig {
            public_read_endpoints: true,
            ..api(&["secret"])
        };
        assert!(is_authorized(&cfg, &Method::GET, &HeaderMap::new()));
        assert!(!is_authorized(&cfg, &Method::POST, &HeaderMap::new()));
    }

    #[test]
    fn development_flag_allows_everything() {
        let cfg = ApiConfig {
            insecure_no_auth: true,
            ..api(&[])
        };
        assert!(is_authorized(&cfg, &Method::POST, &HeaderMap::new()));
    }

    #[test]
    fn valid_key_matches() {
        let keys = ["secret".to_string(), "other".to_string()];
        let token = "secret";
        assert!(keys.iter().any(|k| constant_time_eq(k, token)));
    }

    #[test]
    fn invalid_key_rejected() {
        let keys = ["secret".to_string()];
        let token = "wrong";
        assert!(!keys.iter().any(|k| constant_time_eq(k, token)));
    }

    #[test]
    fn constant_time_eq_same_length_different_content() {
        // Must reject without short-circuiting.
        assert!(!constant_time_eq("aaaaaa", "aaaaab"));
        assert!(!constant_time_eq("abc", "abd"));
    }

    #[test]
    fn constant_time_eq_different_length_rejected() {
        assert!(!constant_time_eq("short", "longer-string"));
        assert!(!constant_time_eq("", "x"));
    }

    #[test]
    fn constant_time_eq_identical_strings() {
        assert!(constant_time_eq("my-api-token-123", "my-api-token-123"));
        assert!(constant_time_eq("", ""));
    }
}
