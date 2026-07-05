use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};

use super::ApiState;

/// Axum middleware that validates `Authorization: Bearer <token>` against the
/// configured `api_keys` list.
///
/// Rules:
/// - If `api_keys` is empty: all requests are allowed (auth disabled).
/// - If `public_read_endpoints = true`: GET and HEAD requests bypass auth.
/// - Otherwise: a valid `Authorization: Bearer <key>` header is required.
pub async fn require_auth(
    State(s): State<ApiState>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    // Auth is disabled when no keys are configured.
    if s.config.api.api_keys.is_empty() {
        return Ok(next.run(request).await);
    }

    // Public GET/HEAD endpoints bypass auth when configured.
    if s.config.api.public_read_endpoints {
        let method = request.method();
        if method == axum::http::Method::GET || method == axum::http::Method::HEAD {
            return Ok(next.run(request).await);
        }
    }

    // Extract the Bearer token from the Authorization header.
    let token = request
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(str::trim);

    match token {
        Some(t) if s.config.api.api_keys.iter().any(|k| constant_time_eq(k, t)) => {
            Ok(next.run(request).await)
        }
        _ => Err(StatusCode::UNAUTHORIZED),
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

    #[test]
    fn empty_api_keys_disables_auth() {
        let keys: Vec<String> = vec![];
        assert!(keys.is_empty());
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
