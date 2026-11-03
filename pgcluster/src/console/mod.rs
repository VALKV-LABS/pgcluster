//! `pgcluster console`: a local web page for trying every operation and checking
//! the result.
//!
//! The page cannot call the APIs itself: they use a private CA, need keys it
//! should not hold, and allow no other origin. So this small server holds the
//! keys and passes requests on. It is a tool for one operator on their own
//! machine, not a portal for users, and it answers only on loopback names.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::{
    body::Bytes,
    extract::{Path, RawQuery, Request, State},
    http::{header, HeaderMap, Method, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{any, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::postgres::{PgConnectOptions, PgRow, PgSslMode};
use sqlx::{Column, ConnectOptions, Connection, Executor, PgConnection, Row, ValueRef};

use crate::cli::admin::generate_password;

const PAGE: &str = include_str!("index.html");
/// A write the page makes carries this. Another site's page cannot add it
/// without a CORS preflight, which this server never answers.
const WRITE_HEADER: &str = "x-pgcluster-console";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const HEALTH_TIMEOUT: Duration = Duration::from_secs(2);
const SQL_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_SQL_ROWS: usize = 200;

#[derive(Clone)]
pub struct ConsoleConfig {
    pub admin_url: Option<String>,
    pub admin_key: Option<String>,
    pub admin_ca: Option<Vec<u8>>,
    pub cluster_urls: Vec<String>,
    pub cluster_key: Option<String>,
    pub cluster_sql_url: Option<String>,
}

#[derive(Clone)]
struct Console {
    config: Arc<ConsoleConfig>,
    admin_http: reqwest::Client,
    cluster_http: reqwest::Client,
    health_http: reqwest::Client,
}

pub fn router(config: ConsoleConfig) -> Result<Router> {
    let mut admin = reqwest::Client::builder().timeout(REQUEST_TIMEOUT);
    if let Some(pem) = &config.admin_ca {
        admin = admin
            .tls_built_in_root_certs(false)
            .add_root_certificate(reqwest::Certificate::from_pem(pem).context("parse admin CA")?);
    }
    let console = Console {
        admin_http: admin.build().context("build HTTP client")?,
        cluster_http: reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("build HTTP client")?,
        health_http: reqwest::Client::builder()
            .timeout(HEALTH_TIMEOUT)
            .build()
            .context("build HTTP client")?,
        config: Arc::new(config),
    };
    Ok(Router::new()
        .route("/", get(|| async { Html(PAGE) }))
        .route("/console/overview", get(overview))
        .route("/console/password", post(password))
        .route("/console/sql", post(run_sql))
        .route("/admin/v1/*path", any(forward_admin))
        .route("/cluster/api/*path", any(forward_cluster))
        .layer(middleware::from_fn(guard))
        .with_state(console))
}

/// A page on any website can make a browser call localhost, and DNS rebinding
/// can make that look same-origin. The checks here refuse both.
async fn guard(request: Request, next: Next) -> Response {
    if !host_allowed(request.headers()) {
        return (
            StatusCode::FORBIDDEN,
            "this console answers on localhost only",
        )
            .into_response();
    }
    let reads = matches!(*request.method(), Method::GET | Method::HEAD);
    if !reads && !request.headers().contains_key(WRITE_HEADER) {
        return (StatusCode::FORBIDDEN, "missing console header").into_response();
    }
    next.run(request).await
}

fn host_allowed(headers: &HeaderMap) -> bool {
    let Some(host) = headers.get(header::HOST).and_then(|h| h.to_str().ok()) else {
        return false;
    };
    let name = match host.rsplit_once(':') {
        Some((name, port)) if port.bytes().all(|b| b.is_ascii_digit()) => name,
        _ => host,
    };
    matches!(name, "localhost" | "127.0.0.1" | "[::1]")
}

fn unavailable(what: &str, detail: impl std::fmt::Display) -> Response {
    let body =
        json!({ "error": { "code": "unavailable", "message": format!("{what}: {detail}") } });
    (StatusCode::BAD_GATEWAY, Json(body)).into_response()
}

/// Pass on an upstream reply as it came.
async fn relay(response: reqwest::Response) -> Response {
    let status =
        StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_string();
    match response.bytes().await {
        Ok(body) => (status, [(header::CONTENT_TYPE, content_type)], body).into_response(),
        Err(e) => unavailable("read the reply", e),
    }
}

fn upstream_method(method: &Method) -> Option<reqwest::Method> {
    reqwest::Method::from_bytes(method.as_str().as_bytes()).ok()
}

async fn forward_admin(
    State(console): State<Console>,
    method: Method,
    Path(path): Path<String>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    let (Some(base), Some(key)) = (&console.config.admin_url, &console.config.admin_key) else {
        return unavailable("pgcluster-admin", "not configured");
    };
    let Some(method) = upstream_method(&method) else {
        return unavailable("HTTP method", method);
    };
    let query = query.map(|q| format!("?{q}")).unwrap_or_default();
    let result = console
        .admin_http
        .request(method, format!("{base}/v1/{path}{query}"))
        .bearer_auth(key)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await;
    match result {
        Ok(response) => relay(response).await,
        Err(e) => unavailable("pgcluster-admin", e),
    }
}

#[derive(Deserialize)]
struct Health {
    #[serde(default)]
    raft_leader: bool,
}

/// The URL of the Raft leader, or of any node that answers.
async fn pick_node(console: &Console) -> Option<String> {
    let mut reachable = None;
    for url in &console.config.cluster_urls {
        let Ok(response) = console
            .health_http
            .get(format!("{url}/health"))
            .send()
            .await
        else {
            continue;
        };
        let leader = response.json::<Health>().await.is_ok_and(|h| h.raft_leader);
        if leader {
            return Some(url.clone());
        }
        reachable.get_or_insert_with(|| url.clone());
    }
    reachable
}

async fn forward_cluster(
    State(console): State<Console>,
    method: Method,
    Path(path): Path<String>,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    let Some(base) = pick_node(&console).await else {
        return unavailable("pgcluster", "no node answered");
    };
    let Some(method) = upstream_method(&method) else {
        return unavailable("HTTP method", method);
    };
    let query = query.map(|q| format!("?{q}")).unwrap_or_default();
    let mut request = console
        .cluster_http
        .request(method, format!("{base}/api/{path}{query}"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body);
    if let Some(key) = &console.config.cluster_key {
        request = request.bearer_auth(key);
    }
    match request.send().await {
        Ok(response) => relay(response).await,
        Err(e) => unavailable("pgcluster", e),
    }
}

#[derive(Serialize)]
struct Overview {
    admin: bool,
    cluster: bool,
    cluster_primary: Option<String>,
    cluster_sql: bool,
}

async fn overview(State(console): State<Console>) -> Json<Overview> {
    let admin = match &console.config.admin_url {
        Some(base) => console
            .admin_http
            .get(format!("{base}/health"))
            .timeout(HEALTH_TIMEOUT)
            .send()
            .await
            .is_ok_and(|r| r.status().is_success()),
        None => false,
    };
    let mut cluster = false;
    let mut cluster_primary = None;
    if let Some(base) = pick_node(&console).await {
        if let Ok(response) = console
            .health_http
            .get(format!("{base}/health"))
            .send()
            .await
        {
            let health: Value = response.json().await.unwrap_or(Value::Null);
            cluster = true;
            cluster_primary = health["primary"].as_str().map(str::to_string);
        }
    }
    Json(Overview {
        admin,
        cluster,
        cluster_primary,
        cluster_sql: console.config.cluster_sql_url.is_some(),
    })
}

/// Generated here, on the operator's machine. Only the verifier goes further.
async fn password() -> Json<Value> {
    let (password, scram_verifier) = generate_password();
    Json(json!({ "password": password, "scram_verifier": scram_verifier }))
}

#[derive(Deserialize)]
#[serde(tag = "target", rename_all = "lowercase")]
enum SqlTarget {
    /// A database from the management plane, verified against its own CA.
    Database {
        host: String,
        port: u16,
        database: String,
        username: String,
        password: String,
        ca_cert_pem: String,
    },
    /// The three-node cluster in `--cluster-sql-url`.
    Cluster,
}

#[derive(Deserialize)]
struct SqlRequest {
    #[serde(flatten)]
    target: SqlTarget,
    sql: String,
}

#[derive(Serialize, Default)]
struct SqlResult {
    columns: Vec<String>,
    rows: Vec<Vec<Option<String>>>,
    truncated: bool,
}

fn text_row(row: &PgRow) -> Vec<Option<String>> {
    (0..row.len())
        .map(|i| {
            let value = row.try_get_raw(i).ok()?;
            if value.is_null() {
                return None;
            }
            // The simple protocol sends every value as text.
            value.as_str().ok().map(str::to_string)
        })
        .collect()
}

async fn query(options: PgConnectOptions, sql: &str) -> Result<SqlResult> {
    let mut connection = PgConnection::connect_with(&options)
        .await
        .context("connect")?;
    // A plain string runs on the simple protocol, so anything psql accepts works.
    let rows: Vec<PgRow> = connection
        .fetch_all(sql)
        .await
        .context("run the statement")?;
    connection.close().await.ok();
    let mut result = SqlResult::default();
    if let Some(first) = rows.first() {
        result.columns = first
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
    }
    result.truncated = rows.len() > MAX_SQL_ROWS;
    result.rows = rows.iter().take(MAX_SQL_ROWS).map(text_row).collect();
    Ok(result)
}

async fn run_sql(State(console): State<Console>, Json(request): Json<SqlRequest>) -> Response {
    let options = match request.target {
        SqlTarget::Database {
            host,
            port,
            database,
            username,
            password,
            ca_cert_pem,
        } => PgConnectOptions::new()
            .host(&host)
            .port(port)
            .database(&database)
            .username(&username)
            .password(&password)
            .ssl_mode(PgSslMode::VerifyFull)
            .ssl_root_cert_from_pem(ca_cert_pem.into_bytes()),
        SqlTarget::Cluster => {
            let Some(url) = &console.config.cluster_sql_url else {
                return unavailable("cluster SQL", "not configured");
            };
            match url.parse::<PgConnectOptions>() {
                Ok(options) => options,
                Err(e) => return unavailable("cluster SQL", e),
            }
        }
    };
    let options = options.disable_statement_logging();
    match tokio::time::timeout(SQL_TIMEOUT, query(options, &request.sql)).await {
        Ok(Ok(result)) => Json(result).into_response(),
        Ok(Err(e)) => {
            let body = json!({ "error": { "code": "sql", "message": format!("{e:#}") } });
            (StatusCode::BAD_REQUEST, Json(body)).into_response()
        }
        Err(_) => unavailable("the statement", "timed out"),
    }
}

/// Serve the console until the process is stopped.
pub async fn serve(listen: &str, config: ConsoleConfig) -> Result<()> {
    // sqlx's rustls pulls in aws-lc-rs too, so pin ring, as the server does.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let app = router(config)?;
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("bind {listen}"))?;
    println!("pgcluster console on http://{}", listener.local_addr()?);
    axum::serve(listener, app.into_make_service())
        .await
        .context("serve")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use tower::ServiceExt;

    fn app() -> Router {
        router(ConsoleConfig {
            admin_url: None,
            admin_key: None,
            admin_ca: None,
            cluster_urls: vec![],
            cluster_key: None,
            cluster_sql_url: None,
        })
        .unwrap()
    }

    async fn status(request: HttpRequest<Body>) -> StatusCode {
        app().oneshot(request).await.unwrap().status()
    }

    fn get(host: &str) -> HttpRequest<Body> {
        HttpRequest::builder()
            .uri("/")
            .header(header::HOST, host)
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn loopback_names_get_the_page() {
        assert_eq!(status(get("localhost:18080")).await, StatusCode::OK);
        assert_eq!(status(get("127.0.0.1:8080")).await, StatusCode::OK);
        assert_eq!(status(get("[::1]:8080")).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn another_name_is_refused_so_dns_rebinding_gains_nothing() {
        assert_eq!(status(get("evil.example.com")).await, StatusCode::FORBIDDEN);
        assert_eq!(
            status(get("localhost.evil.example.com:80")).await,
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn a_write_needs_the_console_header() {
        let request = HttpRequest::post("/admin/v1/hosts")
            .header(header::HOST, "localhost")
            .body(Body::empty())
            .unwrap();
        assert_eq!(status(request).await, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn an_unconfigured_upstream_says_so() {
        let request = HttpRequest::post("/admin/v1/hosts")
            .header(header::HOST, "localhost")
            .header(WRITE_HEADER, "1")
            .body(Body::empty())
            .unwrap();
        assert_eq!(status(request).await, StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn the_password_comes_with_a_verifier() {
        let Json(body) = password().await;
        assert!(body["scram_verifier"]
            .as_str()
            .unwrap()
            .starts_with("SCRAM-SHA-256$"));
    }

    #[test]
    fn the_page_escapes_what_it_shows() {
        // Names and logs come from servers, so nothing may reach the page as HTML.
        assert!(!PAGE.contains("innerHTML"));
        assert!(!PAGE.contains("<script src"), "no third-party code");
    }
}
