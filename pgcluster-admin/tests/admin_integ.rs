//! `pgcluster-admin` against a real Postgres, over real TLS.
//!
//! Each test creates its own database. Run with:
//!
//!   ADMIN_TEST_DATABASE_URL=postgres://postgres:test@localhost:5499/postgres \
//!     cargo test -p pgcluster-admin --test admin_integ -- --ignored

use std::sync::{Arc, Once};
use std::time::Duration;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use pgcluster_admin::ca::Ca;
use pgcluster_admin::config::AdminConfig;
use pgcluster_admin::reconciler::Reconciler;
use pgcluster_admin::secrets::SecretBox;
use pgcluster_admin::state::AdminState;
use pgcluster_admin::{agent, api, tls, MIGRATOR};
use rcgen::{CertificateParams, KeyPair, PKCS_ECDSA_P256_SHA256};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

const VERIFIER: &str = "SCRAM-SHA-256$4096:c2FsdHNhbHRzYWx0$c3RvcmVkc3RvcmVk:c2VydmVyc2VydmVy";
const API_KEY: &str = "a-test-key-that-is-long-enough-to-pass";

struct Fixture {
    pool: PgPool,
    state: AdminState,
    admin_url: String,
    agent_url: String,
    http: reqwest::Client,
    ca_pem: String,
}

fn install_crypto() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

impl Fixture {
    async fn new() -> Option<Self> {
        let base = match std::env::var("ADMIN_TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) => {
                eprintln!("skipping: ADMIN_TEST_DATABASE_URL not set");
                return None;
            }
        };
        install_crypto();
        let name = format!("admin_{}", uuid::Uuid::new_v4().simple());
        let admin = PgPool::connect(&base).await.expect("connect");
        sqlx::query(&format!("CREATE DATABASE {name}"))
            .execute(&admin)
            .await
            .unwrap();
        let (prefix, _) = base.rsplit_once('/').unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&format!("{prefix}/{name}"))
            .await
            .unwrap();
        MIGRATOR.run(&pool).await.unwrap();

        let ca = Arc::new(Ca::generate("test-ca").unwrap());
        let secrets = SecretBox::from_base64(&BASE64.encode([9u8; 32])).unwrap();
        let hash = pgcluster_admin::state::hash_api_key(API_KEY).unwrap();
        let state = AdminState::new(pool.clone(), ca.clone(), secrets, &[hash]).unwrap();

        let issued = ca.issue_server(&["127.0.0.1".to_string()]).unwrap();
        let agent_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let agent_url = format!("https://{}", agent_listener.local_addr().unwrap());
        tokio::spawn(tls::serve(
            agent_listener,
            tls::agent_tls(&ca, &issued).unwrap(),
            agent::router(state.clone()),
        ));
        let api_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let admin_url = format!("https://{}", api_listener.local_addr().unwrap());
        tokio::spawn(tls::serve(
            api_listener,
            tls::server_tls(&issued).unwrap(),
            api::router(state.clone()),
        ));

        let ca_pem = ca.cert_pem().to_string();
        let http = client(&ca_pem, None);
        Some(Self {
            pool,
            state,
            admin_url,
            agent_url,
            http,
            ca_pem,
        })
    }

    async fn api(&self, method: &str, path: &str, body: Option<Value>) -> (u16, Value) {
        let mut request = self
            .http
            .request(method.parse().unwrap(), format!("{}{path}", self.admin_url))
            .bearer_auth(API_KEY);
        if let Some(body) = body {
            request = request.json(&body);
        }
        decode(request.send().await.unwrap()).await
    }

    async fn add_host(&self, name: &str) -> (String, String) {
        let (status, body) = self
            .api("POST", "/v1/hosts", Some(json!({ "name": name })))
            .await;
        assert_eq!(status, 201, "{body}");
        let token = body["enrollment_token"].as_str().unwrap();
        let (secret, fingerprint) = token.split_once('.').unwrap();
        assert_eq!(fingerprint, self.state.ca.fingerprint());
        (body["host"]["id"].as_str().unwrap().into(), secret.into())
    }

    async fn enroll_raw(&self, secret: &str) -> (u16, Value, KeyPair) {
        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
        let csr = CertificateParams::new(Vec::<String>::new())
            .unwrap()
            .serialize_request(&key)
            .unwrap()
            .pem()
            .unwrap();
        let response = self
            .http
            .post(format!("{}/agent/v1/enroll", self.agent_url))
            .json(&json!({ "token": secret, "csr_pem": csr }))
            .send()
            .await
            .unwrap();
        let (status, body) = decode(response).await;
        (status, body, key)
    }

    /// A client holding the certificate of an enrolled host.
    async fn enroll(&self, secret: &str) -> reqwest::Client {
        let (status, body, key) = self.enroll_raw(secret).await;
        assert_eq!(status, 200, "{body}");
        let identity = format!(
            "{}\n{}",
            body["certificate_pem"].as_str().unwrap(),
            key.serialize_pem()
        );
        client(&self.ca_pem, Some(&identity))
    }

    async fn report(&self, host: &reqwest::Client, observed: i64, instances: Value) -> u16 {
        let response = host
            .post(format!("{}/agent/v1/report", self.agent_url))
            .json(&json!({
                "observed_generation": observed,
                "version": "test",
                "address": "127.0.0.1",
                "capacity": { "vcpu": 4.0, "memory_gb": 8.0, "storage_gb": 100.0 },
                "instances": instances,
            }))
            .send()
            .await
            .unwrap();
        response.status().as_u16()
    }

    async fn desired(&self, host: &reqwest::Client, generation: i64) -> (u16, Value) {
        let url = format!(
            "{}/agent/v1/desired-state?generation={generation}&wait_seconds=0",
            self.agent_url
        );
        decode(host.get(url).send().await.unwrap()).await
    }

    async fn create_database(&self, name: &str) -> (String, String) {
        let (status, body) = self
            .api("POST", "/v1/databases", Some(database_request(name)))
            .await;
        assert_eq!(status, 202, "{body}");
        (
            body["database_id"].as_str().unwrap().into(),
            body["id"].as_str().unwrap().into(),
        )
    }

    fn reconciler(&self) -> Reconciler {
        Reconciler::new(self.state.clone(), Duration::from_millis(50))
    }

    async fn drain(&self) {
        let reconciler = self.reconciler();
        while reconciler.step_once().await.unwrap() {}
    }

    async fn generation(&self, host_id: &str) -> i64 {
        sqlx::query_scalar("SELECT generation FROM hosts WHERE id = $1")
            .bind(host_id)
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    /// Steps wait a second between tries. Tests need not.
    async fn hurry(&self) {
        sqlx::query("UPDATE operation_steps SET not_before = now() WHERE status = 'pending'")
            .execute(&self.pool)
            .await
            .unwrap();
    }
}

fn client(ca_pem: &str, identity: Option<&str>) -> reqwest::Client {
    let mut builder = reqwest::Client::builder()
        .use_rustls_tls()
        .tls_built_in_root_certs(false)
        .add_root_certificate(reqwest::Certificate::from_pem(ca_pem.as_bytes()).unwrap());
    if let Some(identity) = identity {
        builder = builder.identity(reqwest::Identity::from_pem(identity.as_bytes()).unwrap());
    }
    builder.build().unwrap()
}

async fn decode(response: reqwest::Response) -> (u16, Value) {
    let status = response.status().as_u16();
    let text = response.text().await.unwrap();
    (status, serde_json::from_str(&text).unwrap_or(Value::Null))
}

fn database_request(name: &str) -> Value {
    json!({
        "name": name,
        "version": "18",
        "resources": { "vcpu": 1, "memory_gb": 1, "storage_gb": 10 },
        "owner": { "username": "app_owner", "scram_verifier": VERIFIER },
        "allowed_cidrs": ["198.51.100.0/24"],
        "extensions": ["vector"],
    })
}

fn running(database_id: &str, generation: i64) -> Value {
    json!([{
        "database_id": database_id,
        "node": "n1",
        "observed_generation": generation,
        "state": "running",
    }])
}

// ── Service ───────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn health_is_public_and_the_api_needs_a_key() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let url = format!("{}/health", f.admin_url);
    assert_eq!(f.http.get(&url).send().await.unwrap().status(), 200);

    let url = format!("{}/v1/hosts", f.admin_url);
    let (status, body) = decode(f.http.get(&url).send().await.unwrap()).await;
    assert_eq!(status, 401);
    assert_eq!(body["error"]["code"], "unauthorized");

    let wrong = f.http.get(&url).bearer_auth("nope").send().await.unwrap();
    assert_eq!(wrong.status(), 401);
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn migrations_run_twice_without_harm() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    MIGRATOR.run(&f.pool).await.unwrap();
}

// ── Enrolment ─────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn a_host_enrols_once_and_a_reused_token_is_rejected() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (_, secret) = f.add_host("a").await;
    let (status, ..) = f.enroll_raw(&secret).await;
    assert_eq!(status, 200);
    let (status, body, _) = f.enroll_raw(&secret).await;
    assert_eq!(status, 401, "{body}");
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn concurrent_enrolments_with_one_token_give_one_certificate() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (_, secret) = f.add_host("a").await;
    let (first, second, third) = tokio::join!(
        f.enroll_raw(&secret),
        f.enroll_raw(&secret),
        f.enroll_raw(&secret)
    );
    let wins = [first.0, second.0, third.0]
        .iter()
        .filter(|s| **s == 200)
        .count();
    assert_eq!(wins, 1);
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn an_expired_token_is_rejected() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (_, secret) = f.add_host("a").await;
    sqlx::query("UPDATE enrollment_tokens SET expires_at = now() - interval '1 second'")
        .execute(&f.pool)
        .await
        .unwrap();
    let (status, ..) = f.enroll_raw(&secret).await;
    assert_eq!(status, 401);
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn a_bad_request_does_not_burn_the_token() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (_, secret) = f.add_host("a").await;
    let response = f
        .http
        .post(format!("{}/agent/v1/enroll", f.agent_url))
        .json(&json!({ "token": secret, "csr_pem": "not a request" }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    let (status, ..) = f.enroll_raw(&secret).await;
    assert_eq!(status, 200);
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn tokens_are_stored_hashed() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (_, secret) = f.add_host("a").await;
    let stored: String = sqlx::query_scalar("SELECT token_hash FROM enrollment_tokens")
        .fetch_one(&f.pool)
        .await
        .unwrap();
    assert_ne!(stored, secret);
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn state_needs_a_client_certificate() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (status, _) = f.desired(&f.http, 0).await;
    assert_eq!(status, 401);
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn a_certificate_from_another_ca_fails_the_handshake() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let other = Ca::generate("other").unwrap();
    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
    let csr = CertificateParams::new(Vec::<String>::new())
        .unwrap()
        .serialize_request(&key)
        .unwrap()
        .pem()
        .unwrap();
    let cert = other.sign_host_csr(&csr, "impostor").unwrap();
    let identity = format!("{cert}\n{}", key.serialize_pem());
    let impostor = client(&f.ca_pem, Some(&identity));
    let url = format!("{}/agent/v1/desired-state", f.agent_url);
    assert!(impostor.get(url).send().await.is_err());
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn a_deleted_host_loses_access() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (host_id, secret) = f.add_host("a").await;
    let host = f.enroll(&secret).await;
    assert_eq!(f.desired(&host, 0).await.0, 200);
    let (status, _) = f.api("DELETE", &format!("/v1/hosts/{host_id}"), None).await;
    assert_eq!(status, 204);
    assert_eq!(f.desired(&host, 0).await.0, 401);
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn a_host_rotates_its_certificate() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (_, secret) = f.add_host("a").await;
    let host = f.enroll(&secret).await;
    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
    let csr = CertificateParams::new(Vec::<String>::new())
        .unwrap()
        .serialize_request(&key)
        .unwrap()
        .pem()
        .unwrap();
    let response = host
        .post(format!("{}/agent/v1/certificate", f.agent_url))
        .json(&json!({ "csr_pem": csr }))
        .send()
        .await
        .unwrap();
    let (status, body) = decode(response).await;
    assert_eq!(status, 200, "{body}");
    let identity = format!(
        "{}\n{}",
        body["certificate_pem"].as_str().unwrap(),
        key.serialize_pem()
    );
    assert_eq!(
        f.desired(&client(&f.ca_pem, Some(&identity)), 0).await.0,
        200
    );
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn bootstrap_hosts_load_once_and_cannot_revive_a_token() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let secret = "a-token-terraform-generated";
    let config: AdminConfig = toml::from_str(&format!(
        r#"
[api]
api_key_hashes = ["{hash}"]
[tls]
ca_dir = "/nonexistent"
server_names = ["x"]
[[bootstrap_hosts]]
name = "boot-1"
token_sha256 = "{hash}"
"#,
        hash = agent::token_hash(secret),
    ))
    .unwrap();
    pgcluster_admin::load_bootstrap_hosts(&f.pool, &config)
        .await
        .unwrap();
    assert_eq!(f.enroll_raw(secret).await.0, 200);

    pgcluster_admin::load_bootstrap_hosts(&f.pool, &config)
        .await
        .unwrap();
    assert_eq!(f.enroll_raw(secret).await.0, 401);
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn a_duplicate_host_name_conflicts() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    f.add_host("a").await;
    let (status, _) = f
        .api("POST", "/v1/hosts", Some(json!({ "name": "a" })))
        .await;
    assert_eq!(status, 409);
}

// ── Desired state ─────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn long_poll_returns_when_the_generation_moves() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (host_id, secret) = f.add_host("a").await;
    let host = f.enroll(&secret).await;
    let generation = f.generation(&host_id).await;

    // Nothing has changed, so the wait runs out.
    let (status, _) = f.desired(&host, generation).await;
    assert_eq!(status, 304);

    // Answers as soon as a change lands, well before the 30 second wait.
    let url = format!(
        "{}/agent/v1/desired-state?generation={generation}&wait_seconds=30",
        f.agent_url
    );
    let poll = tokio::spawn({
        let host = host.clone();
        async move { decode(host.get(url).send().await.unwrap()).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    sqlx::query("UPDATE hosts SET generation = generation + 1 WHERE id = $1")
        .bind(&host_id)
        .execute(&f.pool)
        .await
        .unwrap();
    let (status, body) = tokio::time::timeout(Duration::from_secs(5), poll)
        .await
        .expect("the long poll should end when the generation moves")
        .unwrap();
    assert_eq!(status, 200);
    assert_eq!(body["generation"], generation + 1);
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn a_host_sees_only_its_own_instances() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (_, a_secret) = f.add_host("a").await;
    let a = f.enroll(&a_secret).await;
    assert_eq!(f.report(&a, 0, json!([])).await, 204);
    f.create_database("orders").await;
    f.drain().await;

    let (_, b_secret) = f.add_host("b").await;
    let b = f.enroll(&b_secret).await;
    let (_, a_state) = f.desired(&a, 0).await;
    let (_, b_state) = f.desired(&b, 0).await;
    assert_eq!(a_state["instances"].as_array().unwrap().len(), 1);
    assert_eq!(b_state["instances"].as_array().unwrap().len(), 0);
}

// ── Databases and operations ──────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn a_database_goes_through_its_whole_life() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (host_id, secret) = f.add_host("a").await;
    let host = f.enroll(&secret).await;
    assert_eq!(f.report(&host, 0, json!([])).await, 204);

    let (database_id, operation_id) = f.create_database("orders").await;
    let path = format!("/v1/databases/{database_id}");
    let (_, body) = f.api("GET", &path, None).await;
    assert_eq!(body["status"], "provisioning");
    assert_eq!(body["endpoint"], Value::Null);

    f.drain().await;
    let (_, state) = f.desired(&host, 0).await;
    let instance = &state["instances"][0];
    assert_eq!(instance["database_name"], "orders");
    assert_eq!(instance["node"], "n1");
    assert_eq!(instance["port_base"], 20000);
    assert_eq!(instance["generation"], 1);
    assert_eq!(instance["operation_id"], operation_id.as_str());
    assert_eq!(instance["owner"]["scram_verifier"], VERIFIER);
    assert!(instance["secrets"]["key_pem"]
        .as_str()
        .unwrap()
        .contains("PRIVATE KEY"));
    let (_, body) = f.api("GET", &path, None).await;
    assert_eq!(body["status"], "provisioning", "not active until applied");

    // Agents post step output against the operation.
    let response = host
        .post(format!(
            "{}/agent/v1/operations/{operation_id}/log",
            f.agent_url
        ))
        .json(&json!({ "message": "initdb finished" }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 204);

    f.report(
        &host,
        f.generation(&host_id).await,
        running(&database_id, 1),
    )
    .await;
    f.hurry().await;
    f.drain().await;

    let (_, body) = f.api("GET", &path, None).await;
    assert_eq!(body["status"], "active", "{body}");
    assert_eq!(body["endpoint"]["host"], "127.0.0.1");
    assert_eq!(body["endpoint"]["port"], 20000);
    assert!(body["owner_username"] == "app_owner");
    assert!(
        !body.to_string().contains("SCRAM"),
        "the API must not echo the verifier"
    );

    let (status, body) = f
        .api("GET", &format!("{path}/operations/{operation_id}"), None)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["status"], "succeeded");
    assert_eq!(body["steps"].as_array().unwrap().len(), 3);
    let logs = body["logs"].to_string();
    assert!(logs.contains("initdb finished"), "{logs}");

    // A whole-list change moves the database to a new generation.
    let before = f.generation(&host_id).await;
    let (status, operation) = f
        .api(
            "PUT",
            &format!("{path}/allowlist"),
            Some(json!({ "allowed_cidrs": [] })),
        )
        .await;
    assert_eq!(status, 202);
    assert_eq!(operation["target_generation"], 2);
    let (_, state) = f.desired(&host, before).await;
    assert_eq!(state["instances"][0]["allowed_cidrs"], json!([]));
    let (_, list) = f.api("GET", &format!("{path}/allowlist"), None).await;
    assert_eq!(list["allowed_cidrs"], json!([]));
    f.report(
        &host,
        f.generation(&host_id).await,
        running(&database_id, 2),
    )
    .await;
    f.hurry().await;
    f.drain().await;

    // A host cannot be removed while it runs something.
    let (status, _) = f.api("DELETE", &format!("/v1/hosts/{host_id}"), None).await;
    assert_eq!(status, 409);

    let (status, _) = f.api("DELETE", &path, None).await;
    assert_eq!(status, 202);
    let (status, _) = f.api("DELETE", &path, None).await;
    assert_eq!(status, 409, "a second delete conflicts");
    f.drain().await;
    let (_, state) = f.desired(&host, 0).await;
    assert_eq!(state["instances"], json!([]));
    f.report(&host, f.generation(&host_id).await, json!([]))
        .await;
    f.hurry().await;
    f.drain().await;
    assert_eq!(f.api("GET", &path, None).await.0, 404);
    let (status, _) = f.api("DELETE", &format!("/v1/hosts/{host_id}"), None).await;
    assert_eq!(status, 204);

    // The name is free again.
    f.create_database("orders").await;
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn secrets_are_encrypted_at_rest() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (_, secret) = f.add_host("a").await;
    let host = f.enroll(&secret).await;
    f.report(&host, 0, json!([])).await;
    f.create_database("orders").await;
    f.drain().await;
    let (_, state) = f.desired(&host, 0).await;
    let password = state["instances"][0]["secrets"]["superuser_password"]
        .as_str()
        .unwrap();
    let stored: Vec<u8> = sqlx::query_scalar("SELECT secrets FROM databases")
        .fetch_one(&f.pool)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&stored);
    assert!(!text.contains(password));
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn bad_requests_get_structured_errors() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let mut request = database_request("orders");
    request["owner"]["scram_verifier"] = json!("plaintext");
    let (status, body) = f.api("POST", "/v1/databases", Some(request)).await;
    assert_eq!(status, 422);
    assert_eq!(body["error"]["code"], "invalid_request");

    let (status, body) = f.api("POST", "/v1/databases", Some(json!({}))).await;
    assert_eq!(status, 422);
    assert_eq!(body["error"]["code"], "invalid_request");

    let mut request = database_request("orders");
    request["hosts"] = json!(["no-such-host"]);
    let (status, _) = f.api("POST", "/v1/databases", Some(request)).await;
    assert_eq!(status, 422);

    f.create_database("orders").await;
    let (status, _) = f
        .api("POST", "/v1/databases", Some(database_request("orders")))
        .await;
    assert_eq!(status, 409);
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn a_list_pages_through_everything() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    for name in ["a", "b", "c"] {
        f.create_database(name).await;
    }
    let (_, first) = f.api("GET", "/v1/databases?limit=2", None).await;
    assert_eq!(first["items"].as_array().unwrap().len(), 2);
    let token = first["next_page_token"].as_str().unwrap();
    let (_, second) = f
        .api(
            "GET",
            &format!("/v1/databases?limit=2&page_token={token}"),
            None,
        )
        .await;
    assert_eq!(second["items"].as_array().unwrap().len(), 1);
    assert_eq!(second["next_page_token"], Value::Null);
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn ports_do_not_collide_on_a_host() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (_, secret) = f.add_host("a").await;
    let host = f.enroll(&secret).await;
    f.report(&host, 0, json!([])).await;
    f.create_database("one").await;
    f.create_database("two").await;
    f.drain().await;
    let (_, state) = f.desired(&host, 0).await;
    let mut ports: Vec<i64> = state["instances"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["port_base"].as_i64().unwrap())
        .collect();
    ports.sort();
    assert_eq!(ports, vec![20000, 20010]);
}

// ── Reconciler ────────────────────────────────────────────────────────────────

/// A restarted `pgcluster-admin` picks up after the last finished step: the
/// placement, its secrets and its generation bump happen only once.
#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn a_restart_resumes_an_operation_with_no_duplicate_work() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (host_id, secret) = f.add_host("a").await;
    let host = f.enroll(&secret).await;
    f.report(&host, 0, json!([])).await;
    let (database_id, operation_id) = f.create_database("orders").await;

    // The first process places the node, then dies while waiting for the host.
    let first = tokio::spawn(f.reconciler().run());
    let mut placed = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let done: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM operation_steps WHERE name = 'place' AND status = 'done'",
        )
        .fetch_one(&f.pool)
        .await
        .unwrap();
        if done == 1 {
            placed = true;
            break;
        }
    }
    assert!(placed, "the first process should place the node");
    first.abort();
    let _ = first.await;

    let generation = f.generation(&host_id).await;
    let secrets_before: Vec<u8> = sqlx::query_scalar("SELECT secrets FROM nodes")
        .fetch_one(&f.pool)
        .await
        .unwrap();

    // The host applies the change while nothing runs.
    f.report(&host, generation, running(&database_id, 1)).await;
    f.hurry().await;

    // A second process finishes the rest.
    let second = tokio::spawn(f.reconciler().run());
    let mut status = String::new();
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        status = sqlx::query_scalar("SELECT status FROM operations WHERE id = $1")
            .bind(&operation_id)
            .fetch_one(&f.pool)
            .await
            .unwrap();
        if status == "succeeded" {
            break;
        }
    }
    second.abort();
    assert_eq!(status, "succeeded");

    let attempts: i32 =
        sqlx::query_scalar("SELECT attempts FROM operation_steps WHERE name = 'place'")
            .fetch_one(&f.pool)
            .await
            .unwrap();
    assert_eq!(attempts, 1, "place must run once");
    assert_eq!(f.generation(&host_id).await, generation, "no second bump");
    let secrets_after: Vec<u8> = sqlx::query_scalar("SELECT secrets FROM nodes")
        .fetch_one(&f.pool)
        .await
        .unwrap();
    assert_eq!(secrets_before, secrets_after, "no new secrets");
    let nodes: i64 = sqlx::query_scalar("SELECT count(*) FROM nodes")
        .fetch_one(&f.pool)
        .await
        .unwrap();
    assert_eq!(nodes, 1);
}

/// A crash partway through a step leaves nothing behind, because the step and
/// its claim share a transaction.
#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn a_crash_inside_a_step_rolls_its_work_back() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (_, secret) = f.add_host("a").await;
    let host = f.enroll(&secret).await;
    f.report(&host, 0, json!([])).await;
    f.create_database("orders").await;

    // Hold the row a step locks, so the first attempt cannot finish.
    let mut blocker = f.pool.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM databases FOR UPDATE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let stuck = {
        let reconciler = f.reconciler();
        tokio::spawn(async move { reconciler.step_once().await })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    stuck.abort();
    let _ = stuck.await;
    blocker.rollback().await.unwrap();

    // The dead attempt's rollback lands on its connection a moment later. Until
    // then SKIP LOCKED passes over the step, as it would for a live worker.
    let mut nodes = 0i64;
    for _ in 0..50 {
        f.drain().await;
        nodes = sqlx::query_scalar("SELECT count(*) FROM nodes")
            .fetch_one(&f.pool)
            .await
            .unwrap();
        if nodes > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(nodes, 1);
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn parallel_reconcilers_do_each_step_once() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (host_id, secret) = f.add_host("a").await;
    let host = f.enroll(&secret).await;
    f.report(&host, 0, json!([])).await;
    let before = f.generation(&host_id).await;
    for n in 0..5 {
        f.create_database(&format!("db{n}")).await;
    }

    let workers: Vec<_> = (0..4).map(|_| tokio::spawn(f.reconciler().run())).collect();
    tokio::time::sleep(Duration::from_secs(2)).await;
    workers.iter().for_each(|w| w.abort());

    let places: i64 = sqlx::query_scalar(
        "SELECT sum(attempts) FROM operation_steps WHERE name = 'place' AND status = 'done'",
    )
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(places, 5);
    assert_eq!(f.generation(&host_id).await, before + 5);
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn a_database_runs_one_operation_at_a_time() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (host_id, secret) = f.add_host("a").await;
    let host = f.enroll(&secret).await;
    f.report(&host, 0, json!([])).await;
    let (database_id, _) = f.create_database("orders").await;
    let path = format!("/v1/databases/{database_id}/allowlist");
    f.api("PUT", &path, Some(json!({ "allowed_cidrs": [] })))
        .await;

    f.drain().await;
    let queued: String = sqlx::query_scalar("SELECT status FROM operations WHERE kind = 'update'")
        .fetch_one(&f.pool)
        .await
        .unwrap();
    assert_eq!(queued, "pending", "the update waits for the create");

    f.report(
        &host,
        f.generation(&host_id).await,
        running(&database_id, 2),
    )
    .await;
    f.hurry().await;
    f.drain().await;
    let statuses: Vec<String> = sqlx::query_scalar("SELECT status FROM operations ORDER BY seq")
        .fetch_all(&f.pool)
        .await
        .unwrap();
    assert_eq!(statuses, vec!["succeeded", "succeeded"]);
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn an_operation_past_its_deadline_fails_and_marks_the_database() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (database_id, _) = f.create_database("orders").await;
    sqlx::query("UPDATE operations SET deadline = now() - interval '1 second'")
        .execute(&f.pool)
        .await
        .unwrap();
    f.drain().await;
    let (_, body) = f
        .api("GET", &format!("/v1/databases/{database_id}"), None)
        .await;
    assert_eq!(body["status"], "error");
    assert!(body["status_reason"].as_str().unwrap().contains("place"));
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn a_delete_supersedes_a_stuck_create() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (database_id, _) = f.create_database("orders").await;
    let path = format!("/v1/databases/{database_id}");
    f.drain().await;
    let (status, _) = f.api("DELETE", &path, None).await;
    assert_eq!(status, 202);
    f.drain().await;
    f.hurry().await;
    f.drain().await;
    assert_eq!(f.api("GET", &path, None).await.0, 404);
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn a_failed_or_lost_node_degrades_the_database_and_it_recovers() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (host_id, secret) = f.add_host("a").await;
    let host = f.enroll(&secret).await;
    f.report(&host, 0, json!([])).await;
    let (database_id, _) = f.create_database("orders").await;
    f.drain().await;
    let path = format!("/v1/databases/{database_id}");
    let generation = f.generation(&host_id).await;
    f.report(&host, generation, running(&database_id, 1)).await;
    f.hurry().await;
    f.drain().await;

    let failed = json!([{
        "database_id": database_id,
        "node": "n1",
        "observed_generation": 1,
        "state": "failed",
        "error": "out of disk",
    }]);
    f.report(&host, generation, failed).await;
    f.reconciler().sweep().await.unwrap();
    let (_, body) = f.api("GET", &path, None).await;
    assert_eq!(body["status"], "degraded");
    assert!(body["status_reason"]
        .as_str()
        .unwrap()
        .contains("out of disk"));

    f.report(&host, generation, running(&database_id, 1)).await;
    f.reconciler().sweep().await.unwrap();
    let (_, body) = f.api("GET", &path, None).await;
    assert_eq!(body["status"], "active");
    assert_eq!(body["status_reason"], Value::Null);

    sqlx::query("UPDATE hosts SET last_seen_at = now() - interval '5 minutes'")
        .execute(&f.pool)
        .await
        .unwrap();
    f.reconciler().sweep().await.unwrap();
    let (_, body) = f.api("GET", &path, None).await;
    assert_eq!(body["status"], "degraded");
    let (_, hosts) = f.api("GET", "/v1/hosts", None).await;
    assert_eq!(hosts["items"][0]["status"], "offline");
}

/// A flood of enrolment requests is turned away and leaves the pool usable.
#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn an_enrolment_flood_cannot_exhaust_the_metadata_database() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (_, secret) = f.add_host("a").await;

    // Hold the token's row, so each enrolment sits on a database connection.
    let mut blocker = f.pool.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM enrollment_tokens FOR UPDATE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let url = format!("{}/agent/v1/enroll", f.agent_url);
    let body = json!({ "token": secret, "csr_pem": "not a request" });
    let requests: Vec<_> = (0..30)
        .map(|_| {
            let (http, url, body) = (f.http.clone(), url.clone(), body.clone());
            tokio::spawn(async move { http.post(url).json(&body).send().await })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Everyone else still gets a connection.
    let (status, _) = f.api("GET", "/v1/hosts", None).await;
    assert_eq!(status, 200);
    blocker.rollback().await.unwrap();

    let mut refused = 0;
    for request in requests {
        if request.await.unwrap().unwrap().status() == 429 {
            refused += 1;
        }
    }
    assert!(refused >= 20, "only {refused} of 30 were refused");
}

async fn cert_of(f: &Fixture, host: &reqwest::Client) -> String {
    let (_, state) = f.desired(host, 0).await;
    state["instances"][0]["secrets"]["cert_pem"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn active_database(f: &Fixture) -> (String, String, reqwest::Client) {
    let (host_id, secret) = f.add_host("a").await;
    let host = f.enroll(&secret).await;
    f.report(&host, 0, json!([])).await;
    let (database_id, _) = f.create_database("orders").await;
    f.drain().await;
    f.report(
        &host,
        f.generation(&host_id).await,
        running(&database_id, 1),
    )
    .await;
    f.hurry().await;
    f.drain().await;
    (host_id, database_id, host)
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn a_new_certificate_is_not_renewed() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (host_id, _, _) = active_database(&f).await;
    let generation = f.generation(&host_id).await;
    f.reconciler().renew_certificates().await.unwrap();
    assert_eq!(f.generation(&host_id).await, generation);
}

/// A certificate near its end is reissued as an ordinary tracked change.
#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn a_certificate_near_its_end_is_renewed_through_desired_state() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (host_id, database_id, host) = active_database(&f).await;
    let old_cert = cert_of(&f, &host).await;
    let old_generation = f.generation(&host_id).await;
    let (_, state) = f.desired(&host, 0).await;
    let old_ca = state["instances"][0]["secrets"]["ca_cert_pem"].clone();
    let old_key = state["instances"][0]["secrets"]["superuser_password"].clone();

    sqlx::query("UPDATE nodes SET cert_not_after = now() + interval '1 day'")
        .execute(&f.pool)
        .await
        .unwrap();
    f.reconciler().renew_certificates().await.unwrap();

    assert_eq!(f.generation(&host_id).await, old_generation + 1);
    let (_, state) = f.desired(&host, 0).await;
    let instance = &state["instances"][0];
    assert_ne!(cert_of(&f, &host).await, old_cert);
    assert_eq!(instance["generation"], 2);
    assert_eq!(instance["secrets"]["ca_cert_pem"], old_ca, "same CA");
    assert_eq!(instance["secrets"]["superuser_password"], old_key);

    // Renewing again finds nothing to do, and the renewed cert is good for months.
    f.reconciler().renew_certificates().await.unwrap();
    assert_eq!(f.generation(&host_id).await, old_generation + 1);
    let days: f64 = sqlx::query_scalar(
        "SELECT extract(epoch FROM cert_not_after - now())::float8 / 86400 FROM nodes",
    )
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert!(days > 80.0, "{days}");

    f.report(
        &host,
        f.generation(&host_id).await,
        running(&database_id, 2),
    )
    .await;
    f.hurry().await;
    f.drain().await;
    let statuses: Vec<String> = sqlx::query_scalar("SELECT status FROM operations ORDER BY seq")
        .fetch_all(&f.pool)
        .await
        .unwrap();
    assert_eq!(statuses, vec!["succeeded", "succeeded"]);
}

#[tokio::test]
#[ignore = "requires Postgres (ADMIN_TEST_DATABASE_URL)"]
async fn a_host_that_is_away_keeps_its_certificate_until_it_returns() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let (host_id, _, _) = active_database(&f).await;
    for sql in [
        "UPDATE nodes SET cert_not_after = NULL",
        "UPDATE hosts SET last_seen_at = now() - interval '5 minutes'",
    ] {
        sqlx::query(sql).execute(&f.pool).await.unwrap();
    }
    let generation = f.generation(&host_id).await;
    f.reconciler().renew_certificates().await.unwrap();
    assert_eq!(f.generation(&host_id).await, generation);
}
