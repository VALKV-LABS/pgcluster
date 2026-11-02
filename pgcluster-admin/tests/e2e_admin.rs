//! The management plane end to end, on the Docker stack: the CLI, the admin API,
//! `vk-agent host`, real instances, and clients over verified TLS.
//!
//!   cargo test -p pgcluster-admin --test e2e_admin -- --ignored --test-threads=1 --nocapture
//!
//! Needs Docker. `E2E_COMPOSE_OVERRIDE` adds a compose file, and `E2E_NO_TEARDOWN=1`
//! leaves the stack up.

use std::process::{Command, Output};
use std::time::{Duration, Instant};

use serde_json::Value;
use sqlx::postgres::{PgConnectOptions, PgSslMode};
use sqlx::{ConnectOptions, Connection, PgConnection};

const PROJECT: &str = "pgcluster-admin-e2e";
const ADMIN_URL: &str = "https://localhost:18443";
const API_KEY: &str = "dev-admin-key";
const HOST: &str = "host-1";
const CONSOLE_URL: &str = "http://localhost:18080";

fn root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn compose_command(args: &[&str]) -> Command {
    let mut command = Command::new("docker");
    command
        .args(["compose", "-p", PROJECT, "--profile", "systemd"])
        .args(["-f", "docker/e2e-compose.yml"])
        .args(["-f", "docker/admin-compose.yml"]);
    if let Ok(path) = std::env::var("E2E_COMPOSE_OVERRIDE") {
        command.args(["-f", &path]);
    }
    command.args(args).current_dir(root());
    command
}

fn compose(args: &[&str]) -> Output {
    let output = compose_command(args).output().expect("run docker compose");
    assert!(
        output.status.success(),
        "docker compose {args:?} failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn exec_on(service: &str, script: &str) -> Output {
    compose_command(&["exec", "-T", service, "sh", "-c", script])
        .output()
        .expect("run docker compose exec")
}

/// Run a script on a host, and return what it printed.
fn sh(service: &str, script: &str) -> String {
    let output = exec_on(service, script);
    assert!(
        output.status.success(),
        "{script} failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

/// Run the real CLI on a host, as an operator would.
fn cli(service: &str, args: &str) -> String {
    sh(
        service,
        &format!(
            "cd /tmp && PGCLUSTER_ADMIN_URL=https://pgcluster-admin:8443 \
             PGCLUSTER_ADMIN_KEY={API_KEY} PGCLUSTER_ADMIN_CA=/ca/ca.crt pgcluster {args}"
        ),
    )
}

struct Stack;

impl Stack {
    fn up(services: &[&str]) -> Self {
        compose(&["down", "-v", "--remove-orphans"]);
        let mut args = vec!["up", "-d", "--wait"];
        if std::env::var("E2E_SKIP_BUILD").is_err() {
            args.push("--build");
        }
        args.extend_from_slice(services);
        compose(&args);
        Self
    }
}

impl Drop for Stack {
    fn drop(&mut self) {
        if std::env::var("E2E_NO_TEARDOWN").is_err() {
            let _ = compose_command(&["down", "-v", "--remove-orphans"]).output();
        }
    }
}

fn admin_client() -> reqwest::Client {
    let ca = sh("pgcluster-admin", "cat /ca/ca.crt");
    reqwest::Client::builder()
        .use_rustls_tls()
        .tls_built_in_root_certs(false)
        .add_root_certificate(reqwest::Certificate::from_pem(ca.as_bytes()).unwrap())
        .build()
        .unwrap()
}

async fn api(client: &reqwest::Client, method: &str, path: &str, body: Option<Value>) -> Value {
    let mut request = client
        .request(method.parse().unwrap(), format!("{ADMIN_URL}{path}"))
        .bearer_auth(API_KEY);
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await.unwrap();
    let status = response.status();
    let body: Value = response.json().await.unwrap_or(Value::Null);
    assert!(status.is_success(), "{method} {path}: {status} {body}");
    body
}

async fn wait_for(what: &str, timeout: Duration, mut check: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !check().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_online(client: &reqwest::Client, name: &str) {
    wait_for(
        &format!("{name} to report"),
        Duration::from_secs(90),
        async || {
            let hosts = api(client, "GET", "/v1/hosts", None).await;
            hosts["items"]
                .as_array()
                .unwrap()
                .iter()
                .any(|h| h["name"] == name && h["status"] == "online")
        },
    )
    .await;
}

/// What `database create` printed.
struct Created {
    id: String,
    password: String,
}

fn parse_created(output: &str) -> Created {
    let field = |name: &str| {
        output
            .lines()
            .find_map(|l| l.strip_prefix(&format!("{name} ")))
            .unwrap_or_else(|| panic!("no {name} in\n{output}"))
            .to_string()
    };
    Created {
        id: field("database"),
        password: field("password"),
    }
}

struct Endpoint {
    host: String,
    port: u16,
    database: String,
    ca: String,
}

async fn endpoint(client: &reqwest::Client, id: &str) -> Endpoint {
    let database = api(client, "GET", &format!("/v1/databases/{id}"), None).await;
    let endpoint = &database["endpoint"];
    Endpoint {
        host: endpoint["host"].as_str().unwrap().into(),
        port: endpoint["port"].as_u64().unwrap() as u16,
        database: endpoint["database"].as_str().unwrap().into(),
        ca: endpoint["ca_cert_pem"].as_str().unwrap().into(),
    }
}

impl Endpoint {
    fn options(&self, password: &str) -> PgConnectOptions {
        PgConnectOptions::new()
            .host(&self.host)
            .port(self.port)
            .username("app_owner")
            .password(password)
            .database(&self.database)
            .ssl_mode(PgSslMode::VerifyFull)
            .ssl_root_cert_from_pem(self.ca.clone().into_bytes())
            .disable_statement_logging()
    }

    async fn connect(&self, password: &str) -> Result<PgConnection, sqlx::Error> {
        PgConnection::connect_with(&self.options(password)).await
    }

    async fn wait_connectable(&self, password: &str) -> PgConnection {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            match self.connect(password).await {
                Ok(connection) => return connection,
                Err(e) => {
                    assert!(Instant::now() < deadline, "could not connect: {e}");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }
}

async fn scalar(connection: &mut PgConnection, sql: &str) -> String {
    sqlx::query_scalar::<_, String>(sql)
        .fetch_one(connection)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

const ALL_EXTENSIONS: &str =
    "uuid-ossp pgcrypto pg_trgm unaccent vector pg_stat_statements pg_tle postgis pg_cron";

/// The owner is not a superuser, and every extension works for it.
async fn check_extensions_as_owner(connection: &mut PgConnection) {
    let superuser = scalar(
        connection,
        "SELECT rolsuper::text FROM pg_roles WHERE rolname = current_user",
    )
    .await;
    assert_eq!(superuser, "false");
    for sql in [
        "SELECT uuid_generate_v4()::text",
        "SELECT encode(digest('a', 'sha256'), 'hex')",
        "SELECT similarity('word', 'word')::text",
        "SELECT unaccent('café')",
        "SELECT ('[1,2]'::vector)::text",
        "SELECT count(*)::text FROM pg_stat_statements",
        "SELECT count(*)::text FROM pgtle.available_extensions()",
        "SELECT postgis_version()",
        "SELECT cron.schedule('* * * * *', 'SELECT 1')::text",
    ] {
        scalar(connection, sql).await;
    }
}

fn instance_dir(id: &str) -> String {
    format!("/var/lib/pgcluster/instances/{id}-n1")
}

fn postmaster_pid(service: &str, id: &str) -> String {
    sh(
        service,
        &format!("head -1 {}/pgdata/postmaster.pid", instance_dir(id)),
    )
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn e2e_admin_one_node_database() {
    let _stack = Stack::up(&[HOST]);
    let client = admin_client();
    wait_online(&client, HOST).await;

    // `vk-agent host` runs alone and opens no port.
    let pid = sh(
        HOST,
        "for p in /proc/[0-9]*; do \
           if tr '\\0' ' ' < $p/cmdline 2>/dev/null | grep -q '^vk-agent host'; then \
             basename $p; fi; done",
    );
    assert!(!pid.is_empty(), "the host agent should be its own process");
    let listeners = sh(HOST, "ss -ltnpH");
    assert!(
        !listeners.contains(&format!("pid={pid},")),
        "the host agent must not listen:\n{listeners}"
    );

    // Create through the CLI, which generates the password on this side.
    let output = cli(
        HOST,
        "database create orders --allow 0.0.0.0/0 --extension vector --wait",
    );
    let created = parse_created(&output);
    let id = created.id.as_str();
    let database = api(&client, "GET", &format!("/v1/databases/{id}"), None).await;
    assert_eq!(database["status"], "active", "{database}");
    assert!(
        !database.to_string().contains(&created.password),
        "the password must not reach the API"
    );
    let stored = sh("admin-db", &format!(
        "psql -U postgres -d pgcluster_admin -Atc \"SELECT count(*) FROM operation_logs WHERE message LIKE '%{}%'\"",
        created.password
    ));
    assert_eq!(stored, "0");

    // The database answers over verified TLS, with the owner's password.
    let endpoint = endpoint(&client, id).await;
    let mut owner = endpoint.wait_connectable(&created.password).await;
    let version = scalar(&mut owner, "SELECT current_setting('server_version')").await;
    assert!(version.starts_with("18"), "{version}");
    sqlx::query("CREATE TABLE t (v text)")
        .execute(&mut owner)
        .await
        .unwrap();
    sqlx::query("INSERT INTO t VALUES ('kept')")
        .execute(&mut owner)
        .await
        .unwrap();

    // Plaintext and a wrong password are both refused.
    let plaintext = endpoint
        .options(&created.password)
        .ssl_mode(PgSslMode::Disable);
    assert!(PgConnection::connect_with(&plaintext).await.is_err());
    assert!(endpoint.connect("wrong").await.is_err());

    // An empty allowlist refuses everyone, with no restart.
    let postmaster = postmaster_pid(HOST, id);
    cli(HOST, &format!("database allow {id} --wait"));
    wait_for(
        "the allowlist to close",
        Duration::from_secs(20),
        async || endpoint.connect(&created.password).await.is_err(),
    )
    .await;
    let connected: String = sqlx::query_scalar("SELECT 'still here'")
        .fetch_one(&mut owner)
        .await
        .expect("connected clients stay connected across a reload");
    assert_eq!(connected, "still here");
    assert_eq!(postmaster_pid(HOST, id), postmaster, "no restart");
    cli(HOST, &format!("database allow {id} 0.0.0.0/0 --wait"));
    endpoint.wait_connectable(&created.password).await;

    // A new password replaces the old one.
    let output = cli(HOST, &format!("database rotate-password {id} --wait"));
    let password = output
        .lines()
        .find_map(|l| l.strip_prefix("password "))
        .unwrap()
        .to_string();
    let owner = endpoint.wait_connectable(&password).await;
    assert!(endpoint.connect(&created.password).await.is_err());

    // Extensions install through the API. Preload ones restart Postgres.
    cli(
        HOST,
        &format!("database extensions {id} {ALL_EXTENSIONS} --wait"),
    );
    assert_ne!(postmaster_pid(HOST, id), postmaster, "preload restarts");
    owner.close().await.ok();
    let mut owner = endpoint.wait_connectable(&password).await;
    check_extensions_as_owner(&mut owner).await;

    // Removing one drops it.
    cli(HOST, &format!("database extensions {id} vector --wait"));
    let mut owner = endpoint.wait_connectable(&password).await;
    let trgm = scalar(
        &mut owner,
        "SELECT count(*)::text FROM pg_extension WHERE extname = 'pg_trgm'",
    )
    .await;
    assert_eq!(trgm, "0");
    owner.close().await.ok();

    // A process that dies comes back.
    sh(
        HOST,
        &format!("kill -9 $(cat {}/pgcluster.pid)", instance_dir(id)),
    );
    wait_for("the proxy to return", Duration::from_secs(60), async || {
        endpoint.connect(&password).await.is_ok()
    })
    .await;

    // With pgcluster-admin gone, a host restart still brings the instance back,
    // from the state it kept on disk.
    let mode = sh(HOST, "stat -c %a /var/lib/vk-agent-host/desired.json");
    assert_eq!(mode, "600", "desired state holds secrets");
    compose(&["stop", "pgcluster-admin"]);
    compose(&["restart", HOST]);
    let mut owner = endpoint.wait_connectable(&password).await;
    assert_eq!(scalar(&mut owner, "SELECT v FROM t").await, "kept");
    owner.close().await.ok();
    compose(&["start", "pgcluster-admin"]);
    let client = admin_client();
    wait_online(&client, HOST).await;

    // A host cannot go while it runs something, and a delete removes the data.
    let (host_id, _) = host_ids(&client).await;
    let refused = cli_fails(HOST, &format!("host delete {host_id}"));
    assert!(refused.contains("still runs"), "{refused}");
    cli(HOST, &format!("database delete {id} --wait"));
    let gone = exec_on(HOST, &format!("test -e {}", instance_dir(id)));
    assert!(!gone.status.success(), "the instance directory should go");
    assert!(endpoint.connect(&password).await.is_err());
}

async fn host_ids(client: &reqwest::Client) -> (String, String) {
    let hosts = api(client, "GET", "/v1/hosts", None).await;
    let host = hosts["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|h| h["name"] == HOST)
        .unwrap();
    (
        host["id"].as_str().unwrap().into(),
        host["name"].as_str().unwrap().into(),
    )
}

fn cli_fails(service: &str, args: &str) -> String {
    let output = exec_on(
        service,
        &format!(
            "PGCLUSTER_ADMIN_URL=https://pgcluster-admin:8443 PGCLUSTER_ADMIN_KEY={API_KEY} \
             PGCLUSTER_ADMIN_CA=/ca/ca.crt pgcluster {args}"
        ),
    );
    assert!(!output.status.success());
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// Every supported major gets every extension.
#[tokio::test]
#[ignore = "requires Docker"]
async fn e2e_admin_each_postgres_version() {
    let _stack = Stack::up(&[HOST]);
    let client = admin_client();
    wait_online(&client, HOST).await;
    for version in ["16", "17", "18"] {
        let mut args =
            format!("database create pg{version} --version {version} --allow 0.0.0.0/0 --wait");
        for extension in ALL_EXTENSIONS.split(' ') {
            args.push_str(&format!(" --extension {extension}"));
        }
        let created = parse_created(&cli(HOST, &args));
        let endpoint = endpoint(&client, &created.id).await;
        let mut owner = endpoint.wait_connectable(&created.password).await;
        let actual = scalar(&mut owner, "SELECT current_setting('server_version')").await;
        assert!(actual.starts_with(version), "{actual}");
        check_extensions_as_owner(&mut owner).await;
        owner.close().await.ok();
    }
}

fn unit_state(service: &str, unit: &str) -> String {
    let output = exec_on(service, &format!("systemctl is-active {unit}"));
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

/// The systemd runner, under a real systemd: start, restart, reload, stop.
#[tokio::test]
#[ignore = "requires Docker with privileged containers"]
async fn e2e_admin_systemd_runner() {
    const SYSTEMD_HOST: &str = "host-systemd";
    let _stack = Stack::up(&["pgcluster-admin", SYSTEMD_HOST]);
    let client = admin_client();
    let booted = Instant::now();
    while exec_on(SYSTEMD_HOST, "systemctl enable --now vk-agent-host")
        .status
        .code()
        != Some(0)
    {
        assert!(
            booted.elapsed() < Duration::from_secs(60),
            "systemd did not boot"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    wait_online(&client, SYSTEMD_HOST).await;

    // Start.
    let output = cli(
        SYSTEMD_HOST,
        "database create orders --host host-systemd --allow 127.0.0.0/8 --wait",
    );
    let created = parse_created(&output);
    let id = created.id.as_str();
    let agent_unit = format!("vk-instance-{id}-n1-agent.service");
    let proxy_unit = format!("vk-instance-{id}-n1-pgcluster.service");
    assert_eq!(unit_state(SYSTEMD_HOST, &agent_unit), "active");
    assert_eq!(unit_state(SYSTEMD_HOST, &proxy_unit), "active");
    let enabled = sh(SYSTEMD_HOST, &format!("systemctl is-enabled {agent_unit}"));
    assert_eq!(enabled, "enabled", "an instance must return after a reboot");

    // Postgres runs inside the agent's unit, as the postgres user.
    let cgroup = sh(
        SYSTEMD_HOST,
        &format!("cat /proc/{}/cgroup", postmaster_pid(SYSTEMD_HOST, id)),
    );
    assert!(cgroup.contains(&agent_unit), "{cgroup}");
    let psql = format!(
        "PGPASSWORD={} PGSSLMODE=require psql -h 127.0.0.1 -p 20000 -U app_owner -d orders -Atc",
        created.password
    );
    assert_eq!(sh(SYSTEMD_HOST, &format!("{psql} 'SELECT 1'")), "1");

    // The host agent can stop and come back without touching the instance.
    let postmaster = postmaster_pid(SYSTEMD_HOST, id);
    sh(SYSTEMD_HOST, "systemctl restart vk-agent-host");
    assert_eq!(postmaster_pid(SYSTEMD_HOST, id), postmaster);

    // Reload, for the allowlist, leaves Postgres alone.
    cli(
        SYSTEMD_HOST,
        &format!("database allow {id} 127.0.0.1/32 --wait"),
    );
    assert_eq!(postmaster_pid(SYSTEMD_HOST, id), postmaster);
    assert_eq!(sh(SYSTEMD_HOST, &format!("{psql} 'SELECT 2'")), "2");

    // Restart, for a preload extension.
    cli(
        SYSTEMD_HOST,
        &format!("database extensions {id} pg_cron --wait"),
    );
    assert_ne!(postmaster_pid(SYSTEMD_HOST, id), postmaster);
    assert_eq!(
        sh(
            SYSTEMD_HOST,
            &format!("{psql} \"SELECT extname FROM pg_extension WHERE extname = 'pg_cron'\"")
        ),
        "pg_cron"
    );

    // Stop and remove.
    cli(SYSTEMD_HOST, &format!("database delete {id} --wait"));
    assert_ne!(unit_state(SYSTEMD_HOST, &agent_unit), "active");
    let unit_file = exec_on(
        SYSTEMD_HOST,
        &format!("test -e /etc/systemd/system/{agent_unit}"),
    );
    assert!(!unit_file.status.success(), "the unit file should go");
}

// ── Console ───────────────────────────────────────────────────────────────────

async fn console_call(
    client: &reqwest::Client,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> (u16, Value) {
    let mut request = client
        .request(method.parse().unwrap(), format!("{CONSOLE_URL}{path}"))
        .header("x-pgcluster-console", "1");
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    (status, response.json().await.unwrap_or(Value::Null))
}

async fn console(client: &reqwest::Client, method: &str, path: &str, body: Option<Value>) -> Value {
    let (status, reply) = console_call(client, method, path, body).await;
    assert!(
        (200..300).contains(&status),
        "{method} {path}: {status} {reply}"
    );
    reply
}

async fn console_operation(client: &reqwest::Client, operation: &Value) {
    let path = format!(
        "/admin/v1/databases/{}/operations/{}",
        operation["database_id"].as_str().unwrap(),
        operation["id"].as_str().unwrap()
    );
    wait_for("the operation", Duration::from_secs(120), async || {
        let current = console(client, "GET", &path, None).await;
        assert_ne!(current["status"], "failed", "{current}");
        current["status"] == "succeeded"
    })
    .await;
}

async fn cluster_sql(client: &reqwest::Client, sql: &str) -> Value {
    let body = serde_json::json!({ "target": "cluster", "sql": sql });
    console(client, "POST", "/console/sql", Some(body)).await
}

/// `make try` as its user sees it: everything below goes through the console.
#[tokio::test]
#[ignore = "requires Docker"]
async fn e2e_admin_console_full_stack() {
    let _stack = Stack::up(&[
        "console",
        "pg-replica-1",
        "pg-replica-2",
        "pgcluster-1",
        "pgcluster-2",
        "pgcluster-3",
    ]);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(180))
        .build()
        .unwrap();

    let page = client.get(CONSOLE_URL).send().await.unwrap();
    assert!(page.text().await.unwrap().contains("pgcluster console"));

    wait_for("the stack", Duration::from_secs(120), async || {
        let overview = console(&client, "GET", "/console/overview", None).await;
        let hosts = console(&client, "GET", "/admin/v1/hosts", None).await;
        let online = hosts["items"]
            .as_array()
            .is_some_and(|h| h.iter().any(|h| h["status"] == "online"));
        overview["admin"] == true && overview["cluster_primary"].is_string() && online
    })
    .await;

    // A page on another site cannot drive it.
    let refused = client
        .post(format!("{CONSOLE_URL}/admin/v1/hosts"))
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), 403);
    let rebound = client
        .get(CONSOLE_URL)
        .header("host", "evil.example.com")
        .send()
        .await
        .unwrap();
    assert_eq!(rebound.status(), 403);

    // Create a database, and check it as its owner over verified TLS.
    let secret = console(&client, "POST", "/console/password", None).await;
    let operation = console(
        &client,
        "POST",
        "/admin/v1/databases",
        Some(serde_json::json!({
            "name": "from_console",
            "version": "18",
            "resources": { "vcpu": 1, "memory_gb": 1, "storage_gb": 10 },
            "owner": { "username": "app_owner", "scram_verifier": secret["scram_verifier"] },
            "allowed_cidrs": ["0.0.0.0/0"],
            "extensions": ["vector"],
        })),
    )
    .await;
    console_operation(&client, &operation).await;
    let id = operation["database_id"].as_str().unwrap();
    let database = console(&client, "GET", &format!("/admin/v1/databases/{id}"), None).await;
    let endpoint = &database["endpoint"];
    let result = console(
        &client,
        "POST",
        "/console/sql",
        Some(serde_json::json!({
            "target": "database",
            "host": endpoint["host"],
            "port": endpoint["port"],
            "database": endpoint["database"],
            "username": "app_owner",
            "password": secret["password"],
            "ca_cert_pem": endpoint["ca_cert_pem"],
            "sql": "SELECT current_user, extname FROM pg_extension WHERE extname = 'vector'",
        })),
    )
    .await;
    assert_eq!(
        result["columns"],
        serde_json::json!(["current_user", "extname"])
    );
    assert_eq!(
        result["rows"][0],
        serde_json::json!(["app_owner", "vector"])
    );

    // A statement that fails reports why.
    let (status, reply) = console_call(
        &client,
        "POST",
        "/console/sql",
        Some(serde_json::json!({ "target": "cluster", "sql": "SELECT nonsense" })),
    )
    .await;
    assert_eq!(status, 400);
    assert!(reply["error"]["message"]
        .as_str()
        .unwrap()
        .contains("nonsense"));

    // Switch over, and see writes follow the new primary.
    wait_for(
        "standbys to be seeded",
        Duration::from_secs(120),
        async || {
            let nodes = console(&client, "GET", "/cluster/api/nodes", None).await;
            let nodes = nodes["nodes"].as_array().unwrap();
            let replicas = nodes
                .iter()
                .filter(|n| n["role"].as_str().unwrap().eq_ignore_ascii_case("replica"))
                .count();
            replicas >= 2
        },
    )
    .await;
    cluster_sql(&client, "CREATE TABLE probe (v text)").await;
    cluster_sql(&client, "INSERT INTO probe VALUES ('before')").await;
    let before = console(&client, "GET", "/cluster/api/status", None).await;
    let old_primary = before["primary_node_id"].as_str().unwrap().to_string();
    let nodes = console(&client, "GET", "/cluster/api/nodes", None).await;
    let target = nodes["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["node_id"].as_str().unwrap().to_string())
        .find(|n| *n != old_primary)
        .unwrap();
    console(
        &client,
        "POST",
        "/cluster/api/switchover",
        Some(serde_json::json!({ "target_node_id": target })),
    )
    .await;
    wait_for("the new primary", Duration::from_secs(60), async || {
        let status = console(&client, "GET", "/cluster/api/status", None).await;
        status["primary_node_id"] == target.as_str()
    })
    .await;
    let mut written = false;
    let deadline = Instant::now() + Duration::from_secs(60);
    while !written && Instant::now() < deadline {
        let (status, _) = console_call(
            &client,
            "POST",
            "/console/sql",
            Some(serde_json::json!({
                "target": "cluster",
                "sql": "INSERT INTO probe VALUES ('after')",
            })),
        )
        .await;
        written = status == 200;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(written, "writes should reach the new primary");
    let rows = cluster_sql(&client, "SELECT v FROM probe ORDER BY v DESC").await;
    assert_eq!(rows["rows"], serde_json::json!([["before"], ["after"]]));

    // Replication mode changes online.
    console(
        &client,
        "POST",
        "/cluster/api/replication/mode",
        Some(serde_json::json!({ "mode": "sync" })),
    )
    .await;
    let replication = console(&client, "GET", "/cluster/api/replication", None).await;
    assert_eq!(replication["mode"], "sync");

    // A backup appears, and the WAL archive has no gaps.
    let label = "from-console";
    console(
        &client,
        "POST",
        "/cluster/api/backups",
        Some(serde_json::json!({ "label": label })),
    )
    .await;
    wait_for("the backup", Duration::from_secs(120), async || {
        let backups = console(&client, "GET", "/cluster/api/backups", None).await;
        backups["backups"]
            .as_array()
            .unwrap()
            .iter()
            .any(|b| b["label"] == label)
    })
    .await;
    console(&client, "GET", "/cluster/api/backups/verify", None).await;

    // Delete the database again.
    let operation = console(
        &client,
        "DELETE",
        &format!("/admin/v1/databases/{id}"),
        None,
    )
    .await;
    console_operation(&client, &operation).await;
    let (status, _) =
        console_call(&client, "GET", &format!("/admin/v1/databases/{id}"), None).await;
    assert_eq!(status, 404);
}
