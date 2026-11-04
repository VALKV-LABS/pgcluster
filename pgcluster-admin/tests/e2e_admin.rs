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
const API_KEY: &str = "dev-admin-key-for-the-local-stack-only";
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
        .args([
            "compose",
            "-p",
            PROJECT,
            "--profile",
            "systemd",
            "--profile",
            "ha",
        ])
        .args(["-f", "docker/e2e-compose.yml"])
        .args(["-f", "docker/admin-compose.yml"])
        .args(["-f", "docker/ha-compose.yml"]);
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

async fn api_call(
    client: &reqwest::Client,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> (u16, Value) {
    let mut request = client
        .request(method.parse().unwrap(), format!("{ADMIN_URL}{path}"))
        .bearer_auth(API_KEY);
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    (status, response.json().await.unwrap_or(Value::Null))
}

async fn api(client: &reqwest::Client, method: &str, path: &str, body: Option<Value>) -> Value {
    let (status, reply) = api_call(client, method, path, body).await;
    assert!(
        (200..300).contains(&status),
        "{method} {path}: {status} {reply}"
    );
    reply
}

/// Every test's databases keep their backups in one directory the hosts share.
async fn add_repository(client: &reqwest::Client) -> String {
    let body = serde_json::json!({ "name": "main", "type": "posix", "path": "/backups" });
    let reply = api(client, "POST", "/v1/backup-repositories", Some(body)).await;
    assert_eq!(reply["status"], "verified", "{reply}");
    reply["id"].as_str().unwrap().to_string()
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
    // Unnamed: the proxy pools by transaction, so a named statement can be gone
    // from the backend a later query lands on.
    sqlx::query_scalar::<_, String>(sql)
        .persistent(false)
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
    add_repository(&client).await;

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

/// The certificate each TLS port presents, by fingerprint. Connections made here
/// have no client certificate, which gets far enough to see the server's.
fn served_fingerprint(port: u16, starttls: bool) -> String {
    let starttls = if starttls { "-starttls postgres" } else { "" };
    sh(
        HOST,
        &format!(
            "openssl s_client -connect 127.0.0.1:{port} {starttls} </dev/null 2>/dev/null | \\
             openssl x509 -noout -fingerprint -sha256"
        ),
    )
}

/// A renewed certificate reaches every process, and no client notices.
#[tokio::test]
#[ignore = "requires Docker"]
async fn e2e_admin_certificate_renewal_keeps_clients_connected() {
    let _stack = Stack::up(&[HOST]);
    let client = admin_client();
    wait_online(&client, HOST).await;
    add_repository(&client).await;
    let output = cli(HOST, "database create orders --allow 0.0.0.0/0 --wait");
    let created = parse_created(&output);
    let id = created.id.as_str();
    let endpoint = endpoint(&client, id).await;
    let mut owner = endpoint.wait_connectable(&created.password).await;

    let file = format!("{}/tls/node.crt", instance_dir(id));
    let on_disk = || {
        sh(
            HOST,
            &format!("openssl x509 -in {file} -noout -fingerprint -sha256"),
        )
    };
    let old = on_disk();
    for (port, starttls) in [(20000, true), (20002, true), (20003, false), (20004, false)] {
        assert_eq!(served_fingerprint(port, starttls), old, "port {port}");
    }
    let days = sh(
        HOST,
        &format!(
            "openssl x509 -in {file} -noout -checkend {} >/dev/null && echo long || echo bounded",
            91 * 86_400
        ),
    );
    assert_eq!(days, "bounded", "certificates must not outlive 90 days");
    let postmaster = postmaster_pid(HOST, id);
    let pgcluster_pid = sh(HOST, &format!("cat {}/pgcluster.pid", instance_dir(id)));
    let agent_pid = sh(HOST, &format!("cat {}/vk-agent.pid", instance_dir(id)));

    // Bring the expiry close, and the reconciler renews it by itself.
    sh(
        "admin-db",
        "psql -U postgres -d pgcluster_admin -c \
         \"UPDATE nodes SET cert_not_after = now() + interval '1 day'\"",
    );
    wait_for("the new certificate", Duration::from_secs(60), async || {
        let new = on_disk();
        new != old
            && [(20000, true), (20002, true), (20003, false), (20004, false)]
                .iter()
                .all(|(port, starttls)| served_fingerprint(*port, *starttls) == new)
    })
    .await;

    // Nothing restarted, and the connection opened before it all still works.
    assert_eq!(postmaster_pid(HOST, id), postmaster);
    assert_eq!(
        sh(HOST, &format!("cat {}/pgcluster.pid", instance_dir(id))),
        pgcluster_pid
    );
    assert_eq!(
        sh(HOST, &format!("cat {}/vk-agent.pid", instance_dir(id))),
        agent_pid
    );
    assert_eq!(
        scalar(&mut owner, "SELECT 'still here'").await,
        "still here"
    );
    endpoint.connect(&created.password).await.unwrap();

    // Past the renewal, heartbeats and failover checks carry on, so nothing
    // fences. Self-fencing would have stopped Postgres by now.
    tokio::time::sleep(Duration::from_secs(45)).await;
    assert_eq!(postmaster_pid(HOST, id), postmaster);
    assert_eq!(
        scalar(&mut owner, "SELECT 'still here'").await,
        "still here"
    );
    let database = api(&client, "GET", &format!("/v1/databases/{id}"), None).await;
    assert_eq!(
        database["status"], "active",
        "{}",
        database["status_reason"]
    );
    assert_eq!(database["generation"], 3, "create takes two generations");
}

/// Every supported major gets every extension.
#[tokio::test]
#[ignore = "requires Docker"]
async fn e2e_admin_each_postgres_version() {
    let _stack = Stack::up(&[HOST]);
    let client = admin_client();
    wait_online(&client, HOST).await;
    add_repository(&client).await;
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
    add_repository(&client).await;

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

    // Register a repository, which a host checks.
    let repository = console(
        &client,
        "POST",
        "/admin/v1/backup-repositories",
        Some(serde_json::json!({ "name": "main", "type": "posix", "path": "/backups" })),
    )
    .await;

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
            "backup": { "repository_id": repository["id"], "retention_days": 7 },
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

// ── Three nodes ───────────────────────────────────────────────────────────────

const HA_HOSTS: [&str; 5] = [
    "host-ha-1",
    "host-ha-2",
    "host-ha-3",
    "host-ha-4",
    "host-ha-5",
];

struct HaDatabase {
    id: String,
    password: String,
}

async fn wait_ha_hosts(client: &reqwest::Client) {
    for host in HA_HOSTS {
        wait_online(client, host).await;
    }
}

/// A three-node database, and how to reach it.
async fn create_ha(client: &reqwest::Client, name: &str, mode: &str) -> HaDatabase {
    let hosts = "--host host-ha-1 --host host-ha-2 --host host-ha-3";
    let output = cli(
        "host-ha-1",
        &format!("database create {name} --ha --mode {mode} {hosts} --allow 0.0.0.0/0 --wait"),
    );
    let created = parse_created(&output);
    let database = api(
        client,
        "GET",
        &format!("/v1/databases/{}", created.id),
        None,
    )
    .await;
    assert_eq!(database["status"], "active", "{database}");
    assert_eq!(database["replication_mode"], mode);
    assert_eq!(database["endpoints"].as_array().unwrap().len(), 3);
    HaDatabase {
        id: created.id,
        password: created.password,
    }
}

fn node_endpoint(database: &Value, index: usize) -> Endpoint {
    let address = &database["endpoints"][index];
    Endpoint {
        host: address["host"].as_str().unwrap().into(),
        port: address["port"].as_u64().unwrap() as u16,
        database: database["endpoint"]["database"].as_str().unwrap().into(),
        ca: database["endpoint"]["ca_cert_pem"].as_str().unwrap().into(),
    }
}

impl HaDatabase {
    async fn get(&self, client: &reqwest::Client) -> Value {
        api(client, "GET", &format!("/v1/databases/{}", self.id), None).await
    }

    /// `(node, host name)` of each node.
    async fn nodes(&self, client: &reqwest::Client) -> Vec<(String, String)> {
        let nodes = api(
            client,
            "GET",
            &format!("/v1/databases/{}/nodes", self.id),
            None,
        )
        .await;
        nodes["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| {
                (
                    n["node"].as_str().unwrap().to_string(),
                    n["host_name"].as_str().unwrap().to_string(),
                )
            })
            .collect()
    }

    async fn primary(&self, client: &reqwest::Client) -> Option<String> {
        self.get(client).await["primary"]
            .as_str()
            .map(str::to_string)
    }

    /// Wait for the leader to report a primary other than `not`.
    async fn wait_new_primary(&self, client: &reqwest::Client, not: &str) -> String {
        let mut found = None;
        wait_for("a new primary", Duration::from_secs(120), async || {
            found = self.primary(client).await.filter(|p| p != not);
            found.is_some()
        })
        .await;
        found.unwrap()
    }

    async fn host_of(&self, client: &reqwest::Client, node: &str) -> String {
        self.nodes(client)
            .await
            .into_iter()
            .find(|(name, _)| name == node)
            .unwrap()
            .1
    }

    /// Every node streaming again, seen from whichever proxy answers.
    async fn wait_healthy(&self, client: &reqwest::Client, nodes: usize) {
        let database = self.get(client).await;
        let endpoints = database["endpoints"].as_array().unwrap().len();
        wait_for(
            "every standby to stream",
            Duration::from_secs(180),
            async || {
                for index in 0..endpoints {
                    let endpoint = node_endpoint(&self.get(client).await, index);
                    let Ok(mut connection) = endpoint.connect(&self.password).await else {
                        continue;
                    };
                    let streaming: Result<i64, _> =
                        sqlx::query_scalar("SELECT count(*) FROM pg_stat_replication")
                            .persistent(false)
                            .fetch_one(&mut connection)
                            .await;
                    connection.close().await.ok();
                    if streaming.is_ok_and(|n| n as usize == nodes - 1) {
                        return true;
                    }
                }
                false
            },
        )
        .await;
    }

    async fn run(&self, client: &reqwest::Client, method: &str, path: &str, body: Option<Value>) {
        let path = format!("/v1/databases/{}/{path}", self.id);
        let operation = api(client, method, &path, body).await;
        self.wait_operation(client, &operation).await;
    }

    async fn wait_operation(&self, client: &reqwest::Client, operation: &Value) {
        let path = format!(
            "/v1/databases/{}/operations/{}",
            operation["database_id"].as_str().unwrap(),
            operation["id"].as_str().unwrap()
        );
        wait_for("the operation", Duration::from_secs(300), async || {
            let current = api(client, "GET", &path, None).await;
            assert_ne!(current["status"], "failed", "{current}");
            current["status"] == "succeeded"
        })
        .await;
    }

    /// Status follows a sweep every few seconds, so it settles a little after
    /// the cluster does.
    async fn wait_active(&self, client: &reqwest::Client) {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let last = self.get(client).await;
            if last["status"] == "active" {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "still {}: {}",
                last["status"],
                last["status_reason"]
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn latest_logs(&self, client: &reqwest::Client) -> String {
        let path = format!("/v1/databases/{}/operations", self.id);
        let operations = api(client, "GET", &path, None).await;
        let mut items = operations["items"].as_array().unwrap().clone();
        items.sort_by_key(|o| o["created_at"].as_str().unwrap().to_string());
        let latest = items.last().unwrap()["id"].as_str().unwrap().to_string();
        let detail = api(client, "GET", &format!("{path}/{latest}"), None).await;
        detail["logs"].to_string()
    }
}

/// Clients writing as fast as they can, each commit numbered, so a test can ask
/// which were lost after a failover.
struct Load {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    acked: std::sync::Arc<std::sync::Mutex<Vec<i64>>>,
    failed: std::sync::Arc<std::sync::atomic::AtomicU64>,
    task: tokio::task::JoinHandle<()>,
}

impl Load {
    async fn start(database: &HaDatabase, client: &reqwest::Client, table: &str) -> Self {
        let endpoints: Vec<Endpoint> = {
            let snapshot = database.get(client).await;
            (0..snapshot["endpoints"].as_array().unwrap().len())
                .map(|index| node_endpoint(&snapshot, index))
                .collect()
        };
        let mut owner = endpoints[0].wait_connectable(&database.password).await;
        sqlx::query(&format!("CREATE TABLE IF NOT EXISTS {table} (n bigint)"))
            .persistent(false)
            .execute(&mut owner)
            .await
            .unwrap();
        owner.close().await.ok();

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let acked = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let failed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (flag, record, misses) = (stop.clone(), acked.clone(), failed.clone());
        let password = database.password.clone();
        let insert = format!("INSERT INTO {table} VALUES ($1)");
        let task = tokio::spawn(async move {
            use std::sync::atomic::Ordering;
            let mut n = 0i64;
            let mut connection: Option<PgConnection> = None;
            let mut next_endpoint = 0;
            while !flag.load(Ordering::SeqCst) {
                if connection.is_none() {
                    let endpoint = &endpoints[next_endpoint % endpoints.len()];
                    next_endpoint += 1;
                    let attempt =
                        tokio::time::timeout(Duration::from_secs(3), endpoint.connect(&password))
                            .await;
                    match attempt {
                        Ok(Ok(open)) => connection = Some(open),
                        _ => {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    }
                }
                n += 1;
                let write = sqlx::query(&insert)
                    .bind(n)
                    .persistent(false)
                    .execute(connection.as_mut().unwrap());
                match tokio::time::timeout(Duration::from_secs(5), write).await {
                    Ok(Ok(_)) => record.lock().unwrap().push(n),
                    _ => {
                        misses.fetch_add(1, Ordering::SeqCst);
                        connection = None;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
        Self {
            stop,
            acked,
            failed,
            task,
        }
    }

    fn acked_count(&self) -> usize {
        self.acked.lock().unwrap().len()
    }

    /// Let more commits land, to show the database is serving.
    async fn wait_progress(&self) {
        let start = self.acked_count();
        wait_for("clients to commit", Duration::from_secs(120), async || {
            self.acked_count() >= start + 20
        })
        .await;
    }

    async fn stop(self) -> (Vec<i64>, u64) {
        use std::sync::atomic::Ordering;
        self.stop.store(true, Ordering::SeqCst);
        self.task.await.unwrap();
        let acked = self.acked.lock().unwrap().clone();
        (acked, self.failed.load(Ordering::SeqCst))
    }
}

/// Which of `acked` are gone.
async fn lost(
    database: &HaDatabase,
    client: &reqwest::Client,
    table: &str,
    acked: &[i64],
) -> usize {
    let snapshot = database.get(client).await;
    let count = snapshot["endpoints"].as_array().unwrap().len();
    for index in 0..count {
        let endpoint = node_endpoint(&snapshot, index);
        let Ok(mut owner) = endpoint.connect(&database.password).await else {
            continue;
        };
        let stored: Result<Vec<i64>, _> = sqlx::query_scalar(&format!("SELECT n FROM {table}"))
            .persistent(false)
            .fetch_all(&mut owner)
            .await;
        owner.close().await.ok();
        if let Ok(stored) = stored {
            let stored: std::collections::HashSet<i64> = stored.into_iter().collect();
            return acked.iter().filter(|n| !stored.contains(n)).count();
        }
    }
    panic!("no node answered");
}

fn cut_off(host: &str, peers: &[&str]) {
    let peers = peers.join(", ");
    sh(
        host,
        &format!(
            "nft add table inet partition && \
             nft 'add chain inet partition input {{ type filter hook input priority -10; }}' && \
             nft 'add chain inet partition output {{ type filter hook output priority -10; }}' && \
             nft 'add rule inet partition input ip saddr {{ {peers} }} drop' && \
             nft 'add rule inet partition output ip daddr {{ {peers} }} drop'"
        ),
    );
}

fn heal(host: &str) {
    sh(host, "nft delete table inet partition");
}

fn address_of(host: &str) -> String {
    let number: u32 = host.rsplit('-').next().unwrap().parse().unwrap();
    format!("172.31.77.{}", 10 + number)
}

fn can_reach(from: &str, address: &str, port: u16) -> bool {
    exec_on(
        from,
        &format!("timeout 3 bash -c '</dev/tcp/{address}/{port}'"),
    )
    .status
    .success()
}

/// Nodes land apart, peers reach each other, and nobody else reaches them.
#[tokio::test]
#[ignore = "requires Docker"]
async fn e2e_admin_three_nodes_are_placed_apart_and_fenced_off() {
    let _stack = Stack::up(&HA_HOSTS);
    let client = admin_client();
    wait_ha_hosts(&client).await;
    add_repository(&client).await;

    // host-ha-5 shares a rack with host-ha-1, and placement must not use both.
    let database = create_ha_placed("orders").await;
    let hosts: Vec<String> = database
        .nodes(&client)
        .await
        .into_iter()
        .map(|n| n.1)
        .collect();
    assert_eq!(hosts.len(), 3);
    assert!(
        !(hosts.iter().any(|h| h == "host-ha-1") && hosts.iter().any(|h| h == "host-ha-5")),
        "{hosts:?} share a failure domain"
    );

    // Refused outright when named: the two share a rack.
    let (status, body) = api_call(
        &client,
        "POST",
        "/v1/databases",
        Some(serde_json::json!({
            "name": "too_close",
            "version": "18",
            "ha": true,
            "hosts": ["host-ha-1", "host-ha-5", "host-ha-2"],
            "resources": { "vcpu": 1, "memory_gb": 1, "storage_gb": 10 },
            "owner": { "username": "app_owner", "scram_verifier":
                "SCRAM-SHA-256$4096:c2FsdA==$c3RvcmVk:c2VydmVy" },
            "backup": { "repository_id": "none", "retention_days": 7 },
        })),
    )
    .await;
    assert_eq!(status, 422, "{body}");

    let outside = HA_HOSTS
        .iter()
        .find(|h| !hosts.iter().any(|used| used == *h))
        .unwrap();
    let nodes = api(
        &client,
        "GET",
        &format!("/v1/databases/{}/nodes", database.id),
        None,
    )
    .await;
    let (first, second) = (&nodes["items"][0], &nodes["items"][1]);
    let first_host = first["host_name"].as_str().unwrap();
    let second_host = second["host_name"].as_str().unwrap();
    let target = address_of(second_host);
    let base = second["port_base"].as_u64().unwrap() as u16;
    for offset in [2, 3, 4] {
        assert!(
            can_reach(first_host, &target, base + offset),
            "a peer must reach port {offset}"
        );
        assert!(
            !can_reach(outside, &target, base + offset),
            "{outside} must not reach port {offset}"
        );
    }

    // The allowlist reaches the host firewall, and an empty one closes the proxy
    // to everyone, with connected clients left alone.
    let endpoint = node_endpoint(&database.get(&client).await, 1);
    let mut owner = endpoint.wait_connectable(&database.password).await;
    assert!(can_reach(outside, &target, base));
    database
        .run(
            &client,
            "PUT",
            "allowlist",
            Some(serde_json::json!({ "allowed_cidrs": [] })),
        )
        .await;
    assert!(!can_reach(outside, &target, base));
    assert_eq!(
        scalar(&mut owner, "SELECT 'still here'").await,
        "still here"
    );

    // Deleting takes the rules with it.
    let delete = api(
        &client,
        "DELETE",
        &format!("/v1/databases/{}", database.id),
        None,
    )
    .await;
    database.wait_operation(&client, &delete).await;
    let tables = sh(second_host, "nft list tables");
    assert!(!tables.contains("pgcluster_"), "{tables}");
}

async fn create_ha_placed(name: &str) -> HaDatabase {
    let output = cli(
        "host-ha-1",
        &format!("database create {name} --ha --allow 0.0.0.0/0 --wait"),
    );
    let created = parse_created(&output);
    HaDatabase {
        id: created.id,
        password: created.password,
    }
}

/// Mode, switchover and restart, each through the API, under client load.
#[tokio::test]
#[ignore = "requires Docker"]
async fn e2e_admin_day_to_day_operations_run_online() {
    let _stack = Stack::up(&HA_HOSTS);
    let client = admin_client();
    wait_ha_hosts(&client).await;
    add_repository(&client).await;
    let database = create_ha(&client, "orders", "async").await;
    database.wait_healthy(&client, 3).await;
    let load = Load::start(&database, &client, "load_ops").await;
    load.wait_progress().await;

    // The mode changes with no restart.
    let endpoint = node_endpoint(&database.get(&client).await, 0);
    let path = format!("/v1/databases/{}", database.id);
    let operation = api(
        &client,
        "PATCH",
        &path,
        Some(serde_json::json!({ "replication_mode": "sync" })),
    )
    .await;
    database.wait_operation(&client, &operation).await;
    wait_for(
        "synchronous replication",
        Duration::from_secs(30),
        async || {
            let Ok(mut owner) = endpoint.connect(&database.password).await else {
                return false;
            };
            let names = scalar(&mut owner, "SHOW synchronous_standby_names").await;
            owner.close().await.ok();
            names.starts_with("ANY 1")
        },
    )
    .await;
    assert_eq!(database.get(&client).await["replication_mode"], "sync");

    // Switchover, as an operation with logs.
    let before = database.primary(&client).await.unwrap();
    database
        .run(&client, "POST", "switchover", Some(serde_json::json!({})))
        .await;
    let after = database.wait_new_primary(&client, &before).await;
    assert_ne!(before, after);
    assert!(
        database.latest_logs(&client).await.contains("switchover"),
        "{}",
        database.latest_logs(&client).await
    );
    load.wait_progress().await;

    // Rolling restart.
    database.wait_healthy(&client, 3).await;
    database.run(&client, "POST", "restart", None).await;
    assert!(database
        .latest_logs(&client)
        .await
        .contains("rolling_restart"));
    load.wait_progress().await;

    // A preload extension installs by rolling restart, not by stopping all three.
    database.wait_healthy(&client, 3).await;
    database
        .run(
            &client,
            "PUT",
            "extensions",
            Some(serde_json::json!({ "extensions": ["pg_cron", "vector"] })),
        )
        .await;
    load.wait_progress().await;
    let (acked, _) = load.stop().await;
    assert_eq!(lost(&database, &client, "load_ops", &acked).await, 0);
    let mut owner = endpoint.wait_connectable(&database.password).await;
    let cron = scalar(
        &mut owner,
        "SELECT extname::text FROM pg_extension WHERE extname = 'pg_cron'",
    )
    .await;
    assert_eq!(cron, "pg_cron");
}

/// What the spec asks of three nodes: survive primary loss, a partition and a
/// replacement, in every replication mode.
#[tokio::test]
#[ignore = "requires Docker"]
async fn e2e_admin_ha_suite_in_every_mode() {
    let _stack = Stack::up(&HA_HOSTS);
    let client = admin_client();
    wait_ha_hosts(&client).await;
    add_repository(&client).await;

    for mode in ["async", "sync", "sync_strict"] {
        eprintln!("── {mode}");
        let name = format!("ha_{mode}");
        let database = create_ha(&client, &name, mode).await;
        database.wait_healthy(&client, 3).await;
        let safe = mode != "async";

        // Primary loss.
        let table = "load_loss";
        let load = Load::start(&database, &client, table).await;
        load.wait_progress().await;
        let old = database.primary(&client).await.unwrap();
        let old_host = database.host_of(&client, &old).await;
        compose(&["kill", &old_host]);
        let new = database.wait_new_primary(&client, &old).await;
        assert_ne!(old, new);
        load.wait_progress().await;
        let (acked, _) = load.stop().await;
        let missing = lost(&database, &client, table, &acked).await;
        if safe {
            assert_eq!(missing, 0, "{mode} lost {missing} commits");
        }

        // It rejoins with no help, and diverged writes do not come back.
        compose(&["start", &old_host]);
        wait_online(&client, &old_host).await;
        database.wait_healthy(&client, 3).await;

        // A network partition: the primary alone, on the wrong side.
        let table = "load_split";
        let load = Load::start(&database, &client, table).await;
        load.wait_progress().await;
        let old = database.primary(&client).await.unwrap();
        let old_host = database.host_of(&client, &old).await;
        let peers: Vec<String> = database
            .nodes(&client)
            .await
            .into_iter()
            .filter(|(_, host)| *host != old_host)
            .map(|(_, host)| address_of(&host))
            .collect();
        let peers: Vec<&str> = peers.iter().map(String::as_str).collect();
        cut_off(&old_host, &peers);
        let new = database.wait_new_primary(&client, &old).await;
        load.wait_progress().await;

        // The cut-off proxy takes no writes, so there is one history.
        let snapshot = database.get(&client).await;
        let isolated_index = database
            .nodes(&client)
            .await
            .iter()
            .position(|(name, _)| *name == old)
            .unwrap();
        let isolated = node_endpoint(&snapshot, isolated_index);
        if let Ok(mut connection) = isolated.connect(&database.password).await {
            let write = sqlx::query("INSERT INTO load_split VALUES (-1)")
                .persistent(false)
                .execute(&mut connection);
            let outcome = tokio::time::timeout(Duration::from_secs(5), write).await;
            assert!(
                !matches!(outcome, Ok(Ok(_))),
                "an isolated proxy accepted a write"
            );
        }
        heal(&old_host);
        let (acked, _) = load.stop().await;
        database.wait_healthy(&client, 3).await;
        let missing = lost(&database, &client, table, &acked).await;
        if safe {
            assert_eq!(missing, 0, "{mode} lost {missing} commits");
        }
        assert_eq!(
            database.primary(&client).await.as_deref(),
            Some(new.as_str())
        );

        // Replace a standby under load, through a proxy that stays.
        let primary = database.primary(&client).await.unwrap();
        let nodes = database.nodes(&client).await;
        let (leaving, _) = nodes.iter().find(|(name, _)| *name != primary).unwrap();
        let spare = "host-ha-4";
        let table = "load_swap";
        let load = Load::start(&database, &client, table).await;
        load.wait_progress().await;
        let operation = api(
            &client,
            "POST",
            &format!("/v1/databases/{}/nodes/{leaving}/replace", database.id),
            Some(serde_json::json!({ "host_id": spare })),
        )
        .await;
        database.wait_operation(&client, &operation).await;
        load.wait_progress().await;
        let (acked, _) = load.stop().await;
        assert_eq!(lost(&database, &client, table, &acked).await, 0);
        let nodes = database.nodes(&client).await;
        assert_eq!(nodes.len(), 3, "{nodes:?}");
        assert!(nodes.iter().any(|(_, host)| host == spare), "{nodes:?}");
        assert!(!nodes.iter().any(|(name, _)| name == leaving), "{nodes:?}");
        database.wait_healthy(&client, 3).await;
        database.wait_active(&client).await;

        // Hosts are needed for the next mode.
        let delete = api(
            &client,
            "DELETE",
            &format!("/v1/databases/{}", database.id),
            None,
        )
        .await;
        database.wait_operation(&client, &delete).await;
    }

    // Tooling can follow all of that without polling a database.
    let events = api(&client, "GET", "/v1/events?limit=500", None).await;
    let kinds: Vec<&str> = events["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["type"].as_str().unwrap())
        .collect();
    for kind in [
        "node_offline",
        "node_online",
        "failover_completed",
        "backup_completed",
    ] {
        assert!(kinds.contains(&kind), "no {kind} in {kinds:?}");
    }
}

// ── Backups ───────────────────────────────────────────────────────────────────

/// A point-in-time restore lands on exactly the commits before the target, with
/// the source untouched, in a database that works on its own.
#[tokio::test]
#[ignore = "requires Docker"]
async fn e2e_admin_restore_to_a_point_in_time() {
    let _stack = Stack::up(&[HOST]);
    let client = admin_client();
    wait_online(&client, HOST).await;
    let repository = add_repository(&client).await;

    let output = cli(HOST, "database create orders --allow 0.0.0.0/0 --wait");
    let created = parse_created(&output);
    let id = created.id.as_str();
    let source = endpoint(&client, id).await;
    let mut owner = source.wait_connectable(&created.password).await;

    // The first backup existed before the database opened, and the leader reports it.
    wait_for_backups(&client, id, 1).await;
    let (status, _) = api_call(
        &client,
        "DELETE",
        &format!("/v1/backup-repositories/{repository}"),
        None,
    )
    .await;
    assert_eq!(status, 409, "a repository in use stays");

    // Backups are encrypted on the node before they reach the repository.
    let stored = sh(
        HOST,
        &format!("find /backups/{id} -type f -name '*.br' -o -type f -name '*.zst' | head -1"),
    );
    if !stored.is_empty() {
        let header = sh(HOST, &format!("head -c 4 {stored} | od -An -tx1"));
        assert!(!header.contains("28 b5 2f fd"), "a plain zstd frame");
    }

    sqlx::query("CREATE TABLE t (v text)")
        .persistent(false)
        .execute(&mut owner)
        .await
        .unwrap();
    sqlx::query("INSERT INTO t VALUES ('before')")
        .persistent(false)
        .execute(&mut owner)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let target = scalar(
        &mut owner,
        "SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')",
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    sqlx::query("INSERT INTO t VALUES ('after')")
        .persistent(false)
        .execute(&mut owner)
        .await
        .unwrap();
    sqlx::query("SELECT pg_switch_wal()")
        .persistent(false)
        .execute(&mut owner)
        .await
        .ok();

    // The window reaches the target only once the archive really does.
    let window_path = format!("/v1/databases/{id}/restore-window");
    wait_for(
        "the window to pass the target",
        Duration::from_secs(180),
        async || {
            let (status, window) = api_call(&client, "GET", &window_path, None).await;
            status == 200
                && window["latest"].as_str().is_some_and(|latest| {
                    chrono::DateTime::parse_from_rfc3339(latest).unwrap()
                        >= chrono::DateTime::parse_from_rfc3339(&target).unwrap()
                })
        },
    )
    .await;

    // Outside the window is refused.
    let (status, _) = api_call(
        &client,
        "POST",
        &format!("/v1/databases/{id}/restore"),
        Some(serde_json::json!({ "name": "too_old", "target_time": "2001-01-01T00:00:00Z" })),
    )
    .await;
    assert_eq!(status, 422);

    let operation = api(
        &client,
        "POST",
        &format!("/v1/databases/{id}/restore"),
        Some(serde_json::json!({ "name": "orders_copy", "target_time": target })),
    )
    .await;
    let restored_id = operation["database_id"].as_str().unwrap().to_string();
    let restored_db = HaDatabase {
        id: restored_id.clone(),
        password: created.password.clone(),
    };
    restored_db.wait_operation(&client, &operation).await;

    let restored = endpoint(&client, &restored_id).await;
    assert_eq!(restored.database, "orders_copy");
    let mut copy = restored.wait_connectable(&created.password).await;
    let rows = scalar(&mut copy, "SELECT string_agg(v, ',' ORDER BY v) FROM t").await;
    assert_eq!(rows, "before", "only what committed before the target");

    // It takes writes, and they stay out of the source.
    sqlx::query("INSERT INTO t VALUES ('copy')")
        .persistent(false)
        .execute(&mut copy)
        .await
        .unwrap();
    let source_rows = scalar(&mut owner, "SELECT string_agg(v, ',' ORDER BY v) FROM t").await;
    assert_eq!(source_rows, "after,before");

    // It has a backup of its own, and archives to its own place.
    wait_for_backups(&client, &restored_id, 1).await;
    let files = sh(HOST, &format!("ls /backups/{restored_id}"));
    assert!(!files.is_empty());

    // An on-demand backup appears in the list.
    let operation = api(&client, "POST", &format!("/v1/databases/{id}/backup"), None).await;
    restored_db.wait_operation(&client, &operation).await;
    wait_for("the new backup", Duration::from_secs(60), async || {
        let backups = api(&client, "GET", &format!("/v1/databases/{id}/backups"), None).await;
        backups["items"].as_array().unwrap().len() == 2
    })
    .await;
}

async fn wait_for_backups(client: &reqwest::Client, id: &str, count: usize) {
    let path = format!("/v1/databases/{id}/backups");
    wait_for("the backups", Duration::from_secs(60), async || {
        let backups = api(client, "GET", &path, None).await;
        backups["items"].as_array().unwrap().len() == count
    })
    .await;
}

/// With a repository that cannot be written to, a database never opens.
#[tokio::test]
#[ignore = "requires Docker"]
async fn e2e_admin_a_bad_repository_is_refused() {
    let _stack = Stack::up(&[HOST]);
    let client = admin_client();
    wait_online(&client, HOST).await;
    let (status, body) = api_call(
        &client,
        "POST",
        "/v1/backup-repositories",
        Some(serde_json::json!({ "name": "nope", "type": "posix", "path": "/proc/nowhere" })),
    )
    .await;
    assert_eq!(status, 422, "{body}");
    let list = api(&client, "GET", "/v1/backup-repositories", None).await;
    assert_eq!(list["items"], serde_json::json!([]));
}

// ── Secret rotation ───────────────────────────────────────────────────────────

/// Each secret rotates under client load, and the old one stops working.
#[tokio::test]
#[ignore = "requires Docker"]
async fn e2e_admin_secrets_rotate_under_load() {
    let _stack = Stack::up(&HA_HOSTS);
    let client = admin_client();
    wait_ha_hosts(&client).await;
    add_repository(&client).await;
    let database = create_ha(&client, "orders", "sync").await;
    database.wait_healthy(&client, 3).await;
    let load = Load::start(&database, &client, "load_keys").await;
    load.wait_progress().await;

    let host = database.nodes(&client).await[0].1.clone();
    let instance = format!(
        "/var/lib/pgcluster/instances/{}-{}",
        database.id,
        database.nodes(&client).await[0].0
    );
    let read = |file: &str| sh(&host, &format!("cat {instance}/{file}"));
    let port_base = api(
        &client,
        "GET",
        &format!("/v1/databases/{}/nodes", database.id),
        None,
    )
    .await["items"][0]["port_base"]
        .as_u64()
        .unwrap() as u16;

    // Internal passwords.
    let old_env = read("instance.env");
    let old_config = read("pgcluster.toml");
    let old_key = old_config
        .lines()
        .find(|l| l.contains("api_keys"))
        .unwrap()
        .to_string();
    let old_key = old_key.split('"').nth(1).unwrap().to_string();
    let postmaster = sh(&host, &format!("head -1 {instance}/pgdata/postmaster.pid"));
    database
        .run(&client, "POST", "rotate-passwords", None)
        .await;
    assert_ne!(read("instance.env"), old_env);
    assert_eq!(
        sh(&host, &format!("head -1 {instance}/pgdata/postmaster.pid")),
        postmaster,
        "no restart"
    );
    let refused = sh(
        &host,
        &format!(
            "curl -s -o /dev/null -w '%{{http_code}}' -X POST -H 'Authorization: Bearer {old_key}' \
             -H 'Content-Type: application/json' -d '{{}}' http://127.0.0.1:{}/api/backups",
            port_base + 7
        ),
    );
    assert_eq!(refused, "401", "the old API key must stop working");
    let old_super = sh(
        &host,
        &format!(
            "grep -o 'postgres_password = \"[^\"]*\"' {instance}/vk-agent.toml | cut -d'\"' -f2"
        ),
    );
    assert!(!old_super.is_empty());
    load.wait_progress().await;
    database.wait_healthy(&client, 3).await;

    // The database's CA. Clients that take the bundle from the API stay current.
    let before = database.get(&client).await;
    let old_ca = before["endpoint"]["ca_cert_pem"]
        .as_str()
        .unwrap()
        .to_string();
    let endpoint_before = node_endpoint(&before, 0);
    database.run(&client, "POST", "rotate-ca", None).await;
    let after = database.get(&client).await;
    assert_ne!(after["endpoint"]["ca_cert_pem"].as_str().unwrap(), old_ca);
    load.wait_progress().await;
    let (acked, failed) = load.stop().await;
    assert_eq!(failed, 0, "no commit may fail");
    assert_eq!(lost(&database, &client, "load_keys", &acked).await, 0);
    assert_eq!(
        sh(&host, &format!("head -1 {instance}/pgdata/postmaster.pid")),
        postmaster,
        "no restart"
    );
    // A client holding the old CA can no longer verify the proxy.
    assert!(endpoint_before.connect(&database.password).await.is_err());
    let fresh = node_endpoint(&after, 0);
    fresh.wait_connectable(&database.password).await;

    // The admin CA: hosts move with no restart, and old certificates stop working.
    let old_bundle = api(&client, "GET", "/v1/admin-ca", None).await["bundle_pem"]
        .as_str()
        .unwrap()
        .to_string();
    let old_host_cert = sh(&host, "cat /var/lib/vk-agent-host/host.crt");
    let old_host_key = sh(&host, "cat /var/lib/vk-agent-host/host.key");
    let status = api(&client, "POST", "/v1/admin-ca/rotate", None).await;
    let both = status["bundle_pem"].as_str().unwrap().to_string();
    let rotating = reqwest::Client::builder()
        .use_rustls_tls()
        .tls_built_in_root_certs(false)
        .add_root_certificate(reqwest::Certificate::from_pem(both.as_bytes()).unwrap())
        .add_root_certificate(
            reqwest::Certificate::from_pem(
                both.split_inclusive("-----END CERTIFICATE-----\n")
                    .last()
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap(),
        )
        .build()
        .unwrap();
    let mut new_ca = String::new();
    wait_for(
        "the rotation to end",
        Duration::from_secs(180),
        async || {
            let Ok(response) = rotating
                .get(format!("{ADMIN_URL}/v1/admin-ca"))
                .bearer_auth(API_KEY)
                .send()
                .await
            else {
                return false;
            };
            let status: Value = response.json().await.unwrap_or(Value::Null);
            new_ca = status["bundle_pem"].as_str().unwrap_or("").to_string();
            status["phase"].is_null() && !new_ca.is_empty()
        },
    )
    .await;
    assert_ne!(new_ca, old_bundle);
    let renewed = sh(&host, "cat /var/lib/vk-agent-host/host.crt");
    assert_ne!(renewed, old_host_cert, "hosts move to the new CA");
    assert_eq!(
        sh(&host, "cat /var/lib/vk-agent-host/ca.crt").trim(),
        new_ca.trim(),
        "hosts drop the old CA"
    );
    wait_online_with(&rotating, &host).await;

    // A certificate from the old CA no longer gets through.
    let identity = format!("{old_host_cert}\n{old_host_key}\n");
    let impostor = reqwest::Client::builder()
        .use_rustls_tls()
        .tls_built_in_root_certs(false)
        .add_root_certificate(reqwest::Certificate::from_pem(new_ca.as_bytes()).unwrap())
        .identity(reqwest::Identity::from_pem(identity.as_bytes()).unwrap())
        .build()
        .unwrap();
    let attempt = impostor
        .get("https://localhost:18444/agent/v1/desired-state?wait_seconds=0")
        .send()
        .await;
    assert!(attempt.is_err(), "{attempt:?}");

    // The key that encrypts instance secrets: a restart seals everything anew.
    let old_row = sh(
        "admin-db",
        "psql -U postgres -d pgcluster_admin -Atc 'SELECT encode(secrets, $$hex$$) FROM databases LIMIT 1'",
    );
    const NEW_KEY: &str = "CQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQk=";
    let override_file = root().join("docker/rotate-secret-key.yml");
    std::fs::write(
        &override_file,
        format!(
            "services:\n  pgcluster-admin:\n    environment:\n      PGCLUSTER_ADMIN_SECRET_KEY: \"{NEW_KEY}\"\n      PGCLUSTER_ADMIN_OLD_SECRET_KEY: \"nvQ1nHZ3hYkq0m4oGQ3p3k5u2u8Zt7y6vX1cR0eJm9U=\"\n"
        ),
    )
    .unwrap();
    let up = compose_command(&[
        "-f",
        "docker/rotate-secret-key.yml",
        "up",
        "-d",
        "--no-deps",
        "--no-build",
        "--wait",
        "pgcluster-admin",
    ])
    .output()
    .unwrap();
    std::fs::remove_file(&override_file).ok();
    assert!(
        up.status.success(),
        "{}",
        String::from_utf8_lossy(&up.stderr)
    );
    let new_row = sh(
        "admin-db",
        "psql -U postgres -d pgcluster_admin -Atc 'SELECT encode(secrets, $$hex$$) FROM databases LIMIT 1'",
    );
    assert_ne!(old_row, new_row);
    // Desired state still decrypts, so a change reaches hosts. A new client,
    // because the old one's connections were to the process that stopped.
    let rotating = reqwest::Client::builder()
        .use_rustls_tls()
        .tls_built_in_root_certs(false)
        .add_root_certificate(reqwest::Certificate::from_pem(new_ca.as_bytes()).unwrap())
        .build()
        .unwrap();
    let path = format!("/v1/databases/{}/backup", database.id);
    let response = rotating
        .post(format!("{ADMIN_URL}{path}"))
        .bearer_auth(API_KEY)
        .send()
        .await
        .unwrap();
    let operation: Value = response.json().await.unwrap();
    wait_for(
        "a backup after the rekey",
        Duration::from_secs(120),
        async || {
            let path = format!(
                "/v1/databases/{}/operations/{}",
                database.id,
                operation["id"].as_str().unwrap()
            );
            let current: Value = rotating
                .get(format!("{ADMIN_URL}{path}"))
                .bearer_auth(API_KEY)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_ne!(current["status"], "failed", "{current}");
            current["status"] == "succeeded"
        },
    )
    .await;
}

async fn wait_online_with(client: &reqwest::Client, name: &str) {
    wait_for("the host to report", Duration::from_secs(60), async || {
        let Ok(response) = client
            .get(format!("{ADMIN_URL}/v1/hosts"))
            .bearer_auth(API_KEY)
            .send()
            .await
        else {
            return false;
        };
        let hosts: Value = response.json().await.unwrap_or(Value::Null);
        hosts["items"].as_array().is_some_and(|items| {
            items
                .iter()
                .any(|h| h["name"] == name && h["status"] == "online")
        })
    })
    .await;
}

async fn host_status(client: &reqwest::Client, name: &str) -> Value {
    let hosts = api(client, "GET", "/v1/hosts?limit=500", None).await;
    hosts["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|h| h["name"] == name)
        .cloned()
        .unwrap()
}

/// Stages 4 and 5 under client load: a resize, a host drain, and a
/// cross-region replica promoted into a database of its own, with no commit
/// lost along the way.
#[tokio::test]
#[ignore = "requires Docker"]
async fn e2e_admin_resize_drain_and_replica_under_load() {
    let _stack = Stack::up(&HA_HOSTS);
    let client = admin_client();
    wait_ha_hosts(&client).await;
    add_repository(&client).await;
    let database = create_ha(&client, "orders", "async").await;
    database.wait_healthy(&client, 3).await;
    let load = Load::start(&database, &client, "load_day_two").await;
    load.wait_progress().await;
    let path = format!("/v1/databases/{}", database.id);

    // New memory takes a rolling restart, and Postgres comes back with it.
    let resize = serde_json::json!({
        "resources": { "vcpu": 1, "memory_gb": 2, "storage_gb": 12 }
    });
    let operation = api(&client, "PATCH", &path, Some(resize)).await;
    assert_eq!(operation["kind"], "resize", "{operation}");
    database.wait_operation(&client, &operation).await;
    load.wait_progress().await;
    let endpoint = node_endpoint(&database.get(&client).await, 0);
    let mut owner = endpoint.wait_connectable(&database.password).await;
    assert_eq!(scalar(&mut owner, "SHOW shared_buffers").await, "512MB");
    owner.close().await.ok();

    // Drain the host in rack b. The only free rack is d.
    database.wait_healthy(&client, 3).await;
    let drained = host_status(&client, "host-ha-2").await;
    let drain = format!("/v1/hosts/{}/drain", drained["id"].as_str().unwrap());
    api(&client, "POST", &drain, None).await;
    wait_for("the drain", Duration::from_secs(600), async || {
        let host = host_status(&client, "host-ha-2").await;
        assert!(host["drain_error"].is_null(), "{host}");
        host["status"] == "drained"
    })
    .await;
    let hosts: Vec<String> = database
        .nodes(&client)
        .await
        .into_iter()
        .map(|(_, host)| host)
        .collect();
    assert!(hosts.contains(&"host-ha-4".to_string()), "{hosts:?}");
    assert!(!hosts.contains(&"host-ha-2".to_string()), "{hosts:?}");
    load.wait_progress().await;

    // Take it back, and put a replica there, outside the source's racks.
    api(&client, "DELETE", &drain, None).await;
    database.wait_healthy(&client, 3).await;
    let replicas = format!("{path}/replicas");
    let operation = api(
        &client,
        "POST",
        &replicas,
        Some(serde_json::json!({ "name": "orders_dr" })),
    )
    .await;
    database.wait_operation(&client, &operation).await;
    let replica_id = operation["database_id"].as_str().unwrap().to_string();
    let listed = api(&client, "GET", &replicas, None).await;
    assert_eq!(listed["items"][0]["host_name"], "host-ha-2", "{listed}");
    load.wait_progress().await;

    let (acked, _) = load.stop().await;
    assert_eq!(lost(&database, &client, "load_day_two", &acked).await, 0);

    // Promoted, it serves on its own, with what the source had.
    let operation = api(
        &client,
        "POST",
        &format!("{replicas}/{replica_id}/promote"),
        None,
    )
    .await;
    database.wait_operation(&client, &operation).await;
    let promoted = HaDatabase {
        id: replica_id,
        password: database.password.clone(),
    };
    let snapshot = promoted.get(&client).await;
    assert_eq!(snapshot["status"], "active", "{snapshot}");
    assert!(snapshot["replica_of"].is_null());
    let mut owner = node_endpoint(&snapshot, 0)
        .wait_connectable(&promoted.password)
        .await;
    let copied: i64 = scalar(&mut owner, "SELECT count(*) FROM load_day_two")
        .await
        .parse()
        .unwrap();
    assert!(copied > 0, "the replica had the source's rows");
    sqlx::query("INSERT INTO load_day_two VALUES (-1)")
        .persistent(false)
        .execute(&mut owner)
        .await
        .expect("the promoted database takes writes");
}
