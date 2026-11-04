//! Commands that talk to `pgcluster-admin`.

use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Subcommand};
use rand::RngCore;
use serde_json::{json, Value};

use crate::proxy::scram::make_verifier;

const SCRAM_ITERATIONS: u32 = 4096;
const WAIT_POLL: Duration = Duration::from_secs(2);
/// Seeding and backing up take as long as the data does.
const WAIT_TIMEOUT: Duration = Duration::from_secs(6 * 3600 + 60);

#[derive(Args, Clone)]
pub struct AdminConnection {
    /// URL of the admin API
    #[arg(long, env = "PGCLUSTER_ADMIN_URL", global = true)]
    pub admin_url: Option<String>,
    /// API key
    #[arg(
        long,
        env = "PGCLUSTER_ADMIN_KEY",
        hide_env_values = true,
        global = true
    )]
    pub api_key: Option<String>,
    /// CA certificate that signed the admin API's certificate
    #[arg(long, env = "PGCLUSTER_ADMIN_CA", global = true)]
    pub ca_cert: Option<String>,
}

#[derive(Args)]
pub struct HostArgs {
    #[command(flatten)]
    connection: AdminConnection,
    #[command(subcommand)]
    command: HostCommand,
}

#[derive(Subcommand)]
enum HostCommand {
    /// Add a host and print the token that enrols it, once
    Add {
        name: String,
        #[arg(long)]
        failure_domain: Option<String>,
    },
    /// List hosts
    List,
    /// Show one host
    Show { id: String },
    /// Remove a host that runs no instances
    Delete { id: String },
    /// Move every node off a host, with no downtime, and place nothing new there
    Drain { id: String },
    /// Let nodes land on a host again
    Undrain { id: String },
}

#[derive(Args)]
pub struct DatabaseArgs {
    #[command(flatten)]
    connection: AdminConnection,
    #[command(subcommand)]
    command: DatabaseCommand,
}

#[derive(Args)]
pub struct RepositoryArgs {
    #[command(flatten)]
    connection: AdminConnection,
    #[command(subcommand)]
    command: RepositoryCommand,
}

#[derive(Subcommand)]
enum RepositoryCommand {
    /// Register a repository in a directory every host can reach
    AddPosix { name: String, path: String },
    /// Register an S3-compatible repository
    AddS3 {
        name: String,
        bucket: String,
        #[arg(long)]
        prefix: Option<String>,
        #[arg(long)]
        region: Option<String>,
        /// For stores other than AWS
        #[arg(long)]
        endpoint: Option<String>,
        #[arg(long)]
        path_style: bool,
        /// Left out, hosts use their instance role
        #[arg(long, env = "AWS_ACCESS_KEY_ID", hide_env_values = true)]
        access_key_id: Option<String>,
        #[arg(long, env = "AWS_SECRET_ACCESS_KEY", hide_env_values = true)]
        secret_access_key: Option<String>,
    },
    /// List repositories
    List,
    /// Remove a repository no database uses
    Delete { id: String },
}

#[derive(Args)]
pub struct EventArgs {
    #[command(flatten)]
    connection: AdminConnection,
    /// Show only what came after this cursor
    #[arg(long)]
    after: Option<String>,
}

#[derive(Args)]
pub struct AuditArgs {
    #[command(flatten)]
    connection: AdminConnection,
    /// Show only changes made with this key
    #[arg(long)]
    key: Option<String>,
    /// Show only changes to this database
    #[arg(long)]
    database: Option<String>,
}

#[derive(Args)]
pub struct AdminCaArgs {
    #[command(flatten)]
    connection: AdminConnection,
    #[command(subcommand)]
    command: AdminCaCommand,
}

#[derive(Subcommand)]
enum AdminCaCommand {
    /// Show what to trust, and how far a rotation has got
    Show,
    /// Start replacing the admin CA
    Rotate,
}

#[derive(Subcommand)]
enum DatabaseCommand {
    /// Create a database, generating the owner's password on this machine
    Create(CreateArgs),
    /// List databases
    List,
    /// Show one database
    Show { id: String },
    /// Delete a database and its data
    Delete {
        id: String,
        /// Wait for it to finish
        #[arg(long)]
        wait: bool,
    },
    /// Replace the owner's password, generating it on this machine
    RotatePassword {
        id: String,
        #[arg(long)]
        wait: bool,
    },
    /// Replace the whole allowlist. With no blocks, nothing can connect
    Allow {
        id: String,
        cidrs: Vec<String>,
        #[arg(long)]
        wait: bool,
    },
    /// Replace the whole list of extensions
    Extensions {
        id: String,
        extensions: Vec<String>,
        #[arg(long)]
        wait: bool,
    },
    /// Change the replication mode online: async, sync or sync_strict
    Mode {
        id: String,
        mode: String,
        #[arg(long)]
        wait: bool,
    },
    /// Change how many days of restore window to keep
    Retention {
        id: String,
        days: u32,
        #[arg(long)]
        wait: bool,
    },
    /// Hand the primary role to a standby
    Switchover {
        id: String,
        /// The node to hand over to. Left out, the most current standby
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        wait: bool,
    },
    /// Restart Postgres one node at a time
    Restart {
        id: String,
        #[arg(long)]
        wait: bool,
    },
    /// Replace a node, bringing up the new one first
    Replace {
        id: String,
        node: String,
        /// Host to move to, by ID or name
        #[arg(long)]
        host: String,
        #[arg(long)]
        wait: bool,
    },
    /// List a database's nodes
    Nodes { id: String },
    /// Take a backup now
    Backup {
        id: String,
        #[arg(long)]
        wait: bool,
    },
    /// List backups
    Backups { id: String },
    /// Show the earliest and latest times a restore can reach
    RestoreWindow { id: String },
    /// Restore to a point in time, as a new database
    Restore {
        id: String,
        /// Name of the new database
        name: String,
        /// RFC 3339 time, within the restore window
        #[arg(long)]
        time: String,
        #[arg(long)]
        wait: bool,
    },
    /// Replace the database's CA and node certificates
    RotateCa {
        id: String,
        #[arg(long)]
        wait: bool,
    },
    /// Replace the database's internal passwords
    RotatePasswords {
        id: String,
        #[arg(long)]
        wait: bool,
    },
    /// Give every node new resources. Storage only grows
    Resize {
        id: String,
        #[arg(long)]
        vcpu: f64,
        #[arg(long)]
        memory_gb: f64,
        #[arg(long)]
        storage_gb: f64,
        #[arg(long)]
        wait: bool,
    },
    /// Restart onto a newer minor release the hosts have installed
    Upgrade {
        id: String,
        /// A release such as 18.2. Left out, the newest every host has
        #[arg(long)]
        version: Option<String>,
        #[arg(long)]
        wait: bool,
    },
    /// Have every node's proxy present this certificate and key
    Certificate {
        id: String,
        /// PEM file with the certificate, and any intermediates after it
        #[arg(long)]
        cert: String,
        /// PEM file with the private key
        #[arg(long)]
        key: String,
        #[arg(long)]
        wait: bool,
    },
    /// Add a cross-region replica of a three-node database
    ReplicaAdd {
        id: String,
        /// Name of the replica, which it keeps if promoted
        name: String,
        /// Host to run on, by ID or name. Left out, one outside the source's domains
        #[arg(long)]
        host: Option<String>,
        #[arg(long)]
        wait: bool,
    },
    /// List a database's cross-region replicas
    Replicas { id: String },
    /// Detach a replica into a database of its own
    ReplicaPromote {
        id: String,
        replica: String,
        #[arg(long)]
        wait: bool,
    },
    /// Delete a replica
    ReplicaDelete {
        id: String,
        replica: String,
        #[arg(long)]
        wait: bool,
    },
}

#[derive(Args)]
struct CreateArgs {
    name: String,
    /// Postgres major version
    #[arg(long, default_value = "18")]
    version: String,
    /// Name of the owner role
    #[arg(long, default_value = "app_owner")]
    owner: String,
    #[arg(long, default_value_t = 1.0)]
    vcpu: f64,
    #[arg(long, default_value_t = 1.0)]
    memory_gb: f64,
    #[arg(long, default_value_t = 10.0)]
    storage_gb: f64,
    /// Client CIDR block allowed to connect. Repeat for more. With none, nothing can
    #[arg(long = "allow")]
    allowed_cidrs: Vec<String>,
    /// Extension to install. Repeat for more
    #[arg(long = "extension")]
    extensions: Vec<String>,
    /// Three nodes with automatic failover
    #[arg(long)]
    ha: bool,
    /// Replication mode, with --ha: async, sync or sync_strict
    #[arg(long)]
    mode: Option<String>,
    /// Host to place a node on, by ID or name. Repeat for each node
    #[arg(long = "host")]
    hosts: Vec<String>,
    /// Backup repository. With only one registered, that one
    #[arg(long)]
    repository: Option<String>,
    /// Days of restore window to keep
    #[arg(long, default_value_t = 7)]
    retention_days: u32,
    /// Opaque label for grouping databases
    #[arg(long)]
    workspace: Option<String>,
    /// Wait until the database is ready
    #[arg(long)]
    wait: bool,
}

struct AdminClient {
    base: String,
    key: String,
    http: reqwest::Client,
}

impl AdminClient {
    fn new(connection: &AdminConnection) -> Result<Self> {
        let base = connection
            .admin_url
            .clone()
            .context("set --admin-url or PGCLUSTER_ADMIN_URL")?;
        let key = connection
            .api_key
            .clone()
            .context("set --api-key or PGCLUSTER_ADMIN_KEY")?;
        let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(30));
        if let Some(path) = &connection.ca_cert {
            let pem = std::fs::read(path).with_context(|| format!("read {path}"))?;
            builder = builder
                .tls_built_in_root_certs(false)
                .add_root_certificate(reqwest::Certificate::from_pem(&pem)?);
        }
        Ok(Self {
            base: base.trim_end_matches('/').to_string(),
            key,
            http: builder.build().context("build HTTP client")?,
        })
    }

    async fn send(&self, method: &str, path: &str, body: Option<Value>) -> Result<Value> {
        let method = method.parse().context("HTTP method")?;
        let mut request = self
            .http
            .request(method, format!("{}{path}", self.base))
            .bearer_auth(&self.key);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.context("reach pgcluster-admin")?;
        let status = response.status();
        let text = response.text().await.context("read the response")?;
        let value: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        if !status.is_success() {
            let message = value["error"]["message"].as_str().unwrap_or(&text);
            bail!("pgcluster-admin answered {status}: {message}");
        }
        Ok(value)
    }

    /// Follow an operation until it ends.
    async fn wait(&self, operation: &Value) -> Result<()> {
        let path = format!(
            "/v1/databases/{}/operations/{}",
            text(operation, "database_id"),
            text(operation, "id")
        );
        let deadline = Instant::now() + WAIT_TIMEOUT;
        let mut logged = 0;
        loop {
            let current = self.send("GET", &path, None).await?;
            let logs = current["logs"].as_array().cloned().unwrap_or_default();
            for entry in logs.iter().skip(logged) {
                println!("  {}", text(entry, "message"));
            }
            logged = logs.len();
            match text(&current, "status") {
                "succeeded" => return Ok(()),
                "failed" => bail!("the operation failed: {}", text(&current, "error")),
                _ if Instant::now() >= deadline => bail!("timed out waiting for the operation"),
                _ => tokio::time::sleep(WAIT_POLL).await,
            }
        }
    }

    async fn show(&self, path: &str) -> Result<()> {
        pretty(&self.send("GET", path, None).await?)
    }

    /// Start an operation that takes no arguments.
    async fn act(&self, id: &str, action: &str, wait: bool) -> Result<()> {
        let path = format!("/v1/databases/{id}/{action}");
        let operation = self.send("POST", &path, None).await?;
        self.finish(&operation, wait).await
    }

    async fn finish(&self, operation: &Value, wait: bool) -> Result<()> {
        if wait {
            self.wait(operation).await?;
            println!("done");
        } else {
            println!("operation {}", text(operation, "id"));
        }
        Ok(())
    }
}

fn text<'a>(value: &'a Value, key: &str) -> &'a str {
    value[key].as_str().unwrap_or("")
}

fn pretty(value: &Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

/// A password and its verifier. The password stays on this machine.
pub fn generate_password() -> (String, String) {
    let mut rng = rand::thread_rng();
    let mut secret = [0u8; 24];
    let mut salt = [0u8; 16];
    rng.fill_bytes(&mut secret);
    rng.fill_bytes(&mut salt);
    let password = hex::encode(secret);
    let verifier = make_verifier(&password, &salt, SCRAM_ITERATIONS);
    (password, verifier)
}

pub async fn run_host(args: HostArgs) -> Result<()> {
    let client = AdminClient::new(&args.connection)?;
    match args.command {
        HostCommand::Add {
            name,
            failure_domain,
        } => {
            let mut body = json!({ "name": name });
            if let Some(domain) = failure_domain {
                body["failure_domain"] = json!(domain);
            }
            let created = client.send("POST", "/v1/hosts", Some(body)).await?;
            println!("host {}", text(&created["host"], "id"));
            println!("expires {}", text(&created, "expires_at"));
            println!();
            println!("Save this token on the host. It works once, and is not shown again:");
            println!("{}", text(&created, "enrollment_token"));
            Ok(())
        }
        HostCommand::List => pretty(&client.send("GET", "/v1/hosts", None).await?),
        HostCommand::Show { id } => {
            pretty(&client.send("GET", &format!("/v1/hosts/{id}"), None).await?)
        }
        HostCommand::Delete { id } => {
            client
                .send("DELETE", &format!("/v1/hosts/{id}"), None)
                .await?;
            println!("deleted");
            Ok(())
        }
        HostCommand::Drain { id } => {
            let host = client
                .send("POST", &format!("/v1/hosts/{id}/drain"), None)
                .await?;
            println!(
                "{}. Follow it with pgcluster host show {id}",
                text(&host, "status")
            );
            Ok(())
        }
        HostCommand::Undrain { id } => {
            let host = client
                .send("DELETE", &format!("/v1/hosts/{id}/drain"), None)
                .await?;
            println!("{}", text(&host, "status"));
            Ok(())
        }
    }
}

/// The repository a command names, or the only one there is.
async fn choose_repository(client: &AdminClient, named: Option<String>) -> Result<String> {
    if let Some(id) = named {
        return Ok(id);
    }
    let list = client.send("GET", "/v1/backup-repositories", None).await?;
    match list["items"].as_array().map(Vec::as_slice) {
        Some([only]) => Ok(text(only, "id").to_string()),
        Some([]) => bail!("register a repository first, with pgcluster repository add-posix"),
        _ => bail!("several repositories exist, so name one with --repository"),
    }
}

async fn create_database(client: &AdminClient, args: CreateArgs) -> Result<()> {
    let repository = choose_repository(client, args.repository).await?;
    let (password, verifier) = generate_password();
    let mut body = json!({
        "name": args.name,
        "version": args.version,
        "resources": {
            "vcpu": args.vcpu,
            "memory_gb": args.memory_gb,
            "storage_gb": args.storage_gb,
        },
        "owner": { "username": args.owner, "scram_verifier": verifier },
        "allowed_cidrs": args.allowed_cidrs,
        "extensions": args.extensions,
        "ha": args.ha,
        "backup": { "repository_id": repository, "retention_days": args.retention_days },
    });
    if !args.hosts.is_empty() {
        body["hosts"] = json!(args.hosts);
    }
    if let Some(mode) = args.mode {
        body["replication_mode"] = json!(mode);
    }
    if let Some(workspace) = args.workspace {
        body["workspace"] = json!(workspace);
    }
    let operation = client.send("POST", "/v1/databases", Some(body)).await?;
    let id = text(&operation, "database_id").to_string();

    // Printed before waiting: it exists nowhere else, and a wait can fail.
    println!("database {id}");
    println!("owner {}", args.owner);
    println!("password {password}");
    println!("Save this password. It is not stored and is not shown again.");
    if !args.wait {
        println!("operation {}", text(&operation, "id"));
        return Ok(());
    }
    client.wait(&operation).await?;
    let database = client
        .send("GET", &format!("/v1/databases/{id}"), None)
        .await?;
    let endpoint = &database["endpoint"];
    let host = text(endpoint, "host");
    let port = endpoint["port"]
        .as_i64()
        .ok_or_else(|| anyhow!("the database has no endpoint"))?;
    println!(
        "psql \"postgres://{}@{host}:{port}/{}?sslmode=verify-full&sslrootcert=ca.crt\"",
        args.owner, args.name
    );
    std::fs::write("ca.crt", text(endpoint, "ca_cert_pem")).context("write ca.crt")?;
    println!("wrote ca.crt");
    Ok(())
}

pub async fn run_database(args: DatabaseArgs) -> Result<()> {
    let client = AdminClient::new(&args.connection)?;
    match args.command {
        DatabaseCommand::Create(create) => create_database(&client, create).await,
        DatabaseCommand::List => pretty(&client.send("GET", "/v1/databases", None).await?),
        DatabaseCommand::Show { id } => pretty(
            &client
                .send("GET", &format!("/v1/databases/{id}"), None)
                .await?,
        ),
        DatabaseCommand::Delete { id, wait } => {
            let path = format!("/v1/databases/{id}");
            let operation = client.send("DELETE", &path, None).await?;
            client.finish(&operation, wait).await
        }
        DatabaseCommand::RotatePassword { id, wait } => {
            let (password, verifier) = generate_password();
            let operation = client
                .send(
                    "POST",
                    &format!("/v1/databases/{id}/owner-password"),
                    Some(json!({ "scram_verifier": verifier })),
                )
                .await?;
            println!("password {password}");
            println!("Save this password. It is not stored and is not shown again.");
            client.finish(&operation, wait).await
        }
        DatabaseCommand::Allow { id, cidrs, wait } => {
            let operation = client
                .send(
                    "PUT",
                    &format!("/v1/databases/{id}/allowlist"),
                    Some(json!({ "allowed_cidrs": cidrs })),
                )
                .await?;
            client.finish(&operation, wait).await
        }
        DatabaseCommand::Extensions {
            id,
            extensions,
            wait,
        } => {
            let operation = client
                .send(
                    "PUT",
                    &format!("/v1/databases/{id}/extensions"),
                    Some(json!({ "extensions": extensions })),
                )
                .await?;
            client.finish(&operation, wait).await
        }
        DatabaseCommand::Mode { id, mode, wait } => {
            let body = json!({ "replication_mode": mode });
            let path = format!("/v1/databases/{id}");
            let operation = client.send("PATCH", &path, Some(body)).await?;
            client.finish(&operation, wait).await
        }
        DatabaseCommand::Retention { id, days, wait } => {
            let body = json!({ "retention_days": days });
            let path = format!("/v1/databases/{id}");
            let operation = client.send("PATCH", &path, Some(body)).await?;
            client.finish(&operation, wait).await
        }
        DatabaseCommand::Switchover { id, target, wait } => {
            let path = format!("/v1/databases/{id}/switchover");
            let body = json!({ "target": target });
            let operation = client.send("POST", &path, Some(body)).await?;
            client.finish(&operation, wait).await
        }
        DatabaseCommand::Replace {
            id,
            node,
            host,
            wait,
        } => {
            let path = format!("/v1/databases/{id}/nodes/{node}/replace");
            let body = json!({ "host_id": host });
            let operation = client.send("POST", &path, Some(body)).await?;
            client.finish(&operation, wait).await
        }
        DatabaseCommand::Restore {
            id,
            name,
            time,
            wait,
        } => {
            let path = format!("/v1/databases/{id}/restore");
            let body = json!({ "name": name, "target_time": time });
            let operation = client.send("POST", &path, Some(body)).await?;
            println!("database {}", text(&operation, "database_id"));
            client.finish(&operation, wait).await
        }
        DatabaseCommand::Restart { id, wait } => client.act(&id, "restart", wait).await,
        DatabaseCommand::Backup { id, wait } => client.act(&id, "backup", wait).await,
        DatabaseCommand::RotateCa { id, wait } => client.act(&id, "rotate-ca", wait).await,
        DatabaseCommand::RotatePasswords { id, wait } => {
            client.act(&id, "rotate-passwords", wait).await
        }
        DatabaseCommand::Resize {
            id,
            vcpu,
            memory_gb,
            storage_gb,
            wait,
        } => {
            let body = json!({
                "resources": { "vcpu": vcpu, "memory_gb": memory_gb, "storage_gb": storage_gb }
            });
            let path = format!("/v1/databases/{id}");
            let operation = client.send("PATCH", &path, Some(body)).await?;
            client.finish(&operation, wait).await
        }
        DatabaseCommand::Upgrade { id, version, wait } => {
            let path = format!("/v1/databases/{id}/upgrade");
            let operation = client
                .send("POST", &path, Some(json!({ "version": version })))
                .await?;
            client.finish(&operation, wait).await
        }
        DatabaseCommand::Certificate {
            id,
            cert,
            key,
            wait,
        } => {
            let read =
                |path: &str| std::fs::read_to_string(path).with_context(|| format!("read {path}"));
            let body = json!({ "certificate_pem": read(&cert)?, "private_key_pem": read(&key)? });
            let path = format!("/v1/databases/{id}/certificate");
            let operation = client.send("PUT", &path, Some(body)).await?;
            client.finish(&operation, wait).await
        }
        DatabaseCommand::ReplicaAdd {
            id,
            name,
            host,
            wait,
        } => {
            let path = format!("/v1/databases/{id}/replicas");
            let body = json!({ "name": name, "host_id": host });
            let operation = client.send("POST", &path, Some(body)).await?;
            println!("replica {}", text(&operation, "database_id"));
            client.finish(&operation, wait).await
        }
        DatabaseCommand::Replicas { id } => {
            client.show(&format!("/v1/databases/{id}/replicas")).await
        }
        DatabaseCommand::ReplicaPromote { id, replica, wait } => {
            let path = format!("/v1/databases/{id}/replicas/{replica}/promote");
            let operation = client.send("POST", &path, None).await?;
            client.finish(&operation, wait).await
        }
        DatabaseCommand::ReplicaDelete { id, replica, wait } => {
            let path = format!("/v1/databases/{id}/replicas/{replica}");
            let operation = client.send("DELETE", &path, None).await?;
            client.finish(&operation, wait).await
        }
        DatabaseCommand::Nodes { id } => client.show(&format!("/v1/databases/{id}/nodes")).await,
        DatabaseCommand::Backups { id } => {
            client.show(&format!("/v1/databases/{id}/backups")).await
        }
        DatabaseCommand::RestoreWindow { id } => {
            client
                .show(&format!("/v1/databases/{id}/restore-window"))
                .await
        }
    }
}

pub async fn run_repository(args: RepositoryArgs) -> Result<()> {
    let client = AdminClient::new(&args.connection)?;
    let path = "/v1/backup-repositories";
    match args.command {
        RepositoryCommand::AddPosix { name, path: dir } => {
            let body = json!({ "name": name, "type": "posix", "path": dir });
            pretty(&client.send("POST", path, Some(body)).await?)
        }
        RepositoryCommand::AddS3 {
            name,
            bucket,
            prefix,
            region,
            endpoint,
            path_style,
            access_key_id,
            secret_access_key,
        } => {
            let body = json!({
                "name": name,
                "type": "s3",
                "bucket": bucket,
                "prefix": prefix,
                "region": region,
                "endpoint": endpoint,
                "path_style": path_style,
                "access_key_id": access_key_id,
                "secret_access_key": secret_access_key,
            });
            pretty(&client.send("POST", path, Some(body)).await?)
        }
        RepositoryCommand::List => client.show(path).await,
        RepositoryCommand::Delete { id } => {
            client.send("DELETE", &format!("{path}/{id}"), None).await?;
            println!("deleted");
            Ok(())
        }
    }
}

pub async fn run_events(args: EventArgs) -> Result<()> {
    let client = AdminClient::new(&args.connection)?;
    match args.after {
        Some(after) => client.show(&format!("/v1/events?after={after}")).await,
        None => client.show("/v1/events").await,
    }
}

pub async fn run_audit(args: AuditArgs) -> Result<()> {
    let client = AdminClient::new(&args.connection)?;
    let mut filters = Vec::new();
    if let Some(key) = &args.key {
        filters.push(format!("key={key}"));
    }
    if let Some(database) = &args.database {
        filters.push(format!("database_id={database}"));
    }
    client
        .show(&format!("/v1/audit-log?{}", filters.join("&")))
        .await
}

pub async fn run_admin_ca(args: AdminCaArgs) -> Result<()> {
    let client = AdminClient::new(&args.connection)?;
    match args.command {
        AdminCaCommand::Show => client.show("/v1/admin-ca").await,
        AdminCaCommand::Rotate => pretty(&client.send("POST", "/v1/admin-ca/rotate", None).await?),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::scram::Verifier;

    #[test]
    fn a_generated_password_has_a_verifier_postgres_can_store() {
        let (password, verifier) = generate_password();
        assert!(Verifier::parse(&verifier).is_some(), "{verifier}");
        assert!(!verifier.contains(&password));
    }

    #[test]
    fn two_passwords_differ_in_password_and_salt() {
        let (first, first_verifier) = generate_password();
        let (second, second_verifier) = generate_password();
        assert_ne!(first, second);
        assert_ne!(first_verifier, second_verifier);
    }

    #[test]
    fn a_missing_url_or_key_says_which() {
        let connection = AdminConnection {
            admin_url: None,
            api_key: Some("k".into()),
            ca_cert: None,
        };
        let error = AdminClient::new(&connection).err().unwrap().to_string();
        assert!(error.contains("PGCLUSTER_ADMIN_URL"), "{error}");
    }
}
