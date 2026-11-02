//! Commands that talk to `pgcluster-admin`.

use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Subcommand};
use rand::RngCore;
use serde_json::{json, Value};

use crate::proxy::scram::make_verifier;

const SCRAM_ITERATIONS: u32 = 4096;
const WAIT_POLL: Duration = Duration::from_secs(2);
const WAIT_TIMEOUT: Duration = Duration::from_secs(660);

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
}

#[derive(Args)]
pub struct DatabaseArgs {
    #[command(flatten)]
    connection: AdminConnection,
    #[command(subcommand)]
    command: DatabaseCommand,
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
    /// Host to place the database on, by ID or name
    #[arg(long)]
    host: Option<String>,
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
fn generate_password() -> (String, String) {
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
    }
}

async fn create_database(client: &AdminClient, args: CreateArgs) -> Result<()> {
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
    });
    if let Some(host) = args.host {
        body["hosts"] = json!([host]);
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
