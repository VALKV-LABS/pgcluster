pub mod admin;
pub mod certs;
pub mod client;
pub mod config;
pub mod console;
pub mod failover;
pub mod init;
pub mod node;
pub mod output;
pub mod replication;
pub mod server;
pub mod status;
pub mod switchover;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "pgcluster", version, about = "PostgreSQL HA cluster manager")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start the pgcluster server
    Server(server::ServerArgs),
    /// Show cluster status
    Status(status::StatusArgs),
    /// Trigger a planned switchover
    Switchover(switchover::SwitchoverArgs),
    /// Trigger a manual failover
    Failover(failover::FailoverArgs),
    /// Manage cluster nodes
    Node(node::NodeArgs),
    /// Manage replication
    Replication(replication::ReplicationArgs),
    /// Show or validate configuration
    Config(config::ConfigArgs),
    /// Initialize a new cluster (bootstrap)
    Init(init::InitArgs),
    /// Generate a cluster CA and certificates
    Certs(certs::CertsArgs),
    /// Manage hosts through pgcluster-admin
    Host(admin::HostArgs),
    /// Manage databases through pgcluster-admin
    Database(admin::DatabaseArgs),
    /// Manage backup repositories through pgcluster-admin
    Repository(admin::RepositoryArgs),
    /// Read the event feed of pgcluster-admin
    Events(admin::EventArgs),
    /// Read the audit log of pgcluster-admin: each change, and the key that made it
    AuditLog(admin::AuditArgs),
    /// Show or rotate the CA of pgcluster-admin
    AdminCa(admin::AdminCaArgs),
    /// Run a local web page for trying every operation
    Console(console::ConsoleArgs),
}

pub async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Server(args) => server::run(args).await,
        Commands::Status(args) => status::run(args).await,
        Commands::Switchover(args) => switchover::run(args).await,
        Commands::Failover(args) => failover::run(args).await,
        Commands::Node(args) => node::run(args).await,
        Commands::Replication(args) => replication::run(args).await,
        Commands::Config(args) => config::run(args).await,
        Commands::Init(args) => init::run(args).await,
        Commands::Certs(args) => certs::run(args).await,
        Commands::Host(args) => admin::run_host(args).await,
        Commands::Database(args) => admin::run_database(args).await,
        Commands::Repository(args) => admin::run_repository(args).await,
        Commands::Events(args) => admin::run_events(args).await,
        Commands::AuditLog(args) => admin::run_audit(args).await,
        Commands::AdminCa(args) => admin::run_admin_ca(args).await,
        Commands::Console(args) => console::run(args).await,
    }
}
