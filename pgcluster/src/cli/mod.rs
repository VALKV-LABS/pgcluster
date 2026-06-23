pub mod client;
pub mod config;
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
    }
}
