use std::io::Read;

use anyhow::Context;
use clap::{Parser, Subcommand};
use pgcluster_admin::secrets::random_token;
use pgcluster_admin::state::hash_api_key;

/// Management plane for pgcluster.
#[derive(Parser)]
#[command(name = "pgcluster-admin", version)]
struct Args {
    /// Path to the config file.
    #[arg(short, long, default_value = "/etc/pgcluster-admin/config.toml")]
    config: String,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Print a new API key and the hash that goes in the config
    GenerateKey,
    /// Read an API key from standard input and print its hash
    HashKey,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    match args.command {
        Some(Command::GenerateKey) => {
            let key = random_token();
            println!("key  {key}");
            println!("hash {}", hash_api_key(&key)?);
            Ok(())
        }
        Some(Command::HashKey) => {
            let mut key = String::new();
            std::io::stdin()
                .read_to_string(&mut key)
                .context("read the key")?;
            println!("{}", hash_api_key(key.trim_end_matches(['\r', '\n']))?);
            Ok(())
        }
        None => pgcluster_admin::run(&args.config).await,
    }
}
