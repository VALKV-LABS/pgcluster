use anyhow::Result;
use clap::Args;

#[derive(Args)]
pub struct InitArgs {
    /// Path to config file to use for bootstrap
    #[arg(short, long, default_value = "/etc/pgcluster/config.toml")]
    pub config: String,
}

pub async fn run(args: InitArgs) -> Result<()> {
    let cfg = crate::config::PgClusterConfig::from_file(&args.config)?;
    crate::config::validate::validate(&cfg)?;

    // Create data directory
    std::fs::create_dir_all(&cfg.cluster.data_dir)?;
    println!("data_dir created: {}", cfg.cluster.data_dir);

    println!(
        "ready — run 'pgcluster server --config {}' to start",
        args.config
    );
    Ok(())
}
