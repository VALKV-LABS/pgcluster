use anyhow::Result;
use clap::Args;

#[derive(Args)]
pub struct InitArgs {
    /// Path to config file to use for bootstrap
    #[arg(short, long, default_value = "/etc/pgcluster/config.toml")]
    pub config: String,
    /// If set, generate dev TLS certificates into data_dir/certs/
    #[arg(long)]
    pub gen_certs: bool,
}

pub async fn run(args: InitArgs) -> Result<()> {
    let cfg = crate::config::PgClusterConfig::from_file(&args.config)?;
    crate::config::validate::validate(&cfg)?;

    // Create data directory
    std::fs::create_dir_all(&cfg.cluster.data_dir)?;
    println!("data_dir created: {}", cfg.cluster.data_dir);

    if args.gen_certs {
        let data_path = std::path::Path::new(&cfg.cluster.data_dir);
        crate::tls::auto_cert::generate_dev_certs(cfg.raft.node_id, data_path)?;
        println!("dev certs generated in {}/certs/", cfg.cluster.data_dir);
    }

    println!(
        "ready — run 'pgcluster server --config {}' to start",
        args.config
    );
    Ok(())
}
