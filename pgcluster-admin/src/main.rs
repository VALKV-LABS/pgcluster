use clap::Parser;

/// Management plane for pgcluster.
#[derive(Parser)]
#[command(name = "pgcluster-admin", version)]
struct Args {
    /// Path to the config file.
    #[arg(short, long, default_value = "/etc/pgcluster-admin/config.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pgcluster_admin::run(&Args::parse().config).await
}
