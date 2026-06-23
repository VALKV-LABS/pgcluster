use anyhow::Result;
use clap::Args;

#[derive(Args)]
pub struct ServerArgs {
    /// Path to the pgcluster config file
    #[arg(short, long, default_value = "/etc/pgcluster/config.toml")]
    pub config: String,
}

pub async fn run(args: ServerArgs) -> Result<()> {
    crate::run_server(&args.config).await
}
