use anyhow::Result;
use clap::Args;

#[derive(Args)]
pub struct CertsArgs {
    /// Directory for the CA and certificates. An existing CA there is reused.
    #[arg(short, long)]
    pub out: String,
    /// Hostnames or IP addresses peers use to reach each pgcluster, vk-agent
    /// and Postgres. Each gets its own certificate.
    #[arg(required = true)]
    pub names: Vec<String>,
}

pub async fn run(args: CertsArgs) -> Result<()> {
    crate::tls::certs::generate(std::path::Path::new(&args.out), &args.names)?;
    println!(
        "wrote ca.crt and {} certificates to {}",
        args.names.len(),
        args.out
    );
    Ok(())
}
