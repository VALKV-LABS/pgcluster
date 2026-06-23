#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pgcluster::cli::run().await
}
