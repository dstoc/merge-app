use anyhow::Result;
use tracing::error;
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() {
    init_logging();
    if let Err(error) = run().await {
        error!(event = "application_failed", error = %error);
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let config = merge_app::config::load().await?;
    merge_app::service::run(config).await
}

fn init_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .init();
}
