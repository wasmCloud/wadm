use anyhow::Context as _;
use clap::Parser;
use wadm::{config::WadmConfig, start_wadm};

mod logging;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = WadmConfig::parse();

    logging::configure_tracing(
        args.structured_logging,
        args.tracing_enabled,
        args.tracing_endpoint.clone(),
    );

    let mut wadm = start_wadm(args)
        .await
        .context("failed to run wadm")?;
    tokio::select! {
        res = wadm.join_next() => {
            match res {
                Some(Ok(_)) => {
                    tracing::info!("WADM has exited successfully");
                    std::process::exit(0);
                }
                Some(Err(e)) => {
                    tracing::error!("WADM has exited with an error: {:?}", e);
                    std::process::exit(1);
                }
                None => {
                    tracing::info!("WADM server did not start");
                    std::process::exit(0);
                }
            }
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received Ctrl+C, shutting down");
            std::process::exit(0);
        }
    }
}
