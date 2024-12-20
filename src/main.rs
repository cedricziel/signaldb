use acceptor::{serve_otlp_grpc, serve_otlp_http};
use anyhow::{Context, Result};
use tokio::sync::oneshot;

#[tokio::main]
async fn main() -> Result<()> {
    log::info!("Starting signaldb");

    tracing_subscriber::fmt::init();

    let config = common::config::Configuration::load().context("Failed to load configuration")?;

    common::config::CONFIG
        .set(config)
        .context("Failed to set global configuration")?;

    let (otlp_grpc_init_tx, otlp_grpc_init_rx) = oneshot::channel();
    let (_otlp_grpc_shutdown_tx, otlp_grpc_shutdown_rx) = oneshot::channel();
    let (otlp_grpc_stopped_tx, _otlp_grpc_stopped_rx) = oneshot::channel();

    let _otlp_grpc_handle = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .context("Unable to create Tokio runtime for OTLP/gRPC")
            .unwrap();

        runtime.block_on(async {
            serve_otlp_grpc(
                otlp_grpc_init_tx,
                otlp_grpc_shutdown_rx,
                otlp_grpc_stopped_tx,
            )
            .await
            .context("Unable to start OTLP/gRPC acceptor")
            .unwrap();
        });
    });

    otlp_grpc_init_rx
        .await
        .context("Unable to receive init signal for OTLP/gRPC")?;

    let (otlp_http_init_tx, otlp_http_init_rx) = oneshot::channel();
    let (_otlp_http_shutdown_tx, otlp_http_shutdown_rx) = oneshot::channel();
    let (otlp_http_stopped_tx, _otlp_http_stopped_rx) = oneshot::channel();

    let _otlp_http_handle = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .context("Unable to create Tokio runtime for OTLP/HTTP")
            .unwrap();

        runtime.block_on(async {
            serve_otlp_http(
                otlp_http_init_tx,
                otlp_http_shutdown_rx,
                otlp_http_stopped_tx,
            )
            .await
            .context("Unable to start OTLP acceptor for OTLP/HTTP")
            .unwrap();
        });
    });

    otlp_http_init_rx
        .await
        .context("Unable to receive init signal for OTLP/HTTP")?;

    let (querier_http_init_tx, _querier_http_init_rx) = oneshot::channel();
    let (_querier_http_shutdown_tx, querier_http_shutdown_rx) = oneshot::channel();
    let (querier_http_stopped_tx, _querier_http_stopped_rx) = oneshot::channel();

    let _querier_http_handle = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .context("Unable to create Tokio runtime for querier HTTP")
            .unwrap();

        runtime.block_on(async {
            let _ = querier::serve_querier_http(
                querier_http_init_tx,
                querier_http_shutdown_rx,
                querier_http_stopped_tx,
            )
            .await
            .context("Unable to start querier HTTP server")
            .unwrap();
        });
    });

    log::info!("SignalDB started");

    tokio::signal::ctrl_c()
        .await
        .context("Failed to listen for ctrl+c signal")?;

    Ok(())
}
