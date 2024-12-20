use acceptor::{serve_otlp_grpc, serve_otlp_http};
use tokio::sync::oneshot;

#[tokio::main]
async fn main() {
    log::info!("Starting signaldb");

    tracing_subscriber::fmt::init();

    let config = common::config::Configuration::load().map_err(|e| {
        log::error!("Failed to load configuration: {}", e);
        std::process::exit(1)
    })?;
    if let Err(e) = common::config::CONFIG.set(config) {
        log::error!("Failed to set global configuration: {}", e);
        std::process::exit(1)
    }

    let (otlp_grpc_init_tx, otlp_grpc_init_rx) = oneshot::channel();
    let (_otlp_grpc_shutdown_tx, otlp_grpc_shutdown_rx) = oneshot::channel();
    let (otlp_grpc_stopped_tx, _otlp_grpc_stopped_rx) = oneshot::channel();

    let _otlp_grpc_handle = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Unable to create Tokio runtime for OTLP/gRPC");

        runtime.block_on(async {
            serve_otlp_grpc(
                otlp_grpc_init_tx,
                otlp_grpc_shutdown_rx,
                otlp_grpc_stopped_tx,
            )
            .await
            .expect("Unable to start OTLP/gRPC acceptor");
        });
    });

    otlp_grpc_init_rx
        .await
        .expect("Unable to receive init signal for OTLP/gRPC");

    let (otlp_http_init_tx, otlp_http_init_rx) = oneshot::channel();
    let (_otlp_http_shutdown_tx, otlp_http_shutdown_rx) = oneshot::channel();
    let (otlp_http_stopped_tx, _otlp_http_stopped_rx) = oneshot::channel();

    let _otlp_http_handle = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Unable to create Tokio runtime for OTLP/HTTP");

        runtime.block_on(async {
            serve_otlp_http(
                otlp_http_init_tx,
                otlp_http_shutdown_rx,
                otlp_http_stopped_tx,
            )
            .await
            .expect("Unable to start OTLP acceptor for OTLP/HTTP");
        });
    });

    otlp_http_init_rx
        .await
        .expect("Unable to receive init signal for OTLP/HTTP");

    let (querier_http_init_tx, _querier_http_init_rx) = oneshot::channel();
    let (_querier_http_shutdown_tx, querier_http_shutdown_rx) = oneshot::channel();
    let (querier_http_stopped_tx, _querier_http_stopped_rx) = oneshot::channel();

    let _querier_http_handle = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Unable to create Tokio runtime for querier HTTP");

        runtime.block_on(async {
            let _ = querier::serve_querier_http(
                querier_http_init_tx,
                querier_http_shutdown_rx,
                querier_http_stopped_tx,
            )
            .await
            .expect("Unable to start querier HTTP server");
        });
    });

    log::info!("SignalDB started");

    loop {
        std::thread::park();
    }
}
