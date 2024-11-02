use acceptor::{init_acceptor, serve_otlp_http};
use tokio::sync::oneshot;

#[tokio::main]
async fn main() {
    log::info!("Starting signaldb");

    tracing_subscriber::fmt::init();

    let (otlp_grpc_init_tx, otlp_grpc_init_rx) = oneshot::channel();
    let (_otlp_grpc_shutdown_tx, otlp_grpc_shutdown_rx) = oneshot::channel();
    let (otlp_grpc_stopped_tx, _otlp_grpc_stopped_rx) = oneshot::channel();

    let _otlp_grpc_handle = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Unable to create Tokio runtime");

        runtime.block_on(async {
            init_acceptor(
                otlp_grpc_init_tx,
                otlp_grpc_shutdown_rx,
                otlp_grpc_stopped_tx,
            )
            .await
            .expect("Unable to start OTLP acceptor");
        });
    });

    otlp_grpc_init_rx
        .await
        .expect("Unable to receive init signal");

    serve_otlp_http().await;
}
