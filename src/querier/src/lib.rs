use axum::{routing::post, Router};
use tokio::sync::oneshot;

pub async fn query() -> &'static str {
    "query"
}

pub fn query_router() -> Router {
    Router::new().route("/", post(query))
}

pub async fn serve_querier_http(
    init_tx: oneshot::Sender<()>,
    shutdown_rx: oneshot::Receiver<()>,
    stopped_tx: oneshot::Sender<()>,
) -> Result<(), anyhow::Error> {
    let addr = "0.0.0.0:9000";

    log::info!("Starting querier on {}", addr);

    let app = query_router();

    init_tx
        .send(())
        .expect("Unable to send init signal for querier http server");

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            shutdown_rx.await.ok();

            log::info!("Shutting down querier http server");
        })
        .await
        .unwrap();
    stopped_tx
        .send(())
        .expect("Unable to send stopped signal for querier http server");

    Ok(())
}
