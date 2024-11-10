use axum::Router;
use tokio::sync::oneshot;
use tower_http::trace::TraceLayer;

mod query;
mod tempo_endpoints;

pub fn query_router() -> Router {
    Router::new()
        .layer(TraceLayer::new_for_http())
        // nest routes for tempo compatibility
        .nest("/tempo", tempo_endpoints::router())
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
