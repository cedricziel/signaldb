use axum::{routing::get, Router};
use services::tempo::SignalDBQuerier;
use tempo_api::tempopb::querier_server::QuerierServer;
use tokio::sync::oneshot;
use tonic::transport::Server;
use tower_http::trace::TraceLayer;

mod query;
mod services;

pub fn grpc_service() -> Router {
    let querier = SignalDBQuerier {};
    let querier_svc = QuerierServer::new(querier);
    let _service = Server::builder().add_service(querier_svc).into_service();

    Router::new().route("/", get(|| async { "SignalDB Querier" }))
}

pub fn query_router() -> Router {
    Router::new()
        .layer(TraceLayer::new_for_http())
        // nest routes for tempo compatibility
        .nest("/tempo", grpc_service())
}

pub async fn serve_querier_http(
    init_tx: oneshot::Sender<()>,
    shutdown_rx: oneshot::Receiver<()>,
    stopped_tx: oneshot::Sender<()>,
) -> Result<(), anyhow::Error> {
    // FDAP-aligned querier - reads directly from Parquet files via DataFusion
    // No messaging queue needed for FDAP architecture

    let addr = "0.0.0.0:9000";
    log::info!("Starting querier on {addr}");

    let app = query_router();

    init_tx
        .send(())
        .map_err(|_| anyhow::anyhow!("Unable to send init signal for querier http server"))?;

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            shutdown_rx.await.ok();
            log::info!("Shutting down querier http server");
        })
        .await?;

    stopped_tx
        .send(())
        .map_err(|_| anyhow::anyhow!("Unable to send stopped signal for querier http server"))?;

    Ok(())
}
