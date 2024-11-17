use axum::Router;
use services::tempo::SignalDBQuerier;
use tempo_api::tempopb::querier_server::QuerierServer;
use tokio::sync::oneshot;
use tonic::transport::{server::Router as TonicRouter, Server};
use tower_http::trace::TraceLayer;

mod query;
mod services;
mod tempo_endpoints;

pub fn grpc_service() -> TonicRouter {
    let querier = SignalDBQuerier {};

    let querier_svc = QuerierServer::new(querier);
    Server::builder().add_service(querier_svc)
}

pub fn query_router() -> Router {
    Router::new()
        .layer(TraceLayer::new_for_http())
        // nest routes for tempo compatibility
        .nest("/tempo", tempo_endpoints::router())
        .nest("/tempo", grpc_service().into_router())
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
