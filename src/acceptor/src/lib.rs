pub mod handler;
pub mod services;

use std::{net::SocketAddr, sync::Arc, time::SystemTime};

use axum::{
    routing::{get, post},
    Router,
};
use common::dataset::DataSet;
use datafusion::arrow::datatypes::Schema;
use datafusion::parquet::{
    arrow::AsyncArrowWriter,
    file::properties::{WriterProperties, WriterVersion},
};
use opentelemetry_proto::tonic::collector::{
    logs::v1::logs_service_server::LogsServiceServer,
    metrics::v1::metrics_service_server::MetricsServiceServer,
    trace::v1::trace_service_server::TraceServiceServer,
};
use tokio::net::TcpListener;
use tokio::{
    fs::{create_dir_all, File},
    sync::{oneshot, Mutex},
};
// Service bootstrap and configuration
use common::config::Configuration;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
// Flight protocol client
use arrow_flight::client::FlightClient;
use tonic::transport::Endpoint;

use crate::handler::otlp_grpc::TraceHandler;
use crate::services::{
    otlp_log_service::LogAcceptorService, otlp_metric_service::MetricsAcceptorService,
    otlp_trace_service::TraceAcceptorService,
};

pub async fn get_parquet_writer(data_set: DataSet, schema: Schema, config: &Configuration) -> AsyncArrowWriter<File> {
    log::info!("get_parquet_writer");

    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .build();

    // Get storage path from configuration instead of hardcoded path
    let storage_url = config.default_storage_url();
    let base_path = if let Some(path) = storage_url.strip_prefix("file://") {
        path.to_string()
    } else {
        // Fallback to .data/ds if not a file:// URL
        ".data/ds".to_string()
    };

    let dir_path = format!("{}/{}", base_path, data_set.data_type);
    create_dir_all(&dir_path)
        .await
        .expect("Error creating directory");

    let file_path = format!(
        "{}/{}.parquet",
        dir_path,
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    );

    log::info!("Writing parquet file to: {file_path}");

    AsyncArrowWriter::try_new(
        File::create(file_path)
            .await
            .expect("Error creating parquet file"),
        Arc::new(schema),
        Some(props),
    )
    .expect("Error creating parquet writer")
}

pub async fn serve_otlp_grpc(
    init_tx: oneshot::Sender<()>,
    shutdown_rx: oneshot::Receiver<()>,
    stopped_tx: oneshot::Sender<()>,
) -> Result<(), anyhow::Error> {
    let addr: SocketAddr = "0.0.0.0:4317"
        .parse()
        .map_err(|e| anyhow::anyhow!("Failed to parse OTLP/gRPC address: {}", e))?;

    // Load configuration
    let config = Configuration::load()
        .map_err(|e| anyhow::anyhow!("Failed to load configuration: {}", e))?;

    // Initialize service bootstrap for catalog-based discovery
    let advertise_addr =
        std::env::var("ACCEPTOR_ADVERTISE_ADDR").unwrap_or_else(|_| addr.to_string());

    let service_bootstrap = ServiceBootstrap::new(config, ServiceType::Acceptor, advertise_addr)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to initialize service bootstrap: {}", e))?;

    log::info!("Starting OTLP/gRPC acceptor on {addr}");

    // Initialize Flight client to forward telemetry to writer
    let flight_endpoint = std::env::var("FLIGHT_ENDPOINT").unwrap_or_else(|_| {
        // Default Flight endpoint
        "http://127.0.0.1:50051".to_string()
    });
    let endpoint = Endpoint::new(flight_endpoint)
        .map_err(|e| anyhow::anyhow!("Invalid Flight endpoint: {}", e))?;
    let channel = endpoint.connect().await?;
    let flight_client = Arc::new(Mutex::new(FlightClient::new(channel)));
    // Set up OTLP/gRPC services with Flight forwarding
    let log_server = LogsServiceServer::new(LogAcceptorService::new(flight_client.clone()));
    let trace_server = TraceServiceServer::new(TraceAcceptorService::new(TraceHandler::new(
        flight_client.clone(),
    )));
    let metric_server =
        MetricsServiceServer::new(MetricsAcceptorService::new(flight_client.clone()));

    init_tx
        .send(())
        .expect("Unable to send init signal for OTLP/gRPC");
    tonic::transport::Server::builder()
        .add_service(log_server)
        .add_service(trace_server)
        .add_service(metric_server)
        .serve_with_shutdown(addr, async {
            shutdown_rx.await.ok();

            log::info!("Shutting down OTLP acceptor");
            // Graceful service bootstrap shutdown
            if let Err(e) = service_bootstrap.shutdown().await {
                log::error!("Failed to shutdown service bootstrap: {e}");
            }
        })
        .await
        .expect("Unable to start OTLP acceptor");

    stopped_tx
        .send(())
        .expect("Unable to send stopped signal for OTLP/gRPC");
    Ok(())
}

pub fn acceptor_router() -> Router {
    Router::new()
        .route("/v1/traces", post(handle_traces))
        .route("/health", get(health))
}

async fn health() -> &'static str {
    "ok"
}

async fn handle_traces(
    axum::extract::Json(payload): axum::extract::Json<serde_json::Value>,
) -> axum::response::Response<axum::body::Body> {
    log::info!("Got traces: {payload:?}");
    axum::response::Response::builder()
        .status(200)
        .body(axum::body::Body::empty())
        .unwrap()
}

pub async fn serve_otlp_http(
    init_tx: oneshot::Sender<()>,
    shutdown_rx: oneshot::Receiver<()>,
    stopped_tx: oneshot::Sender<()>,
) -> Result<(), anyhow::Error> {
    let addr: SocketAddr = "0.0.0.0:4318"
        .parse()
        .map_err(|e| anyhow::anyhow!("Failed to parse OTLP/HTTP address: {}", e))?;

    log::info!("Starting OTLP/HTTP acceptor on {addr}");

    // Load configuration
    let config = Configuration::load()
        .map_err(|e| anyhow::anyhow!("Failed to load configuration: {}", e))?;

    // Initialize service bootstrap for catalog-based discovery
    let advertise_addr =
        std::env::var("ACCEPTOR_ADVERTISE_ADDR").unwrap_or_else(|_| addr.to_string());

    let service_bootstrap = ServiceBootstrap::new(config, ServiceType::Acceptor, advertise_addr)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to initialize service bootstrap: {}", e))?;

    let app = acceptor_router();

    init_tx
        .send(())
        .expect("Unable to send init signal for OTLP/HTTP");

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            shutdown_rx.await.ok();
            log::info!("Shutting down OTLP/HTTP acceptor");
            // Graceful service bootstrap shutdown
            if let Err(e) = service_bootstrap.shutdown().await {
                log::error!("Failed to shutdown service bootstrap: {e}");
            }
        })
        .await?;

    stopped_tx
        .send(())
        .expect("Unable to send stopped signal for OTLP/HTTP");

    Ok(())
}
