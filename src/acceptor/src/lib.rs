pub mod handler;
pub mod middleware;
pub mod services;

use std::{net::SocketAddr, sync::Arc, time::SystemTime};

use axum::{
    Router,
    routing::{get, post},
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
    fs::{File, create_dir_all},
    sync::oneshot,
};
// Service bootstrap and configuration
use common::config::Configuration;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
// Flight protocol and transport
use common::flight::transport::InMemoryFlightTransport;
// WAL for durability
use common::wal::WalConfig;

use crate::handler::WalManager;
use crate::handler::otlp_grpc::TraceHandler;
use crate::handler::otlp_log_handler::LogHandler;
use crate::handler::otlp_metrics_handler::MetricsHandler;
use crate::services::{
    otlp_log_service::LogAcceptorService, otlp_metric_service::MetricsAcceptorService,
    otlp_trace_service::TraceAcceptorService,
};

pub async fn get_parquet_writer(
    data_set: DataSet,
    schema: Schema,
    config: &Configuration,
) -> AsyncArrowWriter<File> {
    log::info!("get_parquet_writer");

    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .build();

    // Get storage path from configuration DSN
    let storage_dsn = &config.storage.dsn;
    let base_path = if let Some(path) = storage_dsn.strip_prefix("file://") {
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

    // Initialize Flight transport with catalog-aware discovery
    let flight_transport = Arc::new(InMemoryFlightTransport::new(service_bootstrap));

    // Start background connection cleanup
    flight_transport.start_connection_cleanup(std::time::Duration::from_secs(60));

    // Initialize WalManager with separate base configurations for each signal type
    // The WalManager will create tenant/dataset-specific WALs on demand

    // Base WAL config for traces - baseline configuration
    let traces_wal_config = WalConfig {
        wal_dir: std::env::var("ACCEPTOR_WAL_DIR")
            .unwrap_or_else(|_| ".wal".to_string())
            .into(),
        max_segment_size: 64 * 1024 * 1024, // 64MB
        max_buffer_entries: 1000,
        flush_interval_secs: 30,
    };

    // Base WAL config for logs - higher volume, more frequent flushes
    let logs_wal_config = WalConfig {
        wal_dir: std::env::var("ACCEPTOR_WAL_DIR")
            .unwrap_or_else(|_| ".wal".to_string())
            .into(),
        max_segment_size: 64 * 1024 * 1024, // 64MB
        max_buffer_entries: 2000,           // Higher buffer for log volume
        flush_interval_secs: 15,            // Flush more frequently
    };

    // Base WAL config for metrics - highest volume, most aggressive flushing
    let metrics_wal_config = WalConfig {
        wal_dir: std::env::var("ACCEPTOR_WAL_DIR")
            .unwrap_or_else(|_| ".wal".to_string())
            .into(),
        max_segment_size: 128 * 1024 * 1024, // 128MB - larger segments for high volume
        max_buffer_entries: 5000,            // Much higher buffer for metrics
        flush_interval_secs: 10,             // Flush frequently for metrics
    };

    // Create WalManager with the three base configurations
    // WAL paths will be: .wal/{tenant}/{dataset}/{signal}/
    let wal_manager = Arc::new(WalManager::new(
        traces_wal_config,
        logs_wal_config,
        metrics_wal_config,
    ));

    log::info!(
        "✅ Initialized WalManager for multi-tenant WAL isolation (paths: .wal/{{tenant}}/{{dataset}}/{{signal}})"
    );

    // Set up OTLP/gRPC services with handler pattern and WAL Manager integration
    let log_handler = LogHandler::new(flight_transport.clone(), wal_manager.clone());
    let log_server = LogsServiceServer::new(LogAcceptorService::new(log_handler));

    let trace_handler = TraceHandler::new(flight_transport.clone(), wal_manager.clone());
    let trace_server = TraceServiceServer::new(TraceAcceptorService::new(trace_handler));

    let metrics_handler = MetricsHandler::new(flight_transport.clone(), wal_manager.clone());
    let metric_server = MetricsServiceServer::new(MetricsAcceptorService::new(metrics_handler));

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
            // Service bootstrap shutdown is handled via InMemoryFlightTransport's drop impl
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

    // Note: Service bootstrap is handled in the main function for the entire acceptor service

    let app = acceptor_router();

    init_tx
        .send(())
        .expect("Unable to send init signal for OTLP/HTTP");

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            shutdown_rx.await.ok();
            log::info!("Shutting down OTLP/HTTP acceptor");
            // Service bootstrap shutdown is handled in the main function
        })
        .await?;

    stopped_tx
        .send(())
        .expect("Unable to send stopped signal for OTLP/HTTP");

    Ok(())
}
