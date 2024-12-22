mod handler;
pub mod services;

use std::{net::SocketAddr, sync::Arc, time::SystemTime};

use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_schema::Schema;
use axum::{
    routing::{get, post},
    Router,
};
use common::{dataset::DataSet, queue::memory::InMemoryQueue};
use opentelemetry_proto::tonic::collector::{
    logs::v1::logs_service_server::LogsServiceServer,
    metrics::v1::metrics_service_server::MetricsServiceServer,
    trace::v1::trace_service_server::TraceServiceServer,
};
use parquet::{
    arrow::AsyncArrowWriter,
    file::properties::{WriterProperties, WriterVersion},
};
use tokio::net::TcpListener;
use tokio::{
    fs::{create_dir_all, File},
    sync::{oneshot, Mutex},
};

use crate::handler::otlp_grpc::TraceHandler;
use crate::services::{
    flight::SignalDBFlightService, otlp_log_service::LogAcceptorService,
    otlp_metric_service::MetricsAcceptorService, otlp_trace_service::TraceAcceptorService,
};

#[derive(Clone)]
pub struct AcceptorState {
    #[allow(dead_code)]
    queue: Arc<Mutex<InMemoryQueue>>,
}

impl AcceptorState {
    pub fn new() -> Self {
        Self {
            queue: Arc::new(Mutex::new(InMemoryQueue::default())),
        }
    }
}

pub async fn get_parquet_writer(data_set: DataSet, schema: Schema) -> AsyncArrowWriter<File> {
    log::info!("get_parquet_writer");

    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .build();

    let path = format!(".data/ds/{}", data_set.data_type);
    create_dir_all(path)
        .await
        .expect("Error creating directory");

    AsyncArrowWriter::try_new(
        File::create(format!(
            ".data/ds/{}/{}.parquet",
            data_set.data_type,
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .to_string()
        ))
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
    let addr: SocketAddr = "0.0.0.0:4317".parse()
        .map_err(|e| anyhow::anyhow!("Failed to parse OTLP/gRPC address: {}", e))?;

    log::info!("Starting OTLP/gRPC acceptor on {}", addr);

    let state = AcceptorState::new();
    let log_server = LogsServiceServer::new(LogAcceptorService);
    let trace_server = TraceServiceServer::new(TraceAcceptorService::new(TraceHandler::new(
        state.queue.clone(),
    )));
    let metric_server = MetricsServiceServer::new(MetricsAcceptorService);

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
        })
        .await
        .expect("Unable to start OTLP acceptor");

    stopped_tx
        .send(())
        .expect("Unable to send stopped signal for OTLP/gRPC");
    Ok(())
}

pub fn create_flight_service() -> FlightServiceServer<SignalDBFlightService> {
    FlightServiceServer::new(SignalDBFlightService)
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
    log::info!("Got traces: {:?}", payload);
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
    let addr: SocketAddr = "0.0.0.0:4318".parse()
        .map_err(|e| anyhow::anyhow!("Failed to parse OTLP/HTTP address: {}", e))?;

    log::info!("Starting OTLP/HTTP acceptor on {}", addr);

    let app = acceptor_router();

    init_tx
        .send(())
        .expect("Unable to send init signal for OTLP/HTTP");

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            shutdown_rx.await.ok();
            log::info!("Shutting down OTLP/HTTP acceptor");
        })
        .await?;

    stopped_tx
        .send(())
        .expect("Unable to send stopped signal for OTLP/HTTP");

    Ok(())
}
