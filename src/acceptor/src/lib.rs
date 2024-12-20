mod handler;
pub mod services;

use std::{sync::Arc, time::SystemTime};

use anyhow::Ok;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_schema::Schema;
use axum::{
    routing::{get, post},
    Router,
};
use common::dataset::DataSet;
use opentelemetry_proto::tonic::collector::{
    logs::v1::logs_service_server::LogsServiceServer,
    metrics::v1::metrics_service_server::MetricsServiceServer,
    trace::v1::trace_service_server::TraceServiceServer,
};
use parquet::{
    arrow::AsyncArrowWriter,
    file::properties::{WriterProperties, WriterVersion},
};
use services::{
    flight::SignalDBFlightService, otlp_log_service::LogAcceptorService,
    otlp_metric_service::MetricsAcceptorService, otlp_trace_service::TraceAcceptorService,
};
use tokio::{
    fs::{create_dir_all, File},
    sync::oneshot,
};

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
    let addr = "0.0.0.0:4317".parse().unwrap();

    log::info!("Starting OTLP/gRPC acceptor on {}", addr);

    let log_server = LogsServiceServer::new(LogAcceptorService);
    let trace_server = TraceServiceServer::new(TraceAcceptorService);
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

#[tracing::instrument]
async fn root() -> &'static str {
    log::info!("hello world handler");

    "Hello, World!"
}

pub fn acceptor_router() -> Router {
    Router::new()
        .route("/", get(root))
        .route("/v1/traces", post(handle_traces))
}

async fn handle_traces(
    axum::extract::Json(payload): axum::extract::Json<serde_json::Value>,
) -> axum::response::Response<axum::body::Body> {
    log::info!("Received trace: {:?}", payload);
    
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
    let app = acceptor_router();

    let addr = "0.0.0.0:4318";

    log::info!("Starting OTLP/HTTP server on {}", addr);

    init_tx
        .send(())
        .expect("Unable to send init signal for OTLP/HTTP");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            shutdown_rx.await.ok();

            log::info!("Shutting down OTLP/HTTP server");
        })
        .await
        .unwrap();
    stopped_tx
        .send(())
        .expect("Unable to send stopped signal for OTLP/HTTP");

    Ok(())
}
