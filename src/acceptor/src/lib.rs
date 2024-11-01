mod handler;
pub mod services;

use std::sync::Arc;

use anyhow::Ok;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_schema::{DataType, Field, Schema};
use axum::{routing::get, Router};
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
use tokio::sync::oneshot;

pub fn get_parquet_writer() -> AsyncArrowWriter<Vec<u8>> {
    let writer = Vec::new();
    let schema = Schema::new(vec![
        Field::new("c1", DataType::Int32, false),
        Field::new("c2", DataType::Utf8, false),
        Field::new("c3", DataType::Float64, false),
    ]);

    println!("get_parquet_writer");
    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .build();

    AsyncArrowWriter::try_new(writer, Arc::new(schema), Some(props))
        .expect("Error creating parquet writer")
}

pub async fn init_acceptor(
    init_tx: oneshot::Sender<()>,
    shutdown_rx: oneshot::Receiver<()>,
    stopped_tx: oneshot::Sender<()>,
) -> Result<(), anyhow::Error> {
    let addr = "0.0.0.0:4317".parse().unwrap();

    log::info!("Starting OTLP/gRPC acceptor on {}", addr);

    let log_server = LogsServiceServer::new(LogAcceptorService);
    let trace_server = TraceServiceServer::new(TraceAcceptorService);
    let metric_server = MetricsServiceServer::new(MetricsAcceptorService);

    init_tx.send(()).expect("Unable to send init signal");
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

    stopped_tx.send(()).expect("Unable to send stopped signal");
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

pub async fn serve_otlp_http() {
    let app = Router::new().route("/", get(root));

    let addr = "0.0.0.0:4318";

    log::info!("Starting OTLP/HTTP server on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
