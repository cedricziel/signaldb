//! End-to-end verification that a Flight `do_get` failure on the *ticket-parse*
//! path — rejected before any query executes — still records an OpenTelemetry
//! `exception` event and an error status on the `flight_do_get` span.
//!
//! The companion `do_get_span_exception` test covers the execution error
//! boundary; this one covers the earlier parse guards, together exercising the
//! single error boundary that funnels every `do_get` failure through one
//! recording point. See the OTel exception semantic conventions
//! (https://opentelemetry.io/docs/specs/otel/trace/exceptions/).
//!
//! Its own integration-test binary (separate process) so the process-global
//! tracing subscriber it installs is isolated from other tests.

use std::sync::Arc;
use std::time::Duration;

use arrow_flight::Ticket;
use arrow_flight::flight_service_server::FlightService;
use common::config::{Configuration, DatabaseConfig, DiscoveryConfig};
use common::flight::transport::InMemoryFlightTransport;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use object_store::memory::InMemory;
use opentelemetry::trace::{Status, TracerProvider as _};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use querier::QuerierFlightService;
use tonic::Request;
use tracing_subscriber::prelude::*;

#[tokio::test]
async fn parse_rejected_do_get_records_exception_on_span() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("test");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let object_store = Arc::new(InMemory::new());
    let config = Configuration {
        database: DatabaseConfig {
            dsn: "sqlite::memory:".to_string(),
        },
        discovery: Some(DiscoveryConfig {
            dsn: "sqlite::memory:".to_string(),
            heartbeat_interval: Duration::from_secs(5),
            poll_interval: Duration::from_secs(10),
            ttl: Duration::from_secs(60),
        }),
        ..Default::default()
    };
    let bootstrap =
        ServiceBootstrap::new(config, ServiceType::Querier, "localhost:50054".to_string())
            .await
            .unwrap();
    let flight_transport = Arc::new(InMemoryFlightTransport::new(bootstrap));
    let service = QuerierFlightService::new(object_store, flight_transport);

    // A ticket whose payload is not the JSON the query_logs handler expects. It
    // is rejected during ticket parsing — inside the flight_do_get span but well
    // before execution — so it exercises the parse guard, not the execution
    // boundary the companion test covers.
    let ticket = Ticket::new("query_logs:acme:prod:not-json");
    let status = match service.do_get(Request::new(ticket)).await {
        Ok(_) => panic!("malformed ticket must be rejected"),
        Err(status) => status,
    };
    assert_eq!(
        status.code(),
        tonic::Code::InvalidArgument,
        "expected the ticket to be rejected during parsing"
    );

    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    let span = spans
        .iter()
        .find(|s| s.name == "flight_do_get")
        .expect("flight_do_get span exported");

    let event = span
        .events
        .iter()
        .find(|e| e.name == "exception")
        .expect("exception span event recorded on the parse-error path");
    let message = event
        .attributes
        .iter()
        .find(|kv| kv.key.as_str() == "exception.message")
        .map(|kv| kv.value.as_str().to_string())
        .expect("exception.message attribute present");
    assert!(
        !message.is_empty(),
        "exception.message should carry the failure reason"
    );

    assert!(
        matches!(span.status, Status::Error { .. }),
        "expected error span status, got {:?}",
        span.status
    );
}
