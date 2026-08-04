//! End-to-end verification that a failing Flight `do_get` records the reason on
//! the DoGet server span as an OpenTelemetry `exception` event with an error
//! status — the signal that lets an operator diagnose a query failure from the
//! self-monitoring trace after the router has stripped it to a bare HTTP code.
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
async fn failing_do_get_records_exception_on_span() {
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

    // A well-formed metric-labels ticket against a dataset with no metrics
    // tables. This parses cleanly and fails during execution, so it exercises
    // the do_get error boundary rather than the earlier ticket-parse guards.
    let ticket = Ticket::new("query_metric_labels:acme:prod:0:100000000000");
    let result = service.do_get(Request::new(ticket)).await;
    assert!(result.is_err(), "expected the metric-labels query to fail");

    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    let span = spans
        .iter()
        .find(|s| {
            s.name
                .starts_with("arrow.flight.protocol.FlightService/DoGet")
        })
        .expect("DoGet RPC server span exported");

    let event = span
        .events
        .iter()
        .find(|e| e.name == "exception")
        .expect("exception span event recorded on the failing do_get");
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
