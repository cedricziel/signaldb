//! Verifies that `record_span_exception` attaches an OpenTelemetry `exception`
//! span event and an error status to the current span, per the OTel exception
//! semantic conventions (https://opentelemetry.io/docs/specs/otel/trace/exceptions/).
//!
//! Lives in its own integration-test binary (separate process) so the
//! process-global tracing subscriber it installs is isolated from other tests.

use opentelemetry::trace::{Status, TracerProvider as _};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use tracing::Instrument;
use tracing_subscriber::prelude::*;

#[tokio::test]
async fn records_exception_event_and_error_status() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("test");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    tracing::subscriber::set_global_default(subscriber).unwrap();

    // A tonic::Status carrying the same reason the querier would surface for a
    // metrics metadata lookup on a dataset with no metrics tables.
    let status = tonic::Status::invalid_argument("no metrics tables available for this dataset");

    async {
        common::self_monitoring::record_span_exception(&status);
    }
    .instrument(tracing::info_span!("flight_do_get"))
    .await;

    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    let span = spans
        .iter()
        .find(|s| s.name == "flight_do_get")
        .expect("flight_do_get span exported");

    // OTel exception semantic convention: an `exception` span event carrying the
    // reason in `exception.message`.
    let event = span
        .events
        .iter()
        .find(|e| e.name == "exception")
        .expect("exception span event recorded");
    let message = event
        .attributes
        .iter()
        .find(|kv| kv.key.as_str() == "exception.message")
        .map(|kv| kv.value.as_str().to_string())
        .expect("exception.message attribute present");
    assert!(
        message.contains("no metrics tables available for this dataset"),
        "exception.message did not carry the reason: {message}"
    );

    // Span status is marked error so the failure is visible in the trace.
    assert!(
        matches!(span.status, Status::Error { .. }),
        "expected error span status, got {:?}",
        span.status
    );
}
