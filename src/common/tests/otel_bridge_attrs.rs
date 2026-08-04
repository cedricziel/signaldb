//! Pins the production tracing→OTel bridge configuration
//! (`common::self_monitoring::otel_span_layer`) against the semconv
//! registry: the layer must not decorate spans with the
//! tracing-opentelemetry convenience attributes (`busy_ns`, `idle_ns`,
//! `target`, `code.*`) — none of them exist in otel/registry/ or upstream
//! semconv v1.43, and each one fails `weaver registry live-check`.

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use tracing_subscriber::prelude::*;

/// Run `f` under the production bridge layer and return the finished spans.
fn capture_spans(f: impl FnOnce()) -> Vec<SpanData> {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("test");
    let subscriber =
        tracing_subscriber::registry().with(common::self_monitoring::otel_span_layer(tracer));
    tracing::subscriber::with_default(subscriber, f);
    provider.force_flush().unwrap();
    exporter.get_finished_spans().unwrap()
}

#[test]
fn bridge_layer_adds_no_out_of_registry_attributes() {
    let spans = capture_spans(|| {
        let span = common::self_monitoring::spans::job_span(
            "retention_enforcement",
            "acme",
            "production",
            None,
        );
        let _guard = span.enter();
    });
    let span = &spans[0];

    let offending: Vec<&str> = span
        .attributes
        .iter()
        .map(|kv| kv.key.as_str())
        .filter(|key| matches!(*key, "busy_ns" | "idle_ns" | "target") || key.starts_with("code."))
        .collect();
    assert!(
        offending.is_empty(),
        "bridge layer decorated the span with out-of-registry attributes: {offending:?}"
    );
}
