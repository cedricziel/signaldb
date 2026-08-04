//! Pins the production tracing→OTel bridge configuration
//! (`common::self_monitoring::otel_span_layer`) against the semconv
//! registry: the layer must not decorate spans with the
//! tracing-opentelemetry convenience attributes (`busy_ns`, `idle_ns`,
//! `target`, `code.*`) — none of them exist in otel/registry/ or upstream
//! semconv v1.43, and each one fails `weaver registry live-check`.
//!
//! `code.module.name` deserves special mention (#956): semconv v1.43 defines
//! `code.file.path` and `code.line.number` but NOT `code.module.name`, so of
//! the three location attributes the bridge adds it is the one that always
//! fails live-check ("does not exist in the registry" + "collides with
//! existing namespace 'code'"). The bridge emits it in two places, both
//! gated on the same `with_location` flag: span creation (`on_new_span`) and
//! span events (`on_event`). Both paths are pinned here.

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

/// The bridge's `on_event` path has its own code-location block (it adds
/// `code.file.path`, `code.module.name`, and `code.line.number` to every
/// span event when location is enabled). It is gated on the same
/// `with_location` flag as span creation, but nothing pinned it: a future
/// tracing-opentelemetry upgrade splitting the flags, or a refactor of
/// `otel_span_layer`, would silently reintroduce `code.module.name` on the
/// rarely-exercised event paths (e.g. `record_span_exception`) and make the
/// live-check job flaky again (#956).
#[test]
fn bridge_layer_adds_no_code_location_attributes_on_span_events() {
    let spans = capture_spans(|| {
        let span = common::self_monitoring::spans::job_span(
            "retention_enforcement",
            "acme",
            "production",
            None,
        );
        let _guard = span.enter();
        tracing::info!("job tick");
    });
    let span = &spans[0];
    assert!(
        !span.events.is_empty(),
        "expected the tracing event to be recorded as a span event"
    );

    let offending: Vec<&str> = span
        .events
        .iter()
        .flat_map(|event| event.attributes.iter())
        .map(|kv| kv.key.as_str())
        .filter(|key| key.starts_with("code."))
        .collect();
    assert!(
        offending.is_empty(),
        "bridge layer decorated span events with code-location attributes: {offending:?}"
    );
}
