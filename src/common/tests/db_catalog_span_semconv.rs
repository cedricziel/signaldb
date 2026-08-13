//! Conformance test: SQL-catalog operations emit semconv DB CLIENT spans.
//!
//! Own binary (process-isolated OTel/tracing state, same pattern as
//! `http_span_semconv.rs`).

use opentelemetry::trace::{SpanKind, TracerProvider as _};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use tracing_subscriber::prelude::*;

#[tokio::test]
async fn catalog_ops_emit_db_client_spans() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("test");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let catalog = common::catalog::Catalog::new_in_memory().await.unwrap();
    let _ = catalog.list_ingesters().await.unwrap();

    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    let names: Vec<_> = spans.iter().map(|s| s.name.to_string()).collect();
    let span = spans
        .iter()
        .find(|s| s.span_kind == SpanKind::Client)
        .unwrap_or_else(|| panic!("no DB CLIENT span; exported = {names:?}"));

    // Name precedence: `{db.operation.name} {db.namespace}` — never raw SQL.
    assert_eq!(span.name, "SELECT signaldb-catalog");
    let attr = |key: &str| {
        span.attributes
            .iter()
            .find(|kv| kv.key.as_str() == key)
            .map(|kv| kv.value.as_str().to_string())
    };
    assert_eq!(attr("db.system.name").as_deref(), Some("sqlite"));
    assert_eq!(attr("db.operation.name").as_deref(), Some("SELECT"));
    assert_eq!(attr("db.namespace").as_deref(), Some("signaldb-catalog"));
    // The statement text is safe to record here: sqlx binds values rather
    // than interpolating them, so it's a parameterized template with no
    // literals — this is the exact query `list_ingesters` runs.
    assert_eq!(
        attr("db.query.text").as_deref(),
        Some("SELECT id, address, last_seen, service_type, capabilities FROM ingesters")
    );
}
