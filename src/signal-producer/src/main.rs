use std::{thread::sleep, time::Duration};

use opentelemetry::{
    global,
    trace::{Span, SpanBuilder, SpanKind, TraceContextExt, Tracer},
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{trace::Config, Resource};
use opentelemetry_semantic_conventions::trace::{HTTP_REQUEST_METHOD, URL_FULL};

/// produce a couple of signals and send it to a destination
#[tokio::main]
async fn main() {
    // First, create a OTLP exporter builder. Configure it as you need.
    let otlp_exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint("http://localhost:4317")
        .with_timeout(Duration::from_secs(3))
        .with_protocol(opentelemetry_otlp::Protocol::Grpc);

    // Then pass it into pipeline builder
    let tracer_provider = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_trace_config(
            Config::default().with_resource(Resource::new(vec![KeyValue::new(
                "service.name",
                "signal-producer",
            )])),
        )
        .with_exporter(otlp_exporter)
        .install_simple()
        .unwrap();

    global::set_tracer_provider(tracer_provider);

    let tracer = global::tracer("readme_example");

    tracer.in_span("doing_work", |cx| {
        let span = cx.span();

        span.set_attribute(KeyValue::new("question", "what is the answer?"));

        tracer.in_span("doing_work_inside", |cx| {
            let span = cx.span();

            span.set_attribute(KeyValue::new("question", "what is the answer?"));
        });
    });

    let mut span = SpanBuilder::from_name("GET /products/{id}")
        .with_attributes(vec![
            KeyValue::new(HTTP_REQUEST_METHOD, "GET"),
            KeyValue::new(URL_FULL, "https://www.rust-lang.org/"),
        ])
        .with_kind(SpanKind::Server)
        .start(&tracer);

    // write an http span
    tracer.in_span("internal work", |cx| {
        let span = cx.span();

        span.set_attribute(KeyValue::new("my.domain.attr", 42));

        sleep(Duration::from_secs(3));
    });
    span.end();

    // Shutdown exporter
    global::shutdown_tracer_provider();

    tokio::time::sleep(Duration::from_secs(3)).await;
}
