use std::{thread::sleep, time::Duration};

use opentelemetry::{
    global,
    trace::{Span, SpanBuilder, SpanKind, TraceContextExt, Tracer},
    KeyValue,
};
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::{
    trace::SdkTracerProvider,
    Resource,
};
use opentelemetry_semantic_conventions::trace::{HTTP_REQUEST_METHOD, URL_FULL};

/// produce a couple of signals and send it to a destination
#[tokio::main]
async fn main() {
    let resource = Resource::builder()
        .with_attributes(vec![KeyValue::new("service.name", "signal-producer")])
        .build();

    let exporter = SpanExporter::builder()
        .with_tonic()
        .build()
        .expect("Failed to create span exporter");

    let provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(exporter)
        .build();

    global::set_tracer_provider(provider.clone());

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

    // Shutdown provider
    provider.shutdown().expect("Failed to shutdown exporter");

    tokio::time::sleep(Duration::from_secs(3)).await;
}
