use std::{thread::sleep, time::Duration};

use opentelemetry::{
    global,
    trace::{TraceContextExt, Tracer},
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
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

    // write an http span
    tracer.in_span("GET /products/{id}", |cx| {
        let span = cx.span();

        span.set_attribute(KeyValue::new(HTTP_REQUEST_METHOD, "GET"));
        span.set_attribute(KeyValue::new(URL_FULL, "https://www.rust-lang.org/"));

        sleep(Duration::from_secs(3));
    });

    // Shutdown exporter
    global::shutdown_tracer_provider();

    tokio::time::sleep(Duration::from_secs(3)).await;
}
