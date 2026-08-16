//! `signaldb_sdk::ClientBuilder` — the single construction path for the CLI
//! and MCP server (`client-surface-parity`): default headers, timeouts, retry
//! policy, and W3C trace-context propagation of the exec hook.

mod support;

use std::time::Duration;

use signaldb_sdk::{ClientBuildError, ClientBuilder, RetryPolicy};
use support::{Scripted, ScriptedServer};

const TENANTS: &str = r#"{"tenants":[]}"#;

#[tokio::test]
async fn builder_sends_bearer_tenant_dataset_and_custom_headers() {
    let server = ScriptedServer::start(vec![Scripted::ok_json(TENANTS)]).await;
    let client = ClientBuilder::new(format!("{}/", server.base_url))
        .bearer("sk-acme")
        .tenant("acme")
        .dataset("prod")
        .header("X-Custom", "one")
        .header("x-custom", "two")
        .build()
        .expect("builds");
    client.list_tenants().send().await.expect("ok");
    let seen = server.seen();
    assert_eq!(seen.len(), 1);
    assert_eq!(
        seen[0].path, "/api/v1/admin/tenants",
        "trailing slash trimmed"
    );
    assert_eq!(seen[0].header("authorization"), Some("Bearer sk-acme"));
    assert_eq!(seen[0].header("x-tenant-id"), Some("acme"));
    assert_eq!(seen[0].header("x-dataset-id"), Some("prod"));
    assert_eq!(
        seen[0].header("x-custom"),
        Some("two"),
        "later header() calls replace earlier ones"
    );
}

#[test]
fn builder_rejects_invalid_header_values() {
    let err = ClientBuilder::new("http://localhost:1")
        .tenant("bad\nvalue")
        .build()
        .expect_err("newline is not a valid header value");
    assert!(matches!(
        err,
        ClientBuildError::InvalidHeaderValue { ref header } if header == "x-tenant-id"
    ));
    let err = ClientBuilder::new("http://localhost:1")
        .header("bad name", "v")
        .build()
        .expect_err("space is not a valid header name");
    assert!(matches!(err, ClientBuildError::InvalidHeaderName(_)));
}

#[tokio::test]
async fn builder_installs_the_default_retry_policy() {
    let server =
        ScriptedServer::start(vec![Scripted::throttled("0"), Scripted::ok_json(TENANTS)]).await;
    let client = ClientBuilder::new(&server.base_url).build().unwrap();
    client
        .list_tenants()
        .send()
        .await
        .expect("retried by default");
    assert_eq!(server.request_count(), 2);
}

#[tokio::test]
async fn builder_retry_disabled_is_fail_fast() {
    let server =
        ScriptedServer::start(vec![Scripted::throttled("0"), Scripted::ok_json(TENANTS)]).await;
    let client = ClientBuilder::new(&server.base_url)
        .retry(RetryPolicy::disabled())
        .build()
        .unwrap();
    client.list_tenants().send().await.expect_err("not retried");
    assert_eq!(server.request_count(), 1);
}

#[tokio::test]
async fn builder_timeout_applies_per_attempt() {
    // Nothing answers on this listener; the request must time out rather
    // than hang, and a timed-out GET is retried up to the budget.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _keep = tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let _hold = socket;
                tokio::time::sleep(Duration::from_secs(30)).await;
            });
        }
    });
    let client = ClientBuilder::new(format!("http://{addr}"))
        .timeout(Duration::from_millis(100))
        .retry(RetryPolicy {
            max_attempts: 2,
            base: Duration::from_millis(1),
            ..RetryPolicy::default()
        })
        .build()
        .unwrap();
    let started = std::time::Instant::now();
    let err = client.list_tenants().send().await.expect_err("times out");
    assert!(matches!(err, signaldb_sdk::Error::CommunicationError(ref e) if e.is_timeout()));
    assert!(started.elapsed() < Duration::from_secs(5));
}

// --- W3C trace context ----------------------------------------------------

#[cfg(feature = "tracing")]
mod trace_context {
    use super::*;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tracing::Instrument;
    use tracing_subscriber::layer::SubscriberExt;

    #[tokio::test]
    async fn no_otel_layer_means_no_trace_headers() {
        let server = ScriptedServer::start(vec![Scripted::ok_json(TENANTS)]).await;
        let client = ClientBuilder::new(&server.base_url).build().unwrap();
        let span = tracing::info_span!("plain");
        client
            .list_tenants()
            .send()
            .instrument(span)
            .await
            .expect("ok");
        let seen = server.seen();
        assert!(seen[0].header("traceparent").is_none());
    }

    #[tokio::test]
    async fn otel_backed_span_propagates_traceparent() {
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let provider = SdkTracerProvider::builder().build();
        let tracer = provider.tracer("sdk-test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _guard = tracing::subscriber::set_default(subscriber);

        let server = ScriptedServer::start(vec![Scripted::ok_json(TENANTS)]).await;
        let client = ClientBuilder::new(&server.base_url).build().unwrap();
        let span = tracing::info_span!("caller");
        client
            .list_tenants()
            .send()
            .instrument(span)
            .await
            .expect("ok");
        let seen = server.seen();
        let traceparent = seen[0]
            .header("traceparent")
            .expect("traceparent injected from the current span");
        assert!(
            traceparent.starts_with("00-"),
            "W3C traceparent, got {traceparent}"
        );
        assert_eq!(traceparent.split('-').count(), 4);
    }
}
