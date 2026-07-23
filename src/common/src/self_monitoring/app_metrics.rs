//! Application-level metrics following OpenTelemetry semantic conventions.
//!
//! Instruments are created from the **global** meter provider, which
//! `self_monitoring::init_telemetry` installs when self-monitoring is
//! enabled. When it is disabled the global provider is a no-op, so every
//! recording site below costs almost nothing.
//!
//! `service.name` is not repeated as a per-point attribute — each service's
//! meter provider already carries it in its OTel `Resource`.
//!
//! Anti-loop guard: ingestion counters must not count `_system` tenant
//! traffic; recording sites use [`should_count_tenant`].

use std::sync::OnceLock;
use std::time::Instant;

use opentelemetry::global;
use opentelemetry::metrics::{Counter, Gauge, Histogram, UpDownCounter};

use super::suppress::is_self_monitoring_tenant;

/// Shared handle to all application-level instruments.
pub struct AppMetrics {
    // HTTP server metrics (OTel HTTP semantic conventions)
    pub http_request_duration: Histogram<f64>,
    pub http_active_requests: UpDownCounter<i64>,
    pub http_request_body_size: Histogram<u64>,
    pub http_response_body_size: Histogram<u64>,

    // RPC server metrics (OTel RPC semantic conventions)
    pub rpc_server_duration: Histogram<f64>,

    // WAL metrics
    pub wal_entries_written: Counter<u64>,
    pub wal_entries_processed: Counter<u64>,
    pub wal_entries_pending: UpDownCounter<i64>,
    pub wal_flush_duration: Histogram<f64>,

    // Flight metrics
    pub flight_request_duration: Histogram<f64>,
    pub flight_bytes_sent: Counter<u64>,
    pub flight_bytes_received: Counter<u64>,
    pub flight_active_connections: UpDownCounter<i64>,

    // Query metrics
    pub query_duration: Histogram<f64>,
    pub query_rows_returned: Histogram<u64>,
    pub query_errors: Counter<u64>,

    // Ingestion metrics
    pub ingest_spans_received: Counter<u64>,
    pub ingest_logs_received: Counter<u64>,
    pub ingest_metrics_received: Counter<u64>,
    pub ingest_profiles_received: Counter<u64>,
    pub ingest_batches_written: Counter<u64>,
    pub ingest_batch_size: Histogram<u64>,

    // Tenant storage accounting
    pub tenant_storage_usage_bytes: Gauge<u64>,
}

static APP_METRICS: OnceLock<AppMetrics> = OnceLock::new();

/// Global application metrics handle.
///
/// First use binds the instruments to the current global meter provider, so
/// `init_telemetry` eagerly initializes this after installing the provider.
pub fn app_metrics() -> &'static AppMetrics {
    APP_METRICS.get_or_init(AppMetrics::from_global_meter)
}

/// Whether telemetry counters should include this tenant's traffic.
///
/// The `_system` tenant's traffic is SignalDB's own telemetry — counting it
/// would inflate ingestion metrics with self-monitoring data (feedback).
pub fn should_count_tenant(tenant_id: &str) -> bool {
    !is_self_monitoring_tenant(tenant_id)
}

impl AppMetrics {
    fn from_global_meter() -> Self {
        let meter = global::meter("signaldb");
        Self {
            http_request_duration: meter
                .f64_histogram("http.server.request.duration")
                .with_description("Duration of HTTP server requests")
                .with_unit("s")
                .build(),
            http_active_requests: meter
                .i64_up_down_counter("http.server.active_requests")
                .with_description("Number of in-flight HTTP server requests")
                .with_unit("{request}")
                .build(),
            http_request_body_size: meter
                .u64_histogram("http.server.request.body.size")
                .with_description("Size of HTTP server request bodies")
                .with_unit("By")
                .build(),
            http_response_body_size: meter
                .u64_histogram("http.server.response.body.size")
                .with_description("Size of HTTP server response bodies")
                .with_unit("By")
                .build(),
            rpc_server_duration: meter
                .f64_histogram("rpc.server.duration")
                .with_description("Duration of inbound RPC calls")
                .with_unit("ms")
                .build(),
            wal_entries_written: meter
                .u64_counter("signaldb.wal.entries_written")
                .with_description("WAL entries appended")
                .with_unit("{entry}")
                .build(),
            wal_entries_processed: meter
                .u64_counter("signaldb.wal.entries_processed")
                .with_description("WAL entries marked processed")
                .with_unit("{entry}")
                .build(),
            wal_entries_pending: meter
                .i64_up_down_counter("signaldb.wal.entries_pending")
                .with_description("WAL entries appended but not yet processed")
                .with_unit("{entry}")
                .build(),
            wal_flush_duration: meter
                .f64_histogram("signaldb.wal.flush.duration")
                .with_description("Duration of WAL flushes")
                .with_unit("s")
                .build(),
            flight_request_duration: meter
                .f64_histogram("signaldb.flight.request.duration")
                .with_description("Duration of Flight RPC handling")
                .with_unit("s")
                .build(),
            flight_bytes_sent: meter
                .u64_counter("signaldb.flight.bytes_sent")
                .with_description("Bytes sent over Flight")
                .with_unit("By")
                .build(),
            flight_bytes_received: meter
                .u64_counter("signaldb.flight.bytes_received")
                .with_description("Bytes received over Flight")
                .with_unit("By")
                .build(),
            flight_active_connections: meter
                .i64_up_down_counter("signaldb.flight.active_connections")
                .with_description("Active Flight connections")
                .with_unit("{connection}")
                .build(),
            query_duration: meter
                .f64_histogram("signaldb.query.duration")
                .with_description("Duration of query execution")
                .with_unit("s")
                .build(),
            query_rows_returned: meter
                .u64_histogram("signaldb.query.rows_returned")
                .with_description("Rows returned per query")
                .with_unit("{row}")
                .build(),
            query_errors: meter
                .u64_counter("signaldb.query.errors")
                .with_description("Query execution errors")
                .with_unit("{error}")
                .build(),
            ingest_spans_received: meter
                .u64_counter("signaldb.ingest.spans_received")
                .with_description("OTLP spans received")
                .with_unit("{span}")
                .build(),
            ingest_logs_received: meter
                .u64_counter("signaldb.ingest.logs_received")
                .with_description("OTLP log records received")
                .with_unit("{log}")
                .build(),
            ingest_metrics_received: meter
                .u64_counter("signaldb.ingest.metrics_received")
                .with_description("OTLP metric points received")
                .with_unit("{metric}")
                .build(),
            ingest_profiles_received: meter
                .u64_counter("signaldb.ingest.profiles_received")
                .with_description("OTLP profiles received")
                .with_unit("{profile}")
                .build(),
            ingest_batches_written: meter
                .u64_counter("signaldb.ingest.batches_written")
                .with_description("Record batches forwarded to storage")
                .with_unit("{batch}")
                .build(),
            ingest_batch_size: meter
                .u64_histogram("signaldb.ingest.batch_size")
                .with_description("Rows per forwarded record batch")
                .with_unit("{row}")
                .build(),
            tenant_storage_usage_bytes: meter
                .u64_gauge("signaldb.tenant.storage_usage")
                .with_description("Live Iceberg data-file bytes stored per tenant")
                .with_unit("By")
                .build(),
        }
    }
}

/// Decrements `http.server.active_requests` on drop so the gauge stays
/// balanced even if the downstream handler panics.
struct ActiveRequestGuard {
    attrs: [opentelemetry::KeyValue; 2],
}

impl Drop for ActiveRequestGuard {
    fn drop(&mut self) {
        app_metrics().http_active_requests.add(-1, &self.attrs);
    }
}

/// Axum middleware recording OTel HTTP server metrics for every request.
///
/// Attach with `axum::middleware::from_fn(http_metrics_middleware)`.
/// Requests carrying the `_system` tenant header are not measured
/// (anti-loop guard: they are SignalDB's own telemetry exports).
pub async fn http_metrics_middleware(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use opentelemetry::KeyValue;

    let is_system_request = request
        .headers()
        .get("x-tenant-id")
        .and_then(|v| v.to_str().ok())
        .is_some_and(is_self_monitoring_tenant);
    if is_system_request {
        return next.run(request).await;
    }

    let metrics = app_metrics();
    let method = request.method().as_str().to_owned();
    let request_body_size = request
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());

    let active_attrs = [
        KeyValue::new("http.request.method", method.clone()),
        KeyValue::new("url.scheme", "http"),
    ];
    metrics.http_active_requests.add(1, &active_attrs);
    let active_guard = ActiveRequestGuard {
        attrs: active_attrs,
    };
    let start = Instant::now();

    let response = next.run(request).await;

    drop(active_guard);
    let attrs = [
        KeyValue::new("http.request.method", method),
        KeyValue::new(
            "http.response.status_code",
            response.status().as_u16() as i64,
        ),
        KeyValue::new("url.scheme", "http"),
    ];
    metrics
        .http_request_duration
        .record(start.elapsed().as_secs_f64(), &attrs);
    if let Some(size) = request_body_size {
        metrics.http_request_body_size.record(size, &attrs);
    }
    if let Some(size) = response
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
    {
        metrics.http_response_body_size.record(size, &attrs);
    }

    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_metrics_initializes_against_noop_provider() {
        // Without a real global meter provider, instruments are no-ops but
        // must still construct and record without panicking.
        let metrics = app_metrics();
        metrics.wal_entries_written.add(1, &[]);
        metrics.http_active_requests.add(1, &[]);
        metrics.http_active_requests.add(-1, &[]);
        metrics.query_duration.record(0.001, &[]);
    }

    #[test]
    fn system_tenant_not_counted() {
        assert!(!should_count_tenant("_system"));
        assert!(should_count_tenant("acme"));
    }

    #[tokio::test]
    async fn http_metrics_middleware_passes_response_through() {
        use axum::{Router, body::Body, http::Request, routing::get};
        use tower::ServiceExt;

        let app = Router::new()
            .route("/ping", get(|| async { "pong" }))
            .layer(axum::middleware::from_fn(http_metrics_middleware));

        let response = app
            .oneshot(Request::builder().uri("/ping").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
    }
}
