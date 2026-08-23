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
    pub wal_corrupt_entries: Counter<u64>,
    pub wal_list_failures: Counter<u64>,
    pub wal_instances: UpDownCounter<i64>,

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

    // Writer commit-coalescing: groups held back by the floor on the last
    // processing cycle. A sustained non-zero value alongside rising
    // `signaldb.wal.entries_pending` indicates the commit path is stalling.
    pub writer_groups_deferred: Gauge<u64>,

    // WAL entries left unprocessed on the last drain cycle because the
    // per-WAL byte budget (`[writer].max_drain_bytes_per_cycle`) was
    // reached before they were decoded. A sustained non-zero value means a
    // WAL's backlog is larger than one cycle's budget and is draining
    // across several ticks rather than in one.
    pub writer_entries_deferred_by_budget: Gauge<u64>,

    // How long one group's Iceberg commit took, by tenant. Groups commit
    // concurrently (#1306), so a tenant with slow commits shows up as that
    // tenant's latency rather than as everyone's — which is the whole point
    // of the fan-out and the way to tell whether it is holding.
    pub writer_commit_duration: Histogram<f64>,

    // Group commit failures, by tenant and `kind` (`permanent` | `transient`).
    // Only `permanent` counts toward an entry's dead-lettering budget; a
    // sustained `transient` rate without a matching drop in
    // `signaldb.wal.entries_pending` means a catalog/object-store dependency
    // is down (W1).
    pub writer_commit_failures: Counter<u64>,

    // Signal-table reconciliation: tables the writer provisioned ahead of a
    // first write, and provisioning attempts that failed. A rising failure
    // count means the deployment has degraded to create-on-first-write.
    pub writer_tables_provisioned: Counter<u64>,
    pub writer_table_provisioning_failures: Counter<u64>,

    // MCP server audit: one count per tool call by tool and outcome
    // (`ok | truncated | denied | throttled | error`), and the call duration
    // by tool. Prometheus renders them as `signaldb_mcp_tool_calls_total`
    // and `signaldb_mcp_tool_call_duration_seconds`.
    pub mcp_tool_calls: Counter<u64>,
    pub mcp_tool_call_duration: Histogram<f64>,

    // Rate limiting: one increment per rejected request, labelled by the
    // surface that rejected it (`query`, `admin`, `otlp_http`, `otlp_grpc`,
    // `prometheus`) and the exhausted dimension (`query_requests`,
    // `requests`, `bytes`, `quota`). Exported as
    // `signaldb_rate_limit_rejections_total` over Prometheus.
    pub rate_limit_rejections: Counter<u64>,
}

/// Attribute key naming the tool on the MCP metrics (`gen_ai.tool.name`).
pub const MCP_TOOL_ATTR: &str = "gen_ai.tool.name";
/// Attribute key carrying the audit outcome on the MCP call counter.
pub const MCP_OUTCOME_ATTR: &str = "signaldb.mcp.outcome";

impl AppMetrics {
    /// Record one MCP tool call: bumps `signaldb.mcp.tool_calls` for the
    /// `(tool, outcome)` pair and records its duration under the tool.
    pub fn record_mcp_tool_call(&self, tool: &str, outcome: &str, duration: std::time::Duration) {
        use opentelemetry::KeyValue;
        self.mcp_tool_calls.add(
            1,
            &[
                KeyValue::new(MCP_TOOL_ATTR, tool.to_owned()),
                KeyValue::new(MCP_OUTCOME_ATTR, outcome.to_owned()),
            ],
        );
        self.mcp_tool_call_duration.record(
            duration.as_secs_f64(),
            &[KeyValue::new(MCP_TOOL_ATTR, tool.to_owned())],
        );
    }
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

/// Whether a request's `x-tenant-id` header names the `_system` self-monitoring
/// tenant. Shared anti-loop guard used by [`http_metrics_middleware`] and
/// [`http_trace_context_middleware`] to skip instrumenting/re-ingesting
/// SignalDB's own telemetry exports.
fn is_system_tenant_request(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get("x-tenant-id")
        .and_then(|v| v.to_str().ok())
        .is_some_and(is_self_monitoring_tenant)
}

impl AppMetrics {
    fn from_global_meter() -> Self {
        let meter = global::meter_with_scope(
            opentelemetry::InstrumentationScope::builder("signaldb")
                .with_schema_url(super::SIGNALDB_SCHEMA_URL)
                .build(),
        );
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
            wal_corrupt_entries: meter
                .u64_counter("signaldb.wal.corrupt_entries")
                .with_description(
                    "WAL entries discarded during replay because they could not be deserialized",
                )
                .with_unit("{entry}")
                .build(),
            wal_list_failures: meter
                .u64_counter("signaldb.wal.list_failures")
                .with_description(
                    "Attempts to list a WAL's unprocessed entries that failed, so the WAL was \
                     skipped for that processing cycle",
                )
                .with_unit("{failure}")
                .build(),
            wal_instances: meter
                .i64_up_down_counter("signaldb.wal.instances")
                .with_description(
                    "Open WAL instances held by this service, one per tenant/dataset/signal",
                )
                .with_unit("{wal}")
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
            writer_groups_deferred: meter
                .u64_gauge("signaldb.writer.groups_deferred")
                .with_description(
                    "Writer groups deferred by the commit-coalescing floor last cycle",
                )
                .with_unit("{group}")
                .build(),
            writer_entries_deferred_by_budget: meter
                .u64_gauge("signaldb.writer.entries_deferred_by_budget")
                .with_description(
                    "WAL entries left unprocessed last cycle by the per-cycle drain byte budget",
                )
                .with_unit("{entry}")
                .build(),
            writer_commit_duration: meter
                .f64_histogram("signaldb.writer.commit_duration")
                .with_description("Duration of one writer group's Iceberg commit, by tenant")
                .with_unit("s")
                .build(),
            writer_commit_failures: meter
                .u64_counter("signaldb.writer.commit_failures")
                .with_description(
                    "Writer group commit failures, by tenant and kind (permanent | transient)",
                )
                .with_unit("{failure}")
                .build(),
            writer_tables_provisioned: meter
                .u64_counter("signaldb.writer.tables_provisioned")
                .with_description("Signal tables created by the table reconciler")
                .with_unit("{table}")
                .build(),
            writer_table_provisioning_failures: meter
                .u64_counter("signaldb.writer.table_provisioning_failures")
                .with_description("Signal tables the reconciler could not create")
                .with_unit("{table}")
                .build(),
            mcp_tool_calls: meter
                .u64_counter("signaldb.mcp.tool_calls")
                .with_description("MCP tool calls by tool and audit outcome")
                .with_unit("{call}")
                .build(),
            mcp_tool_call_duration: meter
                .f64_histogram("signaldb.mcp.tool_call.duration")
                .with_description("Duration of MCP tool calls by tool")
                .with_unit("s")
                .build(),
            rate_limit_rejections: meter
                .u64_counter("signaldb.rate_limit.rejections")
                .with_description(
                    "Requests rejected by a per-tenant rate limit or quota, by surface and dimension",
                )
                .with_unit("{rejection}")
                .build(),
        }
    }
}

/// Record one rate-limit rejection, labelled by the rejecting surface
/// (`query`, `admin`, `otlp_http`, `otlp_grpc`, `prometheus`) and the
/// exhausted dimension (`query_requests`, `requests`, `bytes`, `quota`).
/// Both are bounded, low-cardinality `&'static str` by design.
pub fn record_rate_limit_rejection(surface: &'static str, kind: &'static str) {
    use opentelemetry::KeyValue;

    app_metrics().rate_limit_rejections.add(
        1,
        &[
            KeyValue::new("surface", surface),
            KeyValue::new("kind", kind),
        ],
    );
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

    if is_system_tenant_request(request.headers()) {
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

/// OTel HTTP server span name: `{method} {http.route}` when a low-cardinality
/// route template is available, else just `{method}`.
///
/// Per the OpenTelemetry HTTP semantic conventions, the route template (e.g.
/// `/tempo/api/traces/{trace_id}`) is what belongs in the span name — never the
/// raw request path, which carries unbounded ids. When no route matched (there
/// is no low-cardinality target) the name falls back to the method alone.
fn http_server_span_name(method: &str, route: Option<&str>) -> String {
    match route {
        Some(route) => format!("{method} {route}"),
        None => method.to_owned(),
    }
}

/// Map an HTTP version to the OTel `network.protocol.version` value.
fn network_protocol_version(version: axum::http::Version) -> &'static str {
    use axum::http::Version;
    match version {
        v if v == Version::HTTP_09 => "0.9",
        v if v == Version::HTTP_10 => "1.0",
        v if v == Version::HTTP_11 => "1.1",
        v if v == Version::HTTP_2 => "2",
        v if v == Version::HTTP_3 => "3",
        _ => "unknown",
    }
}

/// Root each inbound HTTP request in a SERVER span whose parent is the
/// caller-supplied W3C trace context (`traceparent`), so an external client
/// that propagates context sees SignalDB's query trace join theirs instead of
/// starting a detached root. Downstream `#[instrument]` handler spans and the
/// Flight calls they make become children of this span.
///
/// The span follows the OpenTelemetry HTTP semantic conventions: it is named
/// `{method} {http.route}` (low-cardinality route template, not the raw path),
/// tagged `SpanKind::Server`, and carries `http.request.method`, `http.route`,
/// `url.path`, `url.scheme`, `server.address`, `network.protocol.version`,
/// `user_agent.original`, and `http.response.status_code`. A 5xx response marks
/// the span status as error.
///
/// Mirrors [`http_metrics_middleware`]'s anti-loop guard: `_system` tenant
/// requests bypass the span so self-monitoring queries are not re-instrumented
/// and re-ingested. No-op when self-monitoring is disabled (the parent
/// adoption goes through the global propagator, which is then a no-op).
pub async fn http_trace_context_middleware(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use tracing::Instrument;

    if is_system_tenant_request(request.headers()) {
        return next.run(request).await;
    }

    // The matched route template (`http.route`) is the low-cardinality name
    // source; `.layer()` runs after routing, so it is already in extensions.
    let method = request.method().as_str().to_owned();
    let route = request
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|m| m.as_str().to_owned());
    let scheme = request.uri().scheme_str().unwrap_or("http").to_owned();
    // Host header splits into `server.address` / `server.port`.
    let (server_address, server_port) = match request
        .headers()
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok())
    {
        Some(host) => match host.rsplit_once(':') {
            Some((addr, port)) if port.chars().all(|c| c.is_ascii_digit()) => {
                (Some(addr.to_owned()), Some(port.to_owned()))
            }
            _ => (Some(host.to_owned()), None),
        },
        None => (None, None),
    };
    // Client-most X-Forwarded-For entry, when a proxy supplies one.
    let client_address = request
        .headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty());
    let user_agent = request
        .headers()
        .get(axum::http::header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    let span = tracing::info_span!(
        "http.request",
        // `otel.*` fields are interpreted by the tracing-opentelemetry bridge:
        // `otel.name` overrides the span name, `otel.kind` sets the span kind,
        // `otel.status_code` sets the span status.
        otel.name = tracing::field::Empty,
        otel.kind = "server",
        otel.status_code = tracing::field::Empty,
        http.request.method = %method,
        http.route = tracing::field::Empty,
        url.path = %request.uri().path(),
        url.scheme = %scheme,
        server.address = tracing::field::Empty,
        server.port = tracing::field::Empty,
        client.address = tracing::field::Empty,
        network.protocol.version = network_protocol_version(request.version()),
        user_agent.original = tracing::field::Empty,
        http.response.status_code = tracing::field::Empty,
        error.r#type = tracing::field::Empty,
    );
    span.record(
        "otel.name",
        http_server_span_name(&method, route.as_deref()),
    );
    if let Some(route) = &route {
        span.record("http.route", route.as_str());
    }
    if let Some(server_address) = &server_address {
        span.record("server.address", server_address.as_str());
    }
    if let Some(server_port) = server_port.as_ref().and_then(|p| p.parse::<i64>().ok()) {
        span.record("server.port", server_port);
    }
    if let Some(client_address) = &client_address {
        span.record("client.address", client_address.as_str());
    }
    if let Some(user_agent) = &user_agent {
        span.record("user_agent.original", user_agent.as_str());
    }
    // Parent must be adopted before the span is first entered.
    crate::flight::trace_context::set_parent_from_http_headers(&span, request.headers());

    let start = std::time::Instant::now();
    let mut response = next.run(request).instrument(span.clone()).await;

    let status = response.status();
    span.record("http.response.status_code", status.as_u16() as i64);
    // Server spans fail only on 5xx (a 4xx is the caller's problem);
    // `error.type` is the status code as a string, per HTTP semconv.
    if status.is_server_error() {
        span.record("otel.status_code", "ERROR");
        span.record("error.type", status.as_u16().to_string().as_str());
    }
    append_trace_response_headers(&mut response, &span, start.elapsed());
    response
}

/// Named server-side stage durations a handler wants surfaced as
/// `Server-Timing` entries on its response.
///
/// Handlers opt in by inserting a value into the response extensions
/// (e.g. returning `(axum::Extension(timings), body)`); the trace-context
/// middleware drains it and appends one `<name>;dur=<ms>` entry per stage.
/// Names are `&'static str` by design: Server-Timing entry names are a
/// low-cardinality token grammar, not a place for runtime values.
#[derive(Debug, Clone, Default)]
pub struct ServerTimings(Vec<(&'static str, std::time::Duration)>);

impl ServerTimings {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a stage duration under `name` (a Server-Timing token, e.g.
    /// `plan` or `storage_scan`).
    pub fn push(&mut self, name: &'static str, duration: std::time::Duration) {
        self.0.push((name, duration));
    }

    /// The recorded stages in insertion order.
    pub fn entries(&self) -> &[(&'static str, std::time::Duration)] {
        &self.0
    }
}

/// Return the server span's trace context and timing to the caller:
/// `Server-Timing: traceparent;desc="..."` (the de-facto RUM back-channel,
/// readable by browsers via the Performance API even on document/resource
/// requests), the W3C Trace Context Level 2 `traceresponse` header, and
/// `Timing-Allow-Origin` so cross-origin pages may read the timing entries.
///
/// No-op when the span context is invalid — self-monitoring disabled — so an
/// all-zero context is never emitted.
fn append_trace_response_headers(
    response: &mut axum::response::Response,
    span: &tracing::Span,
    elapsed: std::time::Duration,
) {
    use opentelemetry::trace::TraceContextExt;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    // Drain unconditionally: stage timings never travel past this middleware,
    // whether or not they end up in a header.
    let stage_timings = response.extensions_mut().remove::<ServerTimings>();

    let context = span.context();
    let span_context = context.span().span_context().clone();
    let Some(traceparent) = crate::flight::trace_context::format_traceparent(&span_context) else {
        return;
    };

    let mut server_timing = format!("traceparent;desc=\"{traceparent}\"");
    for (name, duration) in stage_timings.iter().flat_map(|t| t.0.iter()) {
        let ms = duration.as_secs_f64() * 1e3;
        server_timing.push_str(&format!(", {name};dur={ms:.3}"));
    }
    let total_ms = elapsed.as_secs_f64() * 1e3;
    server_timing.push_str(&format!(", total;dur={total_ms:.3}"));

    let headers = response.headers_mut();
    if let Ok(value) = axum::http::HeaderValue::from_str(&server_timing) {
        headers.insert(axum::http::HeaderName::from_static("server-timing"), value);
    }
    if let Ok(value) = axum::http::HeaderValue::from_str(&traceparent) {
        headers.insert(axum::http::HeaderName::from_static("traceresponse"), value);
    }
    headers.insert(
        axum::http::HeaderName::from_static("timing-allow-origin"),
        axum::http::HeaderValue::from_static("*"),
    );
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
    fn record_rate_limit_rejection_does_not_panic_against_noop_provider() {
        // No real meter provider is installed in unit tests, so this only
        // exercises that the recording site is wired up (bounded labels,
        // builds and records without panicking); the counted value isn't
        // observable without an exporter.
        record_rate_limit_rejection("query", "query_requests");
        record_rate_limit_rejection("admin", "quota");
    }

    #[test]
    fn system_tenant_not_counted() {
        assert!(!should_count_tenant("_system"));
        assert!(should_count_tenant("acme"));
    }

    #[test]
    fn server_span_name_uses_route_template_else_method() {
        assert_eq!(
            http_server_span_name("GET", Some("/tempo/api/traces/{trace_id}")),
            "GET /tempo/api/traces/{trace_id}"
        );
        // No matched route (e.g. an unrouted request): method only, never the
        // high-cardinality raw path.
        assert_eq!(http_server_span_name("POST", None), "POST");
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

    #[tokio::test]
    async fn http_trace_context_middleware_passes_through_with_and_without_traceparent() {
        use axum::{Router, body::Body, http::Request, routing::get};
        use tower::ServiceExt;

        let app = Router::new()
            .route("/ping", get(|| async { "pong" }))
            .layer(axum::middleware::from_fn(http_trace_context_middleware));

        // With a caller-supplied traceparent (adoption is a no-op without an
        // OTel layer, but must not panic and must pass the response through).
        let with_tp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/ping")
                    .header(
                        "traceparent",
                        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(with_tp.status(), axum::http::StatusCode::OK);

        // Without any trace headers.
        let without = app
            .oneshot(Request::builder().uri("/ping").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(without.status(), axum::http::StatusCode::OK);

        // Without an OTel layer (self-monitoring disabled) the span context is
        // invalid, so no trace/timing response headers may be emitted.
        for response in [&with_tp, &without] {
            assert!(response.headers().get("server-timing").is_none());
            assert!(response.headers().get("traceresponse").is_none());
            assert!(response.headers().get("timing-allow-origin").is_none());
        }
    }
}
