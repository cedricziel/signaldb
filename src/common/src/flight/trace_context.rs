//! W3C Trace Context propagation across SignalDB service boundaries.
//!
//! Lets distributed traces span SignalDB services: the sending side injects
//! the current span's context (`traceparent`/`tracestate`), the receiving
//! side extracts it and re-parents (or links) its processing span.
//!
//! Four carriers are supported, matching how SignalDB's paths already exchange
//! metadata:
//! - **JSON `app_metadata`** on the first `FlightData` message (used by the
//!   Acceptor → Writer `do_put` path, and persisted into the WAL entry so the
//!   asynchronous processor can rejoin the ingest trace)
//! - **gRPC request metadata** headers (used by the Router → Querier `do_get`
//!   path)
//! - **HTTP request headers** (used at the Router's inbound HTTP boundary so a
//!   caller-supplied `traceparent` roots the query trace)
//! - **span links**, for the fan-in case where one processing span serves
//!   entries from several ingest traces (WAL batch processing)
//!
//! All functions go through the global OTel text-map propagator, which is a
//! no-op unless self-monitoring is enabled (it is installed by
//! `self_monitoring::init_telemetry`), so propagation adds no overhead when
//! disabled.

use std::collections::HashMap;

use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::TraceContextExt;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// W3C `traceparent` header/field name.
pub const TRACEPARENT: &str = "traceparent";
/// W3C `tracestate` header/field name.
pub const TRACESTATE: &str = "tracestate";

struct MetadataMapInjector<'a>(&'a mut tonic::metadata::MetadataMap);

impl Injector for MetadataMapInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(key), Ok(value)) = (
            key.parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>(),
            value.parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>(),
        ) {
            self.0.insert(key, value);
        }
    }
}

struct MetadataMapExtractor<'a>(&'a tonic::metadata::MetadataMap);

impl Extractor for MetadataMapExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .filter_map(|k| match k {
                tonic::metadata::KeyRef::Ascii(k) => Some(k.as_str()),
                tonic::metadata::KeyRef::Binary(_) => None,
            })
            .collect()
    }
}

/// Open a semconv Flight `DoGet` CLIENT span and inject *its* context into
/// the request's gRPC metadata, so the receiving Flight server span becomes
/// this span's child (not a sibling under the ambient span). `detail` is
/// the low-cardinality ticket verb. Callers instrument the call future
/// with the returned span and record its outcome via
/// [`crate::self_monitoring::spans::record_rpc_result`].
pub fn do_get_client_span<T>(
    detail: Option<&str>,
    request: &mut tonic::Request<T>,
) -> tracing::Span {
    let span = crate::self_monitoring::spans::rpc_client_span(
        crate::self_monitoring::spans::FLIGHT_DO_GET,
        detail,
        None,
    );
    span.in_scope(|| inject_context_into_request(request));
    span
}

/// Inject the current span's trace context into a tonic request's gRPC
/// metadata (client side, e.g. Router → Querier `do_get`).
pub fn inject_context_into_request<T>(request: &mut tonic::Request<T>) {
    let cx = tracing::Span::current().context();
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&cx, &mut MetadataMapInjector(request.metadata_mut()));
    });
}

/// Extract the remote trace context from a tonic request's gRPC metadata and
/// set it as `span`'s parent (server side, e.g. Querier `do_get`).
///
/// IMPORTANT: must be called before `span` is first entered —
/// tracing-opentelemetry consumes the span builder on first enter and the
/// parent can no longer be changed afterwards. Create the span, call this,
/// then run the handler via `.instrument(span)`.
pub fn set_parent_from_request<T>(span: &tracing::Span, request: &tonic::Request<T>) {
    set_parent_from_metadata(span, request.metadata());
}

/// Extract the remote trace context from gRPC metadata and set it as
/// `span`'s parent.
///
/// Variant of [`set_parent_from_request`] for call sites that have already
/// consumed the request and only kept its metadata. Same caveat: must be
/// called before `span` is first entered.
pub fn set_parent_from_metadata(span: &tracing::Span, metadata: &tonic::metadata::MetadataMap) {
    let cx = opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&MetadataMapExtractor(metadata))
    });
    if let Err(err) = span.set_parent(cx) {
        // Benign when no OTel layer is attached (self-monitoring disabled).
        tracing::debug!(error = %err, "Failed to adopt remote trace context");
    }
}

/// The current span's trace context as W3C header values, for embedding in
/// Flight `app_metadata` JSON (client side, e.g. Acceptor → Writer `do_put`).
///
/// Returns `None` when there is no active sampled context to propagate (e.g.
/// self-monitoring disabled).
pub fn current_trace_context_fields() -> Option<(String, Option<String>)> {
    let cx = tracing::Span::current().context();
    let mut carrier: HashMap<String, String> = HashMap::new();
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&cx, &mut carrier);
    });
    let traceparent = carrier.remove(TRACEPARENT)?;
    let tracestate = carrier.remove(TRACESTATE);
    Some((traceparent, tracestate))
}

/// Set a context carried in Flight `app_metadata` fields as `span`'s parent
/// (server side, e.g. Writer `do_put`).
///
/// No-op when `traceparent` is absent or invalid. Like
/// [`set_parent_from_request`], must be called before `span` is first
/// entered.
pub fn set_parent_from_fields(
    span: &tracing::Span,
    traceparent: Option<&str>,
    tracestate: Option<&str>,
) {
    let Some(traceparent) = traceparent else {
        return;
    };
    let mut carrier: HashMap<String, String> = HashMap::new();
    carrier.insert(TRACEPARENT.to_string(), traceparent.to_string());
    if let Some(tracestate) = tracestate {
        carrier.insert(TRACESTATE.to_string(), tracestate.to_string());
    }
    let cx =
        opentelemetry::global::get_text_map_propagator(|propagator| propagator.extract(&carrier));
    if let Err(err) = span.set_parent(cx) {
        // Benign when no OTel layer is attached (self-monitoring disabled).
        tracing::debug!(error = %err, "Failed to adopt remote trace context");
    }
}

/// Link `span` to a trace context carried in W3C header fields, without
/// changing its parent.
///
/// Used when one span serves work fanned in from several distinct traces —
/// e.g. the WAL background processor committing a batch whose entries came
/// from different ingest requests. Unlike a parent, a span may carry many
/// links, so each source ingest trace stays reachable from the batch span.
///
/// No-op when `traceparent` is absent, invalid, or self-monitoring is
/// disabled. Safe to call after the span has been entered (links, unlike the
/// parent, can be added at any time).
pub fn add_link_from_fields(
    span: &tracing::Span,
    traceparent: Option<&str>,
    tracestate: Option<&str>,
) {
    let Some(traceparent) = traceparent else {
        return;
    };
    let mut carrier: HashMap<String, String> = HashMap::new();
    carrier.insert(TRACEPARENT.to_string(), traceparent.to_string());
    if let Some(tracestate) = tracestate {
        carrier.insert(TRACESTATE.to_string(), tracestate.to_string());
    }
    let cx =
        opentelemetry::global::get_text_map_propagator(|propagator| propagator.extract(&carrier));
    let span_context = cx.span().span_context().clone();
    if span_context.is_valid() {
        span.add_link(span_context);
    }
}

/// Render a span context as a W3C `traceparent` value
/// (`00-<trace-id>-<span-id>-<trace-flags>`), e.g. for returning the server
/// side of a trace to HTTP callers (`Server-Timing`/`traceresponse` response
/// headers).
///
/// Returns `None` for an invalid (all-zero) context — the tracing-otel bridge
/// yields one when self-monitoring is disabled — so callers never emit a
/// meaningless header.
pub fn format_traceparent(span_context: &opentelemetry::trace::SpanContext) -> Option<String> {
    if !span_context.is_valid() {
        return None;
    }
    Some(format!(
        "00-{}-{}-{:02x}",
        span_context.trace_id(),
        span_context.span_id(),
        span_context.trace_flags().to_u8(),
    ))
}

struct HeaderMapExtractor<'a>(&'a axum::http::HeaderMap);

impl Extractor for HeaderMapExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

/// Extract the remote trace context from inbound HTTP request headers and set
/// it as `span`'s parent (server side, e.g. the Router's HTTP query APIs).
///
/// Lets an external caller that propagates W3C trace context (`traceparent`)
/// have SignalDB's query trace join it. No-op when the header is absent,
/// invalid, or self-monitoring is disabled. Like [`set_parent_from_metadata`],
/// must be called before `span` is first entered.
pub fn set_parent_from_http_headers(span: &tracing::Span, headers: &axum::http::HeaderMap) {
    let cx = opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderMapExtractor(headers))
    });
    if let Err(err) = span.set_parent(cx) {
        // Benign when no OTel layer is attached (self-monitoring disabled).
        tracing::debug!(error = %err, "Failed to adopt remote trace context from HTTP headers");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::propagation::TextMapPropagator;
    use opentelemetry_sdk::propagation::TraceContextPropagator;

    // The global propagator is process-wide state shared across tests, so
    // these tests exercise the injector/extractor adapters against a local
    // TraceContextPropagator instead of mutating the global.

    const SAMPLE_TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    #[test]
    fn tonic_metadata_extractor_reads_traceparent() {
        let mut request = tonic::Request::new(());
        request.metadata_mut().insert(
            TRACEPARENT,
            SAMPLE_TRACEPARENT.parse().expect("valid metadata value"),
        );

        let propagator = TraceContextPropagator::new();
        let cx = propagator.extract(&MetadataMapExtractor(request.metadata()));
        use opentelemetry::trace::TraceContextExt;
        let span_context = cx.span().span_context().clone();
        assert!(span_context.is_valid());
        assert_eq!(
            span_context.trace_id().to_string(),
            "0af7651916cd43dd8448eb211c80319c"
        );
        assert!(span_context.is_remote());
    }

    #[test]
    fn tonic_metadata_injector_roundtrip() {
        // Extract a context from a known traceparent, inject into fresh
        // metadata, and verify the header round-trips.
        let propagator = TraceContextPropagator::new();
        let mut carrier: HashMap<String, String> = HashMap::new();
        carrier.insert(TRACEPARENT.to_string(), SAMPLE_TRACEPARENT.to_string());
        let cx = propagator.extract(&carrier);

        let mut request = tonic::Request::new(());
        propagator.inject_context(&cx, &mut MetadataMapInjector(request.metadata_mut()));

        let value = request
            .metadata()
            .get(TRACEPARENT)
            .and_then(|v| v.to_str().ok())
            .expect("traceparent injected");
        assert_eq!(value, SAMPLE_TRACEPARENT);
    }

    #[test]
    fn http_header_extractor_reads_traceparent() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            TRACEPARENT,
            SAMPLE_TRACEPARENT.parse().expect("valid header value"),
        );

        let propagator = TraceContextPropagator::new();
        let cx = propagator.extract(&HeaderMapExtractor(&headers));
        use opentelemetry::trace::TraceContextExt;
        let span_context = cx.span().span_context().clone();
        assert!(span_context.is_valid());
        assert_eq!(
            span_context.trace_id().to_string(),
            "0af7651916cd43dd8448eb211c80319c"
        );
        assert!(span_context.is_remote());
    }

    #[test]
    fn add_link_and_http_parent_ignore_missing_or_garbage() {
        // Must not panic on a disabled span / without a global propagator.
        let span = tracing::Span::none();
        add_link_from_fields(&span, None, None);
        add_link_from_fields(&span, Some("garbage"), None);
        set_parent_from_http_headers(&span, &axum::http::HeaderMap::new());
    }

    #[test]
    fn set_parent_ignores_missing_traceparent() {
        // Must not panic on a disabled span / without a global propagator.
        let span = tracing::Span::none();
        set_parent_from_fields(&span, None, None);
        set_parent_from_fields(&span, Some("garbage"), None);
    }

    #[test]
    fn current_trace_context_fields_is_none_without_otel_layer() {
        // Without an OTel-enabled subscriber and global propagator, there is
        // no sampled context to propagate.
        assert!(current_trace_context_fields().is_none());
    }

    #[test]
    fn format_traceparent_renders_sampled_context() {
        use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};
        let ctx = SpanContext::new(
            TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").expect("valid trace id"),
            SpanId::from_hex("b7ad6b7169203331").expect("valid span id"),
            TraceFlags::SAMPLED,
            false,
            TraceState::default(),
        );
        assert_eq!(
            format_traceparent(&ctx).as_deref(),
            Some(SAMPLE_TRACEPARENT)
        );
    }

    #[test]
    fn format_traceparent_reflects_unsampled_flags() {
        use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};
        let ctx = SpanContext::new(
            TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").expect("valid trace id"),
            SpanId::from_hex("b7ad6b7169203331").expect("valid span id"),
            TraceFlags::default(),
            false,
            TraceState::default(),
        );
        assert_eq!(
            format_traceparent(&ctx).as_deref(),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00")
        );
    }

    #[test]
    fn format_traceparent_rejects_invalid_context() {
        use opentelemetry::trace::SpanContext;
        // An all-zero (invalid) context must never be rendered into a header.
        assert_eq!(format_traceparent(&SpanContext::empty_context()), None);
    }
}
