//! W3C Trace Context propagation across Apache Arrow Flight calls.
//!
//! Lets distributed traces span SignalDB services: the Flight client side
//! injects the current span's context (`traceparent`/`tracestate`), the
//! server side extracts it and re-parents its processing span.
//!
//! Two carriers are supported, matching how SignalDB's Flight paths already
//! exchange metadata:
//! - **JSON `app_metadata`** on the first `FlightData` message (used by the
//!   Acceptor → Writer `do_put` path)
//! - **gRPC request metadata** headers (used by the Router → Querier `do_get`
//!   path)
//!
//! All functions go through the global OTel text-map propagator, which is a
//! no-op unless self-monitoring is enabled (it is installed by
//! `self_monitoring::init_telemetry`), so propagation adds no overhead when
//! disabled.

use std::collections::HashMap;

use opentelemetry::propagation::{Extractor, Injector};
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
    let cx = opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&MetadataMapExtractor(request.metadata()))
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
}
