//! Recording errors on spans per the OpenTelemetry exception conventions.
//!
//! See <https://opentelemetry.io/docs/specs/otel/trace/exceptions/>.
//!
//! The `exception` span event is written through the OpenTelemetry API rather
//! than derived from the `ERROR` log event: both `tracing` bridges read the
//! same `message` field — the span layer as the event name, the log layer as
//! the body — so one event cannot be both a named `exception` and a log line
//! with a body (#1825).

use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Record `error` on the current span as an OpenTelemetry `exception` event and
/// set the span status to error.
///
/// Call this at an error boundary — a point where an error is about to be
/// converted into a transport status (e.g. a Flight `tonic::Status`) and the
/// underlying reason would otherwise be lost. Context (which operation, which
/// tenant) belongs on the surrounding span, not here.
///
/// Safe to call when no span or telemetry is active: `Span::current()` is a
/// disabled span and the span operations become no-ops.
pub fn record_span_exception(error: &(dyn std::error::Error + 'static)) {
    let message = error.to_string();
    tracing::error!("{message}");
    let span = tracing::Span::current();
    span.add_event(
        "exception",
        vec![opentelemetry::KeyValue::new(
            opentelemetry_semantic_conventions::attribute::EXCEPTION_MESSAGE,
            message.clone(),
        )],
    );
    span.set_status(opentelemetry::trace::Status::error(message));
}
