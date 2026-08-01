//! Recording errors on spans per the OpenTelemetry exception conventions.
//!
//! See <https://opentelemetry.io/docs/specs/otel/trace/exceptions/>.
//!
//! The OTLP span layer installed in [`crate::cli::init_logging`] converts an
//! `ERROR`-level `tracing` event that carries an `error` field into an
//! `exception` span event (`exception.type`, `exception.message`,
//! `exception.stacktrace`). This module pairs that with an explicit error
//! status so the failure is both attributed and visibly red in the trace —
//! the two things an error return needs to be diagnosable after the fact.

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
/// disabled span and both operations become no-ops.
pub fn record_span_exception(error: &(dyn std::error::Error + 'static)) {
    // Recording the `error` field via `Display` (with no `message`) is the shape
    // the layer turns into a span event named `exception` carrying
    // `exception.message` — the bare `error` field instead hits `record_error`,
    // which adds the attribute but never names the event. The ERROR level also
    // lands in the console/log layers, giving these previously-silent paths a
    // log line. `error_events_to_status` marks the span, and we set it
    // explicitly too so the helper does not rely on the layer's config.
    tracing::error!(error = %error);
    tracing::Span::current().set_status(opentelemetry::trace::Status::error(error.to_string()));
}
