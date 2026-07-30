//! # Emit helpers
//!
//! Thin wrappers over the OpenTelemetry API that keep the [scenarios](crate::scenarios)
//! readable and keep the three signals correlated:
//!
//! - [`open`] / [`close`] create spans with **simulated** start/end timestamps
//!   so a whole distributed trace can be laid out with realistic latencies
//!   without the generator actually sleeping.
//! - [`log`] emits a log record *inside* a span's context, so the backend can
//!   join it to the trace by `trace_id`/`span_id`.
//! - [`record`] records a histogram measurement inside a span's context, so the
//!   metric point carries a trace **exemplar** pointing back at the span.

use std::borrow::Cow;
use std::time::{Duration, SystemTime};

use opentelemetry::logs::{AnyValue, LogRecord, Logger, Severity};
use opentelemetry::metrics::Histogram;
use opentelemetry::trace::{SpanKind, TraceContextExt, Tracer};
use opentelemetry::{Context, KeyValue, Value};

use crate::fleet::ServiceTelemetry;

/// A scenario-local clock. All timestamps in a trace are derived from a single
/// base instant so parent spans strictly contain their children.
#[derive(Clone, Copy)]
pub struct Clock {
    base: SystemTime,
}

impl Clock {
    /// Anchors the clock `age_ms` in the past, so emitted traces look like they
    /// happened a moment ago rather than exactly now.
    pub fn starting(age_ms: f64) -> Self {
        Self {
            base: SystemTime::now() - Duration::from_micros((age_ms * 1_000.0) as u64),
        }
    }

    /// The wall-clock time `offset_ms` after the base instant.
    pub fn at(&self, offset_ms: f64) -> SystemTime {
        self.base + Duration::from_micros((offset_ms * 1_000.0).max(0.0) as u64)
    }
}

/// Opens a span in `svc`, parented to `parent`, starting at `start`. Returns a
/// [`Context`] carrying the new span; pass it as the parent of child spans and
/// close it with [`close`].
pub fn open(
    svc: &ServiceTelemetry,
    name: impl Into<Cow<'static, str>>,
    kind: SpanKind,
    parent: &Context,
    start: SystemTime,
    attrs: Vec<KeyValue>,
) -> Context {
    let span = svc
        .tracer
        .span_builder(name)
        .with_kind(kind)
        .with_start_time(start)
        .with_attributes(attrs)
        .start_with_context(&svc.tracer, parent);
    parent.with_span(span)
}

/// Ends the span held by `cx` at `end`.
pub fn close(cx: &Context, end: SystemTime) {
    cx.span().end_with_timestamp(end);
}

/// Marks the span held by `cx` as errored and records an exception event.
pub fn fail(cx: &Context, message: &str, exception_type: &str) {
    let span = cx.span();
    span.set_status(opentelemetry::trace::Status::error(message.to_string()));
    span.add_event(
        "exception",
        vec![
            KeyValue::new("exception.type", exception_type.to_string()),
            KeyValue::new("exception.message", message.to_string()),
            KeyValue::new("exception.escaped", true),
        ],
    );
}

/// Emits a log record correlated to the span held by `cx` (the SDK captures the
/// active span's `trace_id`/`span_id` from the attached context).
pub fn log(
    svc: &ServiceTelemetry,
    cx: &Context,
    severity: Severity,
    body: String,
    attrs: Vec<KeyValue>,
) {
    let _guard = cx.clone().attach();
    let mut record = svc.logger.create_log_record();
    record.set_timestamp(SystemTime::now());
    record.set_severity_number(severity);
    record.set_severity_text(severity_text(severity));
    record.set_body(AnyValue::from(body));
    for kv in attrs {
        record.add_attribute(kv.key.as_str().to_string(), value_to_any(&kv.value));
    }
    svc.logger.emit(record);
}

/// Records a histogram measurement correlated to the span held by `cx`, so the
/// point carries a trace exemplar.
pub fn record(cx: &Context, hist: &Histogram<f64>, value: f64, attrs: &[KeyValue]) {
    let _guard = cx.clone().attach();
    hist.record(value, attrs);
}

fn severity_text(severity: Severity) -> &'static str {
    match severity {
        Severity::Trace | Severity::Trace2 | Severity::Trace3 | Severity::Trace4 => "TRACE",
        Severity::Debug | Severity::Debug2 | Severity::Debug3 | Severity::Debug4 => "DEBUG",
        Severity::Info | Severity::Info2 | Severity::Info3 | Severity::Info4 => "INFO",
        Severity::Warn | Severity::Warn2 | Severity::Warn3 | Severity::Warn4 => "WARN",
        Severity::Error | Severity::Error2 | Severity::Error3 | Severity::Error4 => "ERROR",
        Severity::Fatal | Severity::Fatal2 | Severity::Fatal3 | Severity::Fatal4 => "FATAL",
    }
}

fn value_to_any(value: &Value) -> AnyValue {
    match value {
        Value::Bool(b) => AnyValue::from(*b),
        Value::I64(i) => AnyValue::from(*i),
        Value::F64(f) => AnyValue::from(*f),
        Value::String(s) => AnyValue::from(s.as_str().to_string()),
        other => AnyValue::from(other.to_string()),
    }
}
