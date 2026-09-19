//! Public evaluator entry points: `CompiledProgram::apply_{traces,logs,metrics}`.

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use opentelemetry_proto::tonic::metrics::v1::Metric;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::Span;

use crate::compile::{CompiledProgram, MapTarget, ScalarTarget};
use crate::engine::{Frame, run_statement};
use crate::error::ApplyError;
use crate::value::Value;

/// How a runtime error in one statement affects the rest of the program.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ErrorMode {
    /// Record the error and continue with the next statement/item (default upstream
    /// behaviour); the crate itself only logs at `tracing::debug!`.
    #[default]
    Ignore,
    /// Like `Ignore` but the crate emits no log line at all; the caller decides.
    Silent,
    /// Abort on the first runtime error.
    Propagate,
}

/// The error naming an unrecognized `error_mode` string. Kept as a plain
/// string rather than a richer type since it only ever surfaces as a
/// validation message to the caller.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("error_mode `{0}` must be one of ignore, silent, propagate")]
pub struct ParseErrorModeError(pub String);

impl std::str::FromStr for ErrorMode {
    type Err = ParseErrorModeError;

    /// The single source of truth for the string form of `ErrorMode`, used
    /// both to validate a processor at write time and to interpret a stored
    /// value at apply time — an unrecognized string is always an error here,
    /// never silently treated as `Ignore`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ignore" => Ok(Self::Ignore),
            "silent" => Ok(Self::Silent),
            "propagate" => Ok(Self::Propagate),
            other => Err(ParseErrorModeError(other.to_string())),
        }
    }
}

/// Per-statement counters returned by `apply_*`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatementStats {
    /// Number of leaf items whose `where` guard matched (or that had no guard) and
    /// whose editor ran without error.
    pub matched: u64,
    /// Number of leaf items where the statement raised a runtime error.
    pub errors: u64,
}

/// The outcome of applying a [`CompiledProgram`] to a request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApplyReport {
    pub statements: Vec<StatementStats>,
}

impl ApplyReport {
    fn new(len: usize) -> Self {
        ApplyReport {
            statements: vec![StatementStats::default(); len],
        }
    }

    fn record(&mut self, index: usize, matched: bool, errored: bool) {
        let stats = &mut self.statements[index];
        if errored {
            stats.errors += 1;
        } else if matched {
            stats.matched += 1;
        }
    }
}

fn run_program(
    program: &CompiledProgram,
    frame: &mut dyn Frame,
    mode: ErrorMode,
    report: &mut ApplyReport,
) -> Result<(), ApplyError> {
    for (index, stmt) in program.statements.iter().enumerate() {
        match run_statement(frame, stmt) {
            Ok(matched) => report.record(index, matched, false),
            Err(message) => {
                report.record(index, false, true);
                match mode {
                    ErrorMode::Propagate => {
                        return Err(ApplyError {
                            statement: index,
                            message,
                        });
                    }
                    ErrorMode::Ignore => {
                        tracing::debug!(statement = index, error = %message, "ottl statement failed");
                    }
                    ErrorMode::Silent => {}
                }
            }
        }
    }
    Ok(())
}

struct TraceFrame<'a> {
    resource: &'a mut Resource,
    scope: &'a mut InstrumentationScope,
    span: &'a mut Span,
}

impl Frame for TraceFrame<'_> {
    fn get_scalar(&self, target: ScalarTarget) -> Value {
        match target {
            ScalarTarget::ScopeName => Value::String(self.scope.name.clone()),
            ScalarTarget::ScopeVersion => Value::String(self.scope.version.clone()),
            ScalarTarget::SpanName => Value::String(self.span.name.clone()),
            ScalarTarget::SpanKind => Value::Int(i64::from(self.span.kind)),
            ScalarTarget::StatusCode => match self.span.status.as_ref() {
                Some(status) => Value::Int(i64::from(status.code)),
                None => Value::Nil,
            },
            ScalarTarget::StatusMessage => match self.span.status.as_ref() {
                Some(status) => Value::String(status.message.clone()),
                None => Value::Nil,
            },
            _ => Value::Nil,
        }
    }

    fn set_scalar(&mut self, target: ScalarTarget, value: Value) -> Result<(), String> {
        match target {
            ScalarTarget::ScopeName => self.scope.name = coerce_string(value)?,
            ScalarTarget::ScopeVersion => self.scope.version = coerce_string(value)?,
            ScalarTarget::SpanName => self.span.name = coerce_string(value)?,
            ScalarTarget::SpanKind => self.span.kind = coerce_i32(value)?,
            ScalarTarget::StatusCode => {
                let code = coerce_i32(value)?;
                self.status_mut().code = code;
            }
            ScalarTarget::StatusMessage => {
                let message = coerce_string(value)?;
                self.status_mut().message = message;
            }
            other => return Err(format!("{other:?} is not writable for traces")),
        }
        Ok(())
    }

    fn map_mut(&mut self, target: MapTarget) -> &mut Vec<KeyValue> {
        match target {
            MapTarget::Resource => &mut self.resource.attributes,
            MapTarget::Scope => &mut self.scope.attributes,
            MapTarget::Leaf => &mut self.span.attributes,
        }
    }
}

impl TraceFrame<'_> {
    fn status_mut(&mut self) -> &mut opentelemetry_proto::tonic::trace::v1::Status {
        self.span.status.get_or_insert_with(Default::default)
    }
}

struct LogFrame<'a> {
    resource: &'a mut Resource,
    scope: &'a mut InstrumentationScope,
    log: &'a mut LogRecord,
}

impl Frame for LogFrame<'_> {
    fn get_scalar(&self, target: ScalarTarget) -> Value {
        match target {
            ScalarTarget::ScopeName => Value::String(self.scope.name.clone()),
            ScalarTarget::ScopeVersion => Value::String(self.scope.version.clone()),
            ScalarTarget::LogSeverityText => Value::String(self.log.severity_text.clone()),
            ScalarTarget::LogSeverityNumber => Value::Int(self.log.severity_number as i64),
            ScalarTarget::LogBody => match self.log.body.as_ref().map(Value::from_any_value) {
                Some(Value::String(s)) => Value::String(s),
                // Only string bodies are editable/comparable as strings (spec): a
                // non-string body compares as non-equal, never as a string match.
                _ => Value::Nil,
            },
            _ => Value::Nil,
        }
    }

    fn set_scalar(&mut self, target: ScalarTarget, value: Value) -> Result<(), String> {
        match target {
            ScalarTarget::ScopeName => self.scope.name = coerce_string(value)?,
            ScalarTarget::ScopeVersion => self.scope.version = coerce_string(value)?,
            ScalarTarget::LogSeverityText => self.log.severity_text = coerce_string(value)?,
            ScalarTarget::LogSeverityNumber => self.log.severity_number = coerce_i32(value)?,
            ScalarTarget::LogBody => {
                self.log.body = Some(Value::String(coerce_string(value)?).into_any_value())
            }
            other => return Err(format!("{other:?} is not writable for logs")),
        }
        Ok(())
    }

    fn map_mut(&mut self, target: MapTarget) -> &mut Vec<KeyValue> {
        match target {
            MapTarget::Resource => &mut self.resource.attributes,
            MapTarget::Scope => &mut self.scope.attributes,
            MapTarget::Leaf => &mut self.log.attributes,
        }
    }
}

struct MetricFrame<'a> {
    resource: &'a mut Resource,
    scope: &'a mut InstrumentationScope,
    name: &'a mut String,
    description: &'a mut String,
    unit: &'a mut String,
    datapoint_attributes: &'a mut Vec<KeyValue>,
}

impl Frame for MetricFrame<'_> {
    fn get_scalar(&self, target: ScalarTarget) -> Value {
        match target {
            ScalarTarget::ScopeName => Value::String(self.scope.name.clone()),
            ScalarTarget::ScopeVersion => Value::String(self.scope.version.clone()),
            ScalarTarget::MetricName => Value::String(self.name.clone()),
            ScalarTarget::MetricDescription => Value::String(self.description.clone()),
            ScalarTarget::MetricUnit => Value::String(self.unit.clone()),
            _ => Value::Nil,
        }
    }

    fn set_scalar(&mut self, target: ScalarTarget, value: Value) -> Result<(), String> {
        match target {
            ScalarTarget::ScopeName => self.scope.name = coerce_string(value)?,
            ScalarTarget::ScopeVersion => self.scope.version = coerce_string(value)?,
            ScalarTarget::MetricName => *self.name = coerce_string(value)?,
            ScalarTarget::MetricDescription => *self.description = coerce_string(value)?,
            ScalarTarget::MetricUnit => *self.unit = coerce_string(value)?,
            other => return Err(format!("{other:?} is not writable for metrics")),
        }
        Ok(())
    }

    fn map_mut(&mut self, target: MapTarget) -> &mut Vec<KeyValue> {
        match target {
            MapTarget::Resource => &mut self.resource.attributes,
            MapTarget::Scope => &mut self.scope.attributes,
            MapTarget::Leaf => self.datapoint_attributes,
        }
    }
}

fn coerce_string(value: Value) -> Result<String, String> {
    match value {
        Value::String(s) => Ok(s),
        Value::Nil => Ok(String::new()),
        other => Err(format!("expected a string, got a {}", other.type_name())),
    }
}

fn coerce_int(value: Value) -> Result<i64, String> {
    match value {
        Value::Int(i) => Ok(i),
        other => Err(format!("expected an int, got a {}", other.type_name())),
    }
}

fn coerce_i32(value: Value) -> Result<i32, String> {
    let i = coerce_int(value)?;
    i32::try_from(i).map_err(|_| format!("{i} does not fit in i32"))
}

impl CompiledProgram {
    /// Applies the program to every span in `req`, once per span.
    pub fn apply_traces(
        &self,
        req: &mut ExportTraceServiceRequest,
        mode: ErrorMode,
    ) -> Result<ApplyReport, ApplyError> {
        let mut report = ApplyReport::new(self.statements.len());
        for rs in &mut req.resource_spans {
            let mut resource = rs.resource.take().unwrap_or_default();
            for ss in &mut rs.scope_spans {
                let mut scope = ss.scope.take().unwrap_or_default();
                for span in &mut ss.spans {
                    let mut frame = TraceFrame {
                        resource: &mut resource,
                        scope: &mut scope,
                        span,
                    };
                    if let Err(err) = run_program(self, &mut frame, mode, &mut report) {
                        ss.scope = Some(scope);
                        rs.resource = Some(resource);
                        return Err(err);
                    }
                }
                ss.scope = Some(scope);
            }
            rs.resource = Some(resource);
        }
        Ok(report)
    }

    /// Applies the program to every log record in `req`, once per record.
    pub fn apply_logs(
        &self,
        req: &mut ExportLogsServiceRequest,
        mode: ErrorMode,
    ) -> Result<ApplyReport, ApplyError> {
        let mut report = ApplyReport::new(self.statements.len());
        for rl in &mut req.resource_logs {
            let mut resource = rl.resource.take().unwrap_or_default();
            for sl in &mut rl.scope_logs {
                let mut scope = sl.scope.take().unwrap_or_default();
                for log in &mut sl.log_records {
                    let mut frame = LogFrame {
                        resource: &mut resource,
                        scope: &mut scope,
                        log,
                    };
                    if let Err(err) = run_program(self, &mut frame, mode, &mut report) {
                        sl.scope = Some(scope);
                        rl.resource = Some(resource);
                        return Err(err);
                    }
                }
                sl.scope = Some(scope);
            }
            rl.resource = Some(resource);
        }
        Ok(report)
    }

    /// Applies the program to every data point of every metric in `req`, once per
    /// data point (D2: `metric.*`/`resource.*` therefore run once per point too).
    pub fn apply_metrics(
        &self,
        req: &mut ExportMetricsServiceRequest,
        mode: ErrorMode,
    ) -> Result<ApplyReport, ApplyError> {
        let mut report = ApplyReport::new(self.statements.len());
        for rm in &mut req.resource_metrics {
            let mut resource = rm.resource.take().unwrap_or_default();
            for sm in &mut rm.scope_metrics {
                let mut scope = sm.scope.take().unwrap_or_default();
                for metric in &mut sm.metrics {
                    let Metric {
                        name,
                        description,
                        unit,
                        data,
                        ..
                    } = metric;
                    let attrs_list: Vec<&mut Vec<KeyValue>> = match data {
                        Some(Data::Gauge(g)) => g
                            .data_points
                            .iter_mut()
                            .map(|p| &mut p.attributes)
                            .collect(),
                        Some(Data::Sum(s)) => s
                            .data_points
                            .iter_mut()
                            .map(|p| &mut p.attributes)
                            .collect(),
                        Some(Data::Histogram(h)) => h
                            .data_points
                            .iter_mut()
                            .map(|p| &mut p.attributes)
                            .collect(),
                        Some(Data::ExponentialHistogram(h)) => h
                            .data_points
                            .iter_mut()
                            .map(|p| &mut p.attributes)
                            .collect(),
                        Some(Data::Summary(s)) => s
                            .data_points
                            .iter_mut()
                            .map(|p| &mut p.attributes)
                            .collect(),
                        None => Vec::new(),
                    };
                    for attrs in attrs_list {
                        let mut frame = MetricFrame {
                            resource: &mut resource,
                            scope: &mut scope,
                            name: &mut *name,
                            description: &mut *description,
                            unit: &mut *unit,
                            datapoint_attributes: attrs,
                        };
                        if let Err(err) = run_program(self, &mut frame, mode, &mut report) {
                            sm.scope = Some(scope);
                            rm.resource = Some(resource);
                            return Err(err);
                        }
                    }
                }
                sm.scope = Some(scope);
            }
            rm.resource = Some(resource);
        }
        Ok(report)
    }
}
