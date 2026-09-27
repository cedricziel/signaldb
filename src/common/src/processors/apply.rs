//! Applies a tenant's OTTL processors to a decoded OTLP request, before
//! conversion to Arrow (change: tenant-ottl-processors, design D2/D4). Shared
//! by the acceptor's OTLP handlers and the router's eval-results upload.
//!
//! Each matching [`CompiledProcessor`] runs with its *own* `error_mode`
//! (design D4) — statements are not merged into a single program with one
//! strictest mode. A processor whose stored statements failed to compile
//! (`program: None`) is skipped entirely; it never blocks ingest.
//!
//! Loading ([`load`]) is async and running ([`run`]) is synchronous, so a
//! caller that converts off the async runtime can run processors there too;
//! the `apply_*_processors` functions do both.

use std::sync::Arc;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use ottl::ApplyReport;

use super::{CompiledProcessor, ProcessorRegistry};
use crate::self_monitoring::app_metrics::{
    record_processor_rejected_request, record_processor_statement,
};
use crate::self_monitoring::spans::processors_apply_span;

/// Why processors could not be applied to a request.
#[derive(Debug, thiserror::Error)]
pub enum ApplyError {
    /// The tenant's processors could not be loaded. Callers fail closed: with
    /// no cached entry to fall back to, applying zero processors would let
    /// unredacted data through for a tenant that may have redaction rules.
    #[error("failed to load processors for tenant `{tenant_id}`: {reason}")]
    Unavailable { tenant_id: String, reason: String },
    /// A processor in `propagate` error mode hit a runtime error; the whole
    /// request is rejected.
    #[error("processor `{processor}` rejected export: {reason}")]
    Rejected { processor: String, reason: String },
}

/// The processors matching `tenant_id`/`dataset_id`/`signal`, in
/// application order.
pub async fn load(
    registry: &ProcessorRegistry,
    tenant_id: &str,
    dataset_id: &str,
    signal: &str,
) -> Result<Vec<Arc<CompiledProcessor>>, ApplyError> {
    registry
        .for_request(tenant_id, dataset_id, signal)
        .await
        .map_err(|err| ApplyError::Unavailable {
            tenant_id: tenant_id.to_string(),
            reason: err.to_string(),
        })
}

/// Runs `processors` against `request` in order, each with its own
/// `error_mode`, under a `processors.apply` span. Stops at the first
/// `propagate` runtime error and records the rejection counter.
pub fn run<R: ApplyRequest>(
    processors: &[Arc<CompiledProcessor>],
    tenant_id: &str,
    dataset_id: &str,
    request: &mut R,
) -> Result<(), ApplyError> {
    if processors.is_empty() {
        return Ok(());
    }
    let span = processors_apply_span(tenant_id, dataset_id, R::SIGNAL);
    span.record("signaldb.processors.count", processors.len() as i64);
    let _entered = span.enter();
    for processor in processors {
        apply_one(tenant_id, dataset_id, request, processor)?;
    }
    Ok(())
}

async fn load_and_run<R: ApplyRequest>(
    registry: &ProcessorRegistry,
    tenant_id: &str,
    dataset_id: &str,
    request: &mut R,
) -> Result<(), ApplyError> {
    let processors = load(registry, tenant_id, dataset_id, R::SIGNAL).await?;
    run(&processors, tenant_id, dataset_id, request)
}

/// [`load`] then [`run`] the tenant's `traces` processors.
pub async fn apply_trace_processors(
    registry: &ProcessorRegistry,
    tenant_id: &str,
    dataset_id: &str,
    request: &mut ExportTraceServiceRequest,
) -> Result<(), ApplyError> {
    load_and_run(registry, tenant_id, dataset_id, request).await
}

/// [`load`] then [`run`] the tenant's `logs` processors.
pub async fn apply_log_processors(
    registry: &ProcessorRegistry,
    tenant_id: &str,
    dataset_id: &str,
    request: &mut ExportLogsServiceRequest,
) -> Result<(), ApplyError> {
    load_and_run(registry, tenant_id, dataset_id, request).await
}

/// [`load`] then [`run`] the tenant's `metrics` processors.
pub async fn apply_metric_processors(
    registry: &ProcessorRegistry,
    tenant_id: &str,
    dataset_id: &str,
    request: &mut ExportMetricsServiceRequest,
) -> Result<(), ApplyError> {
    load_and_run(registry, tenant_id, dataset_id, request).await
}

/// Records the per-statement outcome counters for one processor's
/// `ApplyReport`. `matched` counts as `applied`; a statement with any
/// runtime errors counts as `error` for those occurrences, `skipped`
/// otherwise (neither matched nor erroring, e.g. a `where` guard that never
/// matched).
fn record_report(tenant_id: &str, processor_name: &str, report: &ApplyReport) {
    for stats in &report.statements {
        if stats.matched > 0 {
            record_processor_statement(tenant_id, processor_name, "applied", stats.matched);
        }
        if stats.errors > 0 {
            record_processor_statement(tenant_id, processor_name, "error", stats.errors);
        }
        if stats.matched == 0 && stats.errors == 0 {
            record_processor_statement(tenant_id, processor_name, "skipped", 1);
        }
    }
}

fn apply_one<R: ApplyRequest>(
    tenant_id: &str,
    dataset_id: &str,
    request: &mut R,
    processor: &CompiledProcessor,
) -> Result<(), ApplyError> {
    let Some(program) = &processor.program else {
        // Failed to compile; skipped, never blocks ingest.
        return Ok(());
    };
    let error_mode = processor
        .record
        .error_mode
        .parse::<ottl::ErrorMode>()
        .unwrap_or_else(|e| {
            // Stored rows are validated at write time (`store.rs`), so this
            // only fires on a value written outside that path; fail open to
            // `Ignore` (never blocks ingest) but make the drift visible.
            tracing::warn!(
                tenant_id = %tenant_id,
                processor = %processor.record.name,
                error = %e,
                "stored processor has an unrecognized error_mode; treating as ignore"
            );
            ottl::ErrorMode::Ignore
        });
    match request.apply(program, error_mode) {
        Ok(report) => {
            record_report(tenant_id, &processor.record.name, &report);
            Ok(())
        }
        Err(err) => {
            record_processor_rejected_request(tenant_id);
            tracing::warn!(
                tenant_id = %tenant_id,
                dataset_id = %dataset_id,
                processor = %processor.record.name,
                error = %err,
                "processor rejected export in propagate error mode"
            );
            Err(ApplyError::Rejected {
                processor: processor.record.name.clone(),
                reason: err.to_string(),
            })
        }
    }
}

/// A decoded OTLP export request an OTTL program can run against.
pub trait ApplyRequest {
    /// The processor `signal` this request type matches.
    const SIGNAL: &'static str;

    fn apply(
        &mut self,
        program: &ottl::CompiledProgram,
        error_mode: ottl::ErrorMode,
    ) -> Result<ApplyReport, ottl::ApplyError>;
}

impl ApplyRequest for ExportTraceServiceRequest {
    const SIGNAL: &'static str = "traces";

    fn apply(
        &mut self,
        program: &ottl::CompiledProgram,
        error_mode: ottl::ErrorMode,
    ) -> Result<ApplyReport, ottl::ApplyError> {
        program.apply_traces(self, error_mode)
    }
}

impl ApplyRequest for ExportLogsServiceRequest {
    const SIGNAL: &'static str = "logs";

    fn apply(
        &mut self,
        program: &ottl::CompiledProgram,
        error_mode: ottl::ErrorMode,
    ) -> Result<ApplyReport, ottl::ApplyError> {
        program.apply_logs(self, error_mode)
    }
}

impl ApplyRequest for ExportMetricsServiceRequest {
    const SIGNAL: &'static str = "metrics";

    fn apply(
        &mut self,
        program: &ottl::CompiledProgram,
        error_mode: ottl::ErrorMode,
    ) -> Result<ApplyReport, ottl::ApplyError> {
        program.apply_metrics(self, error_mode)
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};

    use super::super::ProcessorRecord;
    use super::*;

    fn processor(name: &str, error_mode: &str, statements: &[&str]) -> Arc<CompiledProcessor> {
        let record = ProcessorRecord {
            tenant_id: "acme".to_string(),
            name: name.to_string(),
            dataset: None,
            signal: "logs".to_string(),
            enabled: true,
            priority: 100,
            error_mode: error_mode.to_string(),
            description: None,
            statements: statements.iter().map(|s| s.to_string()).collect(),
            created_at: String::new(),
            updated_at: String::new(),
        };
        Arc::new(CompiledProcessor::compile(record, &ottl::Limits::default()))
    }

    fn logs(body: &str) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(body.to_string())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn body(request: &ExportLogsServiceRequest) -> Option<&any_value::Value> {
        request.resource_logs[0].scope_logs[0].log_records[0]
            .body
            .as_ref()
            .and_then(|b| b.value.as_ref())
    }

    #[test]
    fn processors_run_in_order_and_an_uncompiled_one_is_skipped() {
        let processors = [
            processor("broken", "propagate", &["this is not ottl"]),
            processor("redact", "ignore", &[r#"set(body, "redacted")"#]),
        ];
        assert!(processors[0].program.is_none());
        let mut request = logs("secret");
        run(&processors, "acme", "prod", &mut request).expect("applies");
        assert_eq!(
            body(&request),
            Some(&any_value::Value::StringValue("redacted".to_string()))
        );
    }

    #[test]
    fn a_propagate_runtime_error_rejects_the_request() {
        let statements = [r#"set(body, Int(body))"#];
        let mut request = logs("not a number");
        run(
            &[processor("lenient", "ignore", &statements)],
            "acme",
            "prod",
            &mut request,
        )
        .expect("ignore mode never rejects");

        let err = run(
            &[processor("strict", "propagate", &statements)],
            "acme",
            "prod",
            &mut logs("not a number"),
        )
        .expect_err("propagate rejects");
        assert!(
            matches!(&err, ApplyError::Rejected { processor, .. } if processor == "strict"),
            "{err}"
        );
    }
}
