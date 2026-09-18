//! Applies a tenant's OTTL processors to a decoded OTLP request, before
//! conversion to Arrow (change: tenant-ottl-processors, design D2/D4).
//!
//! Each matching [`common::processors::CompiledProcessor`] runs with its
//! *own* `error_mode` (design D4) — statements are not merged into a single
//! program with one strictest mode. A processor whose stored statements
//! failed to compile (`program: None`) is skipped entirely; it never blocks
//! ingest.

use std::sync::Arc;

use common::auth::TenantContext;
use common::processors::{CompiledProcessor, ProcessorRegistry};
use common::self_monitoring::app_metrics::{
    record_processor_rejected_request, record_processor_statement,
};
use common::self_monitoring::spans::processors_apply_span;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use ottl::ApplyReport;
use tracing::Instrument;

use super::ingest_error::IngestError;

/// Records the per-statement outcome counters for one processor's
/// `ApplyReport`. `matched` counts as `applied`; a statement with any
/// runtime errors counts as `error` for those occurrences, `skipped`
/// otherwise (neither matched nor erroring, e.g. a `where` guard that never
/// matched).
fn record_report(tenant_id: &str, processor_name: &str, report: &ApplyReport) {
    for stats in &report.statements {
        if stats.matched > 0 {
            record_processor_statement(tenant_id, processor_name, "applied");
        }
        if stats.errors > 0 {
            record_processor_statement(tenant_id, processor_name, "error");
        }
        if stats.matched == 0 && stats.errors == 0 {
            record_processor_statement(tenant_id, processor_name, "skipped");
        }
    }
}

/// Runs every matching processor against `request`, in `for_request` order,
/// each with its own `error_mode`. On the first `propagate` runtime error,
/// rejects the whole export as [`IngestError::Invalid`] before any WAL
/// write and records the rejection counter.
macro_rules! apply_processors {
    ($fn_name:ident, $request_ty:ty, $signal:literal) => {
        pub async fn $fn_name(
            registry: &Arc<ProcessorRegistry>,
            tenant_context: &TenantContext,
            request: &mut $request_ty,
        ) -> Result<(), IngestError> {
            let processors = registry
                .for_request(
                    &tenant_context.tenant_id,
                    &tenant_context.dataset_id,
                    $signal,
                )
                .await;
            if processors.is_empty() {
                return Ok(());
            }

            let span = processors_apply_span(
                &tenant_context.tenant_id,
                &tenant_context.dataset_id,
                $signal,
            );
            span.record("signaldb.processors.count", processors.len());

            async {
                for processor in &processors {
                    apply_one(registry, tenant_context, request, processor)?;
                }
                Ok(())
            }
            .instrument(span)
            .await
        }
    };
}

fn apply_one<R>(
    _registry: &Arc<ProcessorRegistry>,
    tenant_context: &TenantContext,
    request: &mut R,
    processor: &Arc<CompiledProcessor>,
) -> Result<(), IngestError>
where
    R: ApplyRequest,
{
    let Some(program) = &processor.program else {
        // Failed to compile; skipped, never blocks ingest.
        return Ok(());
    };
    let error_mode = match processor.record.error_mode.as_str() {
        "propagate" => ottl::ErrorMode::Propagate,
        "silent" => ottl::ErrorMode::Silent,
        _ => ottl::ErrorMode::Ignore,
    };
    match request.apply(program, error_mode) {
        Ok(report) => {
            record_report(&tenant_context.tenant_id, &processor.record.name, &report);
            Ok(())
        }
        Err(err) => {
            record_processor_rejected_request(&tenant_context.tenant_id);
            tracing::warn!(
                tenant_id = %tenant_context.tenant_id,
                dataset_id = %tenant_context.dataset_id,
                processor = %processor.record.name,
                error = %err,
                "processor rejected export in propagate error mode"
            );
            Err(IngestError::Invalid(anyhow::anyhow!(
                "processor `{}` rejected export: {err}",
                processor.record.name
            )))
        }
    }
}

/// A decoded OTLP export request an OTTL program can run against.
trait ApplyRequest {
    fn apply(
        &mut self,
        program: &ottl::CompiledProgram,
        error_mode: ottl::ErrorMode,
    ) -> Result<ApplyReport, ottl::ApplyError>;
}

impl ApplyRequest for ExportTraceServiceRequest {
    fn apply(
        &mut self,
        program: &ottl::CompiledProgram,
        error_mode: ottl::ErrorMode,
    ) -> Result<ApplyReport, ottl::ApplyError> {
        program.apply_traces(self, error_mode)
    }
}

impl ApplyRequest for ExportLogsServiceRequest {
    fn apply(
        &mut self,
        program: &ottl::CompiledProgram,
        error_mode: ottl::ErrorMode,
    ) -> Result<ApplyReport, ottl::ApplyError> {
        program.apply_logs(self, error_mode)
    }
}

impl ApplyRequest for ExportMetricsServiceRequest {
    fn apply(
        &mut self,
        program: &ottl::CompiledProgram,
        error_mode: ottl::ErrorMode,
    ) -> Result<ApplyReport, ottl::ApplyError> {
        program.apply_metrics(self, error_mode)
    }
}

apply_processors!(apply_trace_processors, ExportTraceServiceRequest, "traces");
apply_processors!(apply_log_processors, ExportLogsServiceRequest, "logs");
apply_processors!(
    apply_metric_processors,
    ExportMetricsServiceRequest,
    "metrics"
);
