//! The acceptor's entry points to [`common::processors::apply`]: the shared
//! processor loop, with its failures mapped to [`IngestError`] — a processor
//! load failure is `Unavailable` (fail closed), a `propagate` rejection is
//! `Invalid`, both before any WAL write.

use std::sync::Arc;

use common::auth::TenantContext;
use common::processors::ProcessorRegistry;
use common::processors::apply::{self, ApplyError};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;

use super::ingest_error::IngestError;

impl From<ApplyError> for IngestError {
    fn from(err: ApplyError) -> Self {
        match err {
            ApplyError::Unavailable { .. } => IngestError::Unavailable(err.into()),
            ApplyError::Rejected { .. } => IngestError::Invalid(err.into()),
        }
    }
}

pub async fn apply_trace_processors(
    registry: &Arc<ProcessorRegistry>,
    tenant_context: &TenantContext,
    request: &mut ExportTraceServiceRequest,
) -> Result<(), IngestError> {
    Ok(apply::apply_trace_processors(
        registry,
        &tenant_context.tenant_id,
        &tenant_context.dataset_id,
        request,
    )
    .await?)
}

pub async fn apply_log_processors(
    registry: &Arc<ProcessorRegistry>,
    tenant_context: &TenantContext,
    request: &mut ExportLogsServiceRequest,
) -> Result<(), IngestError> {
    Ok(apply::apply_log_processors(
        registry,
        &tenant_context.tenant_id,
        &tenant_context.dataset_id,
        request,
    )
    .await?)
}

pub async fn apply_metric_processors(
    registry: &Arc<ProcessorRegistry>,
    tenant_context: &TenantContext,
    request: &mut ExportMetricsServiceRequest,
) -> Result<(), IngestError> {
    Ok(apply::apply_metric_processors(
        registry,
        &tenant_context.tenant_id,
        &tenant_context.dataset_id,
        request,
    )
    .await?)
}
