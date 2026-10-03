//! # OTLP Profiles Handler
//!
//! Converts an `ExportProfilesServiceRequest` to Arrow (an infallible
//! conversion — profiles has no equivalent of the traces/logs/metrics
//! conversion-failure path), writes it to the tenant/dataset's profiles
//! WAL, and forwards it to a writer via Flight. Shared by both the gRPC
//! (`services::otlp_profile_service`) and HTTP
//! (`lib::handle_http_profiles`) surfaces.

use std::sync::Arc;

use anyhow::Context;
use common::auth::TenantContext;
use common::flight::conversion::otlp_profiles_to_arrow;
use common::flight::transport::InMemoryFlightTransport;
use common::wal::{WalOperation, record_batch_to_bytes};
use opentelemetry_proto::tonic::collector::profiles::v1development::ExportProfilesServiceRequest;

use super::WalManager;
use super::forward::{spawn_forward_and_mark, spawn_retire_resend};
use super::ingest_error::IngestError;
use super::retry_dedup::{RetryDedup, stamp_batch_fingerprint};
use crate::attribute_limits::{TenantAttributeLimits, cap_profiles, record_drops};

pub struct ProfileHandler {
    /// Flight transport for forwarding telemetry
    flight_transport: Arc<InMemoryFlightTransport>,
    /// WAL manager for multi-tenant WAL isolation
    wal_manager: Arc<WalManager>,
    /// Recognizes a client's resend of a batch already made durable
    retry_dedup: Arc<RetryDedup>,
    /// Per-record attribute guardrails (`[acceptor.attribute_limits]` plus tenant overrides)
    attribute_limits: Arc<TenantAttributeLimits>,
}

#[cfg(any(test, feature = "testing"))]
pub struct MockProfileHandler {
    pub handle_grpc_otlp_profiles_calls: tokio::sync::Mutex<Vec<ExportProfilesServiceRequest>>,
}

#[cfg(any(test, feature = "testing"))]
impl Default for MockProfileHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(test, feature = "testing"))]
impl MockProfileHandler {
    pub fn new() -> Self {
        Self {
            handle_grpc_otlp_profiles_calls: tokio::sync::Mutex::new(Vec::new()),
        }
    }

    pub async fn handle_grpc_otlp_profiles(
        &self,
        _tenant_context: &TenantContext,
        request: ExportProfilesServiceRequest,
    ) -> Result<(), IngestError> {
        self.handle_grpc_otlp_profiles_calls
            .lock()
            .await
            .push(request);
        Ok(())
    }
}

impl ProfileHandler {
    /// Create a new handler with Flight transport and WAL manager
    pub fn new(
        flight_transport: Arc<InMemoryFlightTransport>,
        wal_manager: Arc<WalManager>,
    ) -> Self {
        Self {
            flight_transport,
            wal_manager,
            retry_dedup: Arc::new(RetryDedup::default()),
            attribute_limits: Arc::new(TenantAttributeLimits::default()),
        }
    }

    /// Share one resend-dedup cache (`[acceptor].retry_dedup_window`) with
    /// the acceptor's other handlers; the default is a private cache with
    /// the default window.
    pub fn with_retry_dedup(mut self, retry_dedup: Arc<RetryDedup>) -> Self {
        self.retry_dedup = retry_dedup;
        self
    }

    /// Per-tenant `[acceptor.attribute_limits]`; the default applies the
    /// built-in limits to every tenant.
    pub fn with_attribute_limits(mut self, attribute_limits: Arc<TenantAttributeLimits>) -> Self {
        self.attribute_limits = attribute_limits;
        self
    }

    /// Handle an OTLP profiles export.
    ///
    /// Returns `Ok(())` once the data is durably accepted: written and
    /// flushed to the WAL. A failed Flight forward after that point is not
    /// an error — the WAL retry consumer re-forwards the entry.
    #[tracing::instrument(
        skip_all,
        fields(
            signaldb.tenant.id = %tenant_context.tenant_id,
            signaldb.dataset.id = %tenant_context.dataset_id
        )
    )]
    pub async fn handle_grpc_otlp_profiles(
        &self,
        tenant_context: &TenantContext,
        mut request: ExportProfilesServiceRequest,
    ) -> Result<(), IngestError> {
        tracing::debug!(
            tenant_id = %tenant_context.tenant_id,
            dataset_id = %tenant_context.dataset_id,
            "Handling OTLP profiles request"
        );

        let dropped = cap_profiles(
            &mut request,
            self.attribute_limits.for_tenant(&tenant_context.tenant_id),
        );
        record_drops(&tenant_context.tenant_id, "profiles", &dropped);

        // Get tenant/dataset-specific WAL
        let wal = self
            .wal_manager
            .get_wal(
                &tenant_context.tenant_id,
                &tenant_context.dataset_id,
                "profiles",
            )
            .await
            .context("Failed to get WAL")
            .map_err(IngestError::Unavailable)?;

        // Convert OTLP profiles to Arrow RecordBatch (resolves the dictionary)
        let record_batch = otlp_profiles_to_arrow(&request);

        // Add schema version metadata (v1 for OTLP conversion)
        let mut metadata = serde_json::json!({
            "schema_version": "v1",
            "signal_type": "profiles",
            "tenant_id": tenant_context.tenant_id,
            "dataset_id": tenant_context.dataset_id,
        });
        if let Some((traceparent, tracestate)) =
            common::flight::trace_context::current_trace_context_fields()
        {
            metadata["traceparent"] = traceparent.into();
            if let Some(tracestate) = tracestate {
                metadata["tracestate"] = tracestate.into();
            }
        }

        // Step 1: Write to WAL first for durability
        let batch_bytes = record_batch_to_bytes(&record_batch)
            .context("Failed to serialize record batch")
            .map_err(IngestError::Unavailable)?;

        let ingest_id = stamp_batch_fingerprint(
            &mut metadata,
            &tenant_context.tenant_id,
            &tenant_context.dataset_id,
            &WalOperation::WriteProfiles,
            &batch_bytes,
        );
        // Serialize metadata for WAL storage (enables background processor routing)
        let metadata_str = serde_json::to_string(&metadata).ok();
        let wal_entry_id = wal
            .append(
                WalOperation::WriteProfiles,
                batch_bytes,
                metadata_str.clone(),
            )
            .await
            .context("Failed to write profiles to WAL")
            .map_err(IngestError::Unavailable)?;

        // Flush WAL to ensure durability
        wal.flush()
            .await
            .context("Failed to flush WAL")
            .map_err(IngestError::Unavailable)?;

        tracing::debug!(entry_id = %wal_entry_id, "Profiles written to WAL");

        // Step 2: Forward from WAL to writer via Flight, detached from this
        // request future so a client disconnect cannot cancel it after the
        // flush above (issue #1734). Awaiting the handle keeps behavior for
        // connected clients unchanged. A client's resend of a batch already
        // accepted is retired instead (see `retry_dedup`).
        let forward_task = if self.retry_dedup.is_resend(ingest_id) {
            spawn_retire_resend(
                wal,
                wal_entry_id,
                &tenant_context.tenant_id,
                &WalOperation::WriteProfiles,
            )
        } else {
            spawn_forward_and_mark(
                self.flight_transport.clone(),
                wal,
                wal_entry_id,
                ingest_id,
                record_batch,
                metadata_str,
                "profiles",
            )
        };
        if let Err(e) = forward_task.await {
            tracing::error!(entry_id = %wal_entry_id, error = %e, "Forward-and-mark task for profiles did not complete");
        }

        // Data is durable in the WAL at this point; forward failures are
        // recovered by the retry consumer, so the export is acknowledged.
        Ok(())
    }
}
