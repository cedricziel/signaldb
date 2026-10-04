//! # Batch Forwarding
//!
//! Shared helper for forwarding Arrow RecordBatches from the acceptor to a
//! writer service via Flight. Used by the OTLP/Prometheus handlers on the
//! hot path and by the WAL retry consumer when replaying entries whose
//! initial forward failed. The `DoPut` itself is
//! [`common::flight::forward::forward_batch_to_writer`], re-exported here.

use std::sync::Arc;

use bytes::Bytes;
pub use common::flight::forward::forward_batch_to_writer;
use common::flight::transport::InMemoryFlightTransport;
use common::wal::{Wal, WalOperation};
use datafusion::arrow::record_batch::RecordBatch;
use tracing::Instrument;
use uuid::Uuid;

/// What a failed forward implies about retrying the same batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForwardFailureKind {
    /// The writer could not be reached, or failed for a reason of its own.
    /// The same batch may well be accepted on a later attempt.
    Transient,
    /// The writer received the batch and refused it. The batch is the
    /// problem, so every retry fails identically.
    Permanent,
}

/// Classify a [`forward_batch_to_writer`] error by whether retrying can help.
///
/// Defaults to [`ForwardFailureKind::Transient`] whenever the failure cannot
/// be positively identified as a rejection. The two misjudgements are not
/// symmetric: calling a rejection transient costs retries, while calling a
/// transient failure permanent dead-letters data the writer would have
/// accepted. Ambiguous cases therefore fall on the retry side.
pub fn classify_forward_failure(error: &anyhow::Error) -> ForwardFailureKind {
    // The tonic status survives anyhow's context chain via the root cause;
    // a failure that never reached the writer has no status at all.
    let Some(status) = error.root_cause().downcast_ref::<tonic::Status>() else {
        return ForwardFailureKind::Transient;
    };

    match status.code() {
        // The writer inspected the batch and refused it.
        tonic::Code::InvalidArgument
        | tonic::Code::FailedPrecondition
        | tonic::Code::OutOfRange
        | tonic::Code::Unimplemented => ForwardFailureKind::Permanent,
        // Everything else may clear on its own. `Internal` in particular must
        // stay here: the writer returns it both for a batch it cannot
        // transform and for its own WAL write/flush failures, so it does not
        // identify the batch as the culprit.
        _ => ForwardFailureKind::Transient,
    }
}

/// Forward a WAL-durable batch to the writer and mark its WAL entry
/// processed on success, detached from the caller's future.
///
/// The request handlers call this only after `wal.flush()` has already
/// returned, so the batch is durable no matter what happens next. Running
/// the forward + mark step inline in the request future meant a client
/// disconnect (hyper/axum/tonic drop the future) could cancel it *after* the
/// flush but *before* `mark_processed`, leaving the entry unmarked; the WAL
/// retry consumer then re-forwarded it later and duplicated the data
/// (issue #1734). `tokio::spawn` moves the step onto its own task so
/// dropping the returned `JoinHandle` no longer cancels it — callers that
/// stay connected simply `.await` the handle and see the same latency as
/// before, while a disconnected caller's dropped future leaves the task
/// running to completion.
///
/// Error semantics are unchanged: a forward failure is logged and the entry
/// stays unprocessed for the retry consumer; the caller still acks the
/// request either way.
#[allow(clippy::too_many_arguments)]
pub fn spawn_forward_and_mark(
    flight_transport: Arc<InMemoryFlightTransport>,
    wal: Arc<Wal>,
    wal_entry_id: Uuid,
    ingest_id: Uuid,
    record_batch: RecordBatch,
    ipc_stream: Bytes,
    metadata_json: Option<String>,
    signal: &'static str,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(
        async move {
            match forward_batch_to_writer(
                &flight_transport,
                record_batch,
                Some(ipc_stream),
                metadata_json.as_deref(),
                ingest_id,
            )
            .await
            {
                Ok(()) => {
                    tracing::debug!(signal, "Successfully forwarded batch via Flight protocol");
                    if let Err(e) = wal.mark_processed(wal_entry_id).await {
                        tracing::warn!(entry_id = %wal_entry_id, signal, error = %e, "Failed to mark WAL entry as processed");
                    }
                }
                Err(e) => {
                    tracing::error!(entry_id = %wal_entry_id, signal, error = %e, "Failed to forward batch - data remains in WAL for retry");
                }
            }
        }
        .instrument(tracing::Span::current()),
    )
}

/// Retire a WAL entry that [`super::retry_dedup::RetryDedup`] recognized as
/// a client's resend of a batch already accepted: mark it processed without
/// forwarding it, detached from the caller's future for the same reason as
/// [`spawn_forward_and_mark`]. A failed mark leaves the entry for the retry
/// consumer, which forwards it — the pre-dedup behavior, never data loss.
pub fn spawn_retire_resend(
    wal: Arc<Wal>,
    wal_entry_id: Uuid,
    tenant_id: &str,
    operation: &WalOperation,
) -> tokio::task::JoinHandle<()> {
    let signal = operation.signal();
    common::self_monitoring::app_metrics()
        .acceptor_resends_dropped
        .add(
            1,
            &[
                opentelemetry::KeyValue::new("signaldb.tenant.id", tenant_id.to_string()),
                opentelemetry::KeyValue::new("signal", signal),
            ],
        );
    tracing::debug!(entry_id = %wal_entry_id, signal, "Dropped client resend of an already-accepted batch");
    tokio::spawn(
        async move {
            if let Err(e) = wal.mark_processed(wal_entry_id).await {
                tracing::warn!(entry_id = %wal_entry_id, signal, error = %e, "Failed to mark resent WAL entry as processed");
            }
        }
        .instrument(tracing::Span::current()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Wrap a status the way `forward_batch_to_writer_inner` does, so the
    /// classifier is exercised against the real shape of the error.
    fn do_put_error(status: tonic::Status) -> anyhow::Error {
        anyhow::Error::new(status).context("Flight do_put failed")
    }

    #[test]
    fn writer_rejecting_the_batch_is_permanent() {
        // The hive wedge (#1060): the writer refuses a metrics_sum batch whose
        // non-nullable `value` column contains nulls. No retry can change it.
        let error = do_put_error(tonic::Status::invalid_argument(
            "Schema transformation failed: Column 'value' is declared as non-nullable",
        ));

        assert_eq!(
            classify_forward_failure(&error),
            ForwardFailureKind::Permanent
        );
    }

    #[test]
    fn unreachable_writer_is_transient() {
        let error = do_put_error(tonic::Status::unavailable("connection refused"));

        assert_eq!(
            classify_forward_failure(&error),
            ForwardFailureKind::Transient
        );
    }

    #[test]
    fn internal_is_transient_because_the_writer_uses_it_for_its_own_failures() {
        // `Status::internal` covers "Failed to write to WAL" and "Failed to
        // flush WAL" on the writer, which are writer-side and recoverable.
        // Treating it as permanent would dead-letter perfectly good batches
        // whenever the writer's own storage hiccups.
        let error = do_put_error(tonic::Status::internal("Failed to write to WAL: disk full"));

        assert_eq!(
            classify_forward_failure(&error),
            ForwardFailureKind::Transient
        );
    }

    #[test]
    fn failure_that_never_reached_the_writer_is_transient() {
        // No storage service discoverable — there is no status to inspect,
        // and the batch itself has not been judged by anything.
        let error = anyhow::anyhow!("Failed to get Flight client for storage service: none found");

        assert_eq!(
            classify_forward_failure(&error),
            ForwardFailureKind::Transient
        );
    }

    #[test]
    fn deadline_exceeded_is_transient() {
        let error = do_put_error(tonic::Status::deadline_exceeded("timed out"));

        assert_eq!(
            classify_forward_failure(&error),
            ForwardFailureKind::Transient
        );
    }
}
