//! Recognizes a client's resend of a batch already made durable, so it is
//! acknowledged instead of ingested a second time.
//!
//! An OTLP exporter that times out waiting for an `Export` response retries
//! the identical request. tonic enforces the client's `grpc-timeout` by
//! dropping the handler future, but by then the batch may already be flushed
//! to the WAL, and the forward to the writer runs detached (#1734), so the
//! first copy lands anyway. The resend gets a fresh WAL entry, so a key tied
//! to the entry cannot tell the two apart.
//!
//! A batch is keyed instead by [`batch_fingerprint`]: a hash of its tenant,
//! dataset, WAL operation and serialized bytes. The OTLP-to-Arrow conversion
//! is deterministic, so a byte-identical resend fingerprints identically, and
//! two exports that differ in any row do not. The fingerprint is the
//! `ingest_id` the batch is forwarded under, so the writer's own ingest-id
//! dedup (rendezvous-routed, rebuilt from its WAL at startup) catches a resend
//! that reaches a different acceptor replica or arrives after a restart.
//! [`RetryDedup`] is the cheap first line in front of it: a resend this
//! process already saw is retired without a Flight round trip.

use std::time::Duration;

use common::ingest_dedup::IngestDedup;
use common::wal::WalOperation;
use uuid::Uuid;

/// The fingerprint of one WAL-bound batch, used as its `ingest_id`.
pub fn batch_fingerprint(
    tenant_id: &str,
    dataset_id: &str,
    operation: &WalOperation,
    batch_bytes: &[u8],
) -> Uuid {
    common::ingest_dedup::fingerprint(&[
        tenant_id.as_bytes(),
        dataset_id.as_bytes(),
        operation.signal().as_bytes(),
        batch_bytes,
    ])
}

/// Fingerprint a WAL-bound batch and record the result as `ingest_id` in its
/// WAL entry `metadata`, so the retry consumer forwards the entry under the
/// same id as the hot path. Returns the fingerprint.
pub fn stamp_batch_fingerprint(
    metadata: &mut serde_json::Value,
    tenant_id: &str,
    dataset_id: &str,
    operation: &WalOperation,
    batch_bytes: &[u8],
) -> Uuid {
    let ingest_id = batch_fingerprint(tenant_id, dataset_id, operation, batch_bytes);
    metadata["ingest_id"] = ingest_id.to_string().into();
    ingest_id
}

/// Windowed cache of recently accepted batch fingerprints. See module docs.
pub struct RetryDedup {
    /// `None` when the configured window is zero (cache disabled).
    seen: Option<IngestDedup>,
}

impl RetryDedup {
    /// A cache remembering fingerprints for `window`; a zero window disables
    /// the cache, leaving resends to the writer's dedup.
    pub fn new(window: Duration) -> Self {
        Self {
            seen: (!window.is_zero()).then(|| IngestDedup::new(window)),
        }
    }

    /// Records `fingerprint` as durably accepted and returns whether it
    /// already was within the window, i.e. whether this batch is a resend.
    ///
    /// Call it only once the batch is flushed to the WAL, and with no
    /// `.await` between the flush and this call: recording a batch that never
    /// became durable would make its genuine retry look like a duplicate, and
    /// an await point in between is where a timed-out request is cancelled.
    pub fn is_resend(&self, fingerprint: Uuid) -> bool {
        self.seen
            .as_ref()
            .is_some_and(|seen| seen.check_and_record(fingerprint))
    }
}

impl Default for RetryDedup {
    fn default() -> Self {
        Self::new(common::config::AcceptorConfig::default().retry_dedup_window)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_repeat_fingerprint_is_a_resend() {
        let dedup = RetryDedup::default();
        let fp = batch_fingerprint("acme", "prod", &WalOperation::WriteTraces, b"batch");

        assert!(!dedup.is_resend(fp), "the first delivery is not a resend");
        assert!(dedup.is_resend(fp), "the identical second delivery is");
    }

    #[test]
    fn fingerprint_distinguishes_every_part() {
        let base = batch_fingerprint("acme", "prod", &WalOperation::WriteTraces, b"batch");
        for other in [
            batch_fingerprint("globex", "prod", &WalOperation::WriteTraces, b"batch"),
            batch_fingerprint("acme", "staging", &WalOperation::WriteTraces, b"batch"),
            batch_fingerprint("acme", "prod", &WalOperation::WriteLogs, b"batch"),
            batch_fingerprint("acme", "prod", &WalOperation::WriteTraces, b"batcH"),
            // Same concatenated bytes, split differently across parts.
            batch_fingerprint("acmep", "rod", &WalOperation::WriteTraces, b"batch"),
        ] {
            assert_ne!(base, other);
        }
    }

    #[test]
    fn a_zero_window_disables_the_cache() {
        let dedup = RetryDedup::new(Duration::ZERO);
        let fp = batch_fingerprint("acme", "prod", &WalOperation::WriteTraces, b"batch");

        assert!(!dedup.is_resend(fp));
        assert!(!dedup.is_resend(fp));
    }
}
