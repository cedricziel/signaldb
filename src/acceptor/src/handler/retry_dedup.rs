//! Recognizes a client's resend of a batch this acceptor already made
//! durable, so it is acknowledged instead of ingested a second time.
//!
//! An OTLP exporter that times out waiting for an `Export` response retries
//! the identical request. tonic enforces the client's `grpc-timeout` by
//! dropping the handler future, but by then the batch may already be flushed
//! to the WAL, and the forward to the writer runs detached (#1734), so the
//! first copy lands anyway. The resend gets a fresh WAL entry id, so the
//! writer's ingest-id dedup cannot tell it apart: every such retry used to
//! land every span, log record or data point twice.
//!
//! A batch is keyed by a fingerprint of its tenant, dataset, WAL operation and
//! serialized bytes. The OTLP-to-Arrow conversion is deterministic, so a
//! byte-identical resend fingerprints identically, and two exports that
//! differ in any row do not.

use std::time::Duration;

use common::ingest_dedup::IngestDedup;
use common::wal::WalOperation;
use uuid::Uuid;

/// Windowed cache of recently accepted batch fingerprints. See module docs.
pub struct RetryDedup {
    /// `None` when the configured window is zero (dedup disabled).
    seen: Option<IngestDedup>,
}

impl RetryDedup {
    /// A cache remembering fingerprints for `window`; a zero window disables
    /// dedup entirely.
    pub fn new(window: Duration) -> Self {
        Self {
            seen: (!window.is_zero()).then(|| IngestDedup::new(window)),
        }
    }

    /// The fingerprint of one WAL-bound batch, or `None` when dedup is
    /// disabled (so the batch is not hashed for nothing). Each part is
    /// length-prefixed so no two distinct inputs concatenate to the same
    /// byte stream.
    pub fn fingerprint(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        operation: &WalOperation,
        batch_bytes: &[u8],
    ) -> Option<Uuid> {
        self.seen.as_ref()?;
        let mut hasher = twox_hash::XxHash3_128::new();
        for part in [
            tenant_id.as_bytes(),
            dataset_id.as_bytes(),
            operation.signal().as_bytes(),
            batch_bytes,
        ] {
            hasher.write(&(part.len() as u64).to_le_bytes());
            hasher.write(part);
        }
        Some(Uuid::from_u128(hasher.finish_128()))
    }

    /// Records `fingerprint` as durably accepted and returns whether it
    /// already was within the window, i.e. whether this batch is a resend.
    ///
    /// Call it only once the batch is flushed to the WAL, and with no
    /// `.await` between the flush and this call: recording a batch that never
    /// became durable would make its genuine retry look like a duplicate, and
    /// an await point in between is where a timed-out request is cancelled.
    pub fn is_resend(&self, fingerprint: Option<Uuid>) -> bool {
        match (&self.seen, fingerprint) {
            (Some(seen), Some(fingerprint)) => seen.check_and_record(fingerprint),
            _ => false,
        }
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
        let fp = dedup.fingerprint("acme", "prod", &WalOperation::WriteTraces, b"batch");

        assert!(!dedup.is_resend(fp), "the first delivery is not a resend");
        assert!(dedup.is_resend(fp), "the identical second delivery is");
    }

    #[test]
    fn fingerprint_distinguishes_every_part() {
        let dedup = RetryDedup::default();
        let base = dedup.fingerprint("acme", "prod", &WalOperation::WriteTraces, b"batch");
        for other in [
            dedup.fingerprint("globex", "prod", &WalOperation::WriteTraces, b"batch"),
            dedup.fingerprint("acme", "staging", &WalOperation::WriteTraces, b"batch"),
            dedup.fingerprint("acme", "prod", &WalOperation::WriteLogs, b"batch"),
            dedup.fingerprint("acme", "prod", &WalOperation::WriteTraces, b"batcH"),
            // Same concatenated bytes, split differently across parts.
            dedup.fingerprint("acmep", "rod", &WalOperation::WriteTraces, b"batch"),
        ] {
            assert_ne!(base, other);
        }
    }

    #[test]
    fn a_zero_window_disables_dedup() {
        let dedup = RetryDedup::new(Duration::ZERO);
        let fp = dedup.fingerprint("acme", "prod", &WalOperation::WriteTraces, b"batch");

        assert_eq!(fp, None, "a disabled cache must not hash the batch");
        assert!(!dedup.is_resend(fp));
    }
}
