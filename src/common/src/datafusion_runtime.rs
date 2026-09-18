//! # Shared DataFusion runtime construction
//!
//! One place to build the bounded memory pool that every SignalDB service
//! runs DataFusion under, so the querier and the compactor cannot drift
//! apart on the pool *shape* — which is a correctness property, not a
//! tuning knob.
//!
//! ## Why not `RuntimeEnvBuilder::with_memory_limit`
//!
//! That convenience installs a [`GreedyMemoryPool`]: memory is granted
//! first-come, first-served with no per-consumer reservation. With a
//! single consumer that is fine. With N concurrent *spilling* consumers
//! it is not — they exhaust the pool between them, and a consumer that
//! needs a fresh reservation to perform its spill merge cannot obtain one
//! while its peers hold the memory. The allocation fails even though
//! every consumer reports `can spill: true`.
//!
//! That is exactly what took down compaction on a live deployment
//! (issue #1064): six `ExternalSorter`s peaking at 50–142 MB each against
//! one 512 MB `greedy` pool, failing in the sort before any commit was
//! attempted.
//!
//! [`FairSpillPool`] reserves a share per spilling consumer up front, so
//! a consumer that exceeds its share spills instead of failing.
//!
//! ## What it does not cover
//!
//! `FairSpillPool` divides only the memory left over after *unspillable*
//! consumers have taken theirs — `spill_available = pool_size -
//! unspillable`, then split `num_spill` ways. `ExternalSorterMerge` and
//! `SortPreservingMergeExec` register as unspillable and were observed
//! holding 50–103 MB each on the same deployment. The pool works around
//! those reservations rather than bounding them; what bounds them is
//! limiting the operator fan-out (`compactor.target_partitions`).
//!
//! [`GreedyMemoryPool`]: datafusion::execution::memory_pool::GreedyMemoryPool

use std::num::NonZeroUsize;
use std::sync::Arc;

use datafusion::execution::memory_pool::{FairSpillPool, MemoryPool, TrackConsumersPool};

/// How many consumers a pool error names. Matches DataFusion's own
/// default for `with_memory_limit`; the ranked list is what made #1064
/// diagnosable from logs alone, so it is worth keeping.
const TRACKED_CONSUMERS: usize = 5;

/// Build the bounded memory pool for a DataFusion runtime.
///
/// `limit_bytes` is the total budget and `fraction` the share of it that
/// query operators may use before they spill or fail (`1.0` = all of it),
/// mirroring `RuntimeEnvBuilder::with_memory_limit` so callers can swap
/// one for the other.
///
/// The pool is a [`FairSpillPool`] wrapped in [`TrackConsumersPool`], so
/// exhaustion errors still name the top consumers.
pub fn bounded_memory_pool(limit_bytes: usize, fraction: f64) -> Arc<dyn MemoryPool> {
    let pool_size = (limit_bytes as f64 * fraction) as usize;
    let tracked = NonZeroUsize::new(TRACKED_CONSUMERS).expect("TRACKED_CONSUMERS is non-zero");
    Arc::new(TrackConsumersPool::new(
        FairSpillPool::new(pool_size),
        tracked,
    ))
}

/// The scan/sort shape a DataFusion session runs under: bounded so an
/// `ExternalSorter`'s unspillable per-batch reservation fits the memory pool
/// it runs against, whether that pool belongs to the compactor or the
/// querier (issues #1064, #1359).
///
/// `ExternalSorter` reserves roughly twice an incoming batch's bytes the
/// moment the batch arrives, and that reservation cannot spill — with
/// nothing accumulated yet there is nothing to write out. DataFusion's batch
/// size is counted in *rows*, so its own default of 8192 is only safe for
/// narrow rows: wide rows (profile payloads, JSON attribute blobs) turn a
/// single batch into a reservation several times the pool.
#[derive(Clone, Copy, Debug)]
pub struct ScanShape {
    /// Row count of the batches a scan feeds downstream. `0` leaves
    /// DataFusion's own default (8192 rows) in place.
    pub batch_size: usize,
    /// DataFusion partition fan-out. `0` leaves DataFusion's own default
    /// (available parallelism) in place.
    pub target_partitions: usize,
    /// Memory a spilling sort holds back so its spill merge can run
    /// (`datafusion.execution.sort_spill_reservation_bytes`). Headroom taken
    /// out of the pool, not added to it; `0` means none, which DataFusion
    /// permits.
    pub sort_spill_reservation_bytes: usize,
}

impl ScanShape {
    /// Build a `ScanShape` from config values expressed in MiB, owning the
    /// MiB-to-bytes conversion so callers don't repeat it.
    pub fn from_mb(
        batch_size: usize,
        target_partitions: usize,
        sort_spill_reservation_mb: u64,
    ) -> Self {
        Self {
            batch_size,
            target_partitions,
            sort_spill_reservation_bytes: sort_spill_reservation_mb as usize * 1024 * 1024,
        }
    }

    /// Apply this shape to a `SessionConfig`. `batch_size` and
    /// `target_partitions` of `0` are left at DataFusion's own default —
    /// `with_target_partitions` panics on zero, and a zero batch size would
    /// stall the scan — while the sort-spill reservation is always applied
    /// (`0` is a valid, if inadvisable, DataFusion setting).
    pub fn apply(
        &self,
        mut config: datafusion::prelude::SessionConfig,
    ) -> datafusion::prelude::SessionConfig {
        config = config.with_sort_spill_reservation_bytes(self.sort_spill_reservation_bytes);
        if self.target_partitions > 0 {
            config = config.with_target_partitions(self.target_partitions);
        }
        if self.batch_size > 0 {
            config = config.with_batch_size(self.batch_size);
        }
        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::MemoryConsumer;

    const MB: usize = 1024 * 1024;

    /// The property the greedy pool lacks: with two spilling consumers
    /// registered, neither may take more than half the pool, so one
    /// cannot starve the other into a failed allocation (#1064).
    #[test]
    fn each_spilling_consumer_is_capped_at_its_share() {
        let pool = bounded_memory_pool(100 * MB, 1.0);

        let first = MemoryConsumer::new("sorter-1")
            .with_can_spill(true)
            .register(&pool);
        let second = MemoryConsumer::new("sorter-2")
            .with_can_spill(true)
            .register(&pool);

        first
            .try_grow(50 * MB)
            .expect("first spiller may take its half");
        second
            .try_grow(50 * MB)
            .expect("second spiller may take its half");

        assert!(
            first.try_grow(MB).is_err(),
            "a spiller must not grow past its share even when the pool has room"
        );
    }

    /// Under a greedy pool the same sequence succeeds — one consumer
    /// takes the whole pool. This pins the difference that matters.
    #[test]
    fn a_single_spiller_cannot_consume_the_whole_pool_when_peers_exist() {
        let pool = bounded_memory_pool(100 * MB, 1.0);

        let hog = MemoryConsumer::new("hog")
            .with_can_spill(true)
            .register(&pool);
        let _peer = MemoryConsumer::new("peer")
            .with_can_spill(true)
            .register(&pool);

        assert!(
            hog.try_grow(90 * MB).is_err(),
            "with a peer registered, one spiller must not take 90% of the pool"
        );
    }

    /// Exhaustion errors must keep naming the top consumers: that ranked
    /// list is what identified the six concurrent sorters in #1064.
    #[test]
    fn exhaustion_errors_name_the_top_consumers() {
        let pool = bounded_memory_pool(10 * MB, 1.0);

        let consumer = MemoryConsumer::new("ExternalSorter[0]")
            .with_can_spill(true)
            .register(&pool);
        consumer.try_grow(5 * MB).expect("first grow fits");

        let err = consumer
            .try_grow(100 * MB)
            .expect_err("growing past the pool must fail")
            .to_string();

        assert!(
            err.contains("ExternalSorter[0]"),
            "error must name the consumer, got: {err}"
        );
        assert!(
            err.contains("top memory consumers"),
            "error must carry the ranked consumer list, got: {err}"
        );
    }

    /// A consumer that cannot spill still draws from the whole pool —
    /// the documented gap this helper does not close.
    #[test]
    fn unspillable_consumers_are_not_fair_shared() {
        let pool = bounded_memory_pool(100 * MB, 1.0);

        let _spiller = MemoryConsumer::new("sorter")
            .with_can_spill(true)
            .register(&pool);
        let merge = MemoryConsumer::new("ExternalSorterMerge")
            .with_can_spill(false)
            .register(&pool);

        merge
            .try_grow(90 * MB)
            .expect("unspillable consumers are first-come, first-served");
    }

    #[test]
    fn the_fraction_scales_the_pool() {
        let pool = bounded_memory_pool(100 * MB, 0.5);

        let consumer = MemoryConsumer::new("solo")
            .with_can_spill(false)
            .register(&pool);

        consumer.try_grow(50 * MB).expect("half the budget fits");
        assert!(
            consumer.try_grow(MB).is_err(),
            "the fraction must bound the pool, not just annotate it"
        );
    }

    /// `0` for `batch_size`/`target_partitions` must land on DataFusion's own
    /// defaults, not merely some other positive value —
    /// `with_target_partitions` panics on zero, so `apply` must not forward
    /// it blindly.
    #[test]
    fn scan_shape_zero_batch_size_and_target_partitions_is_auto() {
        let default_config = datafusion::prelude::SessionConfig::new();
        let shape = ScanShape {
            batch_size: 0,
            target_partitions: 0,
            sort_spill_reservation_bytes: 0,
        };

        let config = shape.apply(datafusion::prelude::SessionConfig::new());

        assert_eq!(config.batch_size(), default_config.batch_size());
        assert_eq!(
            config.target_partitions(),
            default_config.target_partitions()
        );
    }

    /// Positive `batch_size`/`target_partitions` must be honored exactly.
    #[test]
    fn scan_shape_honors_configured_batch_size_and_target_partitions() {
        let shape = ScanShape {
            batch_size: 256,
            target_partitions: 3,
            sort_spill_reservation_bytes: 0,
        };

        let config = shape.apply(datafusion::prelude::SessionConfig::new());

        assert_eq!(config.batch_size(), 256);
        assert_eq!(config.target_partitions(), 3);
    }

    /// `from_mb` must own the MiB-to-bytes conversion for
    /// `sort_spill_reservation_bytes` while passing `batch_size`/
    /// `target_partitions` through unchanged.
    #[test]
    fn scan_shape_from_mb_converts_reservation_to_bytes() {
        let shape = ScanShape::from_mb(256, 3, 32);

        assert_eq!(shape.batch_size, 256);
        assert_eq!(shape.target_partitions, 3);
        assert_eq!(shape.sort_spill_reservation_bytes, 32 * MB);
    }

    /// The sort-spill reservation is always applied, including `0` — the
    /// escape hatch is only for `batch_size`/`target_partitions`.
    #[test]
    fn scan_shape_always_applies_the_sort_spill_reservation() {
        let shape = ScanShape {
            batch_size: 0,
            target_partitions: 0,
            sort_spill_reservation_bytes: 32 * MB,
        };

        let config = shape.apply(datafusion::prelude::SessionConfig::new());

        assert_eq!(
            config.options().execution.sort_spill_reservation_bytes,
            32 * MB
        );
    }
}
