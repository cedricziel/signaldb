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
}
