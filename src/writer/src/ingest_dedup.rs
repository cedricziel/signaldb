//! In-memory, per-writer cache of ingest ids seen recently.
//!
//! `do_put`'s `app_metadata` carries an `ingest_id` -- the acceptor WAL entry
//! uuid a batch was forwarded for -- so a resend that the acceptor's
//! rendezvous hashing routes back to this same writer can be recognized and
//! deduped instead of re-inserted (design decision on issue #1734 step 2).
//!
//! The cache is deliberately not the SQL catalog or an external store: an
//! ack-path check against either would add a network round trip and a
//! dependency this writer doesn't otherwise have. It is bounded by a time
//! window ([`WriterConfig::ingest_dedup_window`](common::config::WriterConfig),
//! default 1h) rather than kept forever, and is rebuilt at startup from ingest
//! ids still present in this writer's own WAL entries within the window (see
//! [`crate::flight_iceberg::IcebergWriterFlightService::rebuild_ingest_dedup_from_wal`]).

use std::collections::VecDeque;
use std::collections::hash_map::{Entry, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use uuid::Uuid;

/// A source of wall-clock time, injectable so tests can move past the dedup
/// window instantly instead of sleeping for it.
pub trait Clock: Send + Sync {
    fn now(&self) -> SystemTime;
}

/// The real wall clock.
#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// The map (for O(1) lookup) plus its own entries in ascending-timestamp
/// order (for O(1)-amortized eviction of the ones that aged out) -- see
/// [`IngestDedup::evict_expired`].
#[derive(Default)]
struct Inner {
    map: HashMap<Uuid, SystemTime>,
    order: VecDeque<(SystemTime, Uuid)>,
}

/// Windowed ingest-id dedup cache. See module docs.
pub struct IngestDedup {
    window: Duration,
    clock: Arc<dyn Clock>,
    inner: Mutex<Inner>,
}

impl IngestDedup {
    /// A cache using the real wall clock.
    pub fn new(window: Duration) -> Self {
        Self::with_clock(window, Arc::new(SystemClock))
    }

    /// A cache driven by `clock`, for tests that need to move past the
    /// window without sleeping.
    pub fn with_clock(window: Duration, clock: Arc<dyn Clock>) -> Self {
        Self {
            window,
            clock,
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Records `id` as seen now, unless it is already within the window --
    /// in which case this is a no-op and the id remains a duplicate.
    ///
    /// Returns whether `id` was already present and within the window (a
    /// duplicate). Eviction of ids that have aged out is amortized into
    /// every call (`O(1)` per expired id, via [`Self::evict_expired`])
    /// rather than run on a separate timer or by scanning the whole cache,
    /// so the cost of one call never grows with how many ids are currently
    /// cached (#1748 review: a `HashMap::retain` here was `O(n)` per put).
    pub fn check_and_record(&self, id: Uuid) -> bool {
        let now = self.clock.now();
        let mut inner = self.lock();
        Self::evict_expired(&mut inner, now, self.window);
        match inner.map.entry(id) {
            Entry::Occupied(_) => true,
            Entry::Vacant(entry) => {
                entry.insert(now);
                inner.order.push_back((now, id));
                false
            }
        }
    }

    /// Seeds `id` as first seen at `seen_at` if that is still within the
    /// window relative to the current clock -- used to rebuild the cache
    /// from on-disk WAL entry timestamps at startup. A later seed for an id
    /// already present never overwrites the earlier one, and seeding never
    /// evicts other entries by itself (only piggybacks on the same
    /// amortized eviction `check_and_record` uses).
    ///
    /// Seeds may arrive out of timestamp order (the startup scan walks WALs,
    /// not a global time order), so this inserts into the ascending-order
    /// eviction queue at its sorted position rather than always at the back
    /// -- an `O(n)` insert, acceptable for a startup-only, bounded-size
    /// rebuild.
    ///
    /// Returns whether `id` was inserted (`false` if it was already outside
    /// the window, or already present).
    pub fn seed(&self, id: Uuid, seen_at: SystemTime) -> bool {
        let now = self.clock.now();
        if !Self::within_window(now, seen_at, self.window) {
            return false;
        }
        let mut inner = self.lock();
        match inner.map.entry(id) {
            Entry::Occupied(_) => false,
            Entry::Vacant(entry) => {
                entry.insert(seen_at);
                let pos = inner.order.partition_point(|&(t, _)| t <= seen_at);
                inner.order.insert(pos, (seen_at, id));
                true
            }
        }
    }

    /// Number of ids currently cached (including any not yet evicted by a
    /// call to [`Self::check_and_record`] or [`Self::seed`]). Test-only
    /// introspection.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.lock().map.len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Pops every entry whose timestamp has aged out of `window` from the
    /// front of `inner.order` (ascending, so the first in-window entry ends
    /// the scan) and removes it from `inner.map` -- `O(1)` per expired
    /// entry, never a full scan of the map. The map removal is guarded by a
    /// timestamp match so a stale `order` entry can never evict a fresher
    /// map entry for the same id (not reachable today -- neither
    /// `check_and_record` nor `seed` ever overwrites an existing map entry
    /// -- but the check is what makes that invariant load-bearing rather
    /// than assumed).
    fn evict_expired(inner: &mut Inner, now: SystemTime, window: Duration) {
        while let Some(&(seen_at, id)) = inner.order.front() {
            if Self::within_window(now, seen_at, window) {
                break;
            }
            inner.order.pop_front();
            if let Entry::Occupied(entry) = inner.map.entry(id)
                && *entry.get() == seen_at
            {
                entry.remove();
            }
        }
    }

    /// Whether `seen_at` is still within `window` of `now`. A `seen_at` in
    /// the future relative to `now` (clock skew, or a test that seeds ahead
    /// of its injected clock) is treated as within the window rather than
    /// erroring.
    fn within_window(now: SystemTime, seen_at: SystemTime, window: Duration) -> bool {
        match now.duration_since(seen_at) {
            Ok(age) => age <= window,
            Err(_) => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    /// A clock whose value is set explicitly by the test, so window expiry
    /// can be asserted without sleeping.
    struct FakeClock(StdMutex<SystemTime>);

    impl FakeClock {
        fn at(t: SystemTime) -> Arc<Self> {
            Arc::new(Self(StdMutex::new(t)))
        }

        fn advance(&self, by: Duration) {
            let mut t = self.0.lock().unwrap();
            *t += by;
        }
    }

    impl Clock for FakeClock {
        fn now(&self) -> SystemTime {
            *self.0.lock().unwrap()
        }
    }

    #[test]
    fn first_sighting_is_not_a_duplicate() {
        let dedup = IngestDedup::new(Duration::from_secs(3600));
        assert!(!dedup.check_and_record(Uuid::new_v4()));
    }

    #[test]
    fn repeat_within_window_is_a_duplicate() {
        let dedup = IngestDedup::new(Duration::from_secs(3600));
        let id = Uuid::new_v4();
        assert!(!dedup.check_and_record(id));
        assert!(dedup.check_and_record(id));
    }

    #[test]
    fn repeat_after_window_expiry_is_not_a_duplicate() {
        let clock = FakeClock::at(SystemTime::now());
        let dedup = IngestDedup::with_clock(Duration::from_secs(60), clock.clone());
        let id = Uuid::new_v4();
        assert!(!dedup.check_and_record(id));

        clock.advance(Duration::from_secs(61));
        assert!(
            !dedup.check_and_record(id),
            "an id older than the window must be treated as fresh"
        );
    }

    #[test]
    fn seed_outside_window_is_not_inserted() {
        let clock = FakeClock::at(SystemTime::now());
        let dedup = IngestDedup::with_clock(Duration::from_secs(60), clock.clone());
        let stale = clock.now() - Duration::from_secs(61);
        assert!(!dedup.seed(Uuid::new_v4(), stale));
    }

    #[test]
    fn seed_within_window_then_checked_is_a_duplicate() {
        let clock = FakeClock::at(SystemTime::now());
        let dedup = IngestDedup::with_clock(Duration::from_secs(3600), clock.clone());
        let id = Uuid::new_v4();
        let seen_at = clock.now() - Duration::from_secs(10);
        assert!(dedup.seed(id, seen_at));
        assert!(dedup.check_and_record(id));
    }

    /// #1748 review: eviction must be amortized, not an `O(n)` scan of the
    /// whole cache on every call. Inserting many ids spread across time and
    /// then advancing the clock past the window for the earliest ones must
    /// shrink the cache down to just the ones still in-window, and an
    /// expired id must be accepted again (not remembered as a duplicate).
    #[test]
    fn eviction_shrinks_the_cache_to_ids_still_within_the_window() {
        let clock = FakeClock::at(SystemTime::now());
        let dedup = IngestDedup::with_clock(Duration::from_secs(100), clock.clone());

        let old_ids: Vec<Uuid> = (0..500).map(|_| Uuid::new_v4()).collect();
        for id in &old_ids {
            assert!(!dedup.check_and_record(*id));
        }
        assert_eq!(dedup.len(), 500);

        // Well past the window for everything inserted above.
        clock.advance(Duration::from_secs(200));

        let fresh_ids: Vec<Uuid> = (0..10).map(|_| Uuid::new_v4()).collect();
        for id in &fresh_ids {
            assert!(!dedup.check_and_record(*id));
        }

        assert_eq!(
            dedup.len(),
            fresh_ids.len(),
            "ids older than the window must be evicted, not accumulate unbounded"
        );
        assert!(
            !dedup.check_and_record(old_ids[0]),
            "an id older than the window must be accepted again, not treated as a duplicate"
        );
    }

    /// #1748 review: the startup rebuild walks WALs, not a global time
    /// order, so seeds can arrive with decreasing timestamps. The eviction
    /// queue must still expire each one at its own time, not at the time it
    /// happened to be seeded.
    #[test]
    fn seeds_out_of_order_still_expire_at_their_own_time() {
        let clock = FakeClock::at(SystemTime::now());
        let dedup = IngestDedup::with_clock(Duration::from_secs(100), clock.clone());

        let newer_at = clock.now() - Duration::from_secs(10);
        let older_at = clock.now() - Duration::from_secs(90);
        let newer_id = Uuid::new_v4();
        let older_id = Uuid::new_v4();

        // Seed the newer one first -- out of timestamp order.
        assert!(dedup.seed(newer_id, newer_at));
        assert!(dedup.seed(older_id, older_at));
        assert_eq!(dedup.len(), 2);

        // 15s on: `older_id` (seeded 90s + 15s = 105s ago) is now past the
        // 100s window; `newer_id` (10s + 15s = 25s ago) is not.
        clock.advance(Duration::from_secs(15));
        assert!(
            !dedup.check_and_record(older_id),
            "the older seed must expire on its own timestamp despite being seeded second"
        );
        assert!(
            dedup.check_and_record(newer_id),
            "the newer seed must still be a duplicate"
        );
    }
}
