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

use std::collections::HashMap;
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

/// Windowed ingest-id dedup cache. See module docs.
pub struct IngestDedup {
    window: Duration,
    clock: Arc<dyn Clock>,
    seen: Mutex<HashMap<Uuid, SystemTime>>,
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
            seen: Mutex::new(HashMap::new()),
        }
    }

    /// Records `id` as seen now, unless it is already within the window --
    /// in which case this is a no-op and the id remains a duplicate.
    ///
    /// Returns whether `id` was already present and within the window (a
    /// duplicate). Eviction of ids that have aged out is amortized into
    /// every call rather than run on a separate timer, so the cache never
    /// grows past the ids seen within one window plus this call's cost.
    pub fn check_and_record(&self, id: Uuid) -> bool {
        let now = self.clock.now();
        let mut seen = self
            .seen
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        seen.retain(|_, seen_at| Self::within_window(now, *seen_at, self.window));
        match seen.entry(id) {
            std::collections::hash_map::Entry::Occupied(_) => true,
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(now);
                false
            }
        }
    }

    /// Seeds `id` as first seen at `seen_at` if that is still within the
    /// window relative to the current clock -- used to rebuild the cache
    /// from on-disk WAL entry timestamps at startup. A later seed for an id
    /// already present never overwrites the earlier one, and seeding never
    /// evicts other entries.
    ///
    /// Returns whether `id` was inserted (`false` if it was already outside
    /// the window, or already present).
    pub fn seed(&self, id: Uuid, seen_at: SystemTime) -> bool {
        if !Self::within_window(self.clock.now(), seen_at, self.window) {
            return false;
        }
        let mut seen = self
            .seen
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        seen.entry(id).or_insert(seen_at);
        true
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
}
