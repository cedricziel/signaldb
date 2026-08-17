//! Compact membership set for live file paths.
//!
//! Orphan detection needs one question answered per object-store listing
//! entry: "does any retained snapshot reference this path?" It never needs
//! to enumerate the live paths themselves. [`LiveFileSet`] therefore keeps a
//! 64-bit fingerprint per path instead of the path string, cutting the
//! per-file footprint from a heap-allocated `String` (typically 100–200
//! bytes for an Iceberg data-file path) to one `u64` hash-table slot.
//!
//! ## Data safety
//!
//! A fingerprint collision can only make an *orphan* look live — the file is
//! then kept, never deleted. A live file's own fingerprint is always present,
//! so it can never be classified as orphan. False positives (an unreclaimed
//! orphan) at 64 bits are ~`n / 2^64` per lookup and are corrected on a later
//! cycle only if the colliding live file goes away; the direction is safe.

use std::collections::HashSet;
use std::hash::{DefaultHasher, Hash, Hasher};

/// Set of live file paths stored as fixed-size fingerprints.
///
/// See the module documentation for the memory rationale and why a
/// collision is always on the safe side.
#[derive(Debug, Default, Clone)]
pub struct LiveFileSet {
    fingerprints: HashSet<u64>,
}

impl LiveFileSet {
    /// Create an empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Fingerprint of a path.
    ///
    /// `DefaultHasher::new()` is a fixed-key SipHash, so fingerprints are
    /// stable within a process — which is the only scope a set is ever
    /// built and queried in.
    fn fingerprint(path: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        hasher.finish()
    }

    /// Record a path as live. Returns `true` if it was not already present.
    pub fn insert(&mut self, path: &str) -> bool {
        self.fingerprints.insert(Self::fingerprint(path))
    }

    /// Whether `path` is (fingerprint-)live.
    ///
    /// Always `true` for a path that was inserted; may be `true` for a path
    /// that was not, only ever erring towards "keep the file".
    pub fn contains(&self, path: &str) -> bool {
        self.fingerprints.contains(&Self::fingerprint(path))
    }

    /// Number of distinct fingerprints held — one per live file, which is
    /// also the set's memory proxy (`len() * size_of::<u64>()` payload).
    pub fn len(&self) -> usize {
        self.fingerprints.len()
    }

    /// Whether no path has been inserted.
    pub fn is_empty(&self) -> bool {
        self.fingerprints.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserted_paths_are_always_contained() {
        let mut set = LiveFileSet::new();
        let paths: Vec<String> = (0..10_000)
            .map(|i| format!("t/d/traces/data/hour={}/{i:08}-abc.parquet", i % 24))
            .collect();
        for path in &paths {
            assert!(set.insert(path));
        }
        for path in &paths {
            assert!(set.contains(path), "live path {path} must be contained");
        }
        assert_eq!(set.len(), paths.len());
    }

    #[test]
    fn insert_is_idempotent_per_path() {
        let mut set = LiveFileSet::new();
        assert!(set.insert("a.parquet"));
        assert!(!set.insert("a.parquet"));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn unrelated_paths_are_not_contained() {
        let mut set = LiveFileSet::new();
        set.insert("t/d/traces/data/live.parquet");
        assert!(!set.contains("t/d/traces/data/orphan.parquet"));
        assert!(!set.contains(""));
    }
}
