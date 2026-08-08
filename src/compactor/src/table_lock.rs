//! # Per-table lifecycle serialization (design decision D6)
//!
//! Retention drops, snapshot expiration, and compaction commits are each
//! guarded cross-process by catalog CAS and (for compaction) input-scoped
//! conflict validation (`crate::commit`). But CAS turns a self-conflict into
//! a retry-until-starvation risk rather than clean serialization, and
//! snapshot expiration commits without any conflict retry at all. Inside one
//! compactor process, the three lifecycle actors for the *same* table can
//! simply take turns instead of racing the catalog: [`TableLockRegistry`]
//! hands out one [`tokio::sync::Mutex`] per `(tenant, dataset, table)` triple
//! so a caller holding that table's guard is guaranteed no other lifecycle
//! actor is concurrently touching the same table, while unrelated tables
//! never contend with each other.
//!
//! This is an in-process ordering optimization only — it does not replace
//! catalog CAS or D2's input-scoped conflict validation, which remain the
//! only guarantee that holds across multiple compactor instances.
//!
//! ## Growth
//!
//! The registry never evicts entries: a `Mutex` is created once per table
//! triple the process has ever touched and kept for the life of the
//! process. This is deliberate, not an oversight — the key space is the set
//! of `(tenant, dataset, table)` triples a deployment actually has, which is
//! bounded by configured/registered tenants × datasets × signal tables (at
//! most a handful of signal tables per dataset today: traces, logs, the
//! metrics subtypes, profiles). That is a small, slowly-changing set even at
//! the high end of SignalDB's homelab-first deployment target, so an unused
//! `Mutex<()>` sitting in a `DashMap` for the life of the process is
//! negligible next to the per-table state (leases, metrics, retention
//! config) the rest of the compactor already keeps for the same key space.

use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedMutexGuard};

/// Identifies one table for lock purposes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TableKey {
    tenant_id: String,
    dataset_id: String,
    table_name: String,
}

/// Hands out one async mutex per `(tenant, dataset, table)` triple.
///
/// Cheap to clone (`Arc<DashMap<..>>` internally) — share one instance
/// across every lifecycle actor that needs to serialize against the others
/// on the same table (compaction, retention, snapshot expiration).
#[derive(Debug, Clone, Default)]
pub struct TableLockRegistry {
    locks: Arc<DashMap<TableKey, Arc<Mutex<()>>>>,
}

impl TableLockRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Acquire the lock for `(tenant_id, dataset_id, table_name)`, waiting
    /// if another lifecycle actor currently holds it.
    ///
    /// Returns an owned guard so callers can hold it across `.await` points
    /// and move it into a spawned task without borrowing the registry.
    /// Dropping the guard releases the lock. Locks for other tables are
    /// unaffected — `DashMap` shards its internal locking per key, so
    /// looking up or inserting a different table's entry does not wait on
    /// this one.
    pub async fn lock(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> OwnedMutexGuard<()> {
        let key = TableKey {
            tenant_id: tenant_id.to_string(),
            dataset_id: dataset_id.to_string(),
            table_name: table_name.to_string(),
        };
        // `or_insert_with` only takes the map's per-shard lock briefly to
        // read-or-create the entry; the returned `Arc<Mutex<()>>` is then
        // locked independently, so a lookup for one table never waits on
        // another table's held guard.
        let mutex = self
            .locks
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        mutex.lock_owned().await
    }

    /// Number of distinct tables the registry currently holds a `Mutex`
    /// for. Test-only: production code has no need to inspect registry
    /// size.
    #[cfg(test)]
    pub(crate) fn table_count(&self) -> usize {
        self.locks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    /// The core D6 property at the primitive level: two callers contending
    /// for the *same* table cannot both be inside the critical section at
    /// once, but a *different* table is never blocked by it. Without the
    /// guard (e.g. a registry that handed out a fresh `Mutex` per call
    /// instead of reusing one per key) the same-table assertion below would
    /// fail — both spawned tasks would complete immediately.
    #[tokio::test(flavor = "multi_thread")]
    async fn same_table_serializes_and_different_tables_do_not() {
        let registry = TableLockRegistry::new();

        let held = registry.lock("acme", "prod", "traces").await;

        // Same table: a second lock attempt must wait for the first to be
        // released, not run concurrently with it.
        let registry_same = registry.clone();
        let mut same_table = tokio::spawn(async move {
            let _guard = registry_same.lock("acme", "prod", "traces").await;
        });
        assert!(
            timeout(Duration::from_millis(200), &mut same_table)
                .await
                .is_err(),
            "a second lock on the same table was acquired while the first was still held"
        );

        // A different table must proceed immediately — the guard must not
        // serialize the whole registry, only the contended table.
        let registry_other = registry.clone();
        timeout(
            Duration::from_millis(200),
            registry_other.lock("acme", "prod", "logs"),
        )
        .await
        .expect("a different table's lock must not be blocked by another table's lock");

        drop(held);
        timeout(Duration::from_secs(5), same_table)
            .await
            .expect("same-table lock is acquired once the holder releases it")
            .expect("lock-holding task does not panic");
    }

    /// The registry must reuse one `Mutex` per table rather than growing
    /// unboundedly with every call — see the module-level "Growth" note.
    #[tokio::test]
    async fn repeated_locks_on_one_table_reuse_a_single_entry() {
        let registry = TableLockRegistry::new();
        for _ in 0..5 {
            let _guard = registry.lock("acme", "prod", "traces").await;
        }
        assert_eq!(
            registry.table_count(),
            1,
            "repeated locks on the same table must not create new entries"
        );

        registry.lock("acme", "prod", "logs").await;
        assert_eq!(
            registry.table_count(),
            2,
            "a genuinely different table gets its own entry"
        );
    }
}
