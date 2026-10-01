//! #1342: idle eviction ([`WalManager::evict_idle`]) reclaims only WALs that
//! have gone *quiet* — no append for the idle window and no unprocessed
//! entries. It has no separate cap on how many WAL instances the manager
//! caches at once.
//!
//! A deployment whose concurrently-active `(tenant, dataset, signal)`
//! cardinality alone is large enough to exhaust `RLIMIT_NOFILE` is not
//! helped by this eviction: every one of those WALs is touched inside every
//! idle window (that is what "actively-written" means), so `idle_for()`
//! never crosses the threshold and none of them is ever a candidate. There
//! is no leak here — the ceiling is reached purely from legitimate traffic.
//!
//! This test opens N actively-written, fully-drained WALs (append, flush,
//! mark processed — so the "no unprocessed entries" guard alone would not
//! explain what happens next) and shows that a same-instant `evict_idle`
//! sweep, run with the manager's real default idle window, evicts none of
//! them and the cache still holds all N: nothing bounds concurrently-active
//! WAL count short of the OS file-descriptor limit.

use std::path::PathBuf;
use std::time::Duration;

use common::wal::manager::WalManager;
use common::wal::{WalConfig, WalOperation};
use tempfile::TempDir;

fn wal_config(dir: PathBuf) -> WalConfig {
    WalConfig {
        wal_dir: dir,
        max_segment_size: 1024 * 1024,
        max_buffer_entries: 100,
        flush_interval_secs: 60,
        tenant_id: "default".to_string(),
        dataset_id: "default".to_string(),
        retention_secs: 3600,
        cleanup_interval_secs: 300,
        compaction_threshold: 0.5,
    }
}

/// Concurrently-active `(tenant, dataset, signal)` combinations, standing in
/// for a fleet whose legitimate cardinality alone approaches a descriptor
/// limit. Kept small for test speed — the mechanism under test does not
/// depend on N being anywhere near a real `RLIMIT_NOFILE`.
const ACTIVE_KEY_COUNT: usize = 50;

#[tokio::test]
async fn idle_eviction_does_not_bound_a_fleet_of_actively_written_wals() {
    let temp_dir = TempDir::new().unwrap();
    let manager = WalManager::uniform(wal_config(temp_dir.path().to_path_buf()));

    for i in 0..ACTIVE_KEY_COUNT {
        let wal = manager
            .get_wal(&format!("tenant-{i}"), "production", "traces")
            .await
            .unwrap();
        // Actively written *and* fully drained: isolates idle_for() as the
        // reason eviction skips it, independent of the separate
        // no-unprocessed-entries guard.
        let id = wal
            .append(WalOperation::WriteTraces, b"payload".to_vec(), None)
            .await
            .unwrap();
        wal.flush().await.unwrap();
        wal.mark_processed(id).await.unwrap();
    }
    assert_eq!(manager.wal_count().await, ACTIVE_KEY_COUNT);

    // The idle window a real deployment runs eviction under. Every WAL above
    // was touched moments ago, so none of them is idle_for() >= this.
    let evicted = manager.evict_idle(WalManager::DEFAULT_IDLE_TIMEOUT).await;

    assert_eq!(
        evicted, 0,
        "idle eviction must not reclaim WALs that were just written to"
    );
    assert_eq!(
        manager.wal_count().await,
        ACTIVE_KEY_COUNT,
        "WalManager caches every actively-written WAL with no cap independent \
         of idleness: idle eviction alone does not bound a fleet whose active \
         (tenant, dataset, signal) cardinality exhausts RLIMIT_NOFILE"
    );

    // Sanity: eviction is not simply broken — an idle window of zero still
    // reclaims these same, now-drained WALs. This confirms the fleet-sized
    // sweep above ran the real mechanism, not a no-op.
    let evicted_at_zero = manager.evict_idle(Duration::from_secs(0)).await;
    assert_eq!(
        evicted_at_zero, ACTIVE_KEY_COUNT,
        "the same WALs must be evictable once idle_for() actually crosses the \
         threshold, proving evict_idle itself works"
    );
    assert_eq!(manager.wal_count().await, 0);
}

/// The regression this issue's fix owns: the same actively-written fleet
/// above, against a manager with an explicit `[wal].max_instances` cap,
/// settles at the cap instead of growing without bound.
///
/// Each WAL is force-aged to look idle the instant it is drained
/// (`set_last_append_secs`), isolating "does the cap bound an actively-
/// written fleet" from the cap's separate `CAP_MIN_IDLE` thrash damper,
/// which is a timing detail unrelated to what this test verifies.
#[tokio::test]
async fn an_instance_cap_bounds_the_same_actively_written_fleet() {
    let temp_dir = TempDir::new().unwrap();
    let manager =
        WalManager::uniform(wal_config(temp_dir.path().to_path_buf())).with_max_instances(8);

    for i in 0..ACTIVE_KEY_COUNT {
        let wal = manager
            .get_wal(&format!("tenant-{i}"), "production", "traces")
            .await
            .unwrap();
        let id = wal
            .append(WalOperation::WriteTraces, b"payload".to_vec(), None)
            .await
            .unwrap();
        wal.flush().await.unwrap();
        wal.mark_processed(id).await.unwrap();
        wal.set_last_append_secs(0);
    }

    assert_eq!(
        manager.wal_count().await,
        8,
        "an instance cap must bound the same actively-written fleet that idle \
         eviction alone cannot"
    );
}
