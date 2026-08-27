//! WAL Manager for multi-tenant write-ahead log isolation
//!
//! This module provides WalManager which creates and caches WAL instances
//! per tenant/dataset/signal type combination, ensuring data isolation.
//!
//! Both the acceptor and the writer fan their WALs out through this type
//! (issue #932): one WAL per `(tenant, dataset, signal)` means one tenant's
//! poisoned segment, lock contention, or fsync latency cannot stall another
//! tenant's ingest path.

use crate::wal::{Wal, WalConfig};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Key for WAL cache: (tenant_id, dataset_id, signal_type)
pub type WalKey = (String, String, String);

/// Outcome of one [`WalManager::cleanup_all`] sweep.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CleanupStats {
    /// WALs whose cleanup completed.
    pub swept: usize,
    /// WALs whose cleanup returned an error and kept their segments.
    pub failed: usize,
}

/// Manager for creating and caching per-tenant/dataset WAL instances
///
/// The WalManager ensures that each unique combination of tenant_id, dataset_id,
/// and signal_type gets its own isolated WAL instance. WALs are created lazily
/// on first access and cached for reuse.
///
/// WAL paths follow the pattern: `.wal/{tenant}/{dataset}/{signal}/`
pub struct WalManager {
    /// Cache of WAL instances keyed by (tenant_id, dataset_id, signal_type)
    wals: Arc<Mutex<HashMap<WalKey, Arc<Wal>>>>,
    /// The legacy drain-only WAL [`Self::adopt_root_segments`] found, if any.
    ///
    /// This used to ride `wals` under a sentinel key
    /// ([`Self::LEGACY_ROOT_KEY`]), indistinguishable from a real tenant
    /// entry to any consumer except by comparing against that constant, and
    /// silently dropped with no re-adoption path by [`Self::clear_cache`].
    /// Modeling it as its own field makes the special case visible in the
    /// type and lets `clear_cache` leave it alone (#1308).
    legacy_wal: Arc<Mutex<Option<Arc<Wal>>>>,
    /// Per-key initialization guards to prevent duplicate WAL creation
    init_guards: Arc<Mutex<HashMap<WalKey, Arc<Mutex<()>>>>>,
    /// When [`Self::cleanup_all_if_due`] last let a sweep through; `None`
    /// until the first one.
    last_cleanup: Arc<Mutex<Option<std::time::Instant>>>,
    /// How long a WAL may go without an append before [`Self::evict_idle`]
    /// closes and drops it. Zero disables eviction.
    idle_timeout: std::time::Duration,
    /// Base configuration template for trace WALs
    traces_config: WalConfig,
    /// Base configuration template for log WALs
    logs_config: WalConfig,
    /// Base configuration template for metrics WALs
    metrics_config: WalConfig,
    /// Base configuration template for profile WALs
    profiles_config: WalConfig,
}

impl WalManager {
    /// Create a new WalManager with base configurations for each signal type
    ///
    /// # Arguments
    ///
    /// * `traces_config` - Base WAL configuration for traces
    /// * `logs_config` - Base WAL configuration for logs
    /// * `metrics_config` - Base WAL configuration for metrics
    /// * `profiles_config` - Base WAL configuration for profiles
    ///
    /// The `wal_dir` in each config should point to the base directory (e.g., `.wal`).
    /// The manager will create subdirectories per tenant/dataset/signal.
    pub fn new(
        traces_config: WalConfig,
        logs_config: WalConfig,
        metrics_config: WalConfig,
        profiles_config: WalConfig,
    ) -> Self {
        Self {
            wals: Arc::new(Mutex::new(HashMap::new())),
            legacy_wal: Arc::new(Mutex::new(None)),
            init_guards: Arc::new(Mutex::new(HashMap::new())),
            last_cleanup: Arc::new(Mutex::new(None)),
            idle_timeout: Self::DEFAULT_IDLE_TIMEOUT,
            traces_config,
            logs_config,
            metrics_config,
            profiles_config,
        }
    }

    /// Create a WalManager that uses the same base configuration for every
    /// signal type. `base.wal_dir` is the directory under which the
    /// `{tenant}/{dataset}/{signal}` tree is created.
    pub fn uniform(base: WalConfig) -> Self {
        Self::new(base.clone(), base.clone(), base.clone(), base)
    }

    /// How long a WAL may go without an append before [`Self::evict_idle`]
    /// gives it up.
    ///
    /// Long enough that a tenant reporting on a slow interval keeps its WAL
    /// across cycles, short enough that a tenant that stopped reporting stops
    /// costing descriptors within the hour. Reopening costs one directory
    /// scan, so the penalty for evicting too eagerly is small and the penalty
    /// for never evicting is `RLIMIT_NOFILE` (#1305).
    pub const DEFAULT_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(900);

    /// Override [`Self::DEFAULT_IDLE_TIMEOUT`]. Zero disables idle eviction.
    pub fn with_idle_timeout(mut self, idle_timeout: std::time::Duration) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    /// The key under which [`Self::all_wals`] reports the legacy
    /// drain-only WAL, matching what a pre-#932 writer's entries carry no
    /// routing metadata for. It uses a signal name no `get_wal` caller can
    /// produce, so it never collides with a real tenant/dataset/signal WAL.
    const LEGACY_ROOT_KEY: (&'static str, &'static str, &'static str) =
        ("_legacy", "_legacy", "_root");

    /// Register an already-open WAL under `key`, returning the instance it
    /// displaced, if any. Used by tests that build a WAL by hand and feed it
    /// to a manager, including the writer's (a different crate, hence the
    /// feature gate rather than `cfg(test)`).
    ///
    /// The displaced instance is handed back deliberately: two live `Wal`
    /// values over the same directory keep independent write handles and
    /// offset state, which is the desync class that #883 fixed. A caller that
    /// drops the returned `Arc` without any other clone alive is safe; one
    /// that keeps writing through it is not.
    #[cfg(any(test, feature = "testing"))]
    #[must_use = "the displaced WAL must not keep writing to the same directory"]
    pub async fn register(&self, key: WalKey, wal: Arc<Wal>) -> Option<Arc<Wal>> {
        let displaced = self.wals.lock().await.insert(key, wal);
        if displaced.is_none() {
            Self::record_instance_opened();
        }
        displaced
    }

    /// Every cached WAL — however it got there — counts towards
    /// `signaldb.wal.instances`. The instance count grows with the tenant
    /// count and nothing closes them, so this gauge is the early warning for
    /// file-descriptor and timer pressure; a WAL that entered the cache
    /// through a side door would make it lie.
    fn record_instance_opened() {
        crate::self_monitoring::app_metrics()
            .wal_instances
            .add(1, &[]);
    }

    /// The counterpart to [`Self::record_instance_opened`], for WALs the
    /// manager closes and drops (idle eviction, cache clear). The gauge only
    /// tells the truth if both halves stay in step.
    fn record_instances_closed(count: usize) {
        if count > 0 {
            crate::self_monitoring::app_metrics()
                .wal_instances
                .add(-(count as i64), &[]);
        }
    }

    /// Adopt segments left directly in the base directory by a pre-#932
    /// writer, which kept one global WAL there instead of a per-tenant tree.
    ///
    /// The segments are opened as a single drain-only WAL held in
    /// [`Self::legacy_wal`] and reported under [`Self::LEGACY_ROOT_KEY`] by
    /// [`Self::all_wals`], so a consumer iterating it still processes their
    /// pending entries: an entry that carries routing metadata is routed by
    /// it, and one that does not falls back to the WAL's configured
    /// tenant/dataset, which is why the config below stays `default`/`default`
    /// even though the reported key does not. New writes never go there:
    /// `get_wal` always resolves to the per-tenant tree.
    ///
    /// Once every adopted entry is drained the segments are reclaimed by the
    /// regular [`Self::cleanup_all_if_due`] sweep, so the adoption (and its
    /// warning) stops on the next restart. A warning that keeps repeating
    /// means entries are still undrained. See
    /// `docs/operations/wal-persistence.md`.
    ///
    /// Only `traces_config.wal_dir` is scanned, which covers every manager
    /// built with [`Self::uniform`] (the writer's). A manager built with
    /// [`Self::new`] and per-signal directories would need each of them
    /// scanned; no such manager has legacy root segments to adopt.
    ///
    /// Safe under concurrent invocation: the `legacy_wal` guard is held
    /// across the whole check-create-set sequence (including the `Wal::new`
    /// await), so two overlapping calls cannot both create a `Wal` and have
    /// the second silently replace the first's `Arc` without closing it.
    ///
    /// Returns whether a legacy root WAL was found and adopted.
    pub async fn adopt_root_segments(&self) -> Result<bool, anyhow::Error> {
        let base_dir = self.traces_config.wal_dir.clone();
        if !base_dir.is_dir() {
            return Ok(false);
        }
        if !Self::dir_has_wal_segments(&base_dir).await? {
            return Ok(false);
        }

        let mut legacy_wal = self.legacy_wal.lock().await;
        if legacy_wal.is_some() {
            return Ok(true);
        }

        tracing::warn!(
            wal_dir = %base_dir.display(),
            "Adopting legacy single-directory WAL segments for draining; new writes use \
             the per-tenant/dataset/signal tree"
        );
        // The reported KEY is `_legacy/_legacy/_root` so it can never
        // collide with a real WAL, but the CONFIG keeps `default`/`default`:
        // an entry that carries no metadata is stamped with, and routed by,
        // its WAL's configured tenant/dataset (`WalEntry::tenant_id`), and a
        // pre-#932 writer wrote exactly such entries into a
        // `default`/`default` WAL. Naming the config `_legacy` would
        // silently re-namespace that upgrade-time data into an Iceberg
        // namespace nobody asked for.
        let mut config = self.traces_config.clone();
        config.tenant_id = "default".to_string();
        config.dataset_id = "default".to_string();
        let wal = Wal::new(config).await?;
        *legacy_wal = Some(Arc::new(wal));
        Self::record_instance_opened();
        Ok(true)
    }

    /// Get or create a WAL for the given tenant, dataset, and signal type
    ///
    /// # Arguments
    ///
    /// * `tenant_id` - The tenant identifier
    /// * `dataset_id` - The dataset identifier
    /// * `signal_type` - The signal type ("traces", "logs", "metrics", or "profiles")
    ///
    /// # Returns
    ///
    /// An Arc<Wal> for the specified tenant/dataset/signal combination.
    /// The WAL is created if it doesn't exist, otherwise the cached instance is returned.
    ///
    /// # Errors
    ///
    /// Returns an error if the tenant or dataset id is not a valid identifier,
    /// or if WAL initialization fails.
    ///
    /// # Security
    ///
    /// `tenant_id` and `dataset_id` become path components of the WAL
    /// directory (`{wal_dir}/{tenant}/{dataset}/{signal}`), and
    /// [`std::path::PathBuf::join`] with an absolute path *replaces* the base
    /// while `..` escapes it. Both ids are therefore validated here — at the
    /// point where they turn into a path — rather than trusting every caller
    /// to have validated them at its own boundary. The writer's Flight
    /// surface, for instance, takes them from a request's `app_metadata`, and
    /// can be configured to run without authentication.
    pub async fn get_wal(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        signal_type: &str,
    ) -> Result<Arc<Wal>, anyhow::Error> {
        let tenant_id = crate::auth::validation::validate_id(tenant_id)
            .map_err(|e| anyhow::anyhow!("Invalid tenant ID for WAL path: {e}"))?;
        let dataset_id = crate::auth::validation::validate_id(dataset_id)
            .map_err(|e| anyhow::anyhow!("Invalid dataset ID for WAL path: {e}"))?;
        let (tenant_id, dataset_id) = (tenant_id.as_str(), dataset_id.as_str());

        let key = (
            tenant_id.to_string(),
            dataset_id.to_string(),
            signal_type.to_string(),
        );

        // Fast path: check cache first without per-key guard
        {
            let wals = self.wals.lock().await;
            if let Some(wal) = wals.get(&key) {
                tracing::debug!(
                    "Reusing existing WAL for tenant='{tenant_id}', dataset='{dataset_id}', signal='{signal_type}'"
                );
                return Ok(wal.clone());
            }
        }

        // Get or create per-key initialization guard
        let init_guard = self.init_guard_for(&key).await;

        // Acquire per-key lock to serialize initialization for this specific key
        let _guard = init_guard.lock().await;

        // Double-check cache after acquiring per-key lock (another thread might have created it)
        {
            let wals = self.wals.lock().await;
            if let Some(wal) = wals.get(&key) {
                tracing::debug!(
                    "WAL created by concurrent thread for tenant='{tenant_id}', dataset='{dataset_id}', signal='{signal_type}'"
                );
                return Ok(wal.clone());
            }
        }

        // Create new WAL - we now have exclusive access for this key
        tracing::info!(
            "Creating new WAL for tenant='{tenant_id}', dataset='{dataset_id}', signal='{signal_type}'"
        );

        // Get appropriate base config for this signal type
        let base_config = match signal_type {
            "traces" => &self.traces_config,
            "logs" => &self.logs_config,
            "metrics" => &self.metrics_config,
            "profiles" => &self.profiles_config,
            _ => {
                return Err(anyhow::anyhow!(
                    "Unknown signal type: {signal_type}. Must be 'traces', 'logs', 'metrics', or 'profiles'"
                ));
            }
        };

        // Create tenant/dataset-specific config
        let wal_config = base_config.for_tenant_dataset(tenant_id, dataset_id, signal_type);

        // Initialize WAL
        let mut wal = match Wal::new(wal_config).await {
            Ok(wal) => wal,
            Err(e) => return Err(e),
        };

        // Start background flush for this WAL
        wal.start_background_flush();

        let wal = Arc::new(wal);

        // Cache the WAL
        {
            let mut wals = self.wals.lock().await;
            wals.insert(key.clone(), wal.clone());
        }

        // The per-key guard stays resident. It used to be removed here to
        // bound growth, but it is keyed exactly like the WAL cache, so it
        // grows no faster than that. Keeping it is what lets [`Self::evict_idle`]
        // contend with a concurrent `get_wal` on the *same* mutex — a guard
        // recreated between the two would let a fresh WAL open a directory an
        // eviction is still closing, which is two live instances over one
        // directory (#883).

        Self::record_instance_opened();

        tracing::info!(
            "Successfully created WAL for tenant='{tenant_id}', dataset='{dataset_id}', signal='{signal_type}'"
        );

        Ok(wal)
    }

    /// How many WAL flushes [`Self::flush_all`] runs at once.
    const FLUSH_ALL_CONCURRENCY: usize = 32;

    /// Flush every cached WAL so buffered entries are durable before the
    /// process exits. Errors are logged per WAL and the first is returned
    /// after every WAL has been attempted — one tenant's failing flush must
    /// not skip the others.
    ///
    /// The flushes run concurrently, and the whole sweep is bounded by
    /// `budget`: a shutdown has a container's SIGTERM grace (commonly 10-30 s)
    /// to finish, and one fsync pair per WAL run back-to-back would blow past
    /// it once a deployment has a few hundred tenants — with the WALs late in
    /// the sweep dying silently. On timeout the WALs that were never reached
    /// are named in the error and logged, so lost buffered entries are
    /// attributable instead of invisible.
    pub async fn flush_all_within(&self, budget: std::time::Duration) -> Result<(), anyhow::Error> {
        use futures::StreamExt;

        let wals = self.all_wals().await;
        if wals.is_empty() {
            return Ok(());
        }
        let total = wals.len();
        // One lock, because the two are always written together: a completed
        // flush leaves `unflushed` and may set `first_err` in the same step.
        // Whatever remains in `unflushed` when the budget expires is what was
        // never made durable.
        struct FlushProgress {
            unflushed: HashSet<WalKey>,
            first_err: Option<anyhow::Error>,
        }
        let progress = Arc::new(Mutex::new(FlushProgress {
            unflushed: wals.iter().map(|(key, _)| key.clone()).collect(),
            first_err: None,
        }));

        let sweep = {
            let progress = progress.clone();
            async move {
                futures::stream::iter(wals)
                    .for_each_concurrent(Self::FLUSH_ALL_CONCURRENCY, |(key, wal)| {
                        let progress = progress.clone();
                        async move {
                            let result = wal.flush().await;
                            let mut progress = progress.lock().await;
                            progress.unflushed.remove(&key);
                            if let Err(e) = result {
                                let (tenant, dataset, signal) = &key;
                                tracing::error!(
                                    tenant_id = %tenant,
                                    dataset_id = %dataset,
                                    signal = %signal,
                                    error = %e,
                                    "Failed to flush WAL during shutdown"
                                );
                                progress.first_err.get_or_insert(e);
                            }
                        }
                    })
                    .await
            }
        };

        if tokio::time::timeout(budget, sweep).await.is_err() {
            let unreached: Vec<String> = progress
                .lock()
                .await
                .unflushed
                .iter()
                .map(|(tenant, dataset, signal)| format!("{tenant}/{dataset}/{signal}"))
                .collect();
            tracing::error!(
                unreached = unreached.len(),
                total,
                budget_secs = budget.as_secs_f64(),
                wals = %unreached.join(","),
                "WAL flush sweep did not finish within its budget; buffered entries in the \
                 unreached WALs are not durable"
            );
            return Err(anyhow::anyhow!(
                "WAL flush sweep timed out after {budget:?}: {} of {total} WALs unflushed ({})",
                unreached.len(),
                unreached.join(", ")
            ));
        }

        match progress.lock().await.first_err.take() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// [`Self::flush_all_within`] with the default shutdown budget.
    pub async fn flush_all(&self) -> Result<(), anyhow::Error> {
        self.flush_all_within(std::time::Duration::from_secs(10))
            .await
    }

    /// Run [`Self::cleanup_all`] if at least [`Self::cleanup_interval`] has
    /// passed since the last sweep, otherwise do nothing and report zeros.
    ///
    /// Callers invoke this at every pass boundary — the end of the writer's
    /// drain, the end of the acceptor's retry pass — and the throttle decides
    /// whether the sweep actually runs. Keeping the schedule here rather than
    /// in each loop is what lets a caller ask for cleanup at the only moment
    /// it is safe (no listed entries in flight) without pacing it itself.
    pub async fn cleanup_all_if_due(&self) -> CleanupStats {
        let due = {
            let mut last = self.last_cleanup.lock().await;
            let due = match *last {
                Some(at) => at.elapsed() >= self.cleanup_interval(),
                // Never swept: a process that restarts more often than the
                // interval would otherwise never reclaim anything.
                None => true,
            };
            if due {
                *last = Some(std::time::Instant::now());
            }
            due
        };
        let stats = if due {
            self.cleanup_all().await
        } else {
            CleanupStats::default()
        };

        // Eviction runs at every pass boundary, not on the cleanup throttle.
        // The boundary is what makes closing segments safe (no consumer is
        // mid-pass), but the *decision* is `idle_for` against the idle
        // timeout, so the scan is its own gate — and it is cheap, allocating
        // only for WALs that are actually idle. Pacing it with
        // `cleanup_interval_secs` instead would tie descriptor reclamation to
        // a disk-cleanup knob and delay it past `idle_timeout` whenever an
        // operator raised that knob.
        if !self.idle_timeout.is_zero() {
            self.evict_idle(self.idle_timeout).await;
        }
        stats
    }

    /// How often [`Self::cleanup_all_if_due`] lets a sweep through.
    ///
    /// The smallest `cleanup_interval_secs` across this manager's per-signal
    /// configs: a sweep covers every WAL at once, so it must run as often as
    /// the most eager signal asks for.
    pub fn cleanup_interval(&self) -> std::time::Duration {
        let secs = [
            &self.traces_config,
            &self.logs_config,
            &self.metrics_config,
            &self.profiles_config,
        ]
        .iter()
        .map(|c| c.cleanup_interval_secs)
        .min()
        .unwrap_or(300);
        std::time::Duration::from_secs(secs)
    }

    /// Run [`Wal::cleanup`] over every cached WAL: delete sealed segments
    /// whose entries are all processed, compact the rest.
    ///
    /// Until #1305 nothing called `Wal::cleanup` in any service, so processed
    /// segments were never reclaimed — they accumulated on disk and were
    /// re-read into memory at every start.
    ///
    /// **Call this from the task that drains these WALs, between passes.**
    /// Compaction rewrites sealed segments, and running it alongside a drain
    /// would have it moving entries under a consumer that is mid-pass.
    ///
    /// Different WALs share no state, so the sweep runs them concurrently:
    /// its cost is the slowest single WAL rather than the sum of all of them,
    /// which matters once a deployment has a few hundred tenants.
    ///
    /// One WAL's failure never skips the others. Failures are logged once per
    /// sweep, not once per WAL: at a few hundred tenants a failing disk would
    /// otherwise emit a log flood that self-monitoring re-ingests, which is
    /// the export-churn loop of the #865 incident.
    pub async fn cleanup_all(&self) -> CleanupStats {
        use futures::StreamExt;

        let wals = self.all_wals().await;
        if wals.is_empty() {
            return CleanupStats::default();
        }

        let failures = Arc::new(Mutex::new(Vec::new()));
        let swept = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        futures::stream::iter(wals)
            .for_each_concurrent(Self::CLEANUP_ALL_CONCURRENCY, |(key, wal)| {
                let failures = failures.clone();
                let swept = swept.clone();
                async move {
                    match wal.cleanup().await {
                        Ok(()) => {
                            swept.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        Err(e) => {
                            let (tenant, dataset, signal) = &key;
                            failures
                                .lock()
                                .await
                                .push(format!("{tenant}/{dataset}/{signal}: {e}"));
                        }
                    }
                }
            })
            .await;

        let failures = std::mem::take(&mut *failures.lock().await);
        if !failures.is_empty() {
            tracing::warn!(
                failed = failures.len(),
                wals = %failures.join("; "),
                "WAL cleanup failed for some WALs; their segments stay until the next pass"
            );
        }
        CleanupStats {
            swept: swept.load(std::sync::atomic::Ordering::Relaxed),
            failed: failures.len(),
        }
    }

    /// Close and drop every cached WAL — including the adopted legacy WAL,
    /// see [`Self::adopt_root_segments`] — that has taken no append for
    /// `idle_after` and holds no unprocessed entries. Returns how many were
    /// evicted.
    ///
    /// One `Wal` per `(tenant, dataset, signal)` costs three open file
    /// descriptors and one flush timer, and nothing ever released them: the
    /// count grew with the tenant count until the process hit `RLIMIT_NOFILE`,
    /// at which point `Wal::new` starts failing inside `do_put` and existing
    /// WALs fail segment rotation mid-flush — a write-path failure on data
    /// whose durability was already acknowledged (#1305).
    ///
    /// An idle WAL costs nothing to give up: the next write reopens it, and
    /// the entries are on disk either way.
    ///
    /// Two safety properties this relies on:
    ///
    /// - A WAL with unprocessed entries is **never** evicted. Reopening would
    ///   re-read them, but the cached instance is also what the drain loops
    ///   iterate, so evicting early would stall that tenant until new traffic
    ///   arrived.
    /// - Eviction takes the same per-key init guard `get_wal` takes, and closes
    ///   the instance while holding it. Two live `Wal` values over one
    ///   directory keep independent offset state — the desync class #883 fixed
    ///   — so the old instance must be inert before a new one can be created.
    ///   The legacy WAL has no such guard: `get_wal` rejects
    ///   [`Self::LEGACY_ROOT_KEY`]'s signal as unknown before it ever touches
    ///   a key, so nothing can race it there. The only other writer of
    ///   `legacy_wal` is [`Self::adopt_root_segments`], and the `legacy_wal`
    ///   mutex itself — held across the take, released before the awaits —
    ///   is what serializes against it.
    pub async fn evict_idle(&self, idle_after: std::time::Duration) -> usize {
        // Scan under the map lock and clone only the keys that are actually
        // idle — usually none. Cloning every key on every pass would allocate
        // three strings per cached WAL just to discard them.
        let candidates: Vec<WalKey> = {
            let wals = self.wals.lock().await;
            wals.iter()
                .filter(|(_, wal)| wal.idle_for() >= idle_after)
                .map(|(key, _)| key.clone())
                .collect()
        };

        let mut evicted = 0;
        for key in candidates {
            // Serialize against `get_wal` for this key for the whole
            // check-remove-close sequence.
            let guard = self.init_guard_for(&key).await;
            let _guard = guard.lock().await;

            let Some(wal) = self.wals.lock().await.get(&key).cloned() else {
                continue;
            };
            // Re-check under the guard: a write may have landed since the scan.
            if wal.idle_for() < idle_after {
                continue;
            }
            match wal.get_unprocessed_entries().await {
                Ok(entries) if entries.is_empty() => {}
                Ok(_) => continue,
                Err(e) => {
                    let (tenant, dataset, signal) = &key;
                    tracing::debug!(
                        tenant_id = %tenant,
                        dataset_id = %dataset,
                        signal = %signal,
                        error = %e,
                        "Could not check WAL backlog before eviction; keeping it"
                    );
                    continue;
                }
            }

            self.wals.lock().await.remove(&key);
            if let Err(e) = wal.close().await {
                let (tenant, dataset, signal) = &key;
                tracing::warn!(
                    tenant_id = %tenant,
                    dataset_id = %dataset,
                    signal = %signal,
                    error = %e,
                    "Failed to close an evicted WAL; its descriptors may leak until exit"
                );
            }
            Self::record_instances_closed(1);
            evicted += 1;

            // Retire the guard too, or `init_guards` becomes the same
            // unbounded-growth shape this method exists to fix — one resident
            // entry per `(tenant, dataset, signal)` ever seen, including
            // tenants that never come back.
            //
            // Only safe while no one else holds a clone: a `get_wal` blocked
            // on this guard would otherwise proceed against a mutex no longer
            // in the map, while the next caller creates a *different* one —
            // two creators for one directory. Both the map and this scope hold
            // a clone, so a count of exactly two means nobody is waiting, and
            // the `init_guards` lock is what a new waiter would have to take
            // to clone it.
            let mut guards = self.init_guards.lock().await;
            if Arc::strong_count(&guard) == 2 {
                guards.remove(&key);
            }
        }

        // The legacy WAL is a candidate too: it is drained by the same
        // consumers that iterate `all_wals`, and once drained it goes idle
        // immediately, since nothing ever writes to it again. Take it out of
        // the option before the async backlog check so a concurrent
        // `evict_idle` cannot also pick it up; put it back if it turns out
        // not to be eligible.
        let took_legacy = {
            let mut legacy = self.legacy_wal.lock().await;
            match legacy.as_ref() {
                Some(wal) if wal.idle_for() >= idle_after => legacy.take(),
                _ => None,
            }
        };
        if let Some(wal) = took_legacy {
            match wal.get_unprocessed_entries().await {
                Ok(entries) if entries.is_empty() => {
                    if let Err(e) = wal.close().await {
                        tracing::warn!(
                            error = %e,
                            "Failed to close the evicted legacy WAL; its descriptors may leak \
                             until exit"
                        );
                    }
                    Self::record_instances_closed(1);
                    evicted += 1;
                }
                Ok(_) => {
                    *self.legacy_wal.lock().await = Some(wal);
                }
                Err(e) => {
                    tracing::debug!(
                        error = %e,
                        "Could not check the legacy WAL's backlog before eviction; keeping it"
                    );
                    *self.legacy_wal.lock().await = Some(wal);
                }
            }
        }

        if evicted > 0 {
            tracing::info!(evicted, "Evicted idle WALs");
        }
        evicted
    }

    /// The per-key initialization guard, creating it if absent. Guards are
    /// kept for the manager's lifetime so creation and eviction of the same
    /// key always contend on the *same* mutex; a guard dropped between the two
    /// would let a fresh WAL open a directory an eviction is still closing.
    async fn init_guard_for(&self, key: &WalKey) -> Arc<Mutex<()>> {
        self.init_guards
            .lock()
            .await
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// How many WAL cleanups [`Self::cleanup_all`] runs at once. Matches
    /// [`Self::FLUSH_ALL_CONCURRENCY`]: the same per-WAL file I/O, bounded so
    /// a large tenant count cannot swamp the runtime's blocking pool.
    const CLEANUP_ALL_CONCURRENCY: usize = 32;

    /// Get the number of cached WAL instances, including the adopted legacy
    /// WAL if any.
    ///
    /// Useful for monitoring and debugging.
    pub async fn wal_count(&self) -> usize {
        let ordinary = self.wals.lock().await.len();
        let legacy = usize::from(self.legacy_wal.lock().await.is_some());
        ordinary + legacy
    }

    /// Snapshot of all cached WAL instances with their keys, including the
    /// adopted legacy WAL (see [`Self::adopt_root_segments`]) under
    /// [`Self::LEGACY_ROOT_KEY`] if one was found.
    ///
    /// Used by the WAL retry consumer to scan every tenant/dataset/signal
    /// WAL for unprocessed entries.
    pub async fn all_wals(&self) -> Vec<(WalKey, Arc<Wal>)> {
        let mut all: Vec<(WalKey, Arc<Wal>)> = self
            .wals
            .lock()
            .await
            .iter()
            .map(|(key, wal)| (key.clone(), wal.clone()))
            .collect();
        if let Some(wal) = self.legacy_wal.lock().await.clone() {
            let (tenant, dataset, signal) = Self::LEGACY_ROOT_KEY;
            all.push((
                (tenant.to_string(), dataset.to_string(), signal.to_string()),
                wal,
            ));
        }
        all
    }

    /// Discover WAL directories left on disk by previous runs and open them
    ///
    /// WALs are created lazily on first write, so after a restart a WAL with
    /// pending entries would not be in the cache until new traffic arrives
    /// for that tenant/dataset/signal — and entries from the previous run
    /// would never be retried. This scans the base WAL directories for
    /// `{tenant}/{dataset}/{signal}` layouts and opens each one.
    ///
    /// Returns the number of newly opened WAL instances.
    pub async fn discover_existing_wals(&self) -> Result<usize, anyhow::Error> {
        let mut base_dirs = Vec::new();
        for config in [
            &self.traces_config,
            &self.logs_config,
            &self.metrics_config,
            &self.profiles_config,
        ] {
            if !base_dirs.contains(&config.wal_dir) {
                base_dirs.push(config.wal_dir.clone());
            }
        }

        let mut opened = 0;
        for base_dir in base_dirs {
            if !base_dir.is_dir() {
                continue;
            }
            for (tenant, dataset, signal) in Self::scan_wal_layout(&base_dir).await? {
                let already_cached = {
                    let wals = self.wals.lock().await;
                    wals.contains_key(&(tenant.clone(), dataset.clone(), signal.clone()))
                };
                if already_cached {
                    continue;
                }
                match self.get_wal(&tenant, &dataset, &signal).await {
                    Ok(_) => {
                        opened += 1;
                        tracing::info!(
                            "Discovered existing WAL for tenant='{tenant}', dataset='{dataset}', signal='{signal}'"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to open discovered WAL for tenant='{tenant}', dataset='{dataset}', signal='{signal}': {e}"
                        );
                    }
                }
            }
        }

        Ok(opened)
    }

    /// Whether `dir` directly contains any `wal-*.log` segment files.
    async fn dir_has_wal_segments(dir: &std::path::Path) -> Result<bool, anyhow::Error> {
        let mut files = tokio::fs::read_dir(dir).await?;
        while let Some(file) = files.next_entry().await? {
            if let Some(name) = file.file_name().to_str()
                && name.starts_with("wal-")
                && name.ends_with(".log")
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Scan a base WAL directory for `{tenant}/{dataset}/{signal}` triples
    /// that contain WAL segment files.
    async fn scan_wal_layout(
        base_dir: &std::path::Path,
    ) -> Result<Vec<(String, String, String)>, anyhow::Error> {
        const SIGNALS: [&str; 4] = ["traces", "logs", "metrics", "profiles"];
        let mut found = Vec::new();

        let mut tenants = tokio::fs::read_dir(base_dir).await?;
        while let Some(tenant_entry) = tenants.next_entry().await? {
            if !tenant_entry.file_type().await?.is_dir() {
                continue;
            }
            let Some(tenant) = tenant_entry.file_name().to_str().map(String::from) else {
                continue;
            };

            let mut datasets = tokio::fs::read_dir(tenant_entry.path()).await?;
            while let Some(dataset_entry) = datasets.next_entry().await? {
                if !dataset_entry.file_type().await?.is_dir() {
                    continue;
                }
                let Some(dataset) = dataset_entry.file_name().to_str().map(String::from) else {
                    continue;
                };

                for signal in SIGNALS {
                    let signal_dir = dataset_entry.path().join(signal);
                    if !signal_dir.is_dir() {
                        continue;
                    }
                    // Only open directories that actually contain WAL segments
                    if Self::dir_has_wal_segments(&signal_dir).await? {
                        found.push((tenant.clone(), dataset.clone(), signal.to_string()));
                    }
                }
            }
        }

        Ok(found)
    }

    /// Close and drop all cached per-tenant WAL instances. They are
    /// recreated on next access; the files on disk are untouched.
    ///
    /// Each instance is closed rather than merely dropped: its flush task
    /// holds clones of the WAL's internals, so dropping the `Arc` alone leaves
    /// the timer running and the segments' descriptors open — the leak #1305
    /// is about.
    ///
    /// The adopted legacy WAL (see [`Self::adopt_root_segments`]) is left in
    /// place: it carries no tenant/dataset routing metadata, so once dropped
    /// it could only be found again by a process restart. `wal_count` and
    /// `all_wals` still report it after this call.
    pub async fn clear_cache(&self) {
        let drained: Vec<(WalKey, Arc<Wal>)> = self.wals.lock().await.drain().collect();
        for (key, wal) in &drained {
            if let Err(e) = wal.close().await {
                let (tenant, dataset, signal) = key;
                tracing::warn!(
                    tenant_id = %tenant,
                    dataset_id = %dataset,
                    signal = %signal,
                    error = %e,
                    "Failed to close a WAL while clearing the cache"
                );
            }
        }
        Self::record_instances_closed(drained.len());
        tracing::info!(cleared = drained.len(), "Cleared all cached WAL instances");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::time::Duration;
    use tempfile::TempDir;

    fn create_test_config(base_dir: &Path) -> WalConfig {
        let mut config = WalConfig::with_defaults(base_dir.to_path_buf());
        config.max_segment_size = 1024 * 1024; // 1MB for tests
        config.max_buffer_entries = 100;
        config.flush_interval_secs = 60;
        config
    }

    #[tokio::test]
    async fn test_wal_manager_creates_wal() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let manager = WalManager::new(
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
        );

        let _wal = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();

        // Verify WAL was created successfully
        // We can't access the config directly since it's private,
        // but we can verify the path was created on disk
        let expected_path = base_path.join("acme").join("production").join("traces");
        assert!(expected_path.exists());
    }

    #[tokio::test]
    async fn test_wal_manager_caches_wals() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let manager = WalManager::new(
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
        );

        // Get WAL first time
        let wal1 = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();

        // Get WAL second time - should be cached
        let wal2 = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();

        // Should be the same instance
        assert!(Arc::ptr_eq(&wal1, &wal2));

        // WAL count should be 1
        assert_eq!(manager.wal_count().await, 1);
    }

    #[tokio::test]
    async fn test_wal_manager_isolates_tenants() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let manager = WalManager::new(
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
        );

        let wal_acme = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();

        let wal_globex = manager
            .get_wal("globex", "production", "traces")
            .await
            .unwrap();

        // Different tenants should have different WALs
        assert!(!Arc::ptr_eq(&wal_acme, &wal_globex));

        // Verify different paths were created
        let path_acme = base_path.join("acme").join("production").join("traces");
        let path_globex = base_path.join("globex").join("production").join("traces");
        assert!(path_acme.exists());
        assert!(path_globex.exists());

        // Should have 2 WALs cached
        assert_eq!(manager.wal_count().await, 2);
    }

    #[tokio::test]
    async fn test_wal_manager_isolates_datasets() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let manager = WalManager::new(
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
        );

        let wal_prod = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();

        let wal_staging = manager.get_wal("acme", "staging", "traces").await.unwrap();

        // Different datasets should have different WALs
        assert!(!Arc::ptr_eq(&wal_prod, &wal_staging));

        // Verify different paths were created
        let path_prod = base_path.join("acme").join("production").join("traces");
        let path_staging = base_path.join("acme").join("staging").join("traces");
        assert!(path_prod.exists());
        assert!(path_staging.exists());

        // Should have 2 WALs cached
        assert_eq!(manager.wal_count().await, 2);
    }

    #[tokio::test]
    async fn test_wal_manager_isolates_signals() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let manager = WalManager::new(
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
        );

        let wal_traces = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();

        let wal_logs = manager.get_wal("acme", "production", "logs").await.unwrap();

        // Different signal types should have different WALs
        assert!(!Arc::ptr_eq(&wal_traces, &wal_logs));

        // Verify different paths were created
        let path_traces = base_path.join("acme").join("production").join("traces");
        let path_logs = base_path.join("acme").join("production").join("logs");
        assert!(path_traces.exists());
        assert!(path_logs.exists());

        // Should have 2 WALs cached
        assert_eq!(manager.wal_count().await, 2);
    }

    #[tokio::test]
    async fn test_wal_manager_creates_profiles_wal() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let manager = WalManager::new(
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
        );

        let _wal = manager
            .get_wal("acme", "production", "profiles")
            .await
            .unwrap();

        let expected_path = base_path.join("acme").join("production").join("profiles");
        assert!(expected_path.exists());
    }

    #[tokio::test]
    async fn adopt_root_segments_drains_a_legacy_global_wal_without_writing_to_it() {
        use crate::wal::WalOperation;

        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        // A pre-#932 writer left one global WAL directly in the base dir.
        let legacy = Wal::new(create_test_config(&base_path)).await.unwrap();
        let pending = legacy
            .append(
                WalOperation::WriteTraces,
                b"legacy".to_vec(),
                Some(r#"{"tenant_id":"acme","dataset_id":"production"}"#.to_string()),
            )
            .await
            .unwrap();
        legacy.flush().await.unwrap();
        drop(legacy);

        let manager = WalManager::uniform(create_test_config(&base_path));
        assert!(manager.adopt_root_segments().await.unwrap());
        // Idempotent.
        assert!(manager.adopt_root_segments().await.unwrap());
        assert_eq!(manager.wal_count().await, 1);

        let (key, root_wal) = manager.all_wals().await.into_iter().next().unwrap();
        assert_eq!(
            (key.0.as_str(), key.1.as_str(), key.2.as_str()),
            WalManager::LEGACY_ROOT_KEY
        );
        let entries = root_wal.get_unprocessed_entries().await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, pending);

        // New traffic for the same tenant goes to the per-tenant tree.
        let acme = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();
        assert!(!Arc::ptr_eq(&acme, &root_wal));
        assert!(base_path.join("acme/production/traces").is_dir());
    }

    #[tokio::test]
    async fn adopt_root_segments_is_a_noop_without_legacy_segments() {
        let temp_dir = TempDir::new().unwrap();
        let manager = WalManager::uniform(create_test_config(temp_dir.path()));
        assert!(!manager.adopt_root_segments().await.unwrap());
        assert_eq!(manager.wal_count().await, 0);
    }

    /// Sets up a base dir with one legacy segment and a manager that has
    /// adopted it plus two ordinary per-tenant WALs.
    async fn manager_with_legacy_and_ordinary_wals(base_path: &Path) -> WalManager {
        let legacy = Wal::new(create_test_config(base_path)).await.unwrap();
        legacy
            .append(
                crate::wal::WalOperation::WriteTraces,
                b"legacy".to_vec(),
                None,
            )
            .await
            .unwrap();
        legacy.flush().await.unwrap();
        drop(legacy);

        let manager = WalManager::uniform(create_test_config(base_path));
        assert!(manager.adopt_root_segments().await.unwrap());
        manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();
        manager.get_wal("acme", "production", "logs").await.unwrap();
        manager
    }

    #[tokio::test]
    async fn clear_cache_keeps_the_legacy_wal_but_drops_ordinary_ones() {
        // #1308: the legacy WAL used to ride the same map as ordinary
        // per-tenant WALs, so `clear_cache` dropped it with no re-adoption
        // path short of a process restart. It must now survive.
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();
        let manager = manager_with_legacy_and_ordinary_wals(&base_path).await;
        assert_eq!(manager.wal_count().await, 3, "legacy + 2 ordinary WALs");

        let (_, legacy_before) = manager
            .all_wals()
            .await
            .into_iter()
            .find(|(key, _)| {
                (key.0.as_str(), key.1.as_str(), key.2.as_str()) == WalManager::LEGACY_ROOT_KEY
            })
            .expect("legacy WAL present before clear_cache");

        manager.clear_cache().await;

        assert_eq!(
            manager.wals.lock().await.len(),
            0,
            "ordinary per-tenant WALs must be cleared"
        );

        let all = manager.all_wals().await;
        assert_eq!(all.len(), 1, "only the legacy WAL should remain");
        let (key, legacy_after) = &all[0];
        assert_eq!(
            (key.0.as_str(), key.1.as_str(), key.2.as_str()),
            WalManager::LEGACY_ROOT_KEY
        );
        assert!(
            Arc::ptr_eq(&legacy_before, legacy_after),
            "clear_cache must not close and silently reopen the legacy WAL"
        );

        // Usable: an operation against it does not hit a "closed" error.
        legacy_after.get_unprocessed_entries().await.unwrap();
    }

    #[tokio::test]
    async fn all_wals_yields_legacy_and_ordinary_wals_together() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();
        let manager = manager_with_legacy_and_ordinary_wals(&base_path).await;

        let keys: std::collections::HashSet<WalKey> = manager
            .all_wals()
            .await
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(keys.len(), 3, "legacy plus two ordinary WALs");
        let (tenant, dataset, signal) = WalManager::LEGACY_ROOT_KEY;
        assert!(keys.contains(&(tenant.to_string(), dataset.to_string(), signal.to_string())));
        assert!(keys.contains(&(
            "acme".to_string(),
            "production".to_string(),
            "traces".to_string()
        )));
        assert!(keys.contains(&(
            "acme".to_string(),
            "production".to_string(),
            "logs".to_string()
        )));
    }

    #[tokio::test]
    async fn get_wal_rejects_ids_that_would_escape_the_wal_directory() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();
        let manager = WalManager::uniform(create_test_config(&base_path));
        let escape_target = temp_dir.path().parent().unwrap().join("escaped");

        // `PathBuf::join` with an absolute path REPLACES the base, and `..`
        // walks out of it, so an unvalidated id from a request would be a
        // write primitive anywhere on the filesystem.
        for (tenant, dataset) in [
            ("../../escaped", "production"),
            ("acme", "../../escaped"),
            ("/etc/signaldb", "production"),
            ("acme", "/etc/signaldb"),
            ("acme/production", "traces"),
            ("", "production"),
            ("acme", ""),
        ] {
            let err = match manager.get_wal(tenant, dataset, "traces").await {
                Ok(_) => panic!("id must be rejected: {tenant:?}/{dataset:?}"),
                Err(e) => e.to_string(),
            };
            assert!(
                err.contains("Invalid tenant ID for WAL path")
                    || err.contains("Invalid dataset ID for WAL path"),
                "unexpected error for {tenant:?}/{dataset:?}: {err}"
            );
        }

        assert_eq!(manager.wal_count().await, 0);
        assert!(
            !escape_target.exists(),
            "nothing was created outside the WAL directory"
        );
        assert!(!std::path::Path::new("/etc/signaldb").exists());
    }

    #[tokio::test]
    async fn test_wal_manager_invalid_signal_type() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let manager = WalManager::new(
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
        );

        let result = manager.get_wal("acme", "production", "invalid").await;

        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("Unknown signal type")
        );
    }

    #[tokio::test]
    async fn test_wal_manager_clear_cache() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let manager = WalManager::new(
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
        );

        // Create some WALs
        manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();
        manager.get_wal("acme", "production", "logs").await.unwrap();

        assert_eq!(manager.wal_count().await, 2);

        // Clear cache
        manager.clear_cache().await;

        assert_eq!(manager.wal_count().await, 0);

        // Getting WAL again should create new instance
        manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();
        assert_eq!(manager.wal_count().await, 1);
    }

    #[tokio::test]
    async fn test_wal_manager_concurrent_initialization_no_duplicates() {
        use std::sync::Arc;
        use tokio::task::JoinSet;

        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let manager = Arc::new(WalManager::new(
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
            create_test_config(&base_path),
        ));

        // Spawn 10 concurrent tasks all trying to get the same WAL
        let mut join_set = JoinSet::new();

        for i in 0..10 {
            let manager_clone = Arc::clone(&manager);
            join_set.spawn(async move {
                let wal = manager_clone
                    .get_wal("acme", "production", "traces")
                    .await
                    .unwrap();
                (i, wal)
            });
        }

        // Collect all results
        let mut wals = Vec::new();
        while let Some(result) = join_set.join_next().await {
            let (_task_id, wal) = result.unwrap();
            wals.push(wal);
        }

        // All 10 tasks should have gotten the same WAL instance
        assert_eq!(wals.len(), 10);

        // Verify all WALs are the same instance (same Arc pointer)
        for i in 1..wals.len() {
            assert!(
                Arc::ptr_eq(&wals[0], &wals[i]),
                "WAL at index {i} is not the same instance as WAL at index 0"
            );
        }

        // Verify only 1 WAL was created
        assert_eq!(manager.wal_count().await, 1);
    }

    #[tokio::test]
    async fn cleanup_all_reclaims_every_cached_wal() {
        // Cleanup had no caller in any service (#1305). The sweep is what the
        // writer and acceptor call at the end of a pass, so it must reach
        // every cached WAL, not just the first.
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();
        let manager = WalManager::new(
            crate::wal::test_support::rotating_config(&base_path),
            crate::wal::test_support::rotating_config(&base_path),
            crate::wal::test_support::rotating_config(&base_path),
            crate::wal::test_support::rotating_config(&base_path),
        );

        for signal in ["traces", "logs"] {
            let wal = manager.get_wal("acme", "production", signal).await.unwrap();
            for i in 0..12u64 {
                let id = wal
                    .append(
                        crate::wal::WalOperation::WriteTraces,
                        vec![b'x'; 200 + i as usize],
                        None,
                    )
                    .await
                    .unwrap();
                wal.flush().await.unwrap();
                wal.mark_processed(id).await.unwrap();
            }
            assert!(
                wal.segment_count().await > 1,
                "{signal} WAL should have sealed segments before cleanup"
            );
        }

        let stats = manager.cleanup_all().await;
        assert_eq!(stats.swept, 2, "both cached WALs must be swept");
        assert_eq!(stats.failed, 0, "no WAL should fail cleanup here");
        assert_eq!(
            manager.cleanup_interval(),
            std::time::Duration::from_secs(300),
            "the sweep cadence is the smallest per-signal cleanup interval"
        );

        for signal in ["traces", "logs"] {
            let wal = manager.get_wal("acme", "production", signal).await.unwrap();
            assert_eq!(
                wal.segment_count().await,
                1,
                "{signal} WAL kept sealed segments whose entries were all processed"
            );
        }
    }

    #[tokio::test]
    async fn cleanup_all_if_due_sweeps_once_then_throttles() {
        // The services call this at every pass boundary — many times a minute
        // — and the throttle is what keeps that from compacting segments on
        // every tick. The first call must still sweep: a process restarting
        // more often than the interval would otherwise never reclaim anything.
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();
        let manager = WalManager::new(
            crate::wal::test_support::rotating_config(&base_path),
            crate::wal::test_support::rotating_config(&base_path),
            crate::wal::test_support::rotating_config(&base_path),
            crate::wal::test_support::rotating_config(&base_path),
        );

        let wal = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();
        for i in 0..12u64 {
            let id = wal
                .append(
                    crate::wal::WalOperation::WriteTraces,
                    vec![b'x'; 200 + i as usize],
                    None,
                )
                .await
                .unwrap();
            wal.flush().await.unwrap();
            wal.mark_processed(id).await.unwrap();
        }

        let first = manager.cleanup_all_if_due().await;
        assert_eq!(first.swept, 1, "the first call must sweep");
        assert_eq!(
            wal.segment_count().await,
            1,
            "the swept WAL should have kept only its open segment"
        );

        let second = manager.cleanup_all_if_due().await;
        assert_eq!(
            second,
            CleanupStats::default(),
            "a call inside the interval must not sweep again"
        );
    }

    /// A manager whose four signal configs all point at `base_dir`.
    fn uniform_manager(base_dir: &Path) -> WalManager {
        WalManager::uniform(create_test_config(base_dir))
    }

    /// Append `payload`, make it durable, and mark it processed — i.e. leave
    /// the WAL with an empty backlog, which is the state eviction requires.
    async fn write_and_drain(wal: &Wal, payload: &[u8]) {
        let id = wal
            .append(
                crate::wal::WalOperation::WriteTraces,
                payload.to_vec(),
                None,
            )
            .await
            .unwrap();
        wal.flush().await.unwrap();
        wal.mark_processed(id).await.unwrap();
    }

    #[tokio::test]
    async fn idle_drained_wals_are_evicted_and_their_files_closed() {
        // One WAL per (tenant, dataset, signal) and nothing ever closed them:
        // three file descriptors and one flush timer each, growing with the
        // tenant count until the process hits RLIMIT_NOFILE (#1305). An idle,
        // fully-drained WAL is reopened on the next write, so holding it costs
        // resources for nothing.
        let temp_dir = TempDir::new().unwrap();
        let manager = uniform_manager(temp_dir.path());

        let wal = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();
        write_and_drain(&wal, b"payload").await;
        assert_eq!(manager.wal_count().await, 1);

        // Idle threshold of zero: everything drained is immediately evictable.
        let evicted = manager.evict_idle(Duration::from_secs(0)).await;
        assert_eq!(evicted, 1, "a drained, idle WAL must be evicted");
        assert_eq!(manager.wal_count().await, 0, "the cache must drop it");

        // The evicted instance is inert: its segments are closed, so a caller
        // still holding this clone gets an error rather than silently losing
        // the payload.
        // The append itself only buffers, so the error surfaces at the flush
        // that would otherwise report those bytes as durable.
        let err = match wal
            .append(
                crate::wal::WalOperation::WriteTraces,
                b"after".to_vec(),
                None,
            )
            .await
        {
            Ok(_) => wal
                .flush()
                .await
                .expect_err("an evicted WAL must not accept further writes as durable"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("closed"),
            "expected a closed-WAL error, got: {err}"
        );
    }

    #[tokio::test]
    async fn idle_drained_legacy_wal_is_evicted_too() {
        // #1308: the legacy WAL used to ride `wals` and so was evicted like
        // any other idle, drained WAL. Moving it to its own field must not
        // pin it resident for the process lifetime — it goes idle the
        // instant it is drained, since nothing writes to it again.
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let legacy = Wal::new(create_test_config(&base_path)).await.unwrap();
        let id = legacy
            .append(
                crate::wal::WalOperation::WriteTraces,
                b"legacy".to_vec(),
                None,
            )
            .await
            .unwrap();
        legacy.flush().await.unwrap();
        legacy.mark_processed(id).await.unwrap();
        drop(legacy);

        let manager = uniform_manager(&base_path);
        assert!(manager.adopt_root_segments().await.unwrap());
        assert_eq!(manager.wal_count().await, 1);

        let (_, legacy_wal) = manager.all_wals().await.into_iter().next().unwrap();

        let evicted = manager.evict_idle(Duration::from_secs(0)).await;
        assert_eq!(evicted, 1, "a drained, idle legacy WAL must be evicted");
        assert_eq!(manager.wal_count().await, 0, "the cache must drop it");
        assert!(
            manager.all_wals().await.is_empty(),
            "all_wals must no longer report the evicted legacy WAL's key"
        );

        // The evicted instance is inert, same as an ordinary evicted WAL.
        let err = match legacy_wal
            .append(
                crate::wal::WalOperation::WriteTraces,
                b"after".to_vec(),
                None,
            )
            .await
        {
            Ok(_) => legacy_wal
                .flush()
                .await
                .expect_err("an evicted legacy WAL must not accept further writes as durable"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("closed"),
            "expected a closed-WAL error, got: {err}"
        );
    }

    #[tokio::test]
    async fn a_legacy_wal_with_undrained_entries_is_never_evicted() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let legacy = Wal::new(create_test_config(&base_path)).await.unwrap();
        legacy
            .append(
                crate::wal::WalOperation::WriteTraces,
                b"pending".to_vec(),
                None,
            )
            .await
            .unwrap();
        legacy.flush().await.unwrap();
        drop(legacy);

        let manager = uniform_manager(&base_path);
        assert!(manager.adopt_root_segments().await.unwrap());

        let evicted = manager.evict_idle(Duration::from_secs(0)).await;
        assert_eq!(
            evicted, 0,
            "a legacy WAL with unprocessed entries must be kept"
        );
        assert_eq!(manager.wal_count().await, 1);
    }

    #[tokio::test]
    async fn concurrent_adopt_root_segments_adopts_the_legacy_wal_exactly_once() {
        use tokio::task::JoinSet;

        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let legacy = Wal::new(create_test_config(&base_path)).await.unwrap();
        legacy
            .append(
                crate::wal::WalOperation::WriteTraces,
                b"legacy".to_vec(),
                None,
            )
            .await
            .unwrap();
        legacy.flush().await.unwrap();
        drop(legacy);

        let manager = Arc::new(uniform_manager(&base_path));

        let mut join_set = JoinSet::new();
        for _ in 0..10 {
            let manager = manager.clone();
            join_set.spawn(async move { manager.adopt_root_segments().await.unwrap() });
        }
        while let Some(result) = join_set.join_next().await {
            assert!(result.unwrap());
        }

        assert_eq!(
            manager.wal_count().await,
            1,
            "only one legacy WAL must be adopted, however many callers raced to do it"
        );
    }

    #[tokio::test]
    async fn eviction_retires_the_per_key_init_guard_too() {
        // Guards are kept resident so eviction and `get_wal` contend on the
        // same mutex. That is only sound if eviction also retires the guard:
        // otherwise `init_guards` grows with every tenant ever seen and
        // becomes the same unbounded-growth shape eviction exists to fix,
        // just cheaper per entry.
        let temp_dir = TempDir::new().unwrap();
        let manager = uniform_manager(temp_dir.path());

        let wal = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();
        write_and_drain(&wal, b"payload").await;
        assert_eq!(manager.init_guards.lock().await.len(), 1);

        assert_eq!(manager.evict_idle(Duration::from_secs(0)).await, 1);
        assert!(
            manager.init_guards.lock().await.is_empty(),
            "an evicted key must not leave its init guard behind"
        );
    }

    #[tokio::test]
    async fn a_wal_with_undrained_entries_is_never_evicted() {
        // Eviction must never discard a WAL whose entries have not been
        // committed downstream: reopening re-reads them from disk, but the
        // instance is also what the drain loop iterates, so dropping it early
        // would stall that tenant until new traffic arrived.
        let temp_dir = TempDir::new().unwrap();
        let manager = uniform_manager(temp_dir.path());

        let wal = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();
        wal.append(
            crate::wal::WalOperation::WriteTraces,
            b"pending".to_vec(),
            None,
        )
        .await
        .unwrap();
        wal.flush().await.unwrap();

        let evicted = manager.evict_idle(Duration::from_secs(0)).await;
        assert_eq!(evicted, 0, "a WAL with unprocessed entries must be kept");
        assert_eq!(manager.wal_count().await, 1);
    }

    #[tokio::test]
    async fn a_recently_written_wal_is_not_evicted() {
        let temp_dir = TempDir::new().unwrap();
        let manager = uniform_manager(temp_dir.path());

        let wal = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();
        write_and_drain(&wal, b"payload").await;

        let evicted = manager.evict_idle(Duration::from_secs(3600)).await;
        assert_eq!(evicted, 0, "a WAL written to just now is not idle");
        assert_eq!(manager.wal_count().await, 1);
    }

    #[tokio::test]
    async fn reopening_an_evicted_wal_serves_a_fresh_instance() {
        // Eviction is only safe if the next write gets a *new* instance over
        // the same directory. Two live `Wal` values sharing a directory keep
        // independent offset state, which is the desync class #883 fixed.
        let temp_dir = TempDir::new().unwrap();
        let manager = uniform_manager(temp_dir.path());

        let first = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();
        write_and_drain(&first, b"one").await;

        assert_eq!(manager.evict_idle(Duration::from_secs(0)).await, 1);

        let second = manager
            .get_wal("acme", "production", "traces")
            .await
            .unwrap();
        assert!(
            !Arc::ptr_eq(&first, &second),
            "reopening must not hand back the evicted instance"
        );

        // The fresh instance writes at correct offsets over the same directory.
        let id = second
            .append(crate::wal::WalOperation::WriteTraces, b"two".to_vec(), None)
            .await
            .unwrap();
        second.flush().await.unwrap();
        let entries = second.get_entries().await.unwrap();
        let entry = entries.iter().find(|e| e.id == id).expect("entry present");
        assert_eq!(
            second.read_entry_data(entry).await.unwrap(),
            b"two".to_vec()
        );
    }
}
