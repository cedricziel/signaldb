//! Orphan file detection logic.
//!
//! This module implements the safety-critical orphan detection algorithm
//! that identifies data files no longer referenced by any retained snapshot.
//!
//! ## Detection Algorithm (4 Phases)
//!
//! 1. **Build Live Reference Set**: Union the manifests of every retained
//!    snapshot and collect their non-deleted file paths. Liveness is never
//!    derived from snapshot or manifest age (issue #925) — snapshot
//!    expiration is what shrinks the retained set.
//! 2. **Scan Object Store**: Stream the `.parquet` listing under the table's
//!    data location.
//! 3. **Identify Candidates**: Files not in reference set AND older than
//!    grace period, decided per listing entry as it streams by.
//! 4. **Revalidation**: Re-check orphan status against a fresh live set
//!    before deletion (see [`crate::orphan::cleaner`]).
//!
//! ## Memory and I/O bounds (issue #475)
//!
//! Detection holds two things that grow with the table: the live set and
//! the candidate list. Neither is proportional to the whole listing.
//!
//! - Manifests are deduplicated across retained snapshots before any
//!   manifest is fetched, so a manifest shared by N snapshots is read once;
//!   manifest-list reads are one per retained snapshot, which snapshot
//!   expiration keeps small.
//! - Manifest entries are streamed: only a 64-bit fingerprint per live
//!   file is retained ([`LiveFileSet`]), never the entry or the path.
//! - The object-store listing is streamed and filtered against the live set
//!   as it arrives; only orphan candidates (path + size + mtime) are kept.
//!   Live files, and files inside the grace period, cost nothing beyond the
//!   lookup.

use crate::iceberg::{LiveFileSet, ManifestReader};
use crate::orphan::config::OrphanCleanupConfig;
use crate::orphan::metrics::{OrphanMetrics, SkipReason};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use common::catalog_manager::CatalogManager;
use common::iceberg::names::build_table_location;
use futures::Stream;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use std::sync::Arc;
use tokio_stream::StreamExt;

/// Orphan candidate with metadata.
#[derive(Debug, Clone)]
pub struct OrphanCandidate {
    /// Full path to the orphan file.
    pub path: String,
    /// File size in bytes.
    pub size_bytes: usize,
    /// Last modification timestamp.
    pub last_modified: DateTime<Utc>,
    /// Table identifier (tenant/dataset/table).
    pub table_identifier: String,
}

/// Outcome of one streamed listing pass.
#[derive(Debug, Default)]
struct ScanOutcome {
    /// Files that are neither live nor inside the grace period.
    candidates: Vec<OrphanCandidate>,
    /// Listing entries of the requested kind that were examined.
    scanned: usize,
}

/// Orphan file detector.
///
/// Identifies data files that are no longer referenced by any live snapshot
/// using a multi-phase validation approach.
pub struct OrphanDetector {
    config: OrphanCleanupConfig,
    catalog_manager: Arc<CatalogManager>,
    object_store: Arc<dyn ObjectStore>,
    manifest_reader: ManifestReader,
    metrics: OrphanMetrics,
}

impl OrphanDetector {
    /// Create a new orphan detector.
    pub fn new(
        config: OrphanCleanupConfig,
        catalog_manager: Arc<CatalogManager>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        let manifest_reader = ManifestReader::new();
        Self {
            config,
            catalog_manager,
            object_store,
            manifest_reader,
            metrics: OrphanMetrics::new(),
        }
    }

    /// Return a reference to the accumulated metrics for this detector.
    pub fn metrics(&self) -> &OrphanMetrics {
        &self.metrics
    }

    /// Identify orphan candidates for a specific table.
    ///
    /// This method implements the complete detection algorithm:
    /// 1. Build live file reference set from manifests
    /// 2. Stream the object store listing, retaining only candidates
    ///    (not in reference set + older than grace period)
    ///
    /// # Arguments
    ///
    /// * `tenant_id` - Tenant identifier
    /// * `dataset_id` - Dataset identifier
    /// * `table_name` - Table name (e.g., "traces", "logs", "metrics")
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Table metadata cannot be loaded
    /// - Manifests cannot be read
    /// - Object store is unavailable
    pub async fn identify_orphan_candidates(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<Vec<OrphanCandidate>> {
        tracing::info!(
            signaldb.tenant.id = %tenant_id,
            signaldb.dataset.id = %dataset_id,
            signaldb.table = %table_name,
            "Starting orphan detection"
        );

        // Phase 1: Build live reference set from manifests.
        // Returns None when the threshold guard trips → skip cleanup for this table.
        let live_files = match self
            .build_live_reference_set(tenant_id, dataset_id, table_name)
            .await
            .context("Failed to build live file reference set")?
        {
            Some(set) => set,
            None => {
                // Threshold exceeded; return empty candidate list (cleanup skipped).
                return Ok(vec![]);
            }
        };

        tracing::info!(
            signaldb.tenant.id = %tenant_id,
            signaldb.dataset.id = %dataset_id,
            signaldb.table = %table_name,
            signaldb.job.live_files = live_files.len() as i64,
            "Built live file reference set"
        );

        // Phases 2+3: stream the data listing, keeping only candidates.
        // Format: /{tenant_slug}/{dataset_slug}/{table_name}/data/
        let table_location = build_table_location(tenant_id, dataset_id, table_name);
        let data_path = format!("{table_location}/data/");
        let listing = self
            .object_store
            .list(Some(&ObjectPath::from(data_path.as_str())));
        let outcome = self
            .scan_candidates(
                listing,
                |path| path.ends_with(".parquet"),
                &live_files,
                |_| {},
                &table_location,
            )
            .await
            .with_context(|| format!("Failed to scan object store at path: {data_path}"))?;

        tracing::info!(
            signaldb.tenant.id = %tenant_id,
            signaldb.dataset.id = %dataset_id,
            signaldb.table = %table_name,
            signaldb.job.total_files = outcome.scanned as i64,
            signaldb.job.candidates = outcome.candidates.len() as i64,
            signaldb.job.grace_period_hours = self.config.grace_period_hours as i64,
            "Identified orphan candidates"
        );

        Ok(outcome.candidates)
    }

    /// Build live file reference set from table snapshots.
    ///
    /// Unions the manifests of every retained snapshot and collects all
    /// referenced data file paths.
    ///
    /// Returns `Ok(None)` when the `max_live_files_threshold` guard trips,
    /// signalling that the caller should skip orphan cleanup for this table.
    async fn build_live_reference_set(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<Option<LiveFileSet>> {
        let table = self.load_table(tenant_id, dataset_id, table_name).await?;

        // The live set is the union of every retained snapshot's manifests:
        // Iceberg reuses manifests across snapshots, so a manifest's age says
        // nothing about whether its files are still referenced (issue #925).
        // Snapshot expiration — not an age window here — is what makes files
        // eligible for cleanup.
        let manifests = self
            .manifest_reader
            .collect_retained_manifests(&table)
            .await
            .context("Failed to collect retained snapshot manifests")?;

        // Cheap threshold check using manifest list metadata before reading
        // the manifest files themselves. Sums the file count estimates from
        // ManifestListEntry metadata (no manifest reads needed). If the
        // estimate exceeds the configured cap, skip cleanup with a warning
        // rather than risking excessive memory use.
        if self.config.max_live_files_threshold > 0 {
            let estimated_live_files: usize = manifests
                .iter()
                .map(|m| {
                    m.added_files_count.unwrap_or(0).max(0) as usize
                        + m.existing_files_count.unwrap_or(0).max(0) as usize
                })
                .sum();
            if estimated_live_files > self.config.max_live_files_threshold {
                tracing::warn!(
                    signaldb.tenant.id = %tenant_id,
                    signaldb.dataset.id = %dataset_id,
                    signaldb.table = %table_name,
                    signaldb.job.estimated_live_files = estimated_live_files as i64,
                    signaldb.job.live_files_threshold = self.config.max_live_files_threshold as i64,
                    signaldb.job.skip_reason = SkipReason::LiveFilesThresholdExceeded.as_str(),
                    "Skipping orphan cleanup: estimated live file count exceeds threshold. \
                     Run snapshot expiration first to reduce file counts, or raise \
                     max_live_files_threshold if memory allows."
                );
                self.metrics
                    .record_cleanup_skipped(SkipReason::LiveFilesThresholdExceeded);
                return Ok(None);
            }
        }

        let live_files = self
            .manifest_reader
            .read_live_files(&table, &manifests)
            .await
            .context("Failed to build live file set from manifests")?;

        tracing::debug!(
            signaldb.tenant.id = %tenant_id,
            signaldb.dataset.id = %dataset_id,
            signaldb.table = %table_name,
            signaldb.job.live_files = live_files.len() as i64,
            "Built live file reference set from manifests"
        );

        Ok(Some(live_files))
    }

    /// Identify unreferenced metadata files for a specific table.
    ///
    /// Metadata orphans are files in the table's `metadata/` directory that
    /// nothing references anymore: metadata.json versions that fell out of
    /// the metadata-log, and manifest-list/manifest avro files whose
    /// snapshots have been expired. Iceberg's delete-after-commit pruning
    /// only removes files it still tracks, so a backlog predating pruning
    /// (or left by expired snapshots) is reclaimable only here (#935, #959).
    ///
    /// Protected regardless of age: every metadata-log entry, the current
    /// metadata.json (via version-hint plus the newest scanned version as a
    /// fallback), every retained snapshot's manifest list, every retained
    /// manifest, and `version-hint.text` itself (never scanned).
    pub async fn identify_orphan_metadata_candidates(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<Vec<OrphanCandidate>> {
        let table = self.load_table(tenant_id, dataset_id, table_name).await?;

        let mut live = self
            .build_live_metadata_set(&table)
            .await
            .context("Failed to build live metadata set")?;

        let table_location = build_table_location(tenant_id, dataset_id, table_name);
        let metadata_dir = format!("{table_location}/metadata");

        // The current metadata.json is not part of the metadata-log; protect
        // the file the version hint points at.
        let hint_path = ObjectPath::from(format!("{metadata_dir}/version-hint.text").as_str());
        if let Ok(hint) = self.object_store.get(&hint_path).await
            && let Ok(bytes) = hint.bytes().await
        {
            let hint = String::from_utf8_lossy(&bytes).trim().to_string();
            if !hint.is_empty() {
                // The hint names a version, not a file: the same version is
                // `<hint>.metadata.json` uncompressed and `<hint>.gz.metadata.json`
                // when the table sets `write.metadata.compression-codec = gzip`.
                // Protect both -- getting this wrong deletes the *live* metadata
                // pointer, which is the one file that must never be reclaimed.
                live.insert(&format!("{metadata_dir}/{hint}.metadata.json"));
                live.insert(&format!("{metadata_dir}/{hint}.gz.metadata.json"));
            }
        }

        // Stream the metadata directory for reclaimable file types, tracking
        // the newest metadata.json version seen (live or not) on the way.
        // Version stems are zero-padded, so lexicographic max is the newest.
        let mut newest_metadata_json: Option<String> = None;
        let listing = self
            .object_store
            .list(Some(&ObjectPath::from(format!("{metadata_dir}/").as_str())));
        let mut outcome = self
            .scan_candidates(
                listing,
                |path| path.ends_with(".metadata.json") || path.ends_with(".avro"),
                &live,
                |path| {
                    if path.ends_with(".metadata.json")
                        && newest_metadata_json.as_deref().is_none_or(|n| path > n)
                    {
                        newest_metadata_json = Some(path.to_string());
                    }
                },
                &table_location,
            )
            .await
            .with_context(|| format!("Failed to scan object store at path: {metadata_dir}/"))?;

        // Belt and braces against a stale or missing version hint: never
        // flag the newest metadata.json version we can see.
        if let Some(newest) = newest_metadata_json {
            outcome.candidates.retain(|c| c.path != newest);
        }

        tracing::info!(
            signaldb.tenant.id = %tenant_id,
            signaldb.dataset.id = %dataset_id,
            signaldb.table = %table_name,
            signaldb.job.scanned_metadata_files = outcome.scanned as i64,
            signaldb.job.candidates = outcome.candidates.len() as i64,
            signaldb.job.grace_period_hours = self.config.grace_period_hours as i64,
            "Identified orphan metadata candidates"
        );

        Ok(outcome.candidates)
    }

    /// Build the set of metadata files still referenced by the table:
    /// metadata-log entries, retained snapshots' manifest lists, and every
    /// retained manifest.
    async fn build_live_metadata_set(
        &self,
        table: &iceberg_rust::table::Table,
    ) -> Result<LiveFileSet> {
        let metadata = table.metadata();
        let mut live = LiveFileSet::new();
        for entry in &metadata.metadata_log {
            live.insert(&entry.metadata_file);
        }
        for snapshot in metadata.snapshots.values() {
            live.insert(snapshot.manifest_list());
        }
        for manifest in self
            .manifest_reader
            .collect_retained_manifests(table)
            .await?
        {
            live.insert(&manifest.manifest_path);
        }
        Ok(live)
    }

    /// Stream a listing and keep only orphan candidates.
    ///
    /// Each entry passing `keep` is examined once: `observe` sees its path,
    /// then the two safety checks decide whether it becomes a candidate:
    /// 1. not in the live reference set
    /// 2. older than the grace period
    ///
    /// Only candidates are retained; the listing itself is never
    /// materialised, so memory is O(candidates) on top of the live set.
    async fn scan_candidates(
        &self,
        listing: impl Stream<Item = object_store::Result<ObjectMeta>>,
        keep: impl Fn(&str) -> bool,
        live_files: &LiveFileSet,
        mut observe: impl FnMut(&str),
        table_identifier: &str,
    ) -> Result<ScanOutcome> {
        let grace_period = chrono::Duration::from_std(self.config.grace_period())
            .context("Failed to convert grace period duration")?;
        let cutoff_time = Utc::now() - grace_period;

        let mut outcome = ScanOutcome::default();
        let mut listing = std::pin::pin!(listing);
        while let Some(meta_result) = listing.next().await {
            let meta = meta_result.context("Failed to read object metadata")?;
            let path = meta.location.as_ref();
            if !keep(path) {
                continue;
            }
            outcome.scanned += 1;
            observe(path);

            // Safety check 1: Is file referenced by any snapshot?
            if live_files.contains(path) {
                tracing::trace!(path, "File is referenced by live snapshot, skipping");
                continue;
            }

            // Safety check 2: Is file older than grace period?
            if meta.last_modified > cutoff_time {
                tracing::debug!(
                    path,
                    last_modified = %meta.last_modified,
                    cutoff_time = %cutoff_time,
                    signaldb.job.grace_period_hours = self.config.grace_period_hours as i64,
                    "Skipping recent file (within grace period)"
                );
                continue;
            }

            // File is an orphan candidate
            tracing::debug!(
                path,
                size_bytes = meta.size,
                last_modified = %meta.last_modified,
                "Identified orphan candidate"
            );

            outcome.candidates.push(OrphanCandidate {
                path: path.to_string(),
                size_bytes: meta.size as usize,
                last_modified: meta.last_modified,
                table_identifier: table_identifier.to_string(),
            });
        }

        Ok(outcome)
    }

    /// Load a table from the catalog with a fresh metadata read.
    async fn load_table(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<iceberg_rust::table::Table> {
        let table_identifier = self
            .catalog_manager
            .build_table_identifier(tenant_id, dataset_id, table_name);
        let tabular = self
            .catalog_manager
            .catalog()
            .load_tabular(&table_identifier)
            .await
            .with_context(|| {
                format!("Failed to load table {tenant_id}/{dataset_id}/{table_name}")
            })?;
        match tabular {
            iceberg_rust::catalog::tabular::Tabular::Table(t) => Ok(t),
            _ => anyhow::bail!("Expected table but found different tabular type"),
        }
    }

    /// Build the current live data-file set for a table from a fresh
    /// metadata load.
    ///
    /// Used by pre-deletion revalidation to catch concurrent writes that
    /// referenced a detected candidate between detection and deletion. The
    /// caller builds this once per table per batch — rebuilding it per
    /// candidate would issue one manifest-list read per retained snapshot
    /// for every file.
    pub async fn live_file_set_for_table(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<LiveFileSet> {
        let table = self
            .load_table(tenant_id, dataset_id, table_name)
            .await
            .context("Failed to load table for revalidation")?;
        self.manifest_reader
            .build_live_file_set(&table)
            .await
            .context("Failed to build live file set for revalidation")
    }

    /// Build the current live metadata-file set for a table from a fresh
    /// metadata load: metadata-log entries, retained snapshots' manifest
    /// lists, and every retained manifest.
    ///
    /// Used by pre-deletion revalidation of metadata orphan candidates;
    /// like [`Self::live_file_set_for_table`], the caller builds this once
    /// per table per batch.
    pub async fn live_metadata_set_for_table(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<LiveFileSet> {
        let table = self
            .load_table(tenant_id, dataset_id, table_name)
            .await
            .context("Failed to load table for revalidation")?;
        self.build_live_metadata_set(&table)
            .await
            .context("Failed to build live metadata set for revalidation")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;

    /// Builds a real `OrphanDetector` backed by an in-memory catalog and
    /// object store, so tests drive `scan_candidates` itself rather than
    /// a duplicated copy of its filtering logic.
    async fn make_detector(config: OrphanCleanupConfig) -> OrphanDetector {
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        OrphanDetector::new(config, catalog_manager, object_store)
    }

    fn meta(path: &str, size: u64, age: chrono::Duration) -> object_store::Result<ObjectMeta> {
        Ok(ObjectMeta {
            location: ObjectPath::from(path),
            last_modified: Utc::now() - age,
            size,
            e_tag: None,
            version: None,
        })
    }

    #[tokio::test]
    async fn scan_excludes_files_within_grace_period() {
        // Arrange
        let config = OrphanCleanupConfig {
            grace_period_hours: 24,
            ..Default::default()
        };
        let detector = make_detector(config).await;
        let live_files = LiveFileSet::new();
        let listing = stream::iter(vec![
            meta("recent.parquet", 100, chrono::Duration::hours(1)),
            meta("old.parquet", 200, chrono::Duration::hours(48)),
        ]);

        // Act
        let outcome = detector
            .scan_candidates(
                listing,
                |_| true,
                &live_files,
                |_| {},
                "tenant/dataset/traces",
            )
            .await
            .unwrap();

        // Assert: only the file older than the 24h grace period is a candidate.
        assert_eq!(outcome.scanned, 2);
        assert_eq!(outcome.candidates.len(), 1);
        assert_eq!(outcome.candidates[0].path, "old.parquet");
        assert_eq!(outcome.candidates[0].size_bytes, 200);
        assert_eq!(
            outcome.candidates[0].table_identifier,
            "tenant/dataset/traces"
        );
    }

    #[tokio::test]
    async fn scan_excludes_files_still_referenced_by_a_live_snapshot() {
        // Arrange
        let detector = make_detector(OrphanCleanupConfig::default()).await;
        let mut live_files = LiveFileSet::new();
        live_files.insert("live.parquet");

        // Both files are old enough to clear the grace period; only
        // liveness should distinguish them.
        let listing = stream::iter(vec![
            meta("live.parquet", 100, chrono::Duration::hours(48)),
            meta("orphan.parquet", 200, chrono::Duration::hours(48)),
        ]);

        // Act
        let outcome = detector
            .scan_candidates(
                listing,
                |_| true,
                &live_files,
                |_| {},
                "tenant/dataset/traces",
            )
            .await
            .unwrap();

        // Assert: the file still referenced by a live snapshot is excluded.
        assert_eq!(outcome.candidates.len(), 1);
        assert_eq!(outcome.candidates[0].path, "orphan.parquet");
    }

    #[tokio::test]
    async fn scan_applies_kind_filter_before_observing_or_counting() {
        let config = OrphanCleanupConfig {
            grace_period_hours: 0,
            ..Default::default()
        };
        let detector = make_detector(config).await;
        let live_files = LiveFileSet::new();
        let listing = stream::iter(vec![
            meta("a.parquet", 1, chrono::Duration::hours(1)),
            meta("notes.txt", 1, chrono::Duration::hours(1)),
            meta("b.parquet", 1, chrono::Duration::hours(1)),
        ]);
        let mut observed = Vec::new();

        let outcome = detector
            .scan_candidates(
                listing,
                |p| p.ends_with(".parquet"),
                &live_files,
                |p| observed.push(p.to_string()),
                "t/d/traces",
            )
            .await
            .unwrap();

        assert_eq!(observed, vec!["a.parquet", "b.parquet"]);
        assert_eq!(outcome.scanned, 2);
        assert_eq!(outcome.candidates.len(), 2);
    }

    #[tokio::test]
    async fn scan_retains_only_candidates_from_a_large_mostly_live_listing() {
        // A listing of many files where almost all are live must leave the
        // detector holding just the orphans: the memory proxy is the
        // candidate count, not the listing length.
        let config = OrphanCleanupConfig {
            grace_period_hours: 0,
            ..Default::default()
        };
        let detector = make_detector(config).await;
        let total = 50_000usize;
        let mut live_files = LiveFileSet::new();
        let mut entries = Vec::with_capacity(total);
        for i in 0..total {
            let path = format!("t/d/traces/data/hour={}/{i:08}.parquet", i % 24);
            if i % 1000 != 0 {
                live_files.insert(&path);
            }
            entries.push(meta(&path, 1, chrono::Duration::hours(1)));
        }

        let outcome = detector
            .scan_candidates(
                stream::iter(entries),
                |_| true,
                &live_files,
                |_| {},
                "t/d/traces",
            )
            .await
            .unwrap();

        assert_eq!(outcome.scanned, total);
        assert_eq!(outcome.candidates.len(), total / 1000);
        assert!(
            outcome
                .candidates
                .iter()
                .all(|c| !live_files.contains(&c.path)),
            "no live file may be a candidate"
        );
    }

    #[tokio::test]
    async fn scan_propagates_listing_errors_instead_of_flagging_files() {
        let config = OrphanCleanupConfig {
            grace_period_hours: 0,
            ..Default::default()
        };
        let detector = make_detector(config).await;
        let live_files = LiveFileSet::new();
        let listing = stream::iter(vec![
            meta("a.parquet", 1, chrono::Duration::hours(1)),
            Err(object_store::Error::Generic {
                store: "test",
                source: "listing failed".into(),
            }),
        ]);

        let result = detector
            .scan_candidates(listing, |_| true, &live_files, |_| {}, "t/d/traces")
            .await;

        assert!(result.is_err(), "a listing error must abort detection");
    }
}
