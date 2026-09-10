//! Dead-letter directory bookkeeping shared by the acceptor retry consumer
//! and the writer drain loop: classifying what is sitting under
//! `<wal_dir>/.../dead-letter/`, enforcing `[wal].dead_letter_retention`, and
//! keeping `signaldb.wal.dead_letter_entries` / `signaldb.wal.dead_letter_bytes`
//! honest (#1494 — 45k rejected entries, 493 MB, sat unreported and
//! unexpired on a production WAL for a month).
//!
//! # Why a fresh scan every reconcile, not an incremental counter
//!
//! `signaldb.wal.entries_pending` is updated on every hot-path `append`/
//! `mark_processed_many` call, where an incremental counter is the only
//! affordable option. Dead-lettering is rare by comparison, and the
//! retention sweep already has to read the directory to find expired
//! entries. So [`reconcile_and_sweep`] records the fresh scan's result
//! directly rather than adjusting a running belief: there is nothing to fall
//! out of sync, which is exactly the class of bug this module exists to
//! close (a gauge that undercounts because some path forgot to touch it).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::Result;
use opentelemetry::KeyValue;

#[cfg(test)]
use super::record_batch_to_bytes;
use super::{Wal, WalOperation, bytes_to_record_batch};

/// Why an entry was retired to the dead-letter directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadLetterKind {
    /// `<id>.bin` + `<id>.rejected.json`: the payload decodes fine but the
    /// writer refused it (e.g. a schema mismatch). Replayable once the
    /// rejection cause is fixed.
    Rejected,
    /// Everything else: a `<id>.unreadable.json` marker with no recoverable
    /// bytes, or a bare `<id>.bin` left by [`super::Wal::dead_letter`] whose
    /// payload could not be deserialized. Not safely replayable as-is.
    Unreadable,
}

impl DeadLetterKind {
    /// The `kind` metric-attribute value.
    pub fn as_attr(self) -> &'static str {
        match self {
            DeadLetterKind::Rejected => "rejected",
            DeadLetterKind::Unreadable => "unreadable",
        }
    }
}

/// One dead-letter marker+payload pair (or lone marker/payload) found on
/// disk, grouped by its entry-id stem.
#[derive(Debug, Clone)]
pub struct DeadLetterEntry {
    pub stem: String,
    pub kind: DeadLetterKind,
    pub bin_path: Option<PathBuf>,
    pub marker_path: Option<PathBuf>,
    pub bytes: u64,
    pub modified: SystemTime,
}

/// List every dead-letter entry under `dir`, one per entry-id stem.
///
/// Segment-level `*.corrupt.bin` / `segment-*.corrupt.bin` quarantine files
/// are a different subsystem (`signaldb.wal.corrupt_entries`, keyed by
/// segment offset rather than entry id, and meant to be kept for forensics)
/// and are skipped entirely — never counted, never swept. A missing
/// directory is an empty result, not an error: most WALs never dead-letter
/// anything.
pub async fn list_entries(dir: &Path) -> Result<Vec<DeadLetterEntry>> {
    #[derive(Default)]
    struct Stem {
        bin_path: Option<PathBuf>,
        marker_path: Option<PathBuf>,
        bytes: u64,
        modified: Option<SystemTime>,
        rejected: bool,
    }

    let mut read_dir = match tokio::fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };

    let mut stems: HashMap<String, Stem> = HashMap::new();
    while let Some(file) = read_dir.next_entry().await? {
        let Ok(file_type) = file.file_type().await else {
            continue;
        };
        if !file_type.is_file() {
            continue;
        }
        let name = file.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.contains(".corrupt.") {
            continue;
        }
        let Some((stem, suffix)) = split_stem(name) else {
            continue;
        };
        let metadata = file.metadata().await?;
        let modified = metadata.modified().ok();
        let entry = stems.entry(stem.to_string()).or_default();
        entry.modified = match (entry.modified, modified) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        match suffix {
            "bin" => entry.bin_path = Some(file.path()),
            "rejected.json" => {
                entry.marker_path = Some(file.path());
                entry.rejected = true;
            }
            "unreadable.json" => entry.marker_path = Some(file.path()),
            _ => continue,
        }
        entry.bytes += metadata.len();
    }

    Ok(stems
        .into_iter()
        .map(|(stem, s)| DeadLetterEntry {
            stem,
            kind: if s.rejected {
                DeadLetterKind::Rejected
            } else {
                DeadLetterKind::Unreadable
            },
            bin_path: s.bin_path,
            marker_path: s.marker_path,
            bytes: s.bytes,
            modified: s.modified.unwrap_or(SystemTime::UNIX_EPOCH),
        })
        .collect())
}

/// Split `<uuid>.bin` / `<uuid>.rejected.json` / `<uuid>.unreadable.json`
/// into `(stem, suffix)`. `None` for anything else (`writer.id`, a stray
/// file, a `*.corrupt.*` quarantine file already filtered above).
fn split_stem(name: &str) -> Option<(&str, &str)> {
    if let Some(stem) = name.strip_suffix(".rejected.json") {
        return Some((stem, "rejected.json"));
    }
    if let Some(stem) = name.strip_suffix(".unreadable.json") {
        return Some((stem, "unreadable.json"));
    }
    if let Some(stem) = name.strip_suffix(".bin") {
        return Some((stem, "bin"));
    }
    None
}

/// Aggregate entry/byte counts by [`DeadLetterKind`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DeadLetterStats {
    pub rejected_entries: u64,
    pub rejected_bytes: u64,
    pub unreadable_entries: u64,
    pub unreadable_bytes: u64,
}

impl DeadLetterStats {
    pub fn from_entries(entries: &[DeadLetterEntry]) -> Self {
        let mut stats = Self::default();
        for entry in entries {
            match entry.kind {
                DeadLetterKind::Rejected => {
                    stats.rejected_entries += 1;
                    stats.rejected_bytes += entry.bytes;
                }
                DeadLetterKind::Unreadable => {
                    stats.unreadable_entries += 1;
                    stats.unreadable_bytes += entry.bytes;
                }
            }
        }
        stats
    }
}

/// Outcome of one retention sweep.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SweepResult {
    pub deleted_entries: usize,
    pub freed_bytes: u64,
    pub remaining: DeadLetterStats,
}

/// Delete marker+payload pairs whose newest file is older than `retention`
/// relative to `now`, and return the stats for what is left.
///
/// `retention.is_zero()` disables the sweep — nothing is ever deleted —
/// matching the "`0s` disables" convention the rest of `[wal]`/`[writer]`
/// uses.
pub async fn sweep_expired_at(
    dir: &Path,
    retention: Duration,
    now: SystemTime,
) -> Result<SweepResult> {
    let entries = list_entries(dir).await?;
    let mut deleted_entries = 0usize;
    let mut freed_bytes = 0u64;
    let mut remaining = Vec::with_capacity(entries.len());

    for entry in entries {
        let age = now.duration_since(entry.modified).unwrap_or_default();
        if !retention.is_zero() && age > retention {
            // Only count this entry as reclaimed once every one of its files
            // is actually gone — a partial failure (e.g. the marker deletes
            // but the bin does not) must not report space as freed while the
            // payload is still on disk, nor drop the entry from `remaining`
            // where the gauge and a later sweep would still find it.
            let mut fully_deleted = true;
            for path in [&entry.bin_path, &entry.marker_path].into_iter().flatten() {
                if let Err(e) = tokio::fs::remove_file(path).await
                    && e.kind() != std::io::ErrorKind::NotFound
                {
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "Failed to delete an expired WAL dead-letter file"
                    );
                    fully_deleted = false;
                }
            }
            if fully_deleted {
                deleted_entries += 1;
                freed_bytes += entry.bytes;
            } else {
                remaining.push(entry);
            }
        } else {
            remaining.push(entry);
        }
    }

    Ok(SweepResult {
        deleted_entries,
        freed_bytes,
        remaining: DeadLetterStats::from_entries(&remaining),
    })
}

/// Record `stats` on `signaldb.wal.dead_letter_entries` /
/// `signaldb.wal.dead_letter_bytes`, one data point per kind.
fn record_gauges(
    tenant_id: &str,
    dataset_id: &str,
    signal: &str,
    role: &str,
    stats: &DeadLetterStats,
) {
    let metrics = crate::self_monitoring::app_metrics();
    for (kind, entries, bytes) in [
        (
            DeadLetterKind::Rejected,
            stats.rejected_entries,
            stats.rejected_bytes,
        ),
        (
            DeadLetterKind::Unreadable,
            stats.unreadable_entries,
            stats.unreadable_bytes,
        ),
    ] {
        let attrs = [
            KeyValue::new("signaldb.tenant.id", tenant_id.to_string()),
            KeyValue::new("signaldb.dataset.id", dataset_id.to_string()),
            KeyValue::new("signal", signal.to_string()),
            KeyValue::new("role", role.to_string()),
            KeyValue::new("kind", kind.as_attr()),
        ];
        metrics.wal_dead_letter_entries.record(entries, &attrs);
        metrics.wal_dead_letter_bytes.record(bytes, &attrs);
    }
}

/// Sweep `dir` for entries past `[wal].dead_letter_retention` and record the
/// resulting counts/bytes on the two dead-letter gauges.
///
/// Called once per WAL directory, on the same cadence the acceptor retry
/// consumer and the writer drain loop already walk every WAL (see their call
/// sites): the first call after startup is this gauge's seed, and every
/// later call both re-reports the true state and enforces retention. Logs
/// one INFO line per sweep that actually deleted something.
pub async fn reconcile_and_sweep(
    dir: &Path,
    tenant_id: &str,
    dataset_id: &str,
    signal: &str,
    role: &str,
    retention: Duration,
) -> Result<SweepResult> {
    let result = sweep_expired_at(dir, retention, SystemTime::now()).await?;
    record_gauges(tenant_id, dataset_id, signal, role, &result.remaining);
    if result.deleted_entries > 0 {
        tracing::info!(
            signaldb.tenant.id = tenant_id,
            signaldb.dataset.id = dataset_id,
            signal,
            role,
            deleted_entries = result.deleted_entries,
            freed_bytes = result.freed_bytes,
            retention_secs = retention.as_secs(),
            "WAL dead-letter retention sweep deleted expired entries"
        );
    }
    Ok(result)
}

/// Outcome of a [`purge`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PurgeResult {
    pub deleted_entries: usize,
    pub freed_bytes: u64,
}

/// Delete every dead-letter entry under `dir` matching `kind` (every entry,
/// if `None`), regardless of age — the operator-invoked counterpart to
/// [`sweep_expired_at`]'s age-based deletion, for the `signaldb wal
/// dead-letter purge` subcommand. `dry_run` reports what would be deleted
/// without touching disk.
pub async fn purge(dir: &Path, kind: Option<DeadLetterKind>, dry_run: bool) -> Result<PurgeResult> {
    let entries = list_entries(dir).await?;
    let mut deleted_entries = 0usize;
    let mut freed_bytes = 0u64;

    for entry in entries {
        if let Some(kind) = kind
            && entry.kind != kind
        {
            continue;
        }
        if dry_run {
            deleted_entries += 1;
            freed_bytes += entry.bytes;
            continue;
        }
        // Same accounting rule as `sweep_expired_at`: only count this entry
        // as deleted once every one of its files is actually gone, so a
        // partial failure does not report space as freed while a payload is
        // still on disk.
        let mut fully_deleted = true;
        for path in [&entry.bin_path, &entry.marker_path].into_iter().flatten() {
            if let Err(e) = tokio::fs::remove_file(path).await
                && e.kind() != std::io::ErrorKind::NotFound
            {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "Failed to delete a purged WAL dead-letter file"
                );
                fully_deleted = false;
            }
        }
        if fully_deleted {
            deleted_entries += 1;
            freed_bytes += entry.bytes;
        }
    }

    Ok(PurgeResult {
        deleted_entries,
        freed_bytes,
    })
}

/// Outcome of a [`replay`] pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReplayResult {
    /// Payloads successfully re-appended to the live WAL and removed from
    /// dead-letter.
    pub replayed: usize,
    /// Entries left in place: no intact `.bin` payload, or one that failed
    /// to decode as a record batch.
    pub failed: usize,
}

/// Re-append every intact, decodable `.bin` payload under `dir` (filtered by
/// `kind`, or every entry if `None`) as a fresh entry on `wal`, using
/// `operation` (mapped from the `--signal` flag via
/// [`super::WalOperation::from_signal`]) so it lands correctly attributed for
/// the WAL's own bookkeeping. A successfully re-appended entry's dead-letter
/// pair is removed, so the existing acceptor retry consumer or writer drain
/// loop picks it up through the normal path and nothing lingers once it has.
///
/// An entry with no `.bin` payload, or one that fails to decode
/// ([`bytes_to_record_batch`]), is left in place and counted as `failed`
/// rather than silently dropped: replaying a still-broken payload back into
/// the live WAL would only recreate the failure that dead-lettered it in the
/// first place. `dry_run` decodes and counts but neither appends nor deletes.
pub async fn replay(
    dir: &Path,
    wal: &Wal,
    operation: WalOperation,
    kind: Option<DeadLetterKind>,
    dry_run: bool,
) -> Result<ReplayResult> {
    let entries = list_entries(dir).await?;
    let mut result = ReplayResult::default();

    for entry in entries {
        if let Some(kind) = kind
            && entry.kind != kind
        {
            continue;
        }
        let Some(bin_path) = &entry.bin_path else {
            result.failed += 1;
            continue;
        };
        let bytes = match tokio::fs::read(bin_path).await {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::warn!(
                    path = %bin_path.display(),
                    error = %e,
                    "Failed to read a dead-letter payload for replay; leaving it in place"
                );
                result.failed += 1;
                continue;
            }
        };
        if let Err(e) = bytes_to_record_batch(&bytes) {
            tracing::warn!(
                path = %bin_path.display(),
                error = %e,
                "Dead-letter payload does not decode; leaving it in place"
            );
            result.failed += 1;
            continue;
        }
        if dry_run {
            result.replayed += 1;
            continue;
        }
        if let Err(e) = wal.append(operation.clone(), bytes, None).await {
            tracing::warn!(
                entry_id = %entry.stem,
                error = %e,
                "Failed to re-append a dead-letter payload; leaving it in place"
            );
            result.failed += 1;
            continue;
        }
        for path in [&entry.bin_path, &entry.marker_path].into_iter().flatten() {
            if let Err(e) = tokio::fs::remove_file(path).await
                && e.kind() != std::io::ErrorKind::NotFound
            {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "Replayed a dead-letter payload but failed to remove it from dead-letter"
                );
            }
        }
        result.replayed += 1;
    }

    Ok(result)
}

/// Sweep and reconcile every dead-letter directory a [`super::manager::WalManager`]
/// knows about (via [`super::manager::WalManager::scan_dead_letter_dirs`]),
/// resolving `[wal].dead_letter_retention` off the process-global
/// `common::config::CONFIG` (falling back to
/// [`crate::config::default_dead_letter_retention`] if it is unset, as in a
/// test).
///
/// Shared by the acceptor retry consumer and the writer drain loop — both
/// call this once per pass, at the same cadence they already walk every WAL,
/// with `role` set to `"acceptor"` or `"writer"` respectively.
pub async fn reconcile_all(wal_manager: &super::manager::WalManager, role: &str) {
    let retention = crate::config::CONFIG
        .get()
        .map(|c| c.wal.dead_letter_retention)
        .unwrap_or_else(crate::config::default_dead_letter_retention);

    let dirs = match wal_manager.scan_dead_letter_dirs().await {
        Ok(dirs) => dirs,
        Err(e) => {
            tracing::warn!(role, error = %e, "Failed to scan WAL dead-letter directories");
            return;
        }
    };

    for (tenant, dataset, signal, dir) in dirs {
        if let Err(e) = reconcile_and_sweep(&dir, &tenant, &dataset, &signal, role, retention).await
        {
            tracing::warn!(
                tenant_id = %tenant,
                dataset_id = %dataset,
                signal = %signal,
                role,
                error = %e,
                "Failed to reconcile a WAL dead-letter directory"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::fs;
    use std::fs::OpenOptions;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn set_mtime(path: &Path, age: Duration) {
        let when = SystemTime::now() - age;
        OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(when)
            .unwrap();
    }

    fn write_rejected_pair(dir: &Path, id: &str, age: Duration) {
        let bin = dir.join(format!("{id}.bin"));
        let marker = dir.join(format!("{id}.rejected.json"));
        fs::write(&bin, b"payload").unwrap();
        fs::write(&marker, b"{}").unwrap();
        set_mtime(&bin, age);
        set_mtime(&marker, age);
    }

    #[tokio::test]
    async fn listing_a_missing_directory_is_empty_not_an_error() {
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("dead-letter");
        let entries = list_entries(&missing).await.unwrap();
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn classifies_rejected_pairs_bare_bin_and_unreadable_markers() {
        let dir = TempDir::new().unwrap();
        write_rejected_pair(dir.path(), "a", Duration::from_secs(1));
        // Bare .bin, no marker: `Wal::dead_letter`'s poison case -> unreadable.
        fs::write(dir.path().join("b.bin"), b"poison").unwrap();
        // Marker only, no bytes recoverable -> unreadable.
        fs::write(dir.path().join("c.unreadable.json"), b"{}").unwrap();

        let entries = list_entries(dir.path()).await.unwrap();
        let stats = DeadLetterStats::from_entries(&entries);
        assert_eq!(stats.rejected_entries, 1);
        assert_eq!(stats.unreadable_entries, 2);
        assert!(stats.rejected_bytes > 0);
        assert!(stats.unreadable_bytes > 0);
    }

    #[tokio::test]
    async fn corrupt_quarantine_files_are_never_counted_or_swept() {
        let dir = TempDir::new().unwrap();
        let corrupt = dir.path().join("segment-1-offset-2.corrupt.bin");
        fs::write(&corrupt, b"x").unwrap();
        set_mtime(&corrupt, Duration::from_secs(365 * 24 * 3600));

        let entries = list_entries(dir.path()).await.unwrap();
        assert!(entries.is_empty());

        let result = sweep_expired_at(dir.path(), Duration::from_secs(1), SystemTime::now())
            .await
            .unwrap();
        assert_eq!(result.deleted_entries, 0);
        assert!(corrupt.exists());
    }

    #[tokio::test]
    async fn sweep_deletes_only_pairs_older_than_retention() {
        let dir = TempDir::new().unwrap();
        write_rejected_pair(dir.path(), "old", Duration::from_secs(3600));
        write_rejected_pair(dir.path(), "new", Duration::from_secs(1));

        let result = sweep_expired_at(dir.path(), Duration::from_secs(1800), SystemTime::now())
            .await
            .unwrap();

        assert_eq!(result.deleted_entries, 1);
        assert!(result.freed_bytes > 0);
        assert!(!dir.path().join("old.bin").exists());
        assert!(!dir.path().join("old.rejected.json").exists());
        assert!(dir.path().join("new.bin").exists());
        assert_eq!(result.remaining.rejected_entries, 1);
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn sweep_does_not_count_a_failed_deletion_as_reclaimed() {
        // A partial or total failure to remove an expired pair's files must
        // not be reported as space reclaimed while the payload is still on
        // disk, and the entry must stay in `remaining` so the gauge and a
        // later sweep still see it.
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().unwrap();
        write_rejected_pair(dir.path(), "old", Duration::from_secs(3600));

        // Read-only directory: `remove_file` inside it fails (not
        // `NotFound`), while `list_entries`'s readdir/stat still succeed.
        let mut perms = fs::metadata(dir.path()).unwrap().permissions();
        perms.set_mode(0o555);
        fs::set_permissions(dir.path(), perms).unwrap();

        let result = sweep_expired_at(dir.path(), Duration::from_secs(1800), SystemTime::now())
            .await
            .unwrap();

        // Restore permissions so TempDir cleanup can remove the directory
        // before asserting -- a failed assertion must not leave a
        // non-writable temp dir behind.
        let mut restore = fs::metadata(dir.path()).unwrap().permissions();
        restore.set_mode(0o755);
        fs::set_permissions(dir.path(), restore).unwrap();

        // Whether the restricted permissions actually block deletion depends
        // on whether the process honors Unix permission bits (e.g. it does
        // not when running as root in some CI containers). Either outcome
        // is verified concretely rather than assuming failure.
        if dir.path().join("old.bin").exists() {
            assert_eq!(
                result.deleted_entries, 0,
                "a failed deletion must not be counted as reclaimed"
            );
            assert_eq!(result.freed_bytes, 0);
            assert_eq!(
                result.remaining.rejected_entries, 1,
                "an entry whose deletion failed must stay in remaining"
            );
        } else {
            // Permission bits were bypassed (e.g. running as root); confirm
            // the deletion genuinely succeeded and was counted, rather than
            // silently accepting either outcome.
            assert_eq!(result.deleted_entries, 1);
            assert!(result.freed_bytes > 0);
            assert_eq!(result.remaining.rejected_entries, 0);
        }
    }

    #[tokio::test]
    async fn zero_retention_disables_the_sweep() {
        let dir = TempDir::new().unwrap();
        write_rejected_pair(dir.path(), "old", Duration::from_secs(365 * 24 * 3600));

        let result = sweep_expired_at(dir.path(), Duration::ZERO, SystemTime::now())
            .await
            .unwrap();

        assert_eq!(result.deleted_entries, 0);
        assert!(dir.path().join("old.bin").exists());
        assert_eq!(result.remaining.rejected_entries, 1);
    }

    #[tokio::test]
    async fn reconcile_and_sweep_seeds_from_a_directory_untouched_since_startup() {
        // The startup case: a WAL directory whose dead-letter backlog was
        // written by a previous process. The very first reconcile call must
        // report it accurately — that is the gauge's seed.
        let dir = TempDir::new().unwrap();
        write_rejected_pair(dir.path(), "a", Duration::from_secs(1));
        write_rejected_pair(dir.path(), "b", Duration::from_secs(1));
        fs::write(dir.path().join("c.unreadable.json"), b"{}").unwrap();

        let result = reconcile_and_sweep(
            dir.path(),
            "acme",
            "production",
            "metrics",
            "acceptor",
            Duration::from_secs(30 * 24 * 3600),
        )
        .await
        .unwrap();

        assert_eq!(result.remaining.rejected_entries, 2);
        assert_eq!(result.remaining.unreadable_entries, 1);
        assert_eq!(result.deleted_entries, 0);
    }

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))]).unwrap()
    }

    fn write_rejected_pair_with_payload(dir: &Path, id: &str, payload: &[u8]) {
        fs::write(dir.join(format!("{id}.bin")), payload).unwrap();
        fs::write(dir.join(format!("{id}.rejected.json")), b"{}").unwrap();
    }

    async fn test_wal(base_dir: &Path) -> Wal {
        let mut config = crate::wal::WalConfig::with_defaults(base_dir.to_path_buf());
        config.tenant_id = "acme".to_string();
        config.dataset_id = "production".to_string();
        // Flush every append immediately so `get_unprocessed_entries` (which
        // reads segment state, not the in-memory buffer) sees it without a
        // separate `wal.flush()` call in every test.
        config.max_buffer_entries = 1;
        Wal::new(config).await.unwrap()
    }

    #[tokio::test]
    async fn purge_deletes_only_the_requested_kind() {
        let dir = TempDir::new().unwrap();
        write_rejected_pair(dir.path(), "a", Duration::from_secs(1));
        fs::write(dir.path().join("b.unreadable.json"), b"{}").unwrap();

        let result = purge(dir.path(), Some(DeadLetterKind::Rejected), false)
            .await
            .unwrap();

        assert_eq!(result.deleted_entries, 1);
        assert!(!dir.path().join("a.bin").exists());
        assert!(!dir.path().join("a.rejected.json").exists());
        assert!(
            dir.path().join("b.unreadable.json").exists(),
            "purge scoped to kind=rejected must not touch an unreadable entry"
        );
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn purge_does_not_count_a_failed_deletion_as_reclaimed() {
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().unwrap();
        write_rejected_pair(dir.path(), "a", Duration::from_secs(1));

        let mut perms = fs::metadata(dir.path()).unwrap().permissions();
        perms.set_mode(0o555);
        fs::set_permissions(dir.path(), perms).unwrap();

        let result = purge(dir.path(), None, false).await.unwrap();

        let mut restore = fs::metadata(dir.path()).unwrap().permissions();
        restore.set_mode(0o755);
        fs::set_permissions(dir.path(), restore).unwrap();

        if dir.path().join("a.bin").exists() {
            assert_eq!(
                result.deleted_entries, 0,
                "a failed deletion must not be counted as reclaimed"
            );
            assert_eq!(result.freed_bytes, 0);
        } else {
            assert_eq!(result.deleted_entries, 1);
        }
    }

    #[tokio::test]
    async fn purge_dry_run_reports_without_deleting_anything() {
        let dir = TempDir::new().unwrap();
        write_rejected_pair(dir.path(), "a", Duration::from_secs(1));

        let result = purge(dir.path(), None, true).await.unwrap();

        assert_eq!(result.deleted_entries, 1);
        assert!(
            dir.path().join("a.bin").exists(),
            "dry-run purge must not touch disk"
        );
    }

    #[tokio::test]
    async fn replay_reappends_a_decodable_payload_and_removes_the_pair() {
        let dead_letter_dir = TempDir::new().unwrap();
        let payload = record_batch_to_bytes(&make_batch()).unwrap();
        write_rejected_pair_with_payload(dead_letter_dir.path(), "a", &payload);

        let wal_dir = TempDir::new().unwrap();
        let wal = test_wal(wal_dir.path()).await;

        let result = replay(
            dead_letter_dir.path(),
            &wal,
            WalOperation::WriteMetrics,
            None,
            false,
        )
        .await
        .unwrap();

        assert_eq!(result.replayed, 1);
        assert_eq!(result.failed, 0);
        assert!(
            !dead_letter_dir.path().join("a.bin").exists(),
            "a replayed pair must be removed from dead-letter"
        );
        assert!(!dead_letter_dir.path().join("a.rejected.json").exists());

        let pending = wal.get_unprocessed_entries().await.unwrap();
        assert_eq!(
            pending.len(),
            1,
            "the replayed payload must land as a pending entry on the live WAL"
        );
    }

    #[tokio::test]
    async fn replay_leaves_an_undecodable_payload_in_place() {
        let dead_letter_dir = TempDir::new().unwrap();
        write_rejected_pair_with_payload(dead_letter_dir.path(), "a", b"not a record batch");

        let wal_dir = TempDir::new().unwrap();
        let wal = test_wal(wal_dir.path()).await;

        let result = replay(
            dead_letter_dir.path(),
            &wal,
            WalOperation::WriteMetrics,
            None,
            false,
        )
        .await
        .unwrap();

        assert_eq!(result.replayed, 0);
        assert_eq!(result.failed, 1);
        assert!(
            dead_letter_dir.path().join("a.bin").exists(),
            "an undecodable payload must be left in place, not deleted"
        );
        assert!(wal.get_unprocessed_entries().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn replay_dry_run_decodes_and_counts_without_touching_anything() {
        let dead_letter_dir = TempDir::new().unwrap();
        let payload = record_batch_to_bytes(&make_batch()).unwrap();
        write_rejected_pair_with_payload(dead_letter_dir.path(), "a", &payload);

        let wal_dir = TempDir::new().unwrap();
        let wal = test_wal(wal_dir.path()).await;

        let result = replay(
            dead_letter_dir.path(),
            &wal,
            WalOperation::WriteMetrics,
            None,
            true,
        )
        .await
        .unwrap();

        assert_eq!(result.replayed, 1);
        assert!(dead_letter_dir.path().join("a.bin").exists());
        assert!(wal.get_unprocessed_entries().await.unwrap().is_empty());
    }
}
