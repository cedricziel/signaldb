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
            for path in [&entry.bin_path, &entry.marker_path].into_iter().flatten() {
                if let Err(e) = tokio::fs::remove_file(path).await
                    && e.kind() != std::io::ErrorKind::NotFound
                {
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "Failed to delete an expired WAL dead-letter file"
                    );
                }
            }
            deleted_entries += 1;
            freed_bytes += entry.bytes;
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
    use std::fs;
    use std::fs::OpenOptions;
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
}
