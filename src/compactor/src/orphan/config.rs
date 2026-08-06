//! Orphan file cleanup configuration structures.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Orphan file cleanup configuration.
///
/// This configuration controls the behavior of the orphan file cleanup system,
/// which identifies and removes data files that are no longer referenced by
/// any live Iceberg snapshot.
///
/// ## Defaults
///
/// Cleanup is enabled and deleting by default (#935): with the live set
/// derived from all retained snapshots (#925), a grace period, and
/// pre-delete revalidation, a default deployment actually reclaims the
/// storage that retention and compaction free logically.
///
/// - `enabled`: true (set false to opt out)
/// - `dry_run`: false (set true to log without deleting)
/// - `grace_period_hours`: 24 (prevents deletion of recent files)
///
/// Pre-delete re-validation is unconditional: the live-file set is rebuilt
/// immediately before each deletion batch. It is defense-in-depth on top of a
/// detection algorithm that is correct on its own (#925), not the only thing
/// standing between cleanup and data loss, so it is not switchable.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrphanCleanupConfig {
    /// Enable orphan file cleanup.
    ///
    /// Default: true (#935 — storage is never reclaimed otherwise)
    ///
    /// Env: SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__ENABLED
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Minimum age in hours for a file to be considered for cleanup.
    ///
    /// This grace period prevents deletion of files from in-flight writes.
    /// Files modified within this window are never deleted, even if not
    /// currently referenced in manifests.
    ///
    /// Default: 24 hours
    ///
    /// Env: SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__GRACE_PERIOD_HOURS
    #[serde(default = "default_grace_period_hours")]
    pub grace_period_hours: u64,

    /// Interval in hours between cleanup runs.
    ///
    /// Default: 24 hours (daily cleanup)
    ///
    /// Env: SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__CLEANUP_INTERVAL_HOURS
    #[serde(default = "default_cleanup_interval_hours")]
    pub cleanup_interval_hours: u64,

    /// Maximum files to delete per batch.
    ///
    /// Larger batches are more efficient but take longer to process.
    /// Smaller batches allow for better progress tracking and resumability.
    ///
    /// Default: 1000 files
    ///
    /// Env: SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__BATCH_SIZE
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Dry-run mode: identify orphans but don't delete.
    ///
    /// When enabled, the cleanup system will identify orphan candidates
    /// and log them, but will not actually delete any files. This is useful
    /// for testing and validation.
    ///
    /// Default: false (deletes; set true to observe first)
    ///
    /// Env: SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__DRY_RUN
    #[serde(default = "default_dry_run")]
    pub dry_run: bool,

    /// Maximum estimated live file count before orphan cleanup is skipped.
    ///
    /// Before reading all manifest files, the detector sums the file counts
    /// from the manifest list metadata (cheap). If this estimate exceeds the
    /// threshold, orphan cleanup is skipped with a warning log rather than
    /// risking excessive memory use. Set to 0 to disable the cap.
    ///
    /// Run snapshot expiration and compaction first to reduce file counts
    /// before increasing or removing this threshold.
    ///
    /// Default: 500_000
    ///
    /// Env: SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__MAX_LIVE_FILES_THRESHOLD
    #[serde(default = "default_max_live_files_threshold")]
    pub max_live_files_threshold: usize,
}

// Default value functions for serde
fn default_grace_period_hours() -> u64 {
    24 // 24 hours
}

fn default_cleanup_interval_hours() -> u64 {
    24 // Daily cleanup
}

fn default_batch_size() -> usize {
    1000 // 1000 files per batch
}

fn default_true() -> bool {
    true
}

fn default_dry_run() -> bool {
    false // Delete by default; grace period + revalidation are the rails (#935)
}

fn default_max_live_files_threshold() -> usize {
    500_000
}

impl Default for OrphanCleanupConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            grace_period_hours: default_grace_period_hours(),
            cleanup_interval_hours: default_cleanup_interval_hours(),
            batch_size: default_batch_size(),
            dry_run: default_dry_run(),
            max_live_files_threshold: default_max_live_files_threshold(),
        }
    }
}

impl From<common::config::OrphanCleanupConfig> for OrphanCleanupConfig {
    fn from(config: common::config::OrphanCleanupConfig) -> Self {
        Self {
            enabled: config.enabled,
            grace_period_hours: config.grace_period_hours,
            cleanup_interval_hours: config.cleanup_interval_hours,
            batch_size: config.batch_size,
            dry_run: config.dry_run,
            max_live_files_threshold: config.max_live_files_threshold,
        }
    }
}

impl OrphanCleanupConfig {
    /// Validate the orphan cleanup configuration.
    ///
    /// Checks:
    /// - Grace period is positive
    /// - Cleanup interval is positive
    /// - Batch size is positive
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.grace_period_hours == 0 {
            anyhow::bail!(
                "grace_period_hours must be positive, got {}",
                self.grace_period_hours
            );
        }

        if self.cleanup_interval_hours == 0 {
            anyhow::bail!(
                "cleanup_interval_hours must be positive, got {}",
                self.cleanup_interval_hours
            );
        }

        if self.batch_size == 0 {
            anyhow::bail!("batch_size must be positive, got {}", self.batch_size);
        }

        Ok(())
    }

    /// Get the grace period as a Duration.
    pub fn grace_period(&self) -> Duration {
        Duration::from_secs(self.grace_period_hours * 3600)
    }

    /// Get the cleanup interval as a Duration.
    pub fn cleanup_interval(&self) -> Duration {
        Duration::from_secs(self.cleanup_interval_hours * 3600)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_is_valid() {
        let config = OrphanCleanupConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_default_config_safe_defaults() {
        let config = OrphanCleanupConfig::default();
        assert!(config.enabled, "Should be enabled by default (#935)");
        assert!(!config.dry_run, "Should delete (not dry-run) by default");
        assert_eq!(config.grace_period_hours, 24);
        assert_eq!(config.cleanup_interval_hours, 24);
        assert_eq!(config.batch_size, 1000);
    }

    #[test]
    fn test_zero_grace_period_is_invalid() {
        let config = OrphanCleanupConfig {
            grace_period_hours: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_zero_cleanup_interval_is_invalid() {
        let config = OrphanCleanupConfig {
            cleanup_interval_hours: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_zero_batch_size_is_invalid() {
        let config = OrphanCleanupConfig {
            batch_size: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_duration_conversions() {
        let config = OrphanCleanupConfig {
            grace_period_hours: 24,
            cleanup_interval_hours: 48,
            ..Default::default()
        };

        assert_eq!(
            config.grace_period(),
            Duration::from_secs(24 * 3600),
            "Grace period conversion"
        );
        assert_eq!(
            config.cleanup_interval(),
            Duration::from_secs(48 * 3600),
            "Cleanup interval conversion"
        );
    }
}
