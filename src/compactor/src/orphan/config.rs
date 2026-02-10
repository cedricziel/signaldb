//! Orphan file cleanup configuration structures.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Orphan file cleanup configuration.
///
/// This configuration controls the behavior of the orphan file cleanup system,
/// which identifies and removes data files that are no longer referenced by
/// any live Iceberg snapshot.
///
/// ## Safety Defaults
///
/// - `enabled`: false (must be explicitly enabled)
/// - `dry_run`: true (logs actions without executing)
/// - `grace_period_hours`: 24 (prevents deletion of recent files)
/// - `revalidate_before_delete`: true (extra safety check)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrphanCleanupConfig {
    /// Enable orphan file cleanup.
    ///
    /// Default: false (must be explicitly enabled for safety)
    ///
    /// Env: SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__ENABLED
    #[serde(default)]
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
    /// Default: true (safe default for initial deployment)
    ///
    /// Env: SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__DRY_RUN
    #[serde(default = "default_dry_run")]
    pub dry_run: bool,

    /// Re-validate orphan status before deletion.
    ///
    /// When enabled, files identified as orphans will be re-validated
    /// immediately before deletion to catch any concurrent writes that
    /// may have referenced them.
    ///
    /// Default: true (adds extra safety)
    ///
    /// Env: SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__REVALIDATE_BEFORE_DELETE
    #[serde(default = "default_revalidate_before_delete")]
    pub revalidate_before_delete: bool,

    /// Maximum age in hours for snapshots to include in orphan detection.
    ///
    /// The detector only scans snapshots within this age window. Files
    /// referenced exclusively by snapshots older than this age will NOT
    /// be included in the live file set and may be considered orphaned.
    ///
    /// Default: 720 hours (30 days)
    ///
    /// Env: SIGNALDB__COMPACTOR__ORPHAN_CLEANUP__MAX_SNAPSHOT_AGE_HOURS
    #[serde(default = "default_max_snapshot_age_hours")]
    pub max_snapshot_age_hours: u64,
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

fn default_dry_run() -> bool {
    true // Dry-run enabled by default for safety
}

fn default_revalidate_before_delete() -> bool {
    true // Extra safety by default
}

fn default_max_snapshot_age_hours() -> u64 {
    720 // 30 days
}

impl Default for OrphanCleanupConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            grace_period_hours: default_grace_period_hours(),
            cleanup_interval_hours: default_cleanup_interval_hours(),
            batch_size: default_batch_size(),
            dry_run: default_dry_run(),
            revalidate_before_delete: default_revalidate_before_delete(),
            max_snapshot_age_hours: default_max_snapshot_age_hours(),
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
            revalidate_before_delete: config.revalidate_before_delete,
            max_snapshot_age_hours: config.max_snapshot_age_hours,
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
    /// - Max snapshot age is positive
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

        if self.max_snapshot_age_hours == 0 {
            anyhow::bail!(
                "max_snapshot_age_hours must be positive, got {}",
                self.max_snapshot_age_hours
            );
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

    /// Get the max snapshot age as a Duration.
    pub fn max_snapshot_age(&self) -> Duration {
        Duration::from_secs(self.max_snapshot_age_hours * 3600)
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
        assert!(!config.enabled, "Should be disabled by default");
        assert!(config.dry_run, "Should have dry_run enabled by default");
        assert!(
            config.revalidate_before_delete,
            "Should revalidate by default"
        );
        assert_eq!(config.grace_period_hours, 24);
        assert_eq!(config.cleanup_interval_hours, 24);
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.max_snapshot_age_hours, 720);
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
            max_snapshot_age_hours: 720,
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
        assert_eq!(
            config.max_snapshot_age(),
            Duration::from_secs(720 * 3600),
            "Max snapshot age conversion"
        );
    }
}
