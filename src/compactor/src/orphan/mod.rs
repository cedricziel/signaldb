//! Orphan file cleanup system for SignalDB Compactor Phase 3.
//!
//! This module provides safe and efficient cleanup of data files that are no longer
//! referenced by any live Iceberg snapshot. The system prioritizes safety over
//! performance, ensuring that files are never deleted unless proven to be orphaned.
//!
//! ## Safety Principles
//!
//! 1. **Conservative Detection**: When in doubt, retain the file
//! 2. **Grace Period**: Time-based safety margin prevents deletion of in-flight writes
//! 3. **Multi-Phase Validation**: Optional re-validation before deletion
//! 4. **Tenant Isolation**: Never scan or delete across tenant boundaries
//! 5. **Audit Trail**: All deletion decisions logged with full context
//!
//! ## Architecture
//!
//! - `config`: Configuration structures with safe defaults
//! - `detector`: Orphan file detection logic
//! - `cleaner`: Batch deletion with progress tracking
//!
//! ## Usage
//!
//! ```no_run
//! use compactor::orphan::{OrphanCleanupConfig, OrphanDetector, OrphanCleaner};
//!
//! let config = OrphanCleanupConfig::default();
//! let detector = OrphanDetector::new(config.clone(), catalog_manager, object_store);
//!
//! // Identify orphan candidates
//! let candidates = detector.identify_orphan_candidates(
//!     "tenant-id",
//!     "dataset-id",
//!     "traces",
//! ).await?;
//!
//! // Delete orphans (respects dry_run mode)
//! let cleaner = OrphanCleaner::new(config, object_store);
//! let result = cleaner.delete_orphans_batch(candidates).await?;
//! # Ok::<(), anyhow::Error>(())
//! ```

pub mod cleaner;
pub mod config;
pub mod detector;

// Re-export commonly used types
pub use cleaner::{DeletionResult, OrphanCleaner};
pub use config::OrphanCleanupConfig;
pub use detector::{ObjectStoreFile, OrphanCandidate, OrphanDetector};
