//! SignalDB Compactor Library
//!
//! Provides compaction planning and execution for Iceberg tables.
//!
//! # Phase 2: Full Compaction Execution
//!
//! This library now includes complete compaction execution with:
//! - Manifest reading and file grouping
//! - Parquet file merging with DataFusion
//! - Atomic commits with Iceberg optimistic concurrency
//! - Conflict detection and retry logic
//! - Comprehensive metrics tracking
//!
//! # Phase 3: Retention Management (In Progress)
//!
//! Phase 3 adds retention enforcement capabilities:
//! - Retention configuration with override hierarchy
//! - Snapshot management and expiration
//! - Manifest reading for orphan detection
//! - Partition lifecycle management

pub mod commit;
pub mod executor;
pub mod iceberg;
pub mod metrics;
pub mod orphan;
pub mod planner;
pub mod retention;
pub mod rewriter;

// Re-export commonly used types
pub use commit::{DataFileChange, IcebergCommitter, is_conflict_error};
pub use executor::{
    CompactionExecutor, CompactionJob, CompactionResult, CompactionStatus, DataFileInfo,
    ExecutorConfig,
};
pub use iceberg::{
    ManifestFileInfo, ManifestReader, PartitionInfo, PartitionManager, SnapshotInfo,
    SnapshotManager,
};
pub use metrics::{CompactionMetrics, MetricsSummary};
pub use orphan::{
    DeletionResult, ObjectStoreFile, OrphanCandidate, OrphanCleaner, OrphanCleanupConfig,
    OrphanDetector,
};
pub use planner::{
    CompactionCandidate, CompactionPlanner, FileInfo, PartitionStats, PlannerConfig,
};
pub use retention::{
    DatasetRetentionConfig, RetentionConfig, RetentionCutoff, RetentionPolicyResolver,
    RetentionPolicySource, SignalType, TenantRetentionConfig,
};
pub use rewriter::ParquetRewriter;
