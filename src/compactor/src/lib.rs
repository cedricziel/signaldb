//! SignalDB Compactor Library
//!
//! Provides compaction planning and execution for Iceberg tables.
//!
//! # Phase 3: Retention & Lifecycle Management
//!
//! Includes complete compaction execution with atomic commits, retention
//! enforcement, snapshot expiration, and orphan file cleanup.
//!
//! # Phase 4: Multi-Instance Safety + Scheduling (in progress)
//!
//! - `lease`: Distributed lease management for conflict-free multi-instance operation
//! - `scheduler`: Fair round-robin scheduling across tenants

pub mod commit;
pub mod executor;
pub mod iceberg;
pub mod lease;
pub mod metrics;
pub mod orphan;
pub mod planner;
pub mod retention;
pub mod rewriter;
pub mod scheduler;

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
    OrphanDetector, OrphanMetrics, SkipReason,
};
pub use planner::{
    CompactionCandidate, CompactionPlanner, FileInfo, PartitionStats, PlannerConfig,
};
pub use retention::{
    DatasetRetentionConfig, RetentionConfig, RetentionCutoff, RetentionPolicyResolver,
    RetentionPolicySource, SignalType, TenantRetentionConfig,
};
pub use rewriter::ParquetRewriter;
