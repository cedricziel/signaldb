//! SignalDB Compactor Library
//!
//! Provides compaction planning and execution for Iceberg tables:
//!
//! - `planner`/`scheduler`: identify compaction candidates with fair
//!   round-robin scheduling across tenants
//! - `rewriter`/`commit`/`executor`: read, merge, sort, and rewrite data
//!   files, then atomically replace them in a new Iceberg snapshot
//! - `retention`/`orphan`: retention enforcement, snapshot expiration, and
//!   orphan file cleanup
//! - `lease`: distributed lease management for conflict-free multi-instance
//!   operation
//! - `service`/`lifecycle`: assembly from configuration and the background
//!   cycles, each driven on its own task
//! - `flight`/`http`: on-demand admin actions and observability endpoints

pub mod attr_promotion;
pub mod attr_stats;
pub mod cli;
pub mod commit;
pub mod executor;
pub mod flight;
pub mod http;
pub mod iceberg;
pub mod lease;
mod lifecycle;
pub mod metrics;
pub mod orphan;
pub mod planner;
pub mod retention;
pub mod rewriter;
pub mod scheduler;
pub mod service;
pub mod table_lock;

// Re-export commonly used types
pub use commit::{CommitError, DataFileChange, IcebergCommitter, is_conflict_error};
pub use executor::{
    CompactionExecutor, CompactionJob, CompactionResult, CompactionStatus, DataFileInfo,
    ExecutorConfig,
};
pub use http::ObservabilityState;
pub use iceberg::{
    LiveFileSet, ManifestFileInfo, ManifestReader, PartitionInfo, PartitionManager, SnapshotInfo,
    SnapshotManager,
};
pub use metrics::{CompactionMetrics, MetricsSummary};
pub use orphan::{
    DeletionResult, OrphanCandidate, OrphanCleaner, OrphanCleanupConfig, OrphanDetector,
    OrphanMetrics, SkipReason,
};
pub use planner::{
    CompactionCandidate, CompactionPlanner, FileInfo, PartitionStats, PlannerConfig,
};
pub use retention::{
    DatasetRetentionConfig, RetentionConfig, RetentionCutoff, RetentionPolicyResolver,
    RetentionPolicySource, SignalType, TenantRetentionConfig,
};
pub use rewriter::ParquetRewriter;
pub use service::CompactorService;
pub use table_lock::TableLockRegistry;
