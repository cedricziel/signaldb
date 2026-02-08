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

pub mod commit;
pub mod executor;
pub mod metrics;
pub mod planner;
pub mod rewriter;

// Re-export commonly used types
pub use commit::{DataFileChange, IcebergCommitter, is_conflict_error};
pub use executor::{
    CompactionExecutor, CompactionJob, CompactionResult, CompactionStatus, DataFileInfo,
    ExecutorConfig,
};
pub use metrics::{CompactionMetrics, MetricsSummary};
pub use planner::{
    CompactionCandidate, CompactionPlanner, FileInfo, PartitionStats, PlannerConfig,
};
pub use rewriter::ParquetRewriter;
