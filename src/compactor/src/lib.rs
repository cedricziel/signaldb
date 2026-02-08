//! SignalDB Compactor Library
//!
//! Provides compaction planning and execution for Iceberg tables.
//! Phase 1 includes dry-run planning only.

pub mod planner;

pub use planner::{CompactionCandidate, CompactionPlanner, PartitionStats, PlannerConfig};
