//! Data generators for integration testing
//!
//! This module provides utilities for generating test data with
//! controlled time partitioning and snapshot creation.

mod data_generator;
mod snapshot_generator;

pub use data_generator::{generate_logs, generate_metrics, generate_traces};
pub use snapshot_generator::{SnapshotGenerator, SnapshotInfo};
