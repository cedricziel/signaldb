//! Data generators for integration testing
//!
//! This module provides utilities for generating test data with
//! controlled time partitioning and snapshot creation.

mod data_generator;

pub use data_generator::{
    generate_logs, generate_metrics, generate_profiles, generate_trace_files_with_ids,
    generate_traces,
};
