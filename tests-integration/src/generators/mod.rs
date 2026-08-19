//! Data generators for integration testing
//!
//! This module provides utilities for generating test data with
//! controlled time partitioning and snapshot creation.

mod data_generator;

pub use data_generator::{
    BLOOM_HIGH_SENTINEL, BLOOM_LOW_SENTINEL, BLOOM_TARGET_TRACE_ID,
    generate_bloom_prunable_trace_files, generate_logs, generate_metrics, generate_profiles,
    generate_trace_files_with_ids, generate_traces, generate_wide_trace_files,
};
