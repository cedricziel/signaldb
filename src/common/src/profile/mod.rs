//! # Profile Processing
//!
//! Shared processing logic for the profiles signal that is independent of
//! storage and transport: flamegraph aggregation for visualization.

pub mod aggregation;

pub use aggregation::{
    DiffFlamegraph, Flamegraph, aggregate_profiles_to_diff_flamegraph,
    aggregate_profiles_to_flamegraph,
};
