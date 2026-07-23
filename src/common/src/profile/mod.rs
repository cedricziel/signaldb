//! # Profile Processing
//!
//! Shared processing logic for the profiles signal that is independent of
//! storage and transport: flamegraph aggregation for visualization.

pub mod aggregation;

pub use aggregation::{Flamegraph, aggregate_profiles_to_flamegraph};
