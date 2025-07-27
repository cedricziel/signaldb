// Direct TCP protocol tests
// These tests communicate directly with Heraclitus using the Kafka wire protocol

mod api_versions;
mod client;
mod compression;
mod consumer_group;
mod list_offsets;
mod metadata;
mod produce_fetch;

// Re-export test utilities for TCP tests
#[allow(unused_imports)]
pub use crate::test_utils::*;
