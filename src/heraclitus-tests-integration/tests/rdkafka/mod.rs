// RdKafka-based integration tests
// These tests use the rdkafka client library to test Heraclitus compatibility

mod compatibility;
mod compression;
mod debug;

// Re-export test utilities for rdkafka tests
#[allow(unused_imports)]
pub use crate::test_utils::*;
