// RdKafka-based integration tests
// These tests use the rdkafka client library to test Heraclitus compatibility

mod api_versions_v3_test;
mod compatibility;
mod compression;
mod compression_debug;
mod connection_test;
mod consumer_debug;
mod debug;
mod debug_connection;
mod debug_metadata;
mod minimal;
mod minimal_produce;
mod simple_connect_test;
mod simple_produce_test;
mod tcp_capture;

// Re-export test utilities for rdkafka tests
#[allow(unused_imports)]
pub use crate::test_utils::*;
