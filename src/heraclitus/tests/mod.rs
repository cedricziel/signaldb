// Heraclitus test suite
//
// Test organization:
// - integration/: Integration tests with Heraclitus subprocess
// - e2e/: End-to-end tests with real rdkafka clients
//
// Unit tests are now colocated with the code they test in src/

pub mod e2e;
pub mod integration;
