# Heraclitus Integration Tests

This crate contains integration tests for Heraclitus, the Kafka-compatible agent for SignalDB.

## Prerequisites

- Docker must be running (for MinIO testcontainers)
- CMake must be installed (for rdkafka)

## Running Tests

Run all integration tests:
```bash
cargo test -p heraclitus-tests-integration
```

Run a specific test:
```bash
cargo test -p heraclitus-tests-integration test_rdkafka_produce_consume -- --nocapture
```

## Test Coverage

### MinIO Integration Test (`minio_integration.rs`)
- Tests basic produce/fetch with MinIO backend
- Verifies messages are stored in Parquet format
- Uses raw Kafka protocol for testing

### Kafka Client Test (`kafka_client_test.rs`)
- Tests with real Kafka client (rdkafka)
- Verifies produce and consume operations
- Tests metadata API
- Tests error handling

### End-to-End Test (`e2e_test.rs`)
- Multiple concurrent producers
- Consumer group functionality
- Verifies data persistence in MinIO
- Tests message headers

## Architecture

The tests use:
- **MinioTestContext**: Manages MinIO container lifecycle
- **HeraclitusTestContext**: Manages Heraclitus process lifecycle
- **rdkafka**: Official Kafka client for realistic testing

## Notes

- Tests automatically build the Heraclitus binary
- Each test uses isolated MinIO buckets
- Ports are dynamically allocated to avoid conflicts
- Logs are captured with `RUST_LOG=heraclitus=debug,info`