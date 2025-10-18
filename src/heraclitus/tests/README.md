# Heraclitus Test Suite

The Heraclitus test suite is organized into three main categories for clarity, maintainability, and execution efficiency.

## Test Structure

Tests are organized in two locations:

**Core tests** (`src/heraclitus/tests/`):
```
src/heraclitus/tests/
├── integration/            # Integration tests with Heraclitus subprocess
│   ├── helpers.rs         # Test utilities and context
│   ├── basic_kafka_test.rs       # Basic Kafka protocol tests
│   ├── consumer_group_test.rs    # Consumer group functionality
│   ├── protocol_test.rs         # Advanced protocol tests
│   └── protocol_comparison_test.rs # Protocol compliance tests
│
└── e2e/                   # End-to-end tests with real rdkafka clients
    └── rdkafka_compat_test.rs  # Comprehensive rdkafka compatibility
```

**Extended integration tests** (`src/heraclitus-tests-integration/tests/`):
```
src/heraclitus-tests-integration/tests/
├── rdkafka/               # rdkafka client compatibility tests
│   ├── compatibility.rs  # Full protocol compatibility
│   ├── compression.rs    # Compression support
│   └── ...               # Various rdkafka integration tests
│
└── tcp/                   # Low-level TCP protocol tests
    ├── api_versions.rs   # API version negotiation
    ├── metadata.rs       # Metadata API tests
    ├── produce_fetch.rs  # Producer/consumer tests
    └── consumer_group/   # Consumer group coordination
```

**Benchmarks** (`src/heraclitus/benches/`):
```
src/heraclitus/benches/
└── kafka_protocol_bench.rs # Protocol encoding/decoding benchmarks
```

## Running Tests

### Integration Tests
Tests that spawn a Heraclitus subprocess:
```bash
cargo test -p heraclitus --lib integration
```

### End-to-End Tests
Full compatibility tests with rdkafka clients:
```bash
cargo test -p heraclitus --lib e2e
```

### Extended Integration Tests
TCP and rdkafka compatibility tests:
```bash
cargo test -p heraclitus-tests-integration
```

### All Heraclitus Tests
```bash
cargo test -p heraclitus -p heraclitus-tests-integration
```

### Benchmarks
Performance benchmarks (run with release mode):
```bash
cargo bench -p heraclitus
```

## Test Categories

### Integration Tests (`src/heraclitus/tests/integration/`)
- **Purpose**: Test Heraclitus server functionality
- **Speed**: Medium (100ms - 1s per test)
- **Dependencies**: Heraclitus subprocess
- **Examples**:
  - Kafka protocol compliance
  - Consumer group coordination
  - Multi-connection handling
  - Error scenarios

### End-to-End Tests (`src/heraclitus/tests/e2e/`)
- **Purpose**: Verify compatibility with real Kafka clients
- **Speed**: Slower (1-10s per test)
- **Dependencies**: Heraclitus subprocess + rdkafka client
- **Examples**:
  - Producer/consumer roundtrips
  - Consumer group rebalancing
  - High-throughput scenarios
  - Message headers and metadata

## Writing New Tests

### Integration Test Example
```rust
#[tokio::test]
async fn test_kafka_protocol() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;
    let mut stream = TcpStream::connect(&context.kafka_addr()).await?;
    // ... test protocol interactions
    Ok(())
}
```

### E2E Test Example
```rust
#[tokio::test]
async fn test_rdkafka_produce_consume() -> Result<()> {
    let context = HeraclitusTestContext::new().await?;
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &context.kafka_addr())
        .create()?;
    // ... test with real client
    Ok(())
}
```

## Test Utilities

### `HeraclitusTestContext`
Manages Heraclitus subprocess lifecycle for tests:
- Automatically finds free ports
- Spawns Heraclitus with test configuration
- Provides connection details
- Cleans up on drop

### Test Helpers
- `generate_test_messages()`: Create test data
- Protocol encoding/decoding helpers
- Compression test utilities

## Best Practices

1. **Use appropriate timeouts**: Integration tests should have reasonable timeouts
2. **Clean up resources**: Use RAII patterns for test resources
3. **Test error cases**: Don't just test the happy path
4. **Document complex tests**: Add comments explaining what's being tested
5. **Avoid test interdependence**: Each test should be independent

## Continuous Integration

Tests are run in CI with:
- Integration tests on every commit
- E2E tests on PR
- Extended integration tests on merge to main
- Benchmarks tracked for performance regression