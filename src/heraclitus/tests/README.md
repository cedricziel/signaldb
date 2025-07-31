# Heraclitus Test Suite

The Heraclitus test suite is organized into three main categories for clarity, maintainability, and execution efficiency.

## Test Structure

```
tests/
├── unit/                    # Fast unit tests without external dependencies
│   ├── config_test.rs      # Configuration validation tests
│   ├── compression_test.rs # Compression algorithm tests  
│   ├── storage_test.rs     # Message format conversion tests
│   └── protocol_test.rs    # Basic protocol structure tests
│
├── integration/            # Integration tests with Heraclitus subprocess
│   ├── helpers.rs         # Test utilities and context
│   ├── basic_kafka_test.rs       # Basic Kafka protocol tests
│   ├── consumer_group_test.rs    # Consumer group functionality
│   ├── protocol_test.rs         # Advanced protocol tests
│   └── protocol_comparison_test.rs # Protocol compliance tests
│
└── e2e/                   # End-to-end tests with real rdkafka clients
    └── rdkafka_compat_test.rs  # Comprehensive rdkafka compatibility

benches/                   # Performance benchmarks
└── kafka_protocol_bench.rs # Protocol encoding/decoding benchmarks
```

## Running Tests

### Unit Tests
Fast tests that don't require external dependencies:
```bash
cargo test --package heraclitus --lib tests::unit
```

### Integration Tests
Tests that spawn a Heraclitus subprocess:
```bash
cargo test --package heraclitus --lib tests::integration
```

### End-to-End Tests
Full compatibility tests with rdkafka clients:
```bash
cargo test --package heraclitus --lib tests::e2e
```

### All Tests
```bash
cargo test --package heraclitus
```

### Benchmarks
Performance benchmarks (run with release mode):
```bash
cargo bench --package heraclitus
```

## Test Categories

### Unit Tests (`unit/`)
- **Purpose**: Test individual components in isolation
- **Speed**: Fast (< 1ms per test)
- **Dependencies**: None
- **Examples**: 
  - Configuration defaults and validation
  - Compression/decompression algorithms
  - Message format conversions
  - Protocol encoding/decoding

### Integration Tests (`integration/`)
- **Purpose**: Test Heraclitus server functionality
- **Speed**: Medium (100ms - 1s per test)
- **Dependencies**: Heraclitus subprocess
- **Examples**:
  - Kafka protocol compliance
  - Consumer group coordination
  - Multi-connection handling
  - Error scenarios

### End-to-End Tests (`e2e/`)
- **Purpose**: Verify compatibility with real Kafka clients
- **Speed**: Slower (1-10s per test)
- **Dependencies**: Heraclitus subprocess + rdkafka client
- **Examples**:
  - Producer/consumer roundtrips
  - Consumer group rebalancing
  - High-throughput scenarios
  - Message headers and metadata

## Writing New Tests

### Unit Test Example
```rust
#[test]
fn test_compression_roundtrip() {
    let data = b"test data";
    let compressed = compress_gzip(data).unwrap();
    let decompressed = decompress_gzip(&compressed).unwrap();
    assert_eq!(data, &decompressed[..]);
}
```

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

1. **Keep unit tests fast**: No I/O, no external dependencies
2. **Use appropriate timeouts**: Integration tests should have reasonable timeouts
3. **Clean up resources**: Use RAII patterns for test resources
4. **Test error cases**: Don't just test the happy path
5. **Document complex tests**: Add comments explaining what's being tested
6. **Avoid test interdependence**: Each test should be independent

## Continuous Integration

Tests are run in CI with:
- Unit tests on every commit
- Integration tests on PR
- E2E tests on merge to main
- Benchmarks tracked for performance regression