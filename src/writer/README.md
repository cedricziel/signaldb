# SignalDB Writer

The writer component of SignalDB is responsible for consuming messages from the queue system and persisting them to Iceberg tables with full ACID transaction support. It provides reliable, high-performance data storage with advanced optimization features.

## Goals

1. **ACID Transaction Support**
   - Full transaction semantics with commit/rollback operations
   - Multi-batch transactional writes for data consistency
   - Atomic operations across multiple data ingestion requests
   - Isolation and durability guarantees for concurrent access

2. **High-Performance Data Storage**
   - Apache Iceberg table format for efficient metadata management
   - Intelligent batch processing with automatic splitting (50K rows, 128MB default)
   - Connection pooling for optimal catalog operations
   - Memory-aware processing to prevent OOM issues
   - Configurable concurrent batch processing

3. **Production Reliability**
   - Exponential backoff retry logic for transient failures
   - Comprehensive error handling and recovery mechanisms
   - Catalog caching with configurable TTL (5 minutes default)
   - Performance monitoring and optimization metrics
   - Graceful degradation under high load

4. **Operational Excellence**
   - Run as both a standalone service and an embedded library
   - Extensive logging and observability features
   - Support for graceful shutdown and cleanup
   - Comprehensive testing with 20+ integration tests
   - Production-ready configuration and tuning options

## Architecture

The writer consists of several key components:

### 1. IcebergTableWriter
The core component that provides ACID transaction support:
- **Transaction Management**: Begin, commit, and rollback operations
- **Batch Optimization**: Intelligent splitting and memory management
- **Connection Pooling**: Optimized catalog operations
- **Retry Logic**: Configurable exponential backoff for reliability

### 2. SQL Catalog
Manages Iceberg table metadata and catalog operations:
- **Connection Pooling**: Optimized catalog access for production workloads
- **Catalog Configuration**: SQLite and PostgreSQL backend support
- **DataFusion Integration**: Seamless query engine integration

### 3. Performance Optimization
Advanced features for production workloads:
- **Batch Splitting**: Automatic division of large batches for optimal processing
- **Catalog Caching**: Reduced overhead with configurable TTL
- **Memory Management**: Memory-aware batch processing
- **Concurrent Processing**: Configurable parallel batch execution

### 4. Error Handling & Recovery
Robust error handling for production reliability:
- **Retry Logic**: Exponential backoff with configurable policies
- **Transaction Recovery**: Automatic rollback on failures
- **Graceful Degradation**: Continued operation under partial failures
- **Comprehensive Logging**: Detailed error context and debugging information

## Usage

### Basic Usage

```rust
use writer::{IcebergTableWriter, create_iceberg_writer};
use common::config::Configuration;
use object_store::memory::InMemory;
use std::sync::Arc;

// Create configuration
let config = Configuration::default();
let object_store = Arc::new(InMemory::new());

// Create Iceberg writer
let mut writer = create_iceberg_writer(
    &config,
    object_store,
    "my_tenant",
    "my_dataset",
    "metrics_gauge"
).await?;

// Write data with automatic optimization
writer.write_batch(record_batch).await?;
```

### Advanced Configuration

```rust
use writer::{
    BatchOptimizationConfig, CatalogPoolConfig, RetryConfig,
    create_iceberg_writer_with_pool
};

// Configure connection pooling
let pool_config = CatalogPoolConfig {
    min_connections: 5,
    max_connections: 20,
    connection_timeout_ms: 3000,
    idle_timeout_seconds: 600,
    max_lifetime_seconds: 3600,
};

// Create writer with custom pool
let mut writer = create_iceberg_writer_with_pool(
    &config,
    object_store,
    "my_tenant",
    "my_dataset",
    "metrics_gauge",
    pool_config
).await?;

// Configure batch optimization
let batch_config = BatchOptimizationConfig {
    max_rows_per_batch: 100_000,           // Larger batches
    max_memory_per_batch_bytes: 256 * 1024 * 1024, // 256MB
    enable_auto_split: true,
    target_concurrent_batches: 8,          // More concurrency
    enable_catalog_caching: true,
    catalog_cache_ttl_seconds: 600,        // 10 minute cache
};

writer.set_batch_config(batch_config);

// Configure retry behavior
let retry_config = RetryConfig {
    max_attempts: 5,
    initial_delay: Duration::from_millis(200),
    max_delay: Duration::from_secs(10),
    backoff_multiplier: 2.5,
};

writer.set_retry_config(retry_config);
```

### Transaction Usage

```rust
// Manual transaction management
let transaction_id = writer.begin_transaction().await?;

// Write multiple batches within transaction
writer.write_batch(batch1).await?;
writer.write_batch(batch2).await?;
writer.write_batch(batch3).await?;

// Commit all changes atomically
writer.commit_transaction(&transaction_id).await?;

// Or rollback on error
// writer.rollback_transaction(&transaction_id).await?;

// Automatic transactional batch writing
let batches = vec![batch1, batch2, batch3];
writer.write_batches(batches).await?; // Automatically wrapped in transaction
```

### Performance Monitoring

```rust
// Check catalog cache status
if let Some((age_seconds, is_expired)) = writer.catalog_cache_info() {
    println!("Catalog cache age: {}s, expired: {}", age_seconds, is_expired);
}

// Get current configuration
let batch_config = writer.batch_config();
println!("Max rows per batch: {}", batch_config.max_rows_per_batch);
println!("Catalog caching: {}", batch_config.enable_catalog_caching);

let pool_config = writer.pool_config();
println!("Max connections: {}", pool_config.max_connections);

// Clear cache if needed
writer.clear_catalog_cache();
```

## Configuration

### Environment Variables

The writer can be configured through environment variables:

- `SIGNALDB_SCHEMA_CATALOG_TYPE`: Catalog backend (sql, memory)
- `SIGNALDB_SCHEMA_CATALOG_URI`: Catalog database connection string
- `SIGNALDB_STORAGE_DSN`: Object storage configuration

### Performance Tuning

For production workloads, consider these optimizations:

1. **Connection Pooling**: Increase pool size for high-concurrency workloads
2. **Batch Size**: Adjust based on available memory and data characteristics  
3. **Catalog Caching**: Enable for reduced overhead (enabled by default)
4. **Retry Configuration**: Tune for your network and storage characteristics
5. **Concurrent Batches**: Scale based on CPU cores and I/O capacity

### Production Deployment

```toml
# signaldb.toml - Production configuration
[schema]
catalog_type = "sql"
catalog_uri = "postgres://user:password@catalog-db:5432/iceberg"

[storage]
dsn = "s3://my-data-bucket/iceberg-tables/"

# Performance optimizations are automatic but can be tuned via API
```

## Testing

### Unit Tests
```bash
# Core functionality tests
cargo test -p writer --lib

# Integration tests
cargo test -p writer --test test_sql_insert
cargo test -p writer --test test_transactions  
cargo test -p writer --test test_retry_logic
cargo test -p writer --test test_connection_pooling
cargo test -p writer --test test_batch_optimization
```

### End-to-End Tests
```bash
# Complete workflow validation
cargo test -p writer --test test_e2e_simple

# Performance benchmarks
cargo bench -p writer --bench iceberg_benchmarks --features benchmarks
cargo bench -p writer --bench connection_pool_benchmarks --features benchmarks
```

### Performance Testing
```bash
# Run all benchmarks
./src/writer/run_benchmarks.sh

# Individual benchmark categories
cargo bench --bench iceberg_benchmarks --features benchmarks -- "single_batch"
cargo bench --bench iceberg_benchmarks --features benchmarks -- "transaction_overhead"
cargo bench --bench connection_pool_benchmarks --features benchmarks -- "concurrent"
```

## Monitoring & Observability

### Key Metrics

Monitor these aspects of writer performance:

1. **Transaction Metrics**
   - Transaction success/failure rates
   - Transaction duration and latency
   - Rollback frequency and causes

2. **Batch Processing**
   - Batch size distribution
   - Splitting frequency and patterns
   - Memory usage per batch

3. **Connection Pooling**
   - Pool utilization and contention
   - Connection creation/destruction rates
   - Pool timeout events

4. **Retry Behavior**
   - Retry attempt frequency
   - Success rates after retries
   - Backoff timing effectiveness

### Logging

The writer provides comprehensive logging at multiple levels:

```rust
// Enable debug logging for detailed operation tracking
RUST_LOG=writer=debug cargo run

// Production logging (info level)
RUST_LOG=writer=info cargo run
```

## Migration Guide

### From Direct Parquet Usage

If migrating from direct Parquet writes:

1. **API Compatibility**: Existing `write_batch()` calls continue to work
2. **Configuration**: Add Iceberg catalog configuration
3. **Benefits**: Gain ACID transactions and performance optimizations
4. **Testing**: Use integration tests to validate data consistency

### Performance Comparison

Iceberg integration provides several advantages over direct Parquet:

- **ACID Transactions**: Data consistency guarantees
- **Metadata Management**: Efficient schema evolution and partitioning
- **Query Performance**: Optimized for analytical workloads
- **Operational Benefits**: Better monitoring and debugging capabilities

## Future Enhancements

### Planned Features

1. **Enhanced Performance**
   - Adaptive batch sizing based on data characteristics
   - Intelligent partition pruning and compaction
   - Advanced caching strategies

2. **Operational Improvements**
   - Metrics export (Prometheus integration)
   - Health check endpoints
   - Dynamic configuration updates
   - Advanced monitoring dashboards

3. **Advanced Features**
   - Schema evolution automation
   - Data lifecycle management
   - Cross-table transaction support
   - Multi-region replication

## Troubleshooting

### Common Issues

1. **Transaction Timeouts**: Increase connection pool timeouts
2. **Memory Usage**: Reduce batch sizes or enable auto-splitting
3. **Catalog Connectivity**: Check catalog database availability
4. **Performance**: Enable catalog caching and connection pooling

### Debug Information

Enable detailed logging for troubleshooting:

```bash
RUST_LOG=writer=debug,iceberg_rust=debug,datafusion=debug cargo test
```

For production debugging, use structured logging with correlation IDs and transaction tracking.