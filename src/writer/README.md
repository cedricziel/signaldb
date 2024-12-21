# SignalDB Writer

The writer component of SignalDB is responsible for consuming messages from the queue system and persisting them to object storage. It acts as a critical link between the in-memory message queue and the durable storage layer.

## Goals

1. **Reliable Message Processing**
   - Consume messages from the queue system in a reliable manner
   - Handle different message types and payloads appropriately
   - Ensure proper acknowledgment of processed messages
   - Implement error handling and retry mechanisms

2. **Efficient Data Storage**
   - Write data to object storage in an optimized format (Parquet)
   - Organize data with a logical path structure based on message type and timestamp
   - Support multiple object storage backends (local, S3, Azure, GCP)
   - Handle large data volumes efficiently

3. **Operational Excellence**
   - Run as both a standalone service and an embedded library
   - Provide monitoring and observability through logging
   - Support graceful shutdown and cleanup
   - Include comprehensive testing

## Architecture

The writer consists of several key components:

1. **Queue Consumer**
   - Subscribes to specific message types (Signal messages)
   - Handles message deserialization
   - Manages message acknowledgment

2. **Storage Writer**
   - Writes RecordBatch data to object storage
   - Organizes data in a logical directory structure
   - Supports multiple storage backends through the object_store crate

3. **Error Handling**
   - Custom error types for different failure scenarios
   - Proper error propagation and logging
   - Recovery mechanisms for transient failures

## Usage

### As a Library

```rust
use writer::Writer;
use common::queue::memory::InMemoryQueue;
use object_store::local::LocalFileSystem;

// Create and configure queue
let mut queue = InMemoryQueue::default();
queue.connect(QueueConfig::default()).await?;

// Create object store
let object_store = Arc::new(LocalFileSystem::new_with_prefix("./data")?);

// Create writer
let writer = Writer::new(Box::new(queue), object_store);

// Start the writer
writer.start().await?;
```

### As a Standalone Service

```bash
cargo run -p writer
```

## Configuration

The writer can be configured through environment variables:

- `QUEUE_TYPE`: Type of queue to use (default: "memory")
- `STORAGE_TYPE`: Type of object storage to use (default: "local")
- `STORAGE_PATH`: Base path for object storage (default: "./data")

## Testing

Run the tests using:

```bash
cargo test -p writer
```

## Future Enhancements

1. **Performance Optimizations**
   - Batch writing capabilities
   - Compression options for different data types
   - Configurable buffer sizes

2. **Advanced Features**
   - Data partitioning strategies
   - Schema evolution support
   - Data validation and sanitization

3. **Operational Improvements**
   - Metrics collection
   - Health checks
   - Dynamic configuration updates
