# Heraclitus - High-Performance Kafka Protocol Server

A standalone, high-performance Kafka-compatible protocol server written in Rust.

## Features

- Full Kafka wire protocol compatibility (v0-v3 for most APIs)
- Zero-copy message handling for optimal performance
- Multiple compression algorithms (Gzip, Snappy, LZ4, Zstandard)
- Pluggable storage backends (local filesystem, in-memory)
- Built-in metrics and monitoring via Prometheus
- Consumer group coordination
- SASL/PLAIN authentication support
- Configurable performance tuning

## Quick Start

```bash
# Run with default settings (local storage)
cargo run --bin heraclitus

# Run with in-memory storage
cargo run --bin heraclitus -- --storage-path memory://

# Run with custom ports
cargo run --bin heraclitus -- --kafka-port 9092 --http-port 9093

# Run with debug logging
cargo run --bin heraclitus -- --debug
```

## Configuration

Create a `heraclitus.toml` file:

```toml
kafka_port = 9092
http_port = 9093

[storage]
path = "/var/lib/heraclitus"  # or "memory://" for in-memory

[auth]
enabled = false
mechanism = "PLAIN"
# plain_username = "admin"
# plain_password = "secret"

[batching]
max_batch_size = 1048576      # 1MB
max_batch_messages = 1000
flush_interval_ms = 100

[metrics]
enabled = true
prefix = "heraclitus"

[performance]
tcp_nodelay = true
socket_send_buffer_bytes = 131072  # 128KB
socket_recv_buffer_bytes = 131072  # 128KB
buffer_pool_size = 1000
compression_level = 6
```

Run with configuration file:

```bash
cargo run --bin heraclitus -- --config heraclitus.toml
```

## Architecture

Heraclitus is designed as a standalone server with clean separation of concerns:

### Core Components

- **Protocol Handler**: Implements the Kafka wire protocol with per-API handlers
- **State Manager**: Manages topics, partitions, offsets, and consumer groups
- **Storage Layer**: Efficient storage using Arrow/Parquet format
- **Batch Writer**: Optimizes writes with configurable batching
- **Metrics**: Prometheus-compatible metrics exposed via HTTP

### Handler Architecture

Each Kafka API is implemented as a separate handler:
- **Auth**: ApiVersions, SaslHandshake, SaslAuthenticate
- **Metadata**: Metadata, ListOffsets
- **Produce**: Produce, InitProducerId
- **Fetch**: Fetch
- **Consumer Groups**: JoinGroup, Heartbeat, LeaveGroup, SyncGroup, etc.
- **Admin**: CreateTopics, DeleteTopics

## Supported Kafka APIs

| API | Versions | Status |
|-----|----------|--------|
| Produce | 0-9 | ✅ Full |
| Fetch | 0-12 | ✅ Full |
| ListOffsets | 0-7 | ✅ Full |
| Metadata | 0-12 | ✅ Full |
| FindCoordinator | 0-4 | ✅ Full |
| JoinGroup | 0-7 | ✅ Full |
| Heartbeat | 0-4 | ✅ Full |
| LeaveGroup | 0-4 | ✅ Full |
| SyncGroup | 0-5 | ✅ Full |
| DescribeGroups | 0-5 | ✅ Full |
| ListGroups | 0-4 | ✅ Full |
| OffsetCommit | 0-8 | ✅ Full |
| OffsetFetch | 0-8 | ✅ Full |
| ApiVersions | 0-3 | ✅ Full |
| CreateTopics | 0-7 | ✅ Full |
| DeleteTopics | 0-6 | ✅ Full |
| InitProducerId | 0-4 | ✅ Full |
| SaslHandshake | 0-1 | ✅ Full |
| SaslAuthenticate | 0-2 | ✅ Full |

## Performance Optimizations

- **Zero-copy message handling** using `bytes::Bytes`
- **Async compression** to avoid blocking the event loop
- **Buffer pooling** to reduce allocations
- **Sticky partitioning** for better batching efficiency
- **Configurable socket options** for network tuning

## Building

```bash
# Debug build
cargo build

# Release build with optimizations
cargo build --release

# Run tests
cargo test

# Run benchmarks
cargo bench
```

## Monitoring

Metrics are exposed at `http://localhost:9093/metrics` (by default) in Prometheus format.

Key metrics include:
- Request rates and latencies per API
- Message throughput and sizes
- Consumer group states
- Storage statistics
- Connection counts

## License

Apache License 2.0