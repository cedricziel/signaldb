# SignalDB Acceptor

The acceptor component of SignalDB is responsible for receiving observability data via OTLP (OpenTelemetry Protocol) over both HTTP and gRPC transports. It acts as the primary ingestion point for metrics, logs, and traces, ensuring reliable message processing through Write-Ahead Logging (WAL) and forwarding data to downstream services via Apache Flight.

## Purpose & Overview

The acceptor serves as the entry point for observability signals in SignalDB's distributed architecture. It:
- Accepts OTLP data via HTTP/gRPC endpoints
- Provides immediate durability guarantees through WAL
- Routes data to appropriate downstream services
- Handles service discovery and health checking
- Ensures data integrity and reliable delivery

## Architecture

The acceptor consists of several key components:

### OTLP Services
- **TraceAcceptorService**: Processes trace data via gRPC
- **LogAcceptorService**: Handles log data ingestion
- **MetricsAcceptorService**: Manages metrics collection
- **HTTP Handler**: Provides REST endpoints for OTLP/HTTP

### Core Infrastructure
- **Service Bootstrap**: Automatic service registration and discovery
- **Flight Transport**: High-performance inter-service communication
- **WAL Integration**: Write-Ahead Logging for durability guarantees
- **Parquet Writer**: Direct storage capability for high-throughput scenarios

### Data Flow
1. **Ingestion**: OTLP data received via HTTP (port 4318) or gRPC (port 4317)
2. **WAL Persistence**: Data written to WAL for durability
3. **Service Discovery**: Locate appropriate downstream services
4. **Flight Forwarding**: Route data to writer services via Apache Flight
5. **Acknowledgment**: Confirm successful processing to client

## API Reference

### gRPC Services
- **LogsServiceServer**: Implements OTLP logs collection
- **TraceServiceServer**: Implements OTLP trace collection  
- **MetricsServiceServer**: Implements OTLP metrics collection

### HTTP Endpoints
- `POST /v1/traces`: OTLP trace ingestion
- `GET /health`: Health check endpoint

### Key Functions
- `serve_otlp_grpc()`: Start gRPC server with all OTLP services
- `serve_otlp_http()`: Start HTTP server for OTLP/HTTP
- `get_parquet_writer()`: Create Parquet writer for direct storage

## Usage Examples

### As a Library
```rust
use acceptor::{serve_otlp_grpc, serve_otlp_http};
use tokio::sync::oneshot;

// Start gRPC acceptor
let (init_tx, init_rx) = oneshot::channel();
let (shutdown_tx, shutdown_rx) = oneshot::channel();
let (stopped_tx, stopped_rx) = oneshot::channel();

tokio::spawn(async move {
    serve_otlp_grpc(init_tx, shutdown_rx, stopped_tx).await
});

// Wait for initialization
init_rx.await.unwrap();

// Start HTTP acceptor
let (http_init_tx, http_init_rx) = oneshot::channel();
let (http_shutdown_tx, http_shutdown_rx) = oneshot::channel();
let (http_stopped_tx, http_stopped_rx) = oneshot::channel();

tokio::spawn(async move {
    serve_otlp_http(http_init_tx, http_shutdown_rx, http_stopped_tx).await
});

http_init_rx.await.unwrap();
```

### As a Standalone Service
```bash
# Run acceptor service
cargo run --bin acceptor

# Or with custom configuration
ACCEPTOR_ADVERTISE_ADDR=0.0.0.0:4317 \
ACCEPTOR_WAL_DIR=/custom/wal/path \
cargo run --bin acceptor
```

## Configuration

### Environment Variables
- `ACCEPTOR_ADVERTISE_ADDR`: Address to advertise for service discovery (default: "0.0.0.0:4317")
- `ACCEPTOR_WAL_DIR`: Directory for WAL files (default: ".wal/acceptor")
- `SIGNALDB_*`: Standard SignalDB configuration variables

### Service Discovery
The acceptor automatically registers with the catalog-based discovery system with:
- **Service Type**: `Acceptor`
- **Capabilities**: `TraceIngestion`, `LogIngestion`, `MetricsIngestion`
- **Endpoints**: gRPC (4317), HTTP (4318)

### Storage Configuration
Storage settings are inherited from the global SignalDB configuration:
- `[storage]` section in `signaldb.toml`
- `SIGNALDB_STORAGE_*` environment variables

## Dependencies

### Core Dependencies
- **tonic**: gRPC server implementation
- **axum**: HTTP server framework
- **opentelemetry-proto**: OTLP protocol definitions
- **tokio**: Async runtime
- **datafusion**: Arrow/Parquet integration

### SignalDB Dependencies
- **common**: Shared configuration, discovery, and Flight transport
- **tempo-api**: Protocol buffer definitions (indirect)

## Testing

### Unit Tests
```bash
cargo test -p acceptor
```

### Integration Tests
```bash
# Test with real OTLP clients
cargo test -p acceptor -- --test integration

# Test service discovery
cargo test -p common -- catalog_integration
```

### Manual Testing
```bash
# Send test traces via gRPC
grpcurl -plaintext -d '{"resource_spans":[]}' \
  localhost:4317 \
  opentelemetry.proto.collector.trace.v1.TraceService/Export

# Send test traces via HTTP
curl -X POST http://localhost:4318/v1/traces \
  -H "Content-Type: application/json" \
  -d '{"resourceSpans":[]}'
```

## Integration

### Service Communication
- **Discovery**: Registers with catalog backend (PostgreSQL/SQLite)
- **Downstream**: Communicates with writer services via Flight
- **Monitoring**: Exposes health endpoints and structured logging

### Error Handling
- **WAL Failures**: Automatic retry with exponential backoff
- **Service Discovery**: Fallback to cached service locations
- **Flight Errors**: Connection pooling with automatic failover
- **Client Errors**: Proper HTTP/gRPC status codes

### Performance Characteristics
- **Throughput**: Optimized for high-volume ingestion
- **Latency**: Low-latency acknowledgment via WAL
- **Scalability**: Horizontal scaling via service discovery
- **Reliability**: At-least-once delivery guarantees

## Future Enhancements

### Performance Optimizations
- Batch processing for improved throughput
- Connection pooling optimizations
- Memory-mapped WAL for reduced latency

### Protocol Support
- OTLP/gRPC streaming
- Prometheus remote write compatibility
- Custom protocol adapters

### Operational Features
- Metrics collection and export
- Distributed tracing integration
- Advanced health checking
- Rate limiting and backpressure