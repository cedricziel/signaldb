# SignalDB Signal Producer

The signal producer is a development and testing utility that generates synthetic observability signals for SignalDB. It creates realistic trace data using OpenTelemetry and sends it to SignalDB via OTLP, making it invaluable for development, testing, and demonstration purposes.

## Purpose & Overview

The signal producer serves as a test data generator for SignalDB:
- Generates realistic distributed trace data
- Sends signals via OTLP protocol to acceptor services
- Provides consistent test data for development and testing
- Demonstrates proper OpenTelemetry instrumentation patterns
- Supports load testing and performance evaluation

## Architecture

The signal producer is a simple command-line utility that:

### Signal Generation
- **OpenTelemetry Integration**: Uses official OpenTelemetry Rust SDK
- **Realistic Traces**: Creates hierarchical spans with attributes
- **Service Simulation**: Simulates distributed service interactions
- **Timing Simulation**: Includes realistic timing and duration patterns

### OTLP Export
- **gRPC Transport**: Sends data via OTLP/gRPC protocol
- **Batch Export**: Efficiently batches spans for transmission
- **Resource Attributes**: Includes proper service identification
- **Protocol Compliance**: Follows OpenTelemetry protocol standards

## API Reference

The signal producer is implemented as a single main function that demonstrates:

### Core Components
- **TracerProvider**: OpenTelemetry tracer provider configuration
- **SpanExporter**: OTLP gRPC span exporter
- **Resource**: Service resource identification
- **Tracer**: Span creation and management

### Generated Signals
The producer creates several types of spans:
- Root spans with service context
- Child spans representing internal operations
- HTTP request spans with semantic attributes
- Nested operation spans

## Usage Examples

### Basic Usage
```bash
# Run signal producer
cargo run --bin signal-producer

# The producer will:
# 1. Create a tracer provider
# 2. Generate sample traces
# 3. Send them to localhost:4317 (OTLP/gRPC)
# 4. Shutdown gracefully
```

### With Custom OTLP Endpoint
```bash
# Set custom OTLP endpoint
OTEL_EXPORTER_OTLP_ENDPOINT=http://my-signaldb:4317 \
cargo run --bin signal-producer
```

### Integration with SignalDB
```bash
# Start SignalDB acceptor
cargo run --bin acceptor

# In another terminal, run signal producer
cargo run --bin signal-producer

# Verify traces are ingested
curl http://localhost:3000/api/search
```

### As a Library
```rust
use opentelemetry::{global, trace::Tracer, KeyValue};
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::{trace::SdkTracerProvider, Resource};

// Create a tracer provider
let resource = Resource::builder()
    .with_attributes(vec![KeyValue::new("service.name", "my-service")])
    .build();

let exporter = SpanExporter::builder()
    .with_tonic()
    .build()
    .expect("Failed to create span exporter");

let provider = SdkTracerProvider::builder()
    .with_resource(resource)
    .with_batch_exporter(exporter)
    .build();

global::set_tracer_provider(provider.clone());

// Create traces
let tracer = global::tracer("my-tracer");
tracer.in_span("operation", |cx| {
    let span = cx.span();
    span.set_attribute(KeyValue::new("operation.type", "compute"));
    // Perform work...
});

// Shutdown
provider.shutdown().expect("Failed to shutdown exporter");
```

## Configuration

### Environment Variables
- `OTEL_EXPORTER_OTLP_ENDPOINT`: OTLP endpoint URL (default: "http://localhost:4317")
- `OTEL_EXPORTER_OTLP_HEADERS`: Custom headers for OTLP requests
- `OTEL_RESOURCE_ATTRIBUTES`: Additional resource attributes
- `RUST_LOG`: Logging level configuration

### OpenTelemetry Configuration
The signal producer uses standard OpenTelemetry configuration:
- Service name: "signal-producer"
- Batch span processor for efficient export
- OTLP/gRPC exporter with tonic transport

## Dependencies

### Core Dependencies
- **opentelemetry**: Core OpenTelemetry API and SDK
- **opentelemetry-otlp**: OTLP exporter implementation
- **opentelemetry-sdk**: OpenTelemetry SDK for Rust
- **opentelemetry-semantic-conventions**: Standard semantic conventions
- **tokio**: Async runtime for the main function

### Protocol Dependencies
- **tonic**: gRPC client for OTLP transport
- **prost**: Protocol buffer support

## Testing

### Manual Testing
```bash
# Test signal generation
cargo run --bin signal-producer

# Check logs for successful export
RUST_LOG=debug cargo run --bin signal-producer

# Test with custom endpoint
OTEL_EXPORTER_OTLP_ENDPOINT=http://test-endpoint:4317 \
cargo run --bin signal-producer
```

### Integration Testing
```bash
# Start SignalDB stack
docker compose up -d

# Wait for services to be ready
sleep 10

# Generate test signals
cargo run --bin signal-producer

# Verify data in Grafana
# Open http://localhost:3000 and check Tempo datasource
```

### Load Testing
```bash
# Generate multiple signal batches
for i in {1..10}; do
  cargo run --bin signal-producer &
done
wait

# Check SignalDB metrics and performance
```

## Integration

### Development Workflow
1. Start SignalDB services (acceptor, router, writer, querier)
2. Run signal producer to generate test data
3. Use router APIs to query generated traces
4. Verify data persistence and querying capabilities

### CI/CD Integration
```yaml
# Example GitHub Actions step
- name: Generate test signals
  run: |
    cargo run --bin acceptor &
    sleep 5
    cargo run --bin signal-producer
    
- name: Verify trace ingestion
  run: |
    curl -f http://localhost:3000/api/search
```

### Docker Integration
```dockerfile
# Multi-stage build for signal producer
FROM rust:1.85 as builder
WORKDIR /app
COPY . .
RUN cargo build --release --bin signal-producer

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates
COPY --from=builder /app/target/release/signal-producer /usr/local/bin/
ENTRYPOINT ["signal-producer"]
```

## Generated Trace Structure

### Trace Hierarchy
```
signal-producer (service)
├── doing_work (root span)
│   ├── doing_work_inside (child span)
│   └── attributes: question="what is the answer?"
└── GET /products/{id} (HTTP span)
    ├── internal work (child span)
    │   └── attributes: my.domain.attr=42
    └── attributes: http.request.method=GET, url.full=https://www.rust-lang.org/
```

### Span Attributes
- **Service attributes**: service.name, service.version
- **HTTP attributes**: http.request.method, url.full
- **Custom attributes**: Custom business logic attributes
- **Semantic conventions**: Standard OpenTelemetry semantic conventions

### Timing Characteristics
- Root spans: Variable duration
- HTTP spans: Realistic request timing
- Internal operations: Simulated processing time
- Nested operations: Hierarchical timing relationships

## Future Enhancements

### Signal Variety
- Multiple service simulation
- Different trace patterns (errors, retries, timeouts)
- Metric generation support
- Log generation support

### Load Testing Features
- Configurable signal rate
- Multiple concurrent producers
- Realistic workload patterns
- Performance metrics collection

### Advanced Scenarios
- Multi-service distributed traces
- Error injection and simulation
- Complex dependency chains
- Real-world timing patterns

### Configuration Options
- YAML configuration files
- Command-line parameter support
- Environment-specific presets
- Custom attribute injection

## Troubleshooting

### Common Issues
- **Connection refused**: Ensure SignalDB acceptor is running on port 4317
- **Timeout errors**: Check network connectivity to OTLP endpoint
- **Export failures**: Verify OTLP endpoint configuration
- **No traces visible**: Check acceptor logs for ingestion errors

### Debug Logging
```bash
# Enable detailed logging
RUST_LOG=trace cargo run --bin signal-producer

# Enable OpenTelemetry debug logging
OTEL_LOG_LEVEL=debug cargo run --bin signal-producer
```

### Verification Commands
```bash
# Check if acceptor is listening
nc -zv localhost 4317

# Verify OTLP endpoint connectivity
grpcurl -plaintext localhost:4317 list

# Check SignalDB router for traces
curl "http://localhost:3000/api/search?limit=10"
```