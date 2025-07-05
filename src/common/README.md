# SignalDB Common

The common library provides shared functionality, data structures, and utilities used across all SignalDB components. It serves as the foundation for configuration management, service discovery, inter-service communication, and data persistence patterns.

## Purpose & Overview

The common library centralizes core functionality to ensure consistency across the distributed SignalDB architecture:
- Unified configuration management across all services
- Service discovery and registration mechanisms
- Apache Flight-based inter-service communication
- Write-Ahead Logging (WAL) for durability guarantees
- Shared data models and type definitions
- Catalog-based metadata management

## Architecture

The common library is organized into several key modules:

### Configuration (`config/`)
- **Configuration**: Centralized config loading from TOML files and environment variables
- **Precedence**: Environment variables override TOML, which overrides defaults
- **Validation**: Schema validation and default value handling

### Service Discovery (`service_bootstrap.rs`)
- **ServiceBootstrap**: Automatic service registration and health checking
- **Service Types**: Enumeration of all service types (Acceptor, Writer, Querier, Router)
- **Capability System**: Services advertise their capabilities for discovery

### Catalog Management (`catalog.rs`)
- **Catalog**: Metadata storage and retrieval interface
- **Backend Support**: PostgreSQL and SQLite implementations
- **Health Tracking**: Service heartbeat and availability monitoring

### Flight Communication (`flight/`)
- **Schema Definitions**: Arrow schemas for all data types
- **Transport Layer**: Connection pooling and service discovery integration
- **Data Conversions**: OTLP to Arrow format conversions
- **Streaming**: Support for large dataset transfers

### Write-Ahead Logging (`wal/`)
- **WAL Interface**: Persistent, ordered write logging
- **Durability**: Crash-safe data persistence
- **Cleanup**: Automatic WAL entry management and cleanup

### Data Models (`model/`)
- **Span**: Distributed tracing span representation
- **Trace**: Complete trace data structures
- **Datasets**: Metadata about stored data collections

## API Reference

### Configuration
```rust
use common::config::Configuration;

// Load configuration from file and environment
let config = Configuration::load()?;

// Access configuration sections
let db_config = &config.database;
let storage_config = &config.storage;
```

### Service Discovery
```rust
use common::service_bootstrap::{ServiceBootstrap, ServiceType};

// Register service with discovery
let bootstrap = ServiceBootstrap::new(
    config, 
    ServiceType::Acceptor, 
    "0.0.0.0:4317".to_string()
).await?;

// Service automatically registers and maintains heartbeat
```

### Catalog Operations
```rust
use common::catalog::Catalog;

// Create catalog instance
let catalog = Catalog::new(db_config).await?;

// Register service
catalog.register_service(service_info).await?;

// Discover services by capability
let services = catalog.discover_services_by_capability(capability).await?;
```

### Flight Communication
```rust
use common::flight::transport::InMemoryFlightTransport;

// Create transport with discovery
let transport = InMemoryFlightTransport::new(bootstrap);

// Send data to discovered services
transport.send_to_capability(capability, data).await?;
```

### WAL Operations
```rust
use common::wal::{Wal, WalConfig};

// Initialize WAL
let wal_config = WalConfig::default();
let mut wal = Wal::new(wal_config).await?;

// Write data with durability
let entry_id = wal.write(data).await?;

// Read entries
let entries = wal.read_entries().await?;

// Mark entries as processed
wal.mark_processed(entry_id).await?;
```

## Usage Examples

### Basic Configuration Setup
```rust
use common::config::Configuration;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration
    let config = Configuration::load()?;
    
    // Initialize service discovery
    let bootstrap = ServiceBootstrap::new(
        config,
        ServiceType::Writer,
        "127.0.0.1:8080".to_string()
    ).await?;
    
    // Service is now registered and discoverable
    Ok(())
}
```

### Data Conversion Example
```rust
use common::flight::conversion::conversion_traces::traces_to_record_batch;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;

// Convert OTLP traces to Arrow format
let request: ExportTraceServiceRequest = get_otlp_request();
let record_batch = traces_to_record_batch(&request)?;

// Record batch can now be sent via Flight or stored in Parquet
```

### WAL Integration Pattern
```rust
use common::wal::{Wal, WalConfig};
use std::sync::Arc;

// Initialize WAL for a service
let wal_config = WalConfig {
    wal_dir: "/var/lib/signaldb/wal".into(),
    max_file_size: 64 * 1024 * 1024, // 64MB
    ..Default::default()
};

let mut wal = Wal::new(wal_config).await?;
wal.start_background_flush();
let wal = Arc::new(wal);

// Use WAL for durability
let entry_id = wal.write(serde_json::to_vec(&data)?).await?;
```

## Configuration

### Configuration File Structure
```toml
# signaldb.toml
[database]
url = "postgresql://user:pass@localhost/signaldb"
connection_pool_size = 10

[storage]
url = "file:///var/lib/signaldb/data"
backend = "filesystem"

[queue]
backend = "memory"
capacity = 10000

[discovery]
backend = "catalog"
heartbeat_interval = 30
```

### Environment Variables
```bash
# Override any configuration value
SIGNALDB_DATABASE_URL=postgresql://...
SIGNALDB_STORAGE_URL=s3://bucket/prefix
SIGNALDB_DISCOVERY_BACKEND=catalog
```

### Service-Specific Configuration
Each service can extend the base configuration:
```rust
// Custom configuration for a service
#[derive(Deserialize)]
struct MyServiceConfig {
    #[serde(flatten)]
    common: Configuration,
    
    custom_setting: String,
}
```

## Dependencies

### Core Dependencies
- **tokio**: Async runtime and I/O
- **arrow**: Columnar data format
- **datafusion**: Query engine and Arrow integration
- **serde**: Serialization framework
- **sqlx**: Database connectivity

### Protocol Dependencies
- **opentelemetry-proto**: OTLP protocol definitions
- **prost**: Protocol buffer implementation
- **tonic**: gRPC framework

### Storage Dependencies
- **object_store**: Multi-backend storage abstraction
- **parquet**: Columnar storage format

## Testing

### Unit Tests
```bash
# Run all common tests
cargo test -p common

# Run specific module tests
cargo test -p common -- flight
cargo test -p common -- wal
```

### Integration Tests
```bash
# Test catalog integration with real database
cargo test -p common -- catalog_integration

# Test Flight communication
cargo test -p common -- flight_integration
```

### Configuration Testing
```bash
# Test configuration loading
SIGNALDB_DATABASE_URL=test://url cargo test -p common -- config
```

## Integration

### Service Integration Pattern
```rust
use common::{
    config::Configuration,
    service_bootstrap::{ServiceBootstrap, ServiceType},
    flight::transport::InMemoryFlightTransport,
    wal::{Wal, WalConfig},
};
use std::sync::Arc;

// Standard service initialization pattern
pub async fn initialize_service(
    service_type: ServiceType,
    advertise_addr: String,
) -> Result<ServiceContext, Box<dyn std::error::Error>> {
    
    // Load configuration
    let config = Configuration::load()?;
    
    // Initialize service discovery
    let bootstrap = ServiceBootstrap::new(
        config,
        service_type,
        advertise_addr
    ).await?;
    
    // Initialize Flight transport
    let transport = Arc::new(InMemoryFlightTransport::new(bootstrap));
    
    // Initialize WAL if needed
    let wal_config = WalConfig::default();
    let mut wal = Wal::new(wal_config).await?;
    wal.start_background_flush();
    let wal = Arc::new(wal);
    
    Ok(ServiceContext {
        transport,
        wal,
    })
}
```

### Error Handling
The common library provides standardized error types:
- **ConfigurationError**: Configuration loading and validation failures
- **CatalogError**: Service discovery and metadata operations
- **FlightError**: Inter-service communication failures
- **WalError**: Write-ahead log operations

### Performance Characteristics
- **Configuration**: Loaded once at startup, cached in memory
- **Service Discovery**: Periodic heartbeat updates, cached locally
- **Flight Transport**: Connection pooling, automatic failover
- **WAL**: Asynchronous writes, background flushing

## Future Enhancements

### Configuration Improvements
- Dynamic configuration reloading
- Configuration validation schemas
- Environment-specific configuration profiles

### Discovery Enhancements
- Consul/etcd backend support
- Load balancing strategies
- Circuit breaker patterns

### Performance Optimizations
- Zero-copy Flight data transfers
- WAL compression and compaction
- Connection multiplexing