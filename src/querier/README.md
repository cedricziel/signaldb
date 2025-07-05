# SignalDB Querier

The querier component of SignalDB is responsible for executing queries against stored observability data using Apache Arrow Flight and DataFusion. It provides a high-performance query execution engine that can process SQL queries over Parquet files in object storage.

## Purpose & Overview

The querier serves as the primary query engine in SignalDB's distributed architecture:
- Executes SQL queries against Parquet files in object storage
- Provides Apache Arrow Flight interface for efficient data transfer
- Integrates with DataFusion for advanced query processing
- Handles service discovery and registration
- Supports distributed query execution patterns

## Architecture

The querier consists of several key components:

### Query Engine
- **DataFusion Integration**: SQL query execution over Arrow columnar format
- **Object Store Integration**: Direct querying of Parquet files
- **Flight Service**: Apache Arrow Flight protocol implementation
- **Session Management**: Query session context and state management

### Service Integration
- **Flight Transport**: High-performance inter-service communication
- **Service Discovery**: Automatic registration with catalog system
- **Connection Management**: Efficient connection pooling and cleanup

### Data Processing
- **Parquet Reader**: Efficient columnar data reading
- **Arrow Conversion**: Zero-copy data format conversions
- **Result Streaming**: Large result set streaming capabilities

## API Reference

### Core Classes

#### QuerierFlightService
Main Flight service implementation providing query execution capabilities.

```rust
impl QuerierFlightService {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        flight_transport: Arc<InMemoryFlightTransport>
    ) -> Self
    
    async fn execute_query(&self, sql: &str) -> Result<Vec<RecordBatch>, _>
    async fn execute_distributed_query(&self, query: &str) -> Result<Vec<RecordBatch>, _>
}
```

### Flight Protocol Methods
- `handshake`: Flight protocol handshake
- `list_flights`: List available query endpoints
- `get_schema`: Retrieve schema for query results
- `do_get`: Execute query and stream results
- `do_put`: Not implemented (read-only service)

## Usage Examples

### As a Library
```rust
use querier::QuerierFlightService;
use common::flight::transport::InMemoryFlightTransport;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use object_store::local::LocalFileSystem;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize object store
    let object_store = Arc::new(LocalFileSystem::new_with_prefix("./data")?);
    
    // Create service bootstrap
    let config = Configuration::load()?;
    let bootstrap = ServiceBootstrap::new(
        config,
        ServiceType::Querier,
        "0.0.0.0:50054".to_string()
    ).await?;
    
    // Create Flight transport
    let flight_transport = Arc::new(InMemoryFlightTransport::new(bootstrap));
    
    // Create querier service
    let querier = QuerierFlightService::new(object_store, flight_transport);
    
    // Execute a query
    let results = querier.execute_query("SELECT * FROM 'traces/batch.parquet'").await?;
    println!("Query returned {} batches", results.len());
    
    Ok(())
}
```

### As a Standalone Service
```bash
# Run querier service
cargo run --bin querier

# With custom configuration
QUERIER_FLIGHT_ADDR=0.0.0.0:50054 \
QUERIER_ADVERTISE_ADDR=querier.internal:50054 \
cargo run --bin querier
```

### Flight Client Example
```rust
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use tonic::transport::Channel;

// Connect to querier
let channel = Channel::from_static("http://localhost:50054").connect().await?;
let mut client = FlightServiceClient::new(channel);

// Execute query via Flight
let ticket = Ticket {
    ticket: "SELECT trace_id, span_id FROM 'traces/batch.parquet' LIMIT 10".into(),
};

let mut stream = client.do_get(ticket).await?.into_inner();
while let Some(flight_data) = stream.message().await? {
    // Process flight data
    println!("Received {} bytes", flight_data.data_body.len());
}
```

## Configuration

### Environment Variables
- `QUERIER_FLIGHT_ADDR`: Flight service bind address (default: "0.0.0.0:50054")
- `QUERIER_ADVERTISE_ADDR`: Address to advertise for service discovery (default: same as FLIGHT_ADDR)
- `SIGNALDB_STORAGE_*`: Storage configuration for object store access

### Service Discovery
The querier automatically registers with the catalog system:
- **Service Type**: `Querier`
- **Capabilities**: `QueryExecution`
- **Endpoint**: Flight service address

### DataFusion Configuration
The querier uses DataFusion's default configuration with:
- Object store registration for file access
- Arrow Flight data format
- Session-based query execution

## Dependencies

### Core Dependencies
- **datafusion**: Query engine and SQL processing
- **arrow-flight**: Flight protocol implementation
- **object_store**: Multi-backend storage abstraction
- **tonic**: gRPC framework for Flight transport
- **tokio**: Async runtime

### SignalDB Dependencies
- **common**: Service discovery, configuration, and Flight transport
- Shared schemas and data models

## Testing

### Unit Tests
```bash
# Run querier tests
cargo test -p querier

# Run with logs
RUST_LOG=debug cargo test -p querier
```

### Integration Tests
```bash
# Test with real Parquet files
cargo test -p querier -- --test integration

# Test Flight protocol
cargo test -p querier -- flight_service
```

### Manual Testing
```bash
# Using Flight CLI tools
flight-sql --host localhost --port 50054 \
  --query "SELECT * FROM 'traces/batch.parquet' LIMIT 5"

# Using DataFusion CLI
datafusion-cli --host localhost --port 50054
```

## Integration

### Query Patterns
The querier supports various SQL query patterns:

#### Basic Queries
```sql
-- Query all traces
SELECT * FROM 'traces/batch.parquet'

-- Filter by trace ID
SELECT * FROM 'traces/batch.parquet' 
WHERE trace_id = 'abc123'

-- Aggregate metrics
SELECT service_name, COUNT(*) as span_count
FROM 'traces/batch.parquet'
GROUP BY service_name
```

#### Time-Based Queries
```sql
-- Query recent traces
SELECT * FROM 'traces/batch.parquet'
WHERE start_time > '2024-01-01T00:00:00Z'

-- Time range queries
SELECT * FROM 'traces/batch.parquet'
WHERE start_time BETWEEN '2024-01-01' AND '2024-01-02'
```

### Service Communication
- **Discovery**: Registers with catalog backend
- **Client Integration**: Accessible via Flight protocol
- **Error Handling**: Proper gRPC status codes and error messages

### Performance Characteristics
- **Memory Usage**: Streaming results for large datasets
- **Parallelism**: DataFusion's parallel query execution
- **Caching**: Session-based query planning cache
- **Optimization**: Columnar processing with Arrow

## Error Handling

### Common Error Types
- **Invalid SQL**: Syntax errors in queries
- **Schema Mismatch**: Incompatible data types
- **Storage Errors**: Object store access failures
- **Flight Errors**: Protocol-level communication issues

### Error Responses
```rust
// Status codes returned via Flight protocol
Status::invalid_argument("Invalid query syntax")
Status::internal("Query execution failed")
Status::not_found("Table not found")
Status::unimplemented("Feature not supported")
```

## Future Enhancements

### Query Optimization
- Query result caching
- Predicate pushdown optimization
- Partition pruning for time-based queries
- Materialized view support

### Protocol Extensions
- Streaming query results
- Prepared statement support
- Transaction support
- Custom function registration

### Performance Improvements
- Query plan caching
- Connection pooling optimization
- Parallel query execution
- Memory usage optimization

### Advanced Features
- Full-text search capabilities
- Geospatial query support
- Machine learning integration
- Real-time query capabilities