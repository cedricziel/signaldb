# SignalDB Router

The router component of SignalDB serves as the stateless HTTP API gateway and service orchestration layer. It provides Grafana Tempo-compatible APIs for trace querying and implements Apache Arrow Flight for high-performance inter-service communication.

## Purpose & Overview

The router acts as the primary entry point for external clients:
- Provides Grafana Tempo-compatible HTTP API endpoints
- Routes requests to appropriate backend services via service discovery
- Implements Apache Arrow Flight for efficient inter-service communication
- Handles service load balancing and failover
- Maintains stateless operation for horizontal scalability

## Architecture

The router consists of several key components:

### HTTP API Layer
- **Tempo API Compatibility**: Full Grafana Tempo API support for traces
- **RESTful Endpoints**: Standard HTTP endpoints for trace querying
- **Service Discovery**: Dynamic routing based on service capabilities
- **Load Balancing**: Distributes requests across available services

### Flight Service Layer
- **Arrow Flight Server**: High-performance data transport protocol
- **Service Registration**: Automatic registration with discovery backend
- **Connection Management**: Efficient connection pooling and cleanup

### Service Discovery
- **Catalog Integration**: Uses catalog backend for service location
- **Capability-Based Routing**: Routes requests based on service capabilities
- **Health Monitoring**: Continuous health checking of backend services
- **Failover Support**: Automatic failover to healthy services

## API Reference

### Core Components

#### RouterState Trait
Defines the interface for router state management:
```rust
pub trait RouterState: Debug + Clone + Send + Sync + 'static {
    fn catalog(&self) -> &Catalog;
    fn service_registry(&self) -> &discovery::ServiceRegistry;
}
```

#### InMemoryStateImpl
Concrete implementation of RouterState:
```rust
impl InMemoryStateImpl {
    pub fn new(catalog: Catalog) -> Self
}
```

### HTTP Endpoints

#### Tempo API v1
- `GET /api/echo` - Health check endpoint
- `GET /api/traces/:trace_id` - Query single trace by ID
- `GET /api/search` - Search traces with filters
- `GET /api/search/tags` - List available tags
- `GET /api/search/tag/:tag_name/values` - List tag values

#### Tempo API v2
- `GET /api/v2/search/tags` - Enhanced tag search
- `GET /api/v2/search/tag/:tag_name/values` - Enhanced tag values

### Flight Service Methods
- Standard Apache Arrow Flight protocol methods
- Service registration and discovery
- Connection management and cleanup

## Usage Examples

### As a Library
```rust
use router::{create_router, create_flight_service, InMemoryStateImpl};
use common::config::Configuration;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration
    let config = Configuration::load()?;
    
    // Initialize service discovery
    let bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Router,
        "0.0.0.0:50053".to_string()
    ).await?;
    
    // Create router state
    let state = InMemoryStateImpl::new(bootstrap.catalog().clone());
    
    // Create HTTP router
    let app = create_router(state.clone());
    
    // Create Flight service
    let flight_service = create_flight_service(state);
    
    // Start servers...
    Ok(())
}
```

### As a Standalone Service
```bash
# Run router service
cargo run --bin router

# Router starts on:
# - HTTP API: http://localhost:3000
# - Flight service: grpc://localhost:50053
```

### Client Usage Examples

#### HTTP API Client
```bash
# Health check
curl http://localhost:3000/api/echo

# Query specific trace
curl "http://localhost:3000/api/traces/abc123?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z"

# Search traces
curl "http://localhost:3000/api/search?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z&limit=100"

# List tags
curl http://localhost:3000/api/search/tags

# List tag values
curl http://localhost:3000/api/search/tag/service.name/values
```

#### Flight Client
```rust
use arrow_flight::flight_service_client::FlightServiceClient;
use tonic::transport::Channel;

// Connect to router's Flight service
let channel = Channel::from_static("http://localhost:50053").connect().await?;
let mut client = FlightServiceClient::new(channel);

// Query via Flight protocol
let ticket = arrow_flight::Ticket {
    ticket: "find_trace:abc123".into(),
};

let response = client.do_get(ticket).await?;
// Process response...
```

## Configuration

### Environment Variables
- `SIGNALDB_ROUTER_HTTP_ADDR`: HTTP API bind address (default: "0.0.0.0:3000")
- `SIGNALDB_ROUTER_FLIGHT_ADDR`: Flight service bind address (default: "0.0.0.0:50053")
- `SIGNALDB_*`: Standard SignalDB configuration variables

### Service Discovery Configuration
```toml
[discovery]
backend = "catalog"
heartbeat_interval = 30
poll_interval = 10
ttl = 60
```

### Catalog Configuration
```toml
[database]
url = "postgresql://user:pass@localhost/signaldb"
connection_pool_size = 10
```

## Dependencies

### Core Dependencies
- **axum**: HTTP web framework
- **arrow-flight**: Flight protocol implementation
- **tonic**: gRPC framework
- **tokio**: Async runtime
- **tracing**: Structured logging

### SignalDB Dependencies
- **common**: Service discovery, configuration, and catalog
- **tempo-api**: Tempo protocol definitions

## Testing

### Unit Tests
```bash
# Run router tests
cargo test -p router

# Run with logs
RUST_LOG=debug cargo test -p router
```

### Integration Tests
```bash
# Test HTTP endpoints
cargo test -p router -- --test http_integration

# Test Flight service
cargo test -p router -- --test flight_integration
```

### Manual Testing
```bash
# Test HTTP API
curl http://localhost:3000/api/echo

# Test with actual trace data
curl "http://localhost:3000/api/traces/$(openssl rand -hex 16)"

# Test search functionality
curl "http://localhost:3000/api/search?limit=10"
```

## Integration

### Service Communication Flow

#### Trace Query Flow
1. Client sends HTTP request to router
2. Router uses service discovery to find querier services
3. Router establishes Flight connection to querier
4. Router sends Flight query to querier
5. Querier processes query and returns Arrow data
6. Router converts Arrow data to Tempo format
7. Router returns HTTP response to client

#### Service Discovery Flow
1. Router registers with catalog backend
2. Router polls catalog for available services
3. Router maintains service health status
4. Router routes requests to healthy services

### Error Handling
- **Service Unavailable**: When no backend services are available
- **Internal Server Error**: For Flight communication failures
- **Bad Request**: For malformed query parameters
- **Not Found**: For non-existent traces

### Performance Characteristics
- **Stateless**: No local state, fully horizontally scalable
- **Connection Pooling**: Efficient Flight connection management
- **Async Processing**: Non-blocking request handling
- **Load Balancing**: Distributes load across backend services

## Service Registry

### Capability-Based Routing
The router uses service capabilities to route requests:
- `QueryExecution`: Routes trace queries to querier services
- `Storage`: Routes write operations to writer services
- `TraceIngestion`: Routes ingestion to acceptor services

### Service Discovery
```rust
// Get services by capability
let services = state.service_registry()
    .get_services_by_capability(ServiceCapability::QueryExecution)
    .await?;

// Get Flight client for capability
let mut client = state.service_registry()
    .get_flight_client_for_capability(ServiceCapability::QueryExecution)
    .await?;
```

## Future Enhancements

### HTTP API Enhancements
- GraphQL endpoint support
- OpenAPI/Swagger documentation
- Rate limiting and throttling
- Authentication and authorization

### Performance Optimizations
- Response caching
- Connection pooling improvements
- Request batching
- Streaming response support

### Operational Features
- Circuit breaker patterns
- Retry mechanisms with exponential backoff
- Health check endpoints
- Metrics and monitoring integration

### Protocol Support
- OpenTelemetry native API
- Prometheus remote read
- Jaeger compatibility layer
- Custom protocol adapters

## Error Handling

### Common Error Scenarios
- **No Services Available**: Returns 503 Service Unavailable
- **Service Discovery Failure**: Automatic retry with cached services
- **Flight Communication Error**: Failover to alternative services
- **Data Conversion Error**: Returns appropriate HTTP error codes

### Error Response Format
```json
{
  "error": "Service unavailable",
  "message": "No querier services available",
  "code": 503
}
```

## Monitoring and Observability

### Structured Logging
All operations are logged with structured data:
- Request IDs for correlation
- Service discovery events
- Flight communication status
- Performance metrics

### Health Monitoring
- Service health checks
- Connection pool status
- Background task monitoring
- Graceful shutdown handling