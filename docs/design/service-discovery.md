# Service Discovery Design

## Context

- In a microservice deployment, components (acceptor, writer, router, querier, etc.) must locate one another dynamically
- In monolithic mode services are co-located and discovery is handled internally
- SignalDB supports both catalog-based and NATS-based discovery mechanisms

**Current Implementation Status**: Both catalog-based and NATS-based discovery are implemented and working. The system supports graceful fallback between discovery methods.

## Goals

- ✅ **Achieved**: Decouple service endpoints behind well-known roles
- ✅ **Achieved**: Support dynamic registration, unregistration, and automatic health expiration
- ✅ **Achieved**: Minimize additional infrastructure by reusing existing systems
- ✅ **Achieved**: Provide client-side caching and notifications of topology changes

## Discovery Mechanisms

### 1. Catalog-based Discovery ✅ **Implemented**

**Purpose**: Authoritative service registry using database storage

**Implementation**:
- PostgreSQL or SQLite database stores service instances
- Services register on startup with `ServiceBootstrap::register()`
- Periodic heartbeats maintain liveness
- Other services query catalog for service endpoints

**Current Service Registry Schema**:
```sql
CREATE TABLE ingesters (
    id UUID PRIMARY KEY,
    address TEXT NOT NULL,
    last_seen TIMESTAMP WITH TIME ZONE,
    stopped_at TIMESTAMP WITH TIME ZONE
);
```

### 2. NATS-based Discovery ✅ **Implemented**

**Purpose**: Real-time service registration and discovery

**Implementation**:
- Services publish to subject-based channels
- NATS KV store provides persistent registry
- Heartbeat mechanism with TTL expiration

## Service Roles and Discovery

| Service Role | Discovery Method | Registration | Status |
|-------------|------------------|--------------|--------|
| **acceptor** | Catalog + NATS | `services.acceptor.register` | ✅ Implemented |
| **writer** | Catalog + NATS | `services.writer.register` | ✅ Implemented |
| **router** | Catalog + NATS | `services.router.register` | ✅ Implemented |
| **querier** | Catalog + NATS | `services.querier.register` | ✅ Implemented |

## Registration Process ✅ **Current Implementation**

### 1. Service Startup
```rust
// Each service registers with catalog
let bootstrap = ServiceBootstrap::new(config).await?;
bootstrap.register().await?;

// Optional NATS registration
if let Some(nats_client) = nats_client {
    discovery::register_service(&nats_client, role, instance).await?;
}
```

### 2. Health Monitoring
- **Catalog**: Periodic heartbeat updates to `last_seen` column
- **NATS**: TTL-based key expiration with heartbeat refresh

### 3. Graceful Shutdown
- **Catalog**: Update `stopped_at` timestamp
- **NATS**: Delete registration keys

## Discovery API ✅ **Implemented**

Current discovery functionality in `src/common/src/discovery.rs` and `src/common/src/service_bootstrap.rs`:

```rust
/// Service instance metadata
pub struct Ingester {
    pub id: Uuid,
    pub address: String,
    pub last_seen: Option<DateTime<Utc>>,
    pub stopped_at: Option<DateTime<Utc>>,
}

/// Catalog-based discovery
impl Catalog {
    async fn register_ingester(&self, id: Uuid, address: &str) -> Result<()>;
    async fn list_ingesters(&self) -> Result<Vec<Ingester>>;
    async fn heartbeat(&self, id: Uuid) -> Result<()>;
}

/// Service bootstrap handles registration
impl ServiceBootstrap {
    async fn register(&self) -> Result<()>;
    async fn start_heartbeat(&self) -> Result<()>;
}
```

## Configuration ✅ **Current Options**

### Catalog Configuration
```toml
[database]
url = "sqlite://signaldb.db"  # or PostgreSQL URL

[discovery]
enabled = true
heartbeat_interval = "30s"
```

### NATS Configuration  
```toml
[queue]
kind = "nats"
url = "nats://localhost:4222"
```

## Integration Patterns

### 1. Monolithic Mode ✅ **Working**
- All services in single process
- Discovery via shared catalog instance
- No network-based discovery needed

### 2. Microservices Mode ✅ **Working**  
- Each service deployed independently
- Discovery via catalog database or NATS
- Dynamic endpoint resolution

### 3. Hybrid Mode ✅ **Supported**
- Some services co-located, others distributed
- Discovery handles both local and remote services
- Flexible deployment configurations

## Client-Side Discovery ✅ **Implemented**

Services discover dependencies via:

```rust
// Router discovers queriers
let queriers = catalog.list_ingesters().await?
    .into_iter()
    .filter(|i| i.stopped_at.is_none())
    .collect();

// Flight client connection to discovered service
let endpoint = format!("http://{}", querier.address);
let flight_client = FlightServiceClient::connect(endpoint).await?;
```

## Operational Considerations

### Security
- Catalog access controlled via database credentials
- NATS access can be secured with TLS/JWT

### Performance  
- ✅ **Implemented**: Client-side caching of discovered services
- ✅ **Implemented**: Configurable heartbeat intervals
- ✅ **Implemented**: Graceful handling of service failures

### Reliability
- ✅ **Implemented**: Fallback between discovery mechanisms
- ✅ **Implemented**: Automatic cleanup of stale registrations
- ✅ **Implemented**: Health monitoring and failure detection

## Deployment Examples

### Monolithic Deployment
```bash
# Single binary with embedded discovery
cargo run
```

### Microservices Deployment  
```bash
# Each service discovers others via catalog
cargo run --bin signaldb-acceptor
cargo run --bin signaldb-writer  
cargo run --bin signaldb-router
cargo run --bin signaldb-querier
```

## Future Enhancements *(Planned)*

### Advanced Service Mesh Integration
- Support for service mesh discovery (Consul, etcd)
- Integration with Kubernetes service discovery
- DNS-based service resolution

### Enhanced Health Checking
- Application-level health checks beyond heartbeats
- Service dependency health propagation
- Circuit breaker patterns for failed services

### Multi-Region Support
- Cross-region service discovery
- Geographic proximity-based routing
- Disaster recovery and failover

## Current Status Summary

✅ **Working Features**:
- Catalog-based service registration and discovery
- NATS-based real-time discovery  
- Automatic heartbeat and health monitoring
- Graceful service registration/deregistration
- Support for both monolithic and microservices deployment

🔄 **Future Enhancements**:
- Service mesh integration
- Advanced health checking
- Multi-region capabilities

The discovery system provides a robust foundation for both simple and complex deployment scenarios while maintaining flexibility for future enhancements.