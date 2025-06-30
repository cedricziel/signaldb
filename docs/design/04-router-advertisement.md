How service discovery works for routing and queries

The mechanism is intentionally configuration-light so that you can add or remove services without touching client-facing endpoints. It supports both catalog-based and NATS-based discovery mechanisms.

**Current Implementation Status**: Basic catalog-based discovery is implemented. NATS-based discovery is implemented for simple service registration. Complex sharding and replication are planned for future implementation.

## Service Roles and Responsibilities

| Service | Current Role | Discovery Method | Implementation Status |
|---------|-------------|------------------|---------------------|
| **Acceptor** | OTLP ingestion endpoint | Registers with catalog as "acceptor" | ✅ **Implemented** |
| **Writer** | Data persistence service | Registers with catalog as "writer" | ✅ **Implemented** |
| **Router** | Query routing and HTTP API | Registers with catalog, discovers other services | ✅ **Implemented** |
| **Querier** | Query execution engine | Registers with catalog as "querier" | ✅ **Implemented** |

**Architecture Notes:**
- **Current**: Simple service registration without sharding
- **Future**: Hash-based shard assignment and replica management

## Discovery Mechanisms

### 1. Catalog-based Discovery *(Current Implementation)*

**Purpose**: Stores authoritative service metadata
- List of live service instances and their addresses
- Service health status via heartbeat mechanism
- *(Future: Shard assignments and replica mappings)*

**Implementation**: 
- PostgreSQL or SQLite database stores service registry
- Services register on startup and send periodic heartbeats
- Other services query catalog to discover endpoints
- ✅ **Currently implemented and working**

### 2. NATS-based Discovery *(Current Implementation)*

**Purpose**: Provides fast service registration and discovery
- Real-time service announcements
- Heartbeat-based liveness detection
- Subject-based service discovery

**Implementation**:
- Services publish to `services.{role}.register` subjects
- Heartbeats sent to `services.{role}.heartbeat` subjects  
- NATS KV store used for persistent service registry
- ✅ **Currently implemented and working**

### 3. Future: Advanced Sharding *(Planned)*

**Purpose**: Distribute data across multiple Writers with replication
- Hash-based shard assignment (org_id, bucket, table) → shard_id
- Replica factor configuration for durability
- Automatic shard rebalancing

## End-to-end Resolution Flow

### Current Implementation

1. **Service Startup**
   - Service registers with catalog (PostgreSQL/SQLite)
   - Optionally announces via NATS subjects
   - Begins sending periodic heartbeats

2. **Service Discovery**
   - Router queries catalog for available Writers
   - Router queries catalog for available Queriers
   - Simple round-robin or single-target routing

3. **Request Routing**
   - **Write path**: Acceptor → Writer (via Flight)
   - **Query path**: Router → Querier (via Flight)

### Future Enhanced Flow *(Planned)*

4. **Advanced Write Routing**
   - Router hashes (org_id, bucket, table) → shard_id
   - Looks up shard_id → [writer-A, writer-B] (replication factor)
   - Sends Arrow batch to multiple Writers for durability

5. **Failure Handling**
   - Heartbeat timeouts mark services as unavailable
   - Automatic failover to healthy replicas
   - Background shard reassignment for failed Writers

## Configuration Options

### Current Configuration

| Setting | Purpose | Default | Status |
|---------|---------|---------|--------|
| `discovery.catalog.dsn` | Catalog database connection | SQLite in-memory | ✅ Implemented |
| `discovery.nats.url` | NATS server connection | None (optional) | ✅ Implemented |
| `discovery.heartbeat_interval` | Service heartbeat frequency | 30s | ✅ Implemented |

### Future Configuration *(Planned)*

| Setting | Purpose | Effect |
|---------|---------|--------|
| `router.cache_ttl` | Catalog cache refresh interval | 0 = query each request |
| `replication_factor` | Writers per shard | Increases durability |
| `shard_count` | Total number of shards | Affects distribution granularity |

## Why this design scales

### Current Benefits
- **Zero client impact**: Services, not clients, handle discovery
- **Flexible backends**: Support for PostgreSQL, SQLite, and NATS
- **Simple operation**: Minimal configuration required
- **Service isolation**: Each service type can scale independently

### Future Benefits *(With planned enhancements)*
- **Fast switchover**: NATS provides sub-second detection
- **No thundering herd**: Only services query catalog; writes are distributed
- **Pod-level elasticity**: Adding a Writer means start → register → get shards
- **Automatic rebalancing**: Failed services trigger shard reassignment

## Service Communication

All inter-service communication uses **Apache Arrow Flight**:
- **Acceptor → Writer**: OTLP data converted to Arrow
- **Router → Querier**: Query requests and responses
- **Future: Router → Multiple Writers**: Replicated write requests

The discovery system ensures each service knows the Flight endpoints of its dependencies without hardcoded addresses or complex service mesh configuration.