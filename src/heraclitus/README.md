# Heraclitus - Kafka-Compatible Agent for SignalDB

Heraclitus is a Kafka protocol-compatible agent that enables SignalDB to accept and process streaming data from Kafka producers and serve data to Kafka consumers. It implements a subset of the Kafka wire protocol, allowing existing Kafka clients to seamlessly integrate with SignalDB's distributed observability signal database.

## Features

### Implemented Kafka APIs

- **ApiVersions (0)**: Protocol version negotiation
- **Metadata (3)**: Topic and broker discovery  
- **FindCoordinator (10)**: Consumer group coordinator discovery
- **JoinGroup (11)**: Consumer group membership
- **Heartbeat (12)**: Consumer session management
- **LeaveGroup (13)**: Graceful consumer departure
- **SyncGroup (14)**: Group assignment synchronization
- **DescribeGroups (15)**: Consumer group monitoring
- **SaslHandshake (17)**: SASL authentication initiation
- **SaslAuthenticate (36)**: SASL/PLAIN authentication
- **CreateTopics (19)**: Explicit topic creation
- **DeleteTopics (20)**: Topic deletion
- **InitProducerId (22)**: Producer initialization for idempotency

### Core Capabilities

- **Consumer Group Management**: Full consumer group protocol support including rebalancing
- **SASL/PLAIN Authentication**: Secure client authentication with configurable credentials
- **Topic Management**: Automatic and explicit topic creation with configurable parameters
- **Producer Support**: Basic producer protocol with idempotency foundations
- **Observability Integration**: Prometheus metrics and health check endpoints
- **Graceful Shutdown**: Proper cleanup and connection draining

## Architecture

Heraclitus is designed as a **stateless, horizontally scalable agent** that requires no central infrastructure or coordination services. Each Heraclitus instance operates independently while sharing state through SignalDB's object storage.

### Key Architectural Principles

- **Stateless Design**: All state is persisted to object storage - agents hold no local state
- **No Central Infrastructure**: No ZooKeeper, no central coordinators, no single points of failure
- **Horizontal Scalability**: Run as many Heraclitus instances as needed behind a load balancer
- **Shared Nothing**: Each agent instance is completely independent
- **Eventually Consistent**: State changes propagate through object storage

### How It Works

```
                    Load Balancer
                         |
        +----------------+----------------+
        |                |                |
   Heraclitus-1    Heraclitus-2    Heraclitus-3
        |                |                |
        +----------------+----------------+
                         |
                  Object Storage
                 (Shared State)
```

Each Heraclitus instance:
1. Accepts Kafka client connections independently
2. Reads/writes all state to shared object storage
3. Has no awareness of other Heraclitus instances
4. Can be added or removed without affecting others

### Components

- **Protocol Handler**: Manages Kafka protocol parsing and response generation
- **State Manager**: Handles topic metadata, consumer groups, and producer state via object storage
- **Storage Integration**: Uses SignalDB's object storage for all persistence
- **Metrics**: Prometheus-compatible metrics for monitoring (per-instance)

## Configuration

Heraclitus uses the standard SignalDB configuration with additional Kafka-specific settings:

```toml
[heraclitus]
# Kafka protocol port (default: 9092)
kafka_port = 9092

# Metrics port (default: 9091) 
metrics_port = 9091

# SASL authentication
sasl_enabled = true
sasl_plain_username = "admin"
sasl_plain_password = "admin-secret"

# Topic defaults
default_partitions = 8
default_replication_factor = 1
```

## Usage

### Running Heraclitus

As part of SignalDB monolithic mode:
```bash
cargo run --bin signaldb
```

Or standalone:
```bash
cargo run --bin heraclitus
```

For production deployments, run multiple instances behind a load balancer:
```bash
# Instance 1
HERACLITUS_KAFKA_PORT=9092 cargo run --bin heraclitus

# Instance 2  
HERACLITUS_KAFKA_PORT=9093 cargo run --bin heraclitus

# Instance 3
HERACLITUS_KAFKA_PORT=9094 cargo run --bin heraclitus
```

All instances will coordinate through the shared object storage with no need for leader election or consensus protocols.

### Connecting Kafka Clients

Configure your Kafka client to connect to `localhost:9092` (or configured port):

```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

// If SASL is enabled
props.put("security.protocol", "SASL_PLAINTEXT");
props.put("sasl.mechanism", "PLAIN");
props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";");
```

### Monitoring

Metrics are exposed at `http://localhost:9091/metrics` in Prometheus format:

- `heraclitus_connections_total`: Total client connections
- `heraclitus_requests_total`: Total requests by API key
- `heraclitus_request_duration_seconds`: Request processing latency
- `heraclitus_active_connections`: Current active connections

Health check endpoint: `http://localhost:9091/health`

## Development

### Adding New Kafka APIs

1. Add the API key constant to `kafka_protocol.rs`
2. Create a new module in `protocol/` for request/response handling
3. Update `RequestType` enum in `request.rs`
4. Add handler method in `handler.rs`
5. Update `api_versions.rs` with supported versions
6. Write integration tests

### Testing

Run unit tests:
```bash
cargo test -p heraclitus
```

Run integration tests:
```bash
cargo test -p heraclitus-tests-integration
```

### Current Limitations

- Single broker ID per instance (multi-broker coming soon)
- No partition replication or ISR tracking
- Limited producer protocol (no transactions)
- No log compaction or retention policies
- Consumer offsets stored separately from data
- Eventually consistent state may cause brief inconsistencies during rapid operations

## Roadmap

### High Priority
- Multi-broker support with leader election
- Partition replication with ISR
- DeleteTopics API
- Configuration management (DescribeConfigs/AlterConfigs)
- Transactional producer support

### Medium Priority
- ListGroups API
- Group coordinator election
- DeleteGroups API  
- Incremental fetch optimization
- Session timeout detection
- Partition assignment strategies

### Future Enhancements
- KRaft consensus protocol
- Exactly-once semantics
- Streams API compatibility
- Connect API support

## Contributing

When contributing to Heraclitus:

1. Follow the existing code patterns
2. Add comprehensive tests for new features
3. Update this README for new APIs
4. Ensure all Clippy warnings are resolved
5. Run `cargo fmt` before committing

## License

Heraclitus is part of the SignalDB project and follows the same licensing terms.