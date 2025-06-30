# SignalDB Flight Communication Design

## 1. Introduction

This document outlines the design for Apache Arrow Flight as the primary communication mechanism in SignalDB, both for inter-service communication and external client access. The design leverages the performance benefits of Arrow Flight while maintaining compatibility with the current architecture.

**Current Implementation Status**: Basic Flight communication is implemented and working. WAL integration and advanced features are planned for future implementation.

## 2. Background

### 2.1 Current Architecture

SignalDB currently uses:
- **Apache Arrow Flight** as the primary inter-service communication mechanism ✅ **Implemented**
- OTLP data received via gRPC and HTTP at the Acceptor
- Direct Flight communication between Acceptor and Writer
- Direct Flight communication between Router and Querier
- Object storage integration for Parquet persistence

**Current architecture:**

```
External Clients
      │
      ▼ (OTLP/gRPC)
┌─────────────┐  Flight  ┌─────────────┐    ┌─────────────┐
│   Acceptor  │─────────▶│    Writer   │───▶│   Storage   │
└─────────────┘          └─────────────┘    └─────────────┘
                                                  │
                                                  ▼
┌─────────────┐  Flight  ┌─────────────┐    ┌─────────────┐
│   Clients   │◀─────────│    Router   │───▶│   Querier   │
└─────────────┘          └─────────────┘    └─────────────┘
```

**What's Working:**
1. OTLP clients send telemetry data to the Acceptor
2. Acceptor converts OTLP to Arrow format and forwards via Flight
3. Writer receives Arrow data and persists to Parquet storage
4. Router exposes HTTP endpoints and forwards queries via Flight
5. Querier executes queries against storage and returns results via Flight

### 2.2 Apache Arrow Flight

Apache Arrow Flight is a high-performance client-server framework designed for efficient transfer of large datasets over network interfaces.

Key benefits include:
- Native Arrow format transfer (no serialization/deserialization overhead)
- High throughput, low latency data transfer
- Streaming capabilities
- Built on gRPC with authentication and encryption support

## 3. Design Goals

1. ✅ **Achieved**: Improve performance of data transfer between components
2. ✅ **Achieved**: Provide a high-performance query interface for external clients
3. ✅ **Achieved**: Maintain logical separation of components while supporting monolithic deployment
4. ✅ **Achieved**: Eliminate the need for a separate message bus
5. ✅ **Achieved**: Support both in-process and networked communication with the same code

## 4. Flight Integration Design

### 4.1 Current Implementation

The current architecture uses Flight as the primary data transfer mechanism:

```
External Clients
      │
      ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Acceptor  │───▶│    Writer   │───▶│   Storage   │
└─────────────┘    └─────────────┘    └─────────────┘
                                            │
                                            ▼
                                      ┌─────────────┐
│   Clients   │◀─────────────────────│   Router    │───▶│   Querier   │
└─────────────┘                      └─────────────┘    └─────────────┘
```

All data-intensive communication between components uses Flight.

### 4.2 Component Flight Services ✅ **Implemented**

Each component implements a Flight service:

- **AcceptorFlightService**: Not exposed externally; forwards data to Writer via Flight client
- **WriterFlightService**: Receives data from Acceptor and writes to storage
- **QuerierFlightService**: Executes queries against storage and returns results
- **RouterFlightService**: Exposes HTTP API and forwards requests to Querier via Flight

### 4.3 External Flight Interface

The Router exposes Flight capabilities via HTTP endpoints, providing:
- Query execution via Tempo-compatible API
- Trace retrieval and search functionality
- Administrative operations

## 5. Implementation Details

### 5.1 Current Data Flow ✅ **Working**

#### Trace Ingestion Flow:
1. Acceptor receives OTLP trace data via gRPC
2. Acceptor converts OTLP to Arrow format using `otlp_traces_to_arrow`
3. Acceptor uses Flight `DoPut` to send Arrow data to Writer
4. Writer persists data to Parquet storage via object_store

#### Query Flow:
1. Client sends HTTP query to Router
2. Router forwards query to Querier via Flight
3. Querier executes query using DataFusion against Parquet files
4. Results streamed back to client via Flight → HTTP

### 5.2 Schema Design ✅ **Implemented**

Flight schemas are defined in `src/common/flight/schema.rs` with conversions for:
- OTLP traces → Arrow schema
- OTLP metrics → Arrow schema
- OTLP logs → Arrow schema

### 5.3 Service Discovery Integration ✅ **Implemented**

Components discover each other via:
- Catalog-based service registry
- NATS-based real-time discovery
- Flight endpoint registration and lookup

## 6. Future Enhancements *(Planned)*

### 6.1 Write-Ahead Log (WAL) Integration

To enhance durability and buffering capabilities:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Acceptor  │───▶│     WAL     │───▶│    Writer   │
└─────────────┘    └─────────────┘    └─────────────┘
```

#### Planned WAL Features:
1. **Durability**: Write incoming data to WAL before acknowledgment
2. **Recovery**: Replay unprocessed entries on restart
3. **Batching**: Support efficient batch processing
4. **Retention**: Configurable retention policies
5. **Distributed Operation**: Support replication in distributed deployments

### 6.2 Enhanced Buffering

For handling backpressure and improving performance:
- In-memory buffering in Writer before Parquet persistence
- Configurable flush policies (size, time, or count-based)
- Real-time query support for buffered data

### 6.3 Multi-Writer Replication

For high availability:
- Hash-based data distribution across multiple Writers
- Replication factor configuration
- Automatic failover handling

## 7. Monolithic Binary Implementation ✅ **Current**

The current monolithic binary (`cargo run`) starts all services in a single process:
- Services communicate via Flight using localhost endpoints
- Automatic service discovery via catalog
- Single configuration file for all components

## 8. Client SDK Integration

Flight communication enables:
- High-performance data transfer
- Streaming query results
- Native Arrow format support
- gRPC-based transport with authentication

## 9. Performance Benefits ✅ **Achieved**

Current implementation provides:
- **Zero-copy data transfer**: Arrow format maintained throughout pipeline
- **Streaming capabilities**: Large query results can be streamed
- **Protocol efficiency**: gRPC transport with minimal overhead
- **Schema evolution**: Arrow schema support for versioning

## 10. Deployment Modes

### 10.1 Monolithic Mode ✅ **Current**
- All services in single process
- Flight communication via localhost
- Simplified deployment and configuration

### 10.2 Microservices Mode ✅ **Supported**
- Services deployed independently
- Flight communication via network
- Service discovery via catalog/NATS
- Individual scaling and failure isolation

## 11. Conclusion

Arrow Flight implementation in SignalDB has successfully eliminated the need for a separate message bus while providing high-performance data transfer. The current implementation supports both monolithic and distributed deployment patterns with excellent performance characteristics.

The Flight-based architecture provides a solid foundation for future enhancements including WAL integration, advanced buffering, and multi-writer replication capabilities.