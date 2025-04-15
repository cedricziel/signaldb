# SignalDB Flight Communication Design

## 1. Introduction

This document outlines the design for implementing Apache Arrow Flight as the primary communication mechanism in SignalDB, both for inter-service communication and external client access. The design aims to leverage the performance benefits of Arrow Flight while maintaining compatibility with the existing architecture.

## 2. Background

### 2.1 Current Architecture

SignalDB currently uses:
- A messaging system with both in-memory and NATS backends for inter-service communication
- The acceptor receives OTLP data via gRPC and HTTP
- The querier provides a Tempo-compatible API
- The writer persists data in Parquet format using object storage
- Components communicate through a pub/sub pattern with topics like "batch"

Current architecture diagram:

```
                                 ┌─────────────────┐
                                 │  Message Bus    │
                                 │ (Memory/NATS)   │
                                 └─────────────────┘
                                    ▲           ▲
                                    │           │
                                    │           │
                                    ▼           ▼
┌──────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ OTLP Clients │───▶│   Acceptor  │───▶│    Writer   │───▶│   Storage   │
└──────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                                                                │
                                                                │
                                                                ▼
┌──────────────┐    ┌─────────────┐    ┌─────────────┐
│ HTTP Clients │───▶│    Router   │◀───│   Querier   │
└──────────────┘    └─────────────┘    └─────────────┘
```

In this architecture:
1. OTLP clients send telemetry data to the Acceptor
2. The Acceptor processes and forwards data via the message bus
3. The Writer consumes messages and persists data to storage in Parquet format
4. The Querier reads from storage to serve queries
5. The Router exposes HTTP/gRPC endpoints for clients to query data
6. Components communicate asynchronously through the message bus

### 2.2 Apache Arrow Flight

Apache Arrow Flight is a high-performance client-server framework designed for efficient transfer of large datasets over network interfaces. It's part of the Apache Arrow ecosystem and is specifically optimized for moving Arrow data.

Key benefits include:
- Native Arrow format transfer (no serialization/deserialization overhead)
- High throughput, low latency data transfer
- Streaming capabilities
- Built on gRPC with authentication and encryption support

## 3. Design Goals

1. Improve performance of data transfer between components
2. Provide a high-performance query interface for external clients
3. Maintain logical separation of components while supporting monolithic deployment
4. Reduce or eliminate the need for a separate message bus
5. Support both in-process and networked communication with the same code

## 4. Flight Integration Design

### 4.1 Architecture Overview

The proposed architecture introduces Flight as the primary data transfer mechanism:

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
                                      │   Querier   │
                                      └─────────────┘
                                            │
                                            ▼
┌─────────────┐                      ┌─────────────┐
│   Clients   │◀─────────────────────│   Router    │
└─────────────┘                      └─────────────┘
```

All data-intensive communication between components will use Flight, while a minimal message bus may be retained for service coordination.

### 4.2 In-Process Flight Transport

For monolithic deployment, we'll implement an in-memory Flight transport:

```rust
pub struct InMemoryFlightTransport {
    services: Arc<RwLock<HashMap<String, Box<dyn FlightService>>>>,
}
```

This transport will allow components to communicate via Flight without network overhead when running in the same process.

### 4.3 Component Flight Services

Each component will implement a Flight service:

- **AcceptorFlightService**: Receives data from external sources and forwards to the writer
- **WriterFlightService**: Receives data from the acceptor and writes to storage
- **QuerierFlightService**: Executes queries against storage and returns results
- **RouterFlightService**: Exposes a unified Flight API to external clients

### 4.4 External Flight Interface

The external Flight interface will expose:

- **ListFlights**: Discover available datasets (traces, metrics, logs)
- **GetFlightInfo**: Get metadata about specific datasets
- **DoGet**: Query and retrieve trace data
- **DoAction**: Support administrative operations

## 5. Implementation Details

### 5.1 In-Memory Flight Transport

```rust
pub struct InMemoryFlightClient {
    service_name: String,
    services: Arc<RwLock<HashMap<String, Box<dyn FlightService>>>>,
}

impl InMemoryFlightClient {
    async fn do_get(&self, ticket: Ticket) -> Result<RecordBatchStream> {
        let services = self.services.read().unwrap();
        let service = services.get(&self.service_name).unwrap();
        service.do_get(Request::new(ticket)).await
    }

    // Implement other Flight methods...
}
```

### 5.2 Dual-Mode Operation

Components will be able to operate in both in-process and networked modes:

```rust
enum FlightClientMode {
    InProcess(InMemoryFlightClient),
    Network(NetworkFlightClient),
}

struct FlightClient {
    mode: FlightClientMode,
}
```

### 5.3 Data Flow Examples

#### Trace Ingestion Flow:

1. Acceptor receives OTLP trace data
2. Acceptor converts to Arrow format
3. Acceptor uses Flight to send to Writer (`DoPut`)
4. Writer persists to Parquet storage

#### Query Flow:

1. Client sends query via Flight (`DoGet`)
2. Router forwards to Querier
3. Querier executes query using DataFusion
4. Results streamed back to client via Flight

### 5.4 Schema Design

Flight requires well-defined schemas. For traces:

```rust
let schema = Schema::new(vec![
    Field::new("trace_id", DataType::Utf8, false),
    Field::new("span_id", DataType::Utf8, false),
    Field::new("parent_span_id", DataType::Utf8, true),
    Field::new("name", DataType::Utf8, false),
    Field::new("start_time", DataType::Int64, false),
    Field::new("end_time", DataType::Int64, false),
    Field::new("duration_ns", DataType::Int64, false),
    Field::new("service_name", DataType::Utf8, false),
    // Additional fields for attributes, events, etc.
]);
```

## 6. Data Durability and Buffering

### 6.1 Write-Ahead Log (WAL) in Acceptor

A critical aspect of the current messaging system is its role as a distributed buffer. To maintain this functionality while moving to Flight, we'll implement a Write-Ahead Log (WAL) in the acceptor:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Acceptor  │───▶│     WAL     │───▶│    Writer   │
└─────────────┘    └─────────────┘    └─────────────┘
```

#### WAL Design:

1. **Durability**: All incoming data will be written to the WAL before acknowledgment
2. **Recovery**: On restart, the acceptor can replay unprocessed entries
3. **Batching**: The WAL will support batching for efficient processing
4. **Retention**: Configurable retention policy based on time or space
5. **Distributed Operation**: Support for replication in distributed deployments

#### Implementation:

```rust
pub struct WriteAheadLog {
    storage: Arc<dyn ObjectStore>,
    current_segment: RwLock<WALSegment>,
    config: WALConfig,
}

impl WriteAheadLog {
    // Write data to WAL before sending via Flight
    async fn append(&self, batch: &RecordBatch) -> Result<WALPosition> {
        // Write to current segment
        let position = self.current_segment.write().unwrap().append(batch).await?;

        // Roll segment if needed
        if self.current_segment.read().unwrap().size() > self.config.max_segment_size {
            self.roll_segment().await?;
        }

        Ok(position)
    }

    // Read from WAL during recovery
    async fn read_from(&self, position: WALPosition) -> Result<impl Stream<Item = RecordBatch>> {
        // Implementation to read from a specific position
    }

    // Trim WAL based on acknowledged positions
    async fn trim_to(&self, position: WALPosition) -> Result<()> {
        // Remove segments that are fully acknowledged
    }
}
```

#### Integration with Flight:

The acceptor will:
1. Write incoming data to the WAL
2. Acknowledge the client once data is durably stored
3. Asynchronously send data to the writer via Flight
4. Trim the WAL once the writer confirms processing

This approach provides:
- Durability guarantees similar to the current message bus
- Buffer for handling backpressure
- Recovery mechanism for component failures
- Efficient batch processing

### 6.2 Message Bus Considerations

#### 6.2.1 Hybrid Approach

While Flight with WAL will handle the primary data flow, a minimal message bus may be retained for:

- Service discovery and coordination
- Broadcasting system events
- Handling service outages

#### 6.2.2 Complete Replacement

For a complete replacement of the message bus:

1. Implement retry logic in Flight clients
2. Enhance the WAL with distributed consensus (e.g., Raft)
3. Develop a service registry for component discovery

## 7. Monolithic Binary Implementation

For monolithic deployment:

```rust
fn main() {
    // Create the in-memory Flight transport
    let flight_transport = Arc::new(InMemoryFlightTransport::new());

    // Initialize components with the transport
    let acceptor = AcceptorComponent::new(flight_transport.clone());
    let writer = WriterComponent::new(flight_transport.clone());
    let querier = QuerierComponent::new(flight_transport.clone());

    // Register Flight services
    flight_transport.register_service("acceptor", acceptor.flight_service());
    flight_transport.register_service("writer", writer.flight_service());
    flight_transport.register_service("querier", querier.flight_service());

    // Start the external Flight server for client connections
    let external_flight_server = ExternalFlightServer::new(
        flight_transport.clone(),
        "0.0.0.0:8082",
    );

    // Run everything
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        tokio::join!(
            acceptor.run(),
            writer.run(),
            querier.run(),
            external_flight_server.run(),
        );
    });
}
```

## 8. Implementation Strategy

### 8.1 Phase 1: External Flight API

1. Complete the `SignalDBFlightService` implementation
2. Connect it to the existing querier component
3. Provide basic query capabilities for external clients

### 8.2 Phase 2: In-Memory Transport

1. Implement the in-memory Flight transport
2. Update one component pair (e.g., querier and router) to use Flight
3. Benchmark and validate the approach

### 8.3 Phase 3: Full Integration

1. Implement Flight services for all components
2. Gradually migrate from message bus to Flight
3. Support both monolithic and distributed deployment

## 9. Client SDK Example

```rust
async fn query_traces(client: &FlightClient, trace_id: &str) -> Result<Vec<RecordBatch>> {
    // Create a query descriptor
    let query = format!("SELECT * FROM traces WHERE trace_id = '{}'", trace_id);
    let descriptor = FlightDescriptor::new_cmd(query.into_bytes());

    // Get flight info
    let flight_info = client.get_flight_info(descriptor).await?;

    // Get the data
    let endpoint = &flight_info.endpoint[0];
    let ticket = endpoint.ticket.as_ref().unwrap();

    // Stream results
    let mut stream = client.do_get(ticket.clone()).await?;
    let mut batches = Vec::new();

    while let Some(data) = stream.next().await {
        let batch = arrow_flight::utils::flight_data_to_arrow_batch(&data?, &schema)?;
        batches.push(batch);
    }

    Ok(batches)
}
```

## 10. Conclusion

Implementing Arrow Flight in SignalDB will provide significant performance improvements for both inter-service communication and external client access. The design supports both monolithic and distributed deployment while maintaining the logical separation of components.

The in-memory Flight transport allows for zero-copy data transfer in monolithic deployments, while the same code can run in a distributed environment with minimal changes.
