---
audience: contributor
type: explanation
status: living
sources:
  - src/common/src/flight/**
  - src/router/src/endpoints/flight.rs
  - src/querier/src/flight.rs
  - src/writer/src/flight_iceberg.rs
  - src/compactor/src/flight.rs
---

# SignalDB Flight Communication Design

## 1. Introduction

This document outlines the design for Apache Arrow Flight as the primary communication mechanism in SignalDB, both for inter-service communication and external client access. The design leverages the performance benefits of Arrow Flight while maintaining compatibility with the current architecture.

**Current Implementation Status**: ✅ **Complete** - Full Flight communication with WAL integration is implemented and production-ready. All integration tests passing.

## 2. Background

### 2.1 Current Architecture

SignalDB currently uses:

- **Apache Arrow Flight** as the primary inter-service communication mechanism ✅ **Implemented**
- OTLP data received via gRPC and HTTP at the Acceptor
- Direct Flight communication between Acceptor and Writer
- Direct Flight communication between Router and Querier
- Object storage integration for Parquet persistence

**Current architecture with WAL integration:**

```
External Clients
      │
      ▼ (OTLP/gRPC)
┌─────────────┐    ┌──────┐  Flight  ┌─────────────┐    ┌─────────────┐
│   Acceptor  │───▶│ WAL  │────────▶│    Writer   │───▶│   Storage   │
└─────────────┘    └──────┘          └─────────────┘    └─────────────┘
     (OTLP)        (Disk)     (Flight)       (Parquet)
                                                  │
                                                  ▼
┌─────────────┐    HTTP   ┌─────────────┐  Flight  ┌─────────────┐
│   Clients   │◀─────────│    Router   │───────▶│   Querier   │
└─────────────┘   (Tempo)  └─────────────┘ (DataFusion) └─────────────┘
```

**What's Working (✅ Complete):**

1. OTLP clients send telemetry data to the Acceptor
2. Acceptor writes data to WAL for durability, then converts OTLP to Arrow format
3. Acceptor forwards data to Writer via Flight (with Storage capability routing)
4. Writer receives Arrow data and persists to Parquet storage
5. Writer marks WAL entries as processed after successful storage
6. Router exposes HTTP endpoints (Tempo API) and forwards queries via Flight
7. Querier executes DataFusion queries against Parquet storage
8. All services discover each other via catalog-based service registry

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
6. ✅ **Achieved**: Implement WAL-based durability with automatic recovery
7. ✅ **Achieved**: Provide capability-based service discovery and routing

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

- **Acceptor**: no Flight server; acts as a Flight client forwarding data to the Writer
- **IcebergWriterFlightService**: Receives data from Acceptor and writes to Iceberg tables
- **QuerierFlightService**: Executes queries against storage and returns results
- **SignalDBFlightService** (Router): Exposes HTTP API and forwards requests to Querier via Flight
- **CompactorFlightService**: Admin-only `DoAction` interface for compaction management

### 4.3 External Flight Interface

The Router exposes Flight capabilities via HTTP endpoints, providing:

- Query execution via Tempo-compatible API
- Trace retrieval and search functionality
- Administrative operations

### 4.4 Supported Flight RPC Methods

The following table shows the Flight RPC methods supported by each service:

| Method          | Router       | Querier      | Writer       | Compactor | Description                      |
| --------------- | ------------ | ------------ | ------------ | --------- | -------------------------------- |
| `Handshake`     | ✅           | ✅           | ✅           | ❌        | Protocol version exchange        |
| `ListFlights`   | ✅           | ✅           | Empty stream | ❌        | List available query types       |
| `GetFlightInfo` | ✅           | ❌           | ❌           | ❌        | Get metadata for a query         |
| `GetSchema`     | ✅           | ✅           | ❌           | ❌        | Get schema for a query type      |
| `DoGet`         | ✅           | ✅           | ❌           | ❌        | Execute query and stream results |
| `DoPut`         | ❌           | ❌           | ✅           | ❌        | Write data to storage            |
| `DoExchange`    | ❌           | ❌           | ❌           | ❌        | Not implemented                  |
| `DoAction`      | ❌           | ❌           | ❌           | ✅        | Admin commands (compactor only)  |
| `ListActions`   | Empty stream | Empty stream | Empty stream | ✅        | List admin commands              |

Legend: ✅ implemented, ❌ returns `unimplemented`, "Empty stream" succeeds but yields nothing (a no-op, not an error).

**Note**: The Router is the primary client-facing Flight interface. Clients typically connect to the Router for all query operations. The Compactor's Flight service (`src/compactor/src/flight.rs`) is an admin interface only: `do_action` supports `compact_now`, `compact_status`, and `compact_dry_run`; every other RPC returns `unimplemented`.

#### Ticket and Command Grammar

There are two layers with different grammars -- the Router's descriptor commands and the Querier's `do_get` tickets:

**Router** (`src/router/src/endpoints/flight.rs`) -- `get_flight_info`, `get_schema`, and `list_flights` recognize these `FlightDescriptor` `cmd` values:

| Command               | Description                      | Notes                                                        |
| --------------------- | -------------------------------- | ------------------------------------------------------------ |
| `traces`              | Trace/span schema and metadata   | `do_get` currently returns an **empty stream** (placeholder) |
| `trace_by_id?id={id}` | Single-trace schema and metadata | `do_get` currently returns an **empty stream** (placeholder) |
| `logs`                | Log schema and metadata          | `do_get` currently returns an **empty stream** (placeholder) |
| `metrics`             | Metric schema and metadata       | `do_get` currently returns an **empty stream** (placeholder) |

Any other `do_get` ticket (including `find_trace:...`, `search_traces:...`, and raw SQL) is proxied verbatim to a Querier discovered via the `QueryExecution` capability, with request metadata forwarded.

**Querier** (`parse_ticket` in `src/querier/src/flight.rs`) -- `do_get` tickets use this grammar:

| Ticket                                                                       | Description                                                                                                                                                                                                                                                                                   |
| ---------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `find_trace:{tenant_slug}:{dataset_slug}:{trace_id}[:{start}:{end}]`         | Single trace lookup; the optional trailing segments are unix-second time hints (either may be empty) that prune the scanned range. Routers only append them when a hint is present, so the 3-part form remains valid. A missing trace yields a Flight `not_found` status, not an empty stream |
| `search_traces:{tenant_slug}:{dataset_slug}:{params_json}`                   | Trace search (`SearchQueryParams` as JSON; unknown fields are ignored on deserialization)                                                                                                                                                                                                     |
| `query_logs:{tenant_slug}:{dataset_slug}:{params_json}`                      | LogQL log query (`LogQueryParams` as JSON: LogQL string, nanosecond start/end, limit, direction). Returns the projected log columns ordered by timestamp                                                                                                                                      |
| `query_logs_labels:{tenant_slug}:{dataset_slug}:{start}:{end}`               | Log label names in the nanosecond window                                                                                                                                                                                                                                                      |
| `query_logs_label_values:{tenant_slug}:{dataset_slug}:{label}:{start}:{end}` | Distinct values of one log label in the window                                                                                                                                                                                                                                                |
| `query_logs_series:{tenant_slug}:{dataset_slug}:{params_json}`               | Series (label sets) matching a stream selector (`LogSeriesParams` as JSON)                                                                                                                                                                                                                    |
| `query_logs_detected_fields:{tenant_slug}:{dataset_slug}:{params_json}`      | Attribute-field discovery: sampled keys with inferred type and approximate cardinality (`DetectedFieldsParams` as JSON)                                                                                                                                                                       |
| `query_metric:{tenant_slug}:{dataset_slug}:{params_json}`                    | LogQL metric query (`MetricQueryParams` as JSON: LogQL string, nanosecond start/end, step). Returns a matrix bucketed by `date_bin(step)`                                                                                                                                                     |
| `query_promql:{tenant_slug}:{dataset_slug}:{params_json}`                    | PromQL query (`PromQlQueryParams` as JSON: PromQL string, nanosecond start/end, step). Returns a matrix over the metrics tables                                                                                                                                                               |
| `query_ir:{tenant_slug}:{dataset_slug}:{params_json}`                        | Native Query IR (`IrQueryParams` as JSON: a versioned IR `document` plus the server-stamped `now_ns` for deterministic relative-time resolution). The querier validates and lowers the single-signal IR to a DataFusion plan; returns the declared `rows`/`series`/`table` envelope           |
| anything else                                                                | Treated as a raw SQL query executed via DataFusion                                                                                                                                                                                                                                            |

The standalone querier binary additionally serves Tempo's `tempopb.Querier`
gRPC protocol on the same port as Flight (see the
[Tempo API reference](../users/tempo-api-reference.md#tempo-grpc-querier-protocol));
that protocol does not use tickets.

There is no `trace_by_id?id=...` ticket form at the Querier; that command exists only in the Router's metadata path. The Tempo HTTP endpoints bypass the Router's Flight commands entirely and send `find_trace:`/`search_traces:`/SQL tickets straight to the Querier.

#### Self-Monitoring Anti-Loop Guard

When self-monitoring is enabled, both Flight handlers apply the anti-loop
guard from `src/common/src/self_monitoring/suppress.rs`: requests that touch
the reserved `_system` tenant are processed with OpenTelemetry export
suppressed, so handling SignalDB's own telemetry does not generate more of
it. The Writer's `do_put` suppresses per batch based on the tenant in the
Flight metadata (its background WAL loop does the same per WAL entry), and
the Querier's `do_get` resolves the tenant — authenticated caller, the
`op:{tenant_slug}:...` ticket segment, or the `x-tenant-id` header for raw
SQL — before creating its processing span. The suppression marker is a tokio
task-local: it crosses neither the Flight hop between services nor
`tokio::spawn`, which is why each handler carries its own call site.

#### Boundary Spans (RPC Semantic Conventions)

Every Flight handler roots its request in a semconv RPC SERVER span built by
the factories in `src/common/src/self_monitoring/spans.rs` — the single
sanctioned construction path for boundary spans. Flight has no semantic
convention of its own, so it is modeled as plain gRPC: spans are named by the
fully-qualified logical method, disambiguated by a low-cardinality detail
segment where one exists —
`arrow.flight.protocol.FlightService/DoGet query_ir` (Querier, ticket verb),
`…/DoPut` (Writer), `…/DoAction compact_dry_run` (Compactor, action type) —
and carry `rpc.system.name = grpc`, `rpc.method`, and the string
`rpc.response.status_code`. Status mapping follows the RPC semconv asymmetry:
a server span is marked failed only for server-fault gRPC codes (`UNKNOWN`,
`DEADLINE_EXCEEDED`, `UNIMPLEMENTED`, `INTERNAL`, `UNAVAILABLE`, `DATA_LOSS`);
codes like `NOT_FOUND` are the caller's problem and leave the span status
unset. Raw-SQL tickets have no `op:` prefix, so their first `:`-segment is
query text — the Querier only appends the verb when it matches a short
lowercase identifier, keeping SQL out of span names.

#### Trace Context Propagation

W3C trace context (`traceparent`/`tracestate`) is propagated across SignalDB
service boundaries by `src/common/src/flight/trace_context.rs`, so a single
distributed trace can span the acceptor, writer, router, and querier. Every
function routes through the global OpenTelemetry text-map propagator, which is
a **no-op unless self-monitoring is enabled**. A parent must be adopted before
its span is first entered; span links, by contrast, may be added at any time.

Four carriers move the context, matching how each path already exchanges
metadata:

| Carrier                                               | Path                                    | Direction             |
| ----------------------------------------------------- | --------------------------------------- | --------------------- |
| JSON `app_metadata` on the first `FlightData` message | Acceptor → Writer `do_put`              | inject / extract      |
| gRPC request metadata headers                         | Router → Querier `do_get`               | inject / extract      |
| HTTP request headers                                  | external caller → Router query APIs     | extract (server side) |
| Span links                                            | WAL batch fan-in (background processor) | link                  |

**Write path.** At `do_put` the Writer records the active span's context into
the WAL entry metadata alongside the routing fields. Because the background
`WalProcessor` commits a batch that fans in entries from many independent
ingest requests, its span cannot adopt a single parent — it reads each entry's
stored context and adds one **span link** per distinct ingest trace, keeping
every source trace reachable from the batch span instead of leaving it a
detached root.

**Read path.** An `http_trace_context_middleware` at the Router's HTTP boundary
roots each request in a server span whose parent is the caller-supplied
`traceparent`, so an external client that propagates trace context sees
SignalDB's query trace join theirs. Downstream `#[instrument]` handler spans
become children of that span, and each Router → Querier Flight call runs
inside a semconv RPC CLIENT span (`do_get_client_span`) whose context is
injected into the request metadata — so the querier's SERVER span is the
client span's child and the trace reads SERVER → CLIENT → SERVER. The
middleware mirrors the anti-loop guard above: `_system` tenant requests bypass
the span so self-monitoring queries are not re-instrumented and re-ingested.

#### Error Recording on Query Spans

A failing query is only useful in a trace if the reason survives. By the time a
`do_get` error reaches the caller it has been flattened into a transport
`Status` that the Router strips down to a bare HTTP code, so the querier records
the cause where it still exists: the whole `do_get` body runs inside a single
error boundary that, on any `Err`, calls
`common::self_monitoring::record_span_exception` to attach an OpenTelemetry
`exception` event (`exception.message`) and an error status to the
`…FlightService/DoGet` server span. Because the boundary wraps the entire request, every
failure path — ticket parsing, cross-tenant rejection, query execution, and
result conversion — is captured, not just execution errors. The helper is a
no-op when self-monitoring is disabled (`Span::current()` is the disabled span),
so this costs nothing on the hot path.

## 5. Implementation Details

### 5.1 Current Data Flow ✅ **Working**

#### Trace Ingestion Flow:

```mermaid
sequenceDiagram
    participant C as OTLP client
    participant A as Acceptor
    participant W as Writer (Storage capability)
    participant O as Object store

    C->>A: OTLP traces (gRPC)
    A->>A: convert to Arrow (otlp_traces_to_arrow)
    A->>A: append to Acceptor WAL + flush
    A->>W: Flight DoPut (Arrow batches)
    W->>W: transform v1 to v2, append to Writer WAL
    W-->>A: confirm
    A->>A: mark WAL entry processed
    A-->>C: acknowledge
    Note over W,O: asynchronous, WalProcessor 5s loop
    W->>O: Iceberg commit (Parquet files)
```

1. Acceptor receives OTLP trace data via gRPC
2. Acceptor converts OTLP to Arrow format using `otlp_traces_to_arrow`
3. Acceptor appends the batch to its WAL and flushes for durability
4. Acceptor uses Flight `DoPut` to send Arrow data to Writer (Storage capability)
5. Writer transforms to the v2 storage schema and appends to its own WAL
6. Writer confirms after its WAL flush (it does **not** block the confirm on the Iceberg commit); Acceptor marks its WAL entry as processed
7. Writer's `WalProcessor` asynchronously commits WAL entries to Iceberg (Parquet in the object store), **coalescing** pending entries per `(tenant, dataset, table)` — a group commits when `[writer].commit_interval` elapses or its rows reach `[writer].max_uncommitted_rows`. This caps the Iceberg snapshot / catalog-metadata write rate independent of ingest rate.

Because the commit is asynchronous, ingested data is queryable only once committed (bounded by `commit_interval`). A caller needing read-your-writes forces an immediate commit with the Writer Flight `do_action("flush")` (advertised via `list_actions`). The action is **tenant-scoped**: the scope is taken from the request's `x-tenant-id` (required) and `x-dataset-id` (optional) gRPC metadata — the same tenant identity the ingest path carries — and it force-commits only that tenant's (optionally that dataset's) pending groups. A request without `x-tenant-id` is rejected, so a caller can neither flush every tenant nor a tenant it names only in the payload. Tests use `common::testing::flush_storage_writers(transport, tenant, dataset)` for a deterministic barrier.

#### Query Flow:

```mermaid
sequenceDiagram
    participant C as HTTP client
    participant R as Router
    participant Q as Querier
    participant O as Object store

    C->>R: Tempo API query (HTTP)
    R->>Q: Flight do_get ticket
    Q->>O: DataFusion scan (Parquet)
    Q-->>R: Arrow RecordBatch stream
    R-->>C: JSON response
```

1. Client sends HTTP query to Router
2. Router forwards query to Querier via Flight
3. Querier executes query using DataFusion against Parquet files
4. Results streamed back to client via Flight → HTTP

### 5.2 Schema Design ✅ **Implemented**

Flight schemas are defined in `src/common/src/flight/schema.rs` with conversions for:

- OTLP traces → Arrow schema
- OTLP metrics → Arrow schema
- OTLP logs → Arrow schema

### 5.3 Service Discovery Integration ✅ **Implemented**

Components discover each other via:

- **Catalog-based service registry** with PostgreSQL/SQLite backend
- **ServiceBootstrap pattern** for automatic registration on startup
- **Capability-based routing** (TraceIngestion, Storage, QueryExecution, Routing)
- **Heartbeat monitoring** with automatic TTL-based cleanup
- **Flight endpoint discovery** with connection pooling

## 6. WAL Integration ✅ **Implemented**

Write-Ahead Log provides durability and crash recovery capabilities:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Acceptor  │───▶│     WAL     │───▶│    Writer   │
└─────────────┘    └─────────────┘    └─────────────┘
```

#### Implemented WAL Features:

1. ✅ **Durability**: Write incoming data to WAL before acknowledgment
2. ✅ **Recovery**: Automatic replay of unprocessed entries on restart
3. ✅ **Batching**: Efficient batch processing with configurable flush policies
4. ✅ **Entry Tracking**: WAL entries marked as processed after successful storage
5. ✅ **Configurable Storage**: Persistent WAL directories with segment rotation

#### Future WAL Enhancements:

1. **Compression**: WAL segment compression for storage efficiency
2. **Replication**: WAL replication for high availability
3. **Retention Policies**: Automatic cleanup of old WAL segments

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

The current monolithic binary (`cargo run --bin signaldb`) starts all services in a single process:

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
- Service discovery via the shared catalog database
- Individual scaling and failure isolation

## 11. Conclusion

✅ **Phase 2 Complete**: SignalDB's Arrow Flight implementation with WAL integration is production-ready, providing:

**Achieved Goals:**

- High-performance Flight-based inter-service communication
- WAL-based durability with crash recovery
- Catalog-based service discovery with capability routing
- Complete elimination of message bus dependencies
- Support for both monolithic and distributed deployments
- Integration test coverage in `tests-integration/`

**Performance Benefits:**

- Zero-copy data transfer via Arrow Flight
- Efficient service discovery with connection pooling
- Durability guarantees through WAL persistence
- Streaming query capabilities with DataFusion

**Production Readiness:**

- Robust error handling and retry logic
- Automatic service registration and health monitoring
- Configurable WAL and storage options
- Comprehensive logging and debugging capabilities

The Flight-based architecture with WAL integration provides a solid, production-ready foundation for observability data processing at scale.

> The writer's `do_put` v1→storage transformation resolves materialized-label allowlists per tenant (a tenant schema override replaces the global set).
