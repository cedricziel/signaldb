Deep-dive: the Ingest layer

The ingest tier is the write-front of the FDAP architecture, turning a bursty stream of client writes into well-formed, query-ready Parquet files while guaranteeing durability and low-latency visibility. It is split into two logical services—**Acceptor** and **Writer**—so that each concern can scale or fail independently.

**Current Implementation Status**: The basic data flow is implemented with Apache Arrow Flight as the primary communication mechanism. WAL and replication features are planned for future implementation.

| Stage | Major responsibilities | Key data structure | Implementation Status |
|-------|----------------------|-------------------|---------------------|
| **Acceptor** (stateless) | • Accept OTLP/gRPC writes<br>• Convert OTLP to Arrow format<br>• Forward to Writer via Flight<br>• Return ack to client | Arrow RecordBatch | ✅ **Implemented**<br>Currently handles OTLP ingestion |
| **Writer** (stateful) | • Receive Arrow data via Flight<br>• Store data to Parquet files<br>• Manage object storage integration<br>• *(Future: WAL, in-memory buffering)* | • Arrow buffer<br>• Parquet file<br>• *(Future: WAL segment)* | ✅ **Basic implementation**<br>🔄 **Planned: WAL, replication** |

**Architecture Notes:**
- **Current**: Single Writer per data flow, no replication
- **Future**: Multiple Writers for durability, WAL for crash-safety

## 1 Write path step-by-step

### Current Implementation
1. **Client → Acceptor**
   - OTLP data arrives over gRPC (ports 4317/4318)
   - Acceptor converts OTLP traces/metrics/logs to Arrow format
   - ✅ **Implemented**

2. **Acceptor → Writer**
   - Arrow batch sent via Apache Arrow Flight
   - Writer receives data and stores to Parquet
   - ✅ **Implemented**

3. **Persist to Parquet**
   - Data written directly to object storage (filesystem, S3, etc.)
   - ✅ **Implemented**

### Future Enhanced Implementation
4. **WAL Integration** *(Planned)*
   - Writer performs fsync() to WAL before ack
   - Crash-safety via WAL replay on restart

5. **Buffering & Batching** *(Planned)*
   - In-memory chunks organized by (table, time, shard)
   - Automatic flushing based on size/time thresholds

6. **Replication** *(Planned)*
   - Hash writes to multiple Writers for durability
   - Require acknowledgment from ≥2 Writers

7. **Catalog Integration** *(Planned)*
   - Record Parquet file locations in catalog
   - Enable metadata-driven query optimization

## 2 Making fresh data queryable

### Current State
Data becomes queryable once written to Parquet storage. The Query layer reads directly from stored Parquet files.

### Future State *(Planned)*
Queriers will ask Writers for "recent, not-yet-persisted" chunks first, enabling dashboards to see new points within ~100ms of receipt—even before the Parquet write completes. In-memory Arrow batches will be streamed back over Flight, deduplicated on the coordinator, then merged with older Parquet partitions.

## 3 Scaling & Sizing knobs

| Resource | Primary driver | Guidance |
|----------|---------------|----------|
| CPU | OTLP parsing & Arrow conversion | Vertical scaling gives best $/point; aim for ~70% utilisation |
| RAM | *(Future: Mutable-chunk buffer)* | Keep 1–2 min of peak ingest in memory for real-time queries |
| SSD | *(Future: WAL segments)* | Provide 2–4× RAM; latency matters more than capacity |
| Replicas | Write QPS | Horizontal scale when single-node CPU saturated |

## 4 Failure & back-pressure behaviour

### Current Behavior
- **Writer crash** → Service restart required; potential data loss for in-flight requests
- **Object store outage** → Writer fails to persist; requests fail
- **Acceptor crash** → Stateless; simply restarts and resumes processing

### Future Enhanced Behavior *(Planned)*
- **Writer crash** → WAL replay on restart; no data loss
- **Object store outage** → Writer keeps buffering in RAM and WAL until configurable threshold; Acceptor applies 429 back-pressure beyond that
- **Acceptor crash** → Stateless; simply restarts and resumes load balancing

## 5 Why this split works

### Current Benefits
- **Low coupling**: Acceptors can be upgraded independently; Writers can be redeployed separately
- **Straight arrow to FDAP**: Data is Arrow on Flight wire, Arrow in memory, Parquet at rest—no impedance mismatches
- **Protocol standardization**: OTLP standard enables broad ecosystem compatibility

### Future Benefits *(With planned enhancements)*
- **Durability without latency**: WAL gives crash-safety while staying on local NVMe; client sees ack as soon as WAL fsyncs succeed
- **Real-time query capability**: Fresh data visible before Parquet persistence completes
- **Horizontal scalability**: Hash-based distribution across multiple Writers

This ingest layer therefore turns an unbounded, high-cardinality stream into durable, analytics-ready Parquet files while exposing hot data for sub-second queries—all with simple, horizontally or vertically scalable components.