Deep-dive: the Ingest layer

The ingest tier is the write-front of the FDAP architecture, turning a bursty stream of client writes into well-formed, query-ready Parquet files while guaranteeing durability and low-latency visibility.  It is split into two logical services—Router and Ingester—so that each concern can scale or fail independently.

Stage	Major responsibilities	Key data structure	Why it exists
Router (stateless)	• Accept HTTP/Line Protocol/OTLP/gRPC writes• Validate syntax & schema (consult Catalog)• Replicate every batch to ≥ 2 Ingesters for durability• Return ack to client	Arrow RecordBatch	Keeps ingest fan-in (thousands of clients) separate from stateful buffers; sheds load by simple hashing or token-bucket back-pressure.  ￼
Ingester (stateful)	• Append incoming batches to a Write-Ahead Log (WAL) on local SSD• Add rows to an in-memory Buffer Tree: table → partition-key → chunk• Serve “yet-to-be-persisted” data to Queriers over Flight• Flush closed chunks to Parquet + upload to Object Store• Trim WAL segment after successful persist	• WAL segment• Arrow buffer• Parquet file	Decouples durability from cloud-object latency; gives sub-second query visibility without forcing the query tier to scan the WAL.  ￼ ￼

1 Write path step-by-step
	1.	Client ➜ Router
The batch arrives over HTTPS or gRPC. The Router checks the Catalog to ensure the measurement & tag set matches the declared schema and immediately hashes the write to (say) two target Ingesters.
	2.	Router ➜ Ingester(s)
Each Ingester receives the Arrow batch via gRPC-Flight and performs an fsync(…) into its WAL before ack-ing.  The WAL makes the ingest node crash-safe; on restart it simply replays segments into memory.
	3.	Buffering window
Rows are organised into mutable chunks keyed by (table, day, shard-id).  When a chunk exceeds either X MB or Y seconds it is sealed (becomes immutable) and scheduled for persist.
	4.	Persist to Parquet
The sealed chunk is dictionary-encoded, sorted by primary key (time plus tags), written as a single Parquet file, and uploaded with a content-addressable key like
s3://bucket/db/table/2025/04/25/partition-42/file_000123.parquet.
	5.	Catalog update & WAL trim
The Ingester records the new object key in the Catalog, then truncates the corresponding WAL segment.  At this point the Compactor may later merge many small files, but the Ingester is finished.

2 Making fresh data queryable

Because Queriers ask Ingesters for “recent, not-yet-persisted” chunks first, dashboards see new points within ~100 ms of receipt—even before the Parquet write completes.  The in-memory Arrow batches are streamed back over Flight, deduplicated on the coordinator, then merged with older Parquet partitions.

3 Scaling & Sizing knobs

Resource	Primary driver	Guidance
CPU	Line-protocol parsing & Arrow allocation	Vertical scaling gives best $/point; aim for ~70 % utilisation.  ￼
RAM	Mutable-chunk buffer	Keep 1–2 min of peak ingest in memory for real-time queries.
SSD	WAL segments	Provide 2–4× RAM; latency matters more than capacity.  ￼
Replicas	Write QPS	Horizontal scale only when single-node CPU is saturated; remember replication amplifies traffic linearly.

4 Failure & back-pressure behaviour
	•	Ingester crash → WAL replay on restart; no data loss.
	•	Object store outage → Ingester keeps buffering in RAM and WAL until a configurable persist backlog threshold; beyond that the Router applies 429 back-pressure.
	•	Router crash → Stateless; simply restarts and resumes hashing.

5 Why this split works
	•	Durability without latency: the WAL gives crash-safety while staying on the local NVMe path; the client sees an ack as soon as two WAL fsyncs succeed.
	•	Low coupling: Routers can be upgraded or redeployed independently; Ingesters can be drained one-by-one for maintenance.
	•	Straight arrow to FDAP: data is Arrow on the wire, Arrow in memory, Parquet at rest—no impedance mismatches.

This ingest layer therefore turns an unbounded, high-cardinality stream into durable, analytics-ready Parquet files while exposing hot data for sub-second queries—all with simple, horizontally or vertically scalable components.
