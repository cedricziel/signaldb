---
name: adding-new-signal
description: Step-by-step guide for adding a new signal type or table to SignalDB - schema definition, Flight schema, OTLP conversion, WAL operation, acceptor/writer/querier/router updates, and testing. Use when adding new signal types, metric types, or tables.
---

# Guide: Adding a New Signal Type or Table

Adding a new signal type or table requires changes across multiple crates. Follow this checklist.

## Step 1: Define the Schema

For **traces/logs** (TOML-based schemas):
- Edit `schemas.toml` at the repository root
- Add a new `[signal_type.v1]` section with field definitions
- Update `[metadata]` to set `current_{signal}_version`

For **metrics** (hardcoded schemas):
- Edit `src/common/src/iceberg/schemas.rs`
- Add a new schema function returning `iceberg_rust::spec::Schema`
- Add an entry to the `TableSchema` enum
- Add partition spec (always `Hour(timestamp)`)

## Step 2: Define the Flight Schema (Wire Format)

- Edit `src/common/src/flight/schema.rs`
- Add a new Arrow `Schema` definition for the OTLP->Arrow conversion
- This is the v1 (wire) format -- may differ from Iceberg storage format

## Step 3: Add OTLP->Arrow Conversion

- Add conversion in `src/common/src/flight/conversion/`
- Implement `otlp_{signal}_to_arrow()` function
- Convert OTLP protobuf structures to Arrow RecordBatches using the Flight schema

## Step 4: Add WAL Operation

- Edit `src/common/src/wal/mod.rs`
- Add a new variant to `WalOperation` enum (e.g., `WriteNewSignal`)
- Ensure serialization/deserialization works (bincode)

## Step 5: Update Acceptor

- Edit `src/acceptor/src/`
- Add handler for the new OTLP signal type (gRPC + HTTP)
- Configure WalManager flush settings for the new signal type
- Forward to Writer via Flight `do_put`

## Step 6: Update Writer

- Edit `src/writer/src/flight.rs` -- handle new signal in `do_put`
- If schema differs between Flight v1 and Iceberg v2:
  - Add transformation in `src/writer/src/schema_transform.rs`
- Edit `src/writer/src/processor.rs` -- handle new WalOperation in WalProcessor
- Edit `src/writer/src/storage/iceberg.rs` -- map table name to schema

## Step 7: Update Querier

- Edit `src/querier/src/flight.rs` -- add new query types for the signal
- Add Flight ticket format: `{query_type}:{tenant}:{dataset}:{params}`

## Step 8: Update Router (API)

- Add HTTP endpoints in `src/router/src/tempo.rs` or new handler module
- Add Flight forwarding for new query types

## Step 9: Update Configuration

- If needed, add schema toggle in `[schema.default_schemas]` config
- Update `src/common/src/config/mod.rs`

## Step 10: Tests

- Unit tests in each modified crate
- Integration test in `tests-integration/` for end-to-end flow
- Verify: OTLP ingest -> WAL -> Iceberg -> Query roundtrip

## Key Patterns to Follow

- **Partition by Hour(timestamp)** -- all tables use this
- **Namespace = [tenant_slug, dataset_slug]** -- all tables are tenant-isolated
- **Table creation is lazy** -- happens on first write in WalProcessor
- **Arrow IPC for WAL data** -- RecordBatches serialized with StreamWriter
- **JSON for complex nested types** in Iceberg -- List<Struct> gets serialized to JSON strings

## Reference: Existing Signal Implementations

- **Traces**: Most complete. Look at the full write path from `src/acceptor/` through `src/writer/` to `src/querier/`.
- **Logs**: Similar to traces but simpler (no v1->v2 transform needed).
- **Metrics**: Multiple table types from single signal. Table routing via WAL entry `metadata.target_table`.
