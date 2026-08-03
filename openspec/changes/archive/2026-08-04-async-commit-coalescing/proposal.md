## Why

SignalDB's own self-monitoring (`_system._monitoring`) exposed a structural flaw
in the write path: the writer's Flight `do_put` commits to Iceberg
**synchronously, once per request**, so a high-frequency, low-volume producer
drives one Iceberg snapshot — one new `metadata.json` and one
`UPDATE iceberg_tables` catalog write — every few seconds per table. On hive
this accumulated ~77,919 metadata versions on `_system._monitoring.logs` and
pushed the shared SQLite catalog `UPDATE` to 1.6–7s under the resulting write
contention (issue #888). Because `do_put` is synchronous, the OTLP exporter
blocks end-to-end on that catalog write and trips its gRPC deadline every ~10s,
churning on cancel/retry and dropping self-telemetry (issue #889).

These are one root cause seen from two ends: commit rate is bound to request
rate, and export latency is bound to catalog latency. Tuning batch delays or
timeouts only moves the ceiling. The sustainable fix decouples the commit from
the request and bounds the commit rate structurally, for every tenant.

## What Changes

- **BREAKING** — The writer's Flight `do_put` acknowledges after the ingested
  batch is durably flushed to the **writer WAL**, no longer after the Iceberg
  commit. Ingested data is therefore **eventually queryable** (bounded by the
  coalescing window below), not immediately queryable. Clients and tests that
  assumed read-your-writes must drain explicitly (see the flush primitive).
- The background WAL-processing loop becomes the **sole commit path** and gains
  a per-`(tenant, dataset, table)` **coalescing floor**: it commits a group when
  `elapsed ≥ commit_interval` **OR** `pending_rows ≥ max_uncommitted_rows`
  (an OR — the time trigger guarantees liveness for low-volume tables; the row
  trigger is a burst safety valve, not a minimum to wait for).
- A **force-commit primitive** (repurposing the existing, currently no-op
  `WalOperation::Flush`) drains all pending groups immediately, ignoring the
  floor — the read-your-writes escape hatch for tests and latency-sensitive
  clients.
- Iceberg **metadata growth is bounded** independent of commit rate: metadata
  pruning on the self-monitoring tables and/or short-interval snapshot
  expiration, so the metadata chain stays bounded rather than merely growing
  slower.
- New `[writer]` configuration: `commit_interval` (default 5s) and
  `max_uncommitted_rows` (default ~100k).

Net effect: commit rate is O(`commit_interval`) per table for every tenant and
is decoupled from ingest rate; #889 disappears as a consequence of removing the
synchronous coupling; nothing needs re-tuning as telemetry volume grows.

## Capabilities

### New Capabilities

- `writer-commit-coalescing`: the writer's Iceberg commit model — asynchronous
  WAL-drain as the sole commit path, time/size coalescing of commits per
  `(tenant, dataset, table)`, an on-demand force-commit primitive, and bounded
  Iceberg metadata growth.

### Modified Capabilities

<!-- ingest-durability is unaffected: its at-least-once contract ("forward to a
     writer, writer accepts, mark processed") holds unchanged — the writer's own
     durable WAL is the acceptance point, exactly as the spec already allows.
     No requirement in that spec asserts synchronous Iceberg commit or
     read-your-writes, so no delta is needed there. -->

## Impact

- **writer** crate: `do_put` (`flight_iceberg.rs`) drops the synchronous
  `process_single_entry` loop; `WalProcessor` (`processor.rs`) gains the
  coalescing floor and the `Flush` force-commit handling; new `[writer]` config.
- **common** crate: `[writer]` config fields; `WalOperation::Flush` semantics.
- **compactor** crate: short-interval / metadata-pruning path for bounded
  metadata (or table-property configuration applied at table creation).
- **tests-integration** + **writer**/**querier** tests: ingest-then-query tests
  migrate from `sleep`-based waits to the deterministic force-commit primitive.
- **Operational behavior**: query-after-ingest is now eventually consistent
  within `commit_interval`; self-monitoring (`_system._monitoring`) commit
  amplification and export-timeout churn are eliminated. No change to OTLP
  ingest wire formats, Tempo/LogQL/PromQL query surfaces, Flight wire schemas,
  or on-disk WAL/Iceberg layout.
