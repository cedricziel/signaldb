## Context

See proposal.md. Query IR v1 models terminal relations as rows, tables, or a
one-dimensional stepped series. The trace heatmap spike demonstrated the needed
DataFusion aggregation but also demonstrated that a trace-specific HTTP and
Flight contract duplicates native query semantics.

## Goals / Non-Goals

**Goals:**

- Add a bounded, reusable Query IR v2 heatmap relation and envelope.
- Lower trace duration-by-time count heatmaps server-side over the complete
  selected window.
- Keep filtering, type coercion, tenant/dataset selection, and generated-client
  access on the existing native query path.
- Render the trace UI from the v2 envelope without row-limit dependence.

**Non-Goals:**

- General arithmetic expressions or arbitrary client-provided DataFusion SQL.
- Automatic support for OTLP metric histograms; their stored buckets and
  temporality need a separate metrics-model design.
- Post-heatmap ranking, ordering, or further pipeline transforms in v2.
- Changes to OTLP ingestion, Flight v1 wire schemas, v2 storage schemas, WAL, or
  Iceberg table layout.

## Decisions

### Add a terminal IR v2 heatmap stage and envelope

The document declares `result: "heatmap"` and ends with:

```json
{
  "heatmap": {
    "x": { "step": "1m", "align": "epoch" },
    "y": {
      "of": "duration",
      "bounds": ["1ms", "5ms", "25ms", "100ms", "1s"],
      "overflow": true
    },
    "value": { "fn": "count", "as": "count" }
  }
}
```

This is preferable to a trace-specific route because it shares the native
predicate grammar, timestamp resolution, logical field registry, tenant
boundary, Flight dispatch, OpenAPI description, and generated clients. A new
relation type carries x/y metadata and sparse count cells; the validator gates
this stage and envelope to IR v2 so v1 behavior remains unchanged.

### Lower bins server-side and cap their shape

The planner accepts the trace `duration` logical field and coerces bounds to
integer nanoseconds. It lowers x with epoch-aligned integer buckets and y with a
duration `CASE` expression, then groups by both values and counts rows. Bins are
lower-inclusive and upper-exclusive, with the final bucket accepting all values
at or above its lower bound. Other numeric fields are deliberately excluded
until the response can transport their bound types without loss.

The server caps duration boundaries and computed time-bucket count before
planning. This bounds the output and prevents a client from turning one query
into an unbounded two-dimensional result.

The trace source retains its precise `start_time_unix_nano` predicate plus the
widened timestamp predicate needed for Iceberg hour-partition pruning. All Arrow
and Parquet types used by the planner and result encoding come from DataFusion
reexports to preserve FDAP version alignment.

### Treat heatmap as a new v2 relation, not a series variant

A stepped aggregate series has one x axis and one value; it cannot truthfully
encode a duration axis. A distinct relation/envelope provides a clear, typed
contract without overloading labels or forcing clients to infer bucket indexes.
It also leaves the path open for future log distributions while explicitly
excluding metric histogram lowering from this change.

### Migrate the trace UI through generated clients

The UI builds an IR v2 document with its current time range, step, and trace
filters only when Heatmap is selected. It renders server-returned bounds and
sparse cells into a duration-by-time grid. Histogram and Area continue using the
existing count-series query. The CLI and MCP retain their generic Query IR
submission paths, so no new command or transport is required.

## Risks / Trade-offs

- [IR v2 parser accidentally accepts heatmap under v1] → Explicitly gate both
  stage and envelope by version in validation, with regression tests for v1
  rejection and unchanged v1 documents.
- [Large selected windows produce too many cells] → Validate duration-bound and
  time-bucket caps before DataFusion execution and return a clear client error.
- [Trace planner loses Iceberg pruning] → Reuse and test the dual precise and
  partition timestamp bounds from trace queries.
- [Sparse responses look like missing axis regions] → Return complete axis
  metadata; clients synthesize zero cells only inside the declared window.
- [The dedicated endpoint spike leaks into a release] → Remove its route,
  ticket, generated schemas, and clients before merging the IR v2 change.

## Migration Plan

1. Implement and test v2 parsing, validation, relation inference, and envelope
   shaping while retaining every v1 contract.
2. Add the planner and Flight execution through the existing `query_ir` ticket.
3. Regenerate OpenAPI, Rust SDK, and TypeScript clients; migrate the trace UI.
4. Remove the uncommitted dedicated heatmap spike before merge.
5. Roll back by disabling UI use of v2; v1 documents and storage remain
   untouched, so no data migration or restart-only recovery is required.
