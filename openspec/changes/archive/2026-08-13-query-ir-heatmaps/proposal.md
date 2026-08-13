## Why

The traces UI needs a true latency heatmap: time on the x-axis, duration on the
y-axis, and span count as intensity. A trace-specific endpoint would duplicate
native query semantics and create an API contract that cannot be reused for
other numeric distributions.

## What Changes

- Add a Query IR v2 terminal `heatmap` stage and `heatmap` result envelope for
  bounded time-by-numeric-distribution count aggregates.
- Define typed duration bounds, epoch-aligned time buckets, sparse cells, and
  lower-inclusive/upper-exclusive bucket semantics with a final overflow bucket.
- Enforce server-side bounds on both axes and preserve tenant/dataset isolation,
  predicate semantics, and Iceberg partition pruning.
- Replace the trace UI's status-by-time average-latency heatmap with a Query IR
  v2 duration-by-time count heatmap.
- Expose the capability through the existing native query API and generated
  clients rather than adding a trace-specific HTTP or Flight surface.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `query-ir-core`: Add the IR v2 heatmap stage, relation, envelope, validation,
  and query semantics.
- `explore-ui-signal-volume`: Add a latency heatmap visualization to the traces
  volume controls.

## Impact

- **common**: Query IR v2 document, stage, relation, and envelope contracts.
- **querier**: DataFusion lowering for time and duration buckets over trace
  spans, using DataFusion Arrow reexports and existing tenant table resolution.
- **router**: Native Query IR response shaping and OpenAPI schemas.
- **signaldb-sdk**, **signaldb-cli**, **mcp-server**, **ui**: Regenerated native
  query clients and the trace heatmap UI. No new dedicated command or transport
  is introduced.
- No changes to OTLP ingestion, Tempo/LogQL/PromQL compatibility surfaces,
  Flight wire schemas, or Iceberg/WAL layout.
