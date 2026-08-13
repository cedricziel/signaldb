## Why

An exception observed in ingested telemetry is reachable only by opening the
one trace or log line that happened to carry it — there is no way to see
"what exceptions are occurring, how often, and where" the way an error
tracker does. Two data-model facts make this a two-part gap, not one:

- **Traces**: per the OTel exceptions-on-spans convention, an exception is a
  span _event_ named `exception`, not a span attribute — its
  `exception.type`/`.message`/`.stacktrace` live nested inside that event's
  own attributes. Query IR's field resolver has no path from a logical field
  name to a value inside an events array, so before this change nothing
  could filter, group, or project on it: `exception.type` silently resolved
  to null everywhere on the `traces` source.
- **Logs**: per the exceptions-on-logs convention (the direction OTel is
  moving instrumentation toward), the same attribute names are ordinary
  LogRecord attributes — already resolvable today, no backend gap here.

## What Changes

- **`query-ir-core`**: `exception.type`/`.message`/`.stacktrace`/`.escaped`
  resolve on the `traces` source by reading the span's first `exception`
  event, via a new `ir_event_attr` DataFusion UDF and a `Resolved::
EventAttribute` resolution path — filterable, groupable, and projectable
  like any other field. A span with no captured exception event resolves the
  field absent, even when its status is `Error`. No equivalent change is
  needed for `logs`.
- **New UI capability `explore-ui-errors`**: an Errors tab listing
  exceptions grouped by (type, message, service), ranked by count with
  first/last-seen timestamps, combining independent Query IR aggregates over
  `traces` (spans with a captured `exception` event) and `logs` (records
  with `exception.type`/`.message` attributes) — there is no single query
  spanning both, so results are merged client-side. Selecting a group
  fetches one concrete example's stacktrace, plus a link into the trace
  waterfall when that example carries a trace id.

Not breaking: no OTLP ingest, Tempo/LogQL/PromQL surface, Flight wire
schema, or on-disk Iceberg/WAL change — the events column already existed
and was already stored; this only adds a way to query into it.

Surfaces explicitly scoped out: this is a UI change consuming a
Query-IR-only backend extension. No new HTTP endpoint, OpenAPI operation, or
MCP tool is introduced — grouping/filtering by `exception.type` is already
reachable by anyone issuing their own Query IR request (HTTP, SDK, MCP
`query_ir`), which is what "only use query-ir" means here: extend the one
query surface rather than add a bespoke errors endpoint.

## Capabilities

### New Capabilities

- `explore-ui-errors`: the Errors tab groups and ranks exceptions from
  traces and logs, and lets a user drill into one example's stacktrace and
  originating trace.

### Modified Capabilities

- `query-ir-core`: `exception.type`/`.message`/`.stacktrace`/`.escaped`
  become resolvable logical fields on the `traces` source, read from the
  span's `exception` event rather than span attributes.

## Impact

- **common**: `query_ir::resolver::Resolved` gains an `EventAttribute`
  variant.
- **querier**: `ir_planner.rs` — the new `ir_event_attr` UDF, the traces
  `SchemaResolver`'s exception-field special case, and wiring through the
  three expression-building sites (value/predicate/projection).
- **src/ui**: new `api/errors.ts` and `features/errors/ErrorsView.tsx`; a new
  `errors` signal registered in `urlState.ts`/`ExploreView.tsx`.
- Docs: `docs/users/querying-ir.md` (exception-attribute addressing),
  `docs/users/explore-ui.md` (the Errors tab), and the `architecture` skill.
