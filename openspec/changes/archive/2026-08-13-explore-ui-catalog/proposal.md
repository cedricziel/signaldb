## Why

Live use of the Catalog tab against a real deployment surfaced several gaps:
a matching-span row on an entity's detail page did nothing when clicked
(missing `signal: "traces"` on navigation); database entities had no way to
see their most frequent statements; a trace's waterfall carried no visual
distinction between SERVER/CLIENT/INTERNAL/PRODUCER/CONSUMER spans even
though `span.kind` was already resolvable; and a service's detail page had no
answer to "where does this service's time actually go" across its outbound
dependencies (database, HTTP, RPC, messaging). The database top-statements
view additionally needed SignalDB's own SQL-catalog spans to actually carry
query text, which they did not.

## What Changes

- Fix the Catalog entity detail page's "recent matching spans" row click to
  navigate into the trace waterfall (`signal: "traces"` plus the trace id),
  not just set a meaningless `trace` param.
- A read-only "Top statements" table on database entity pages, ranking
  distinct `db.query.text` values by frequency — via a new `topValues`
  entity-type mechanism, distinct from the existing drillable `breakdown`
  table (clicking a statement does not drill further).
- `span.kind` is fetched via a dedicated Query IR query (not the Tempo wire
  response) and used to color-code waterfall bars (SERVER/CLIENT/
  INTERNAL/PRODUCER/CONSUMER), with a legend and a chip in the span detail
  panel.
- A "Time by dependency" stacked-bar-and-legend section on a service's own
  Catalog detail page, breaking down summed CLIENT-span duration by
  discovered dependency category (`db.system.name`, `http.request.method`,
  `rpc.system`, `messaging.system`), with an "Other" remainder — computed
  client-side from parallel Query IR aggregate queries, no dedicated backend
  aggregation.
- **Backend**: SignalDB's own SQL-catalog DB client spans (register,
  heartbeat, list, deregister ingester operations) record sanitized
  `db.query.text` per the OTel database-spans semantic conventions
  (literals replaced with placeholders; parameterized values never
  inlined) — the data source the Top statements table above depends on.

Not breaking: no OTLP ingest, storage, or Flight wire-schema change. The
Tempo-compatible trace response wire format is explicitly unchanged —
`span.kind` is sourced through a separate Query IR query, not added to the
Tempo JSON shape.

Surfaces explicitly scoped out: this is a web-UI change (plus the one
backend span-attribute addition it depends on). The underlying data
(`span.kind`, `db.query.text`, dependency-category attributes) is already
queryable via Query IR/HTTP/MCP; no new backend query surface is introduced.

## Capabilities

### New Capabilities

- `explore-ui-catalog`: the Catalog tab's entity detail pages navigate
  correctly into the trace waterfall, show a read-only top-statements table
  for database entities, color-code trace waterfall spans by kind, and show
  a service's time-by-dependency-category breakdown.

### Modified Capabilities

- `self-monitoring-traces`: catalog DB client spans additionally capture
  sanitized `db.query.text`.

## Impact

- **src/ui**: `features/catalog/CatalogView.tsx`, `EntityDetail.tsx`,
  `DependencyBreakdown.tsx` (new), `entityTypes.ts`,
  `api/dependencyBreakdown.ts` (new), `features/traces/TracesView.tsx`,
  `api/spanKinds.ts` (new), plus styles/tests.
- **common**: `self_monitoring/spans.rs` (`db.query.text` field on
  `db_client_span`), `catalog.rs` (`record_query_text` helper wired into
  register/heartbeat/list/deregister ingester operations).
- Docs: `docs/operations/self-monitoring-traces.md` gains the
  `db.query.text` mention.
