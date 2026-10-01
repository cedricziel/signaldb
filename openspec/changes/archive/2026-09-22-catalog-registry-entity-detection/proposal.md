## Why

The Catalog tab cannot discover an entity it was not hand-coded to look for, and
cannot see one that reports through a signal other than traces or logs. Both
limits are visible on a live deployment today: `process.pid` and `container.name`
are absent from traces entirely but present on metrics, so the Processes page
renders empty while the data sits one source over, and `otelcol-contrib` — a real
service emitting 7,761 metric points an hour — appears nowhere in the catalog.

Neither limit needs new backend capability. The schema registry already declares
64 entity types and the attributes that identify each (`process` →
`process.pid`, `process.creation.time`). Field discovery already reports, per
signal source, which attributes are present, at what coverage and cardinality,
from maintained statistics rather than a scan. The catalog simply does not read
either one: it iterates a hand-written list of 8 entity types in
`src/ui/src/features/catalog/entityTypes.ts` and measures them with a
trace-shaped RED aggregate reused from the traces group table.

## What Changes

- Entity types are **derived from the tenant's schema registries** rather than a
  frontend constant. An entity type with identifying attributes becomes
  catalogable with no frontend change, so a tenant's own custom registry defines
  its own entity types.
- Entity **presence is detected across every signal source** from field-discovery
  statistics — a metadata read, not a scan — so an entity that only reports
  through metrics or profiles is discovered exactly like one that reports through
  traces.
- **Detection is separated from measurement.** Discovery answers "which entity
  types exist here"; listing an entity type's instances is a separate, on-demand
  grouped read over its identity tuple. Today one query does both, which is why
  the list conflates span counts with metric-point counts.
- **RED metrics become trace-derived and detail-scoped.** Error rate and duration
  percentiles are span concepts; they are shown where they are meaningful and
  omitted (not zero-filled) where the entity was observed only through non-trace
  signals.
- An entity type whose identifying attributes have **no maintained statistics** is
  reported as not yet analyzed, distinct from analyzed-and-absent. A partition
  that has never been compacted must not render as "you have no processes".
- The nav lists entity types **observed in the window**, not all 64, so
  registry-derived breadth does not become 64 empty tabs.

No OTLP ingest, Tempo/LogQL/PromQL surface, Flight schema, or on-disk layout is
touched. Nothing here is BREAKING.

## Capabilities

### New Capabilities

None. The behavior belongs to the existing catalog capability.

### Modified Capabilities

- `explore-ui-catalog`: adds requirements for how entity types are discovered
  (from the schema registry, not a fixed list), how their presence is detected
  (from per-source field-discovery statistics, across every signal), how
  instances are listed (an on-demand grouped read over the identity tuple, with
  absent identity dimensions dropped), how RED is scoped (trace-derived only),
  and how an unanalyzed entity type is reported. The capability's existing
  requirements — detail-page navigation, top-values tables, span-kind colouring,
  dependency breakdown — are unchanged.

## Impact

**Crates**: none. This is a `src/ui` change. The backend surfaces it depends on
are already implemented and deployed:

- `GET /api/v1/schema/entities` (router `endpoints/schema.rs`) — entity
  definitions with their identifying and descriptive attributes.
- `POST /api/v1/query` with `describe { target: "fields" }` (irVersion 4) —
  per-source field lists carrying `coverage`, `cardinality`, `origin`, and an
  `as_of` stamp, answered from the compactor's per-signal scan statistics.
- `POST /api/v1/query` `aggregate` — the existing instance-listing read.

**Frontend**: `features/catalog/entityTypes.ts` (the hand-written registry is
replaced by a registry fetch plus a presentation-only overlay),
`api/catalog.ts` (detection split from measurement), `CatalogView.tsx` and
`EntityDetail.tsx` (nav from detected types; RED columns conditional).

**Depends on**: the pending `query-field-discovery` change, which specifies the
discovery surface this consumes. That surface is deployed; its spec is not yet
archived into `openspec/specs/`. This change adds no requirement to it.

**Related but not required**: the compactor's bounded value sketch (#1329,
`attribute_value_stats`) is not in the deployed release. It would let instance
listing for low-cardinality entity types be answered from statistics instead of a
grouped read — an optimization this change is designed to accept later, not a
prerequisite. The presence statistics this change does depend on
(`attribute_scan_stats`) are deployed and refreshing.
