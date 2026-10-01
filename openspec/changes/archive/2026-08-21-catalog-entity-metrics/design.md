## Context

See proposal.md — Why. What shapes the approach:

- `GET /api/v1/schema/metrics` already returns, per metric definition, its
  `instrument`, `unit`, `attributes`, and `entity_associations` — the entity
  names that metric measures. Coverage in the bundled OTel 1.43 registry is
  concentrated exactly where the Catalog is blindest: all 13 `process.*` →
  `process`, all 45 `system.*` → `host`, all 13 `container.*` → `container`.
- That endpoint takes only a name `prefix` and a limit the server clamps to
  200, with no cursor and no filter by association (`search_metrics` in
  `src/router/src/endpoints/schema.rs`), so it cannot be enumerated or asked
  about an entity.
- **But the entity endpoint can be.** `GET /api/v1/schema/entities/{name}`
  returns a `metrics` array — every metric name describing that entity, merged
  across visible registries (`metrics_for_entity` in
  `src/common/src/schema_registry/mod.rs`), and already surfaced on
  `EntityHit` in the generated clients. A live `host` returns 61 names,
  `nfs.*` among them: a family no prefix guessed from the entity's own name or
  from what happens to be observed would ever reach. This is the association
  lookup, and it decides the selection strategy below.
- The Catalog's entity types are already registry-derived (`deriveEntityTypes`),
  keyed by the registry's entity `name` with dots mapped to underscores. So an
  entity type's registry name — the join key to `entity_associations` — is
  recoverable without new plumbing.
- The UI's metric query surface is Query IR (`buildMetricIrDoc`), which supports
  `where` predicates on attributes and a stepped `aggregate` returning `series`.
  It does **not** yet have a range function (rate/increase/\*\_over_time); the
  Metrics tab falls back to PromQL for those. Per the project rule, first-party
  views go through the IR, not the compat APIs.
- `metrics` and `metrics_histogram` are two IR sources for one OTel signal; the
  Catalog already treats both as one (`RESOURCE_SOURCES`).
- An entity page already computes its identity pins (`EntityPin[]`) for its RED
  aggregate; those same pins identify the entity in metric space, because an
  entity's identity attributes are resource attributes carried on every signal.

## Goals / Non-Goals

**Goals:**

- One round trip per source for the whole panel, not one per metric.
- Metric selection explainable from data the user can also see (the registry),
  with no metric name written into UI code.
- Correct, visible units and instrument kinds — a counter must not be presented
  as though it were a rate.

**Non-Goals:**

- Adding a range/rate stage to the Query IR. Counters are charted as what the
  IR can currently return; see Risks.
- Alerting, thresholds, or anomaly marking on these charts.
- Metrics _of a breakdown row_. A breakdown row is a dimension within the
  entity, not an entity with its own resource attributes, so there are no
  metrics that describe it. The panel stays visible at that depth and keeps
  describing the entity, pinned to the entity's identity.

## Decisions

### Selection: intersect registry associations with what the window holds

The panel's metric set is `entity_associations` ∩ _observed in window_, not one
or the other alone.

- The registry alone would promise metrics a deployment never emits (semconv
  declares far more than any one host reports).
- Observation alone would sweep in every metric that happens to carry the
  entity's resource attributes — for a host, that is nearly the whole tenant.

Both halves are asked directly, and intersected:

1. **What describes this entity.** `GET /api/v1/schema/entities/{name}` returns
   the entity's `metrics` array — the registry's own answer, merged across
   visible registries. 61 names for a live `host`.
2. **What the window holds.** One IR query,
   `aggregate by ["metric.name"]`, returns the tenant's observed metric names.
   80 distinct names on the same deployment.
3. **Definitions for the intersection.** One prefix search per distinct name
   segment of that intersection — for a host, just `system` — collects the
   `instrument` and `unit` each tile needs. The segment ends at the first dot
   _or underscore_, because OTel names are dotted but anything scraped from a
   Prometheus exporter is not; splitting on the dot alone made every
   `otelcol_*` metric its own prefix. Narrowing to the intersection first is
   what keeps this step to one or two calls rather than one per namespace the
   tenant happens to emit.

The bound that matters: the association is read rather than reconstructed, and
the definition lookup is sized by the intersection — not by the registry, and
not by everything the tenant emits. `otelcol_*` and `signaldb.*` never enter,
because the registry does not say they describe anything catalogable.

**Superseded approach**, recorded because the working code took it for a
while: with the entity endpoint's `metrics` array overlooked, the join ran
data-first — discover every observed name, fetch definitions for all of them
by prefix, then filter on each definition's `entity_associations`. It produced
the right answer for `system.*`, but it could only ever find metrics whose
names the tenant was already writing _and_ whose namespace a prefix guess
reached, and it paid for definitions of every metric in the tenant to use a
handful. The premise it rested on — "the registry cannot be asked" — was never
checked against the entity endpoint.

Steps 1 and 2 are cached beyond a range change on the same terms as the
Catalog's other metadata; step 1 is window-scoped and keyed accordingly.

**Alternative considered:** an `?entity=<name>` filter on
`/api/v1/schema/metrics` (#1360). Now redundant for the association itself,
which the entity endpoint already answers. What would still help is a `keys=`
parameter on the metrics search, mirroring the one attributes already have:
step 3 would then fetch definitions for an exact name set instead of widening
to a prefix and filtering back. That is the re-scoped remainder of #1360.

**Alternative considered:** a metric-name discovery call on the Prometheus
compat surface (`/prometheus/api/v1/label_values/__name__`). Rejected: the IR
is this project's own query surface, and the same IR query that discovers the
names is the one the panel already needs.

### Fetch: grouped IR queries, regex-alternated over the names

Per source (`metrics`, `metrics_histogram`) and per aggregation kind, one IR
document. A document carries a single `aggregate` spec, so metrics that must be
aggregated differently (see below) cannot share one — which bounds the panel at
two documents per source, not one, and still nothing like one per metric:

```
from: <source>
pipeline:
  - where: { field: "metric.name", op: "regex", value: "^(name1|name2|…)$" }
  - where: <one clause per identity pin>            # entity pinning
  - aggregate: { by: ["metric.name", …attr dims], aggs: [...], step }
result: series
```

Metrics with no points in the window simply produce no series, which _is_ the
observed-set filter — no separate existence check, and no zero-filled series to
suppress. Splitting the response by its `metric.name` label yields one chart
per metric.

Verified against a live querier before building on it: regex alternation on
`metric.name`, `aggregate by ["metric.name"]` with a step, and an identity pin
on `host.name` all answer as assumed.

**Alternative considered:** one IR document per metric. Rejected: 45 concurrent
queries for a host page, each repeating the same scan predicates.

The name list is chunked when the alternation would grow unreasonable, and the
tile grid is capped with an explicit "showing N of M" — a silently truncated
panel would read as "this is everything the host reports".

### The instrument also decides which source can answer

A `metrics_histogram` row is a whole bucketed histogram, not a scalar — it
carries no value column at all, so a scalar aggregate against it is not merely
wrong but rejected (`aggregate 'avg' requires a numeric field, got string`).
Sending every associated metric to both sources, as the first implementation
did, therefore fails the whole panel the moment a tenant has one histogram.

The registry's `instrument` is what routes each metric to the source that can
answer for it: histogram and exponentialhistogram to `metrics_histogram`
through a `histogram_quantile` stage, everything else to `metrics` through the
scalar aggregate. Nothing is ever asked of a source that cannot answer.

Charting a histogram means charting a quantile — buckets have no single level
to plot — so the panel takes p95, the number a duration histogram exists to
answer. The stage's default `rate` mode is also the correct reading of the
cumulative temporality most histogram instrumentation emits, which makes
histograms, ironically, the one instrument the panel charts _correctly_ today.

### Aggregation follows the instrument, from the registry

The registry gives each metric's `instrument`, so the query picks the
aggregation rather than assuming one: gauge and updowncounter aggregate with
`avg` over the step; counter with `max`, so a cumulative series reads
monotonically instead of being averaged into nonsense.

Each tile is labelled with the metric's `unit` and instrument, taken from the
same registry record — so a cumulative counter is legible as a counter.

### Identity pinning reuses the page's existing pins

The panel builds its `where` clauses from the same `EntityPin[]` the page's RED
aggregate uses. This is what keeps the panel correct for entity types nobody
anticipated: whatever identifies the entity in trace space identifies it in
metric space, because both are resource attributes.

Entity types whose identity is a _span_ attribute (database, message
destination) have no counterpart on a metric point. Those types also carry no
`entity_associations` in practice, so the panel is absent for the same reason it
should be — no special case needed.

### The list's sparkline metric is the first observed association

The registry does not declare a "headline" metric, and inventing a curated one
per entity type is exactly the hardcoding this change exists to avoid. The
column therefore charts the _first_ associated metric that is observed for that
entity type in the window — deterministic (registry order is stable), derived,
and named in the column header so the user knows what they are looking at.

One IR query serves the whole column: filter to that one metric name, aggregate
by the entity type's identity dimensions plus step, and index the resulting
series by identity to match rows.

**Alternative considered:** picking the association with the highest row
coverage. Rejected for v1 — it needs the data before it can choose what to ask
for, so it costs a round trip to save a naming surprise.

## Risks / Trade-offs

- **Counters chart cumulatively, not as rates** (no IR range stage) → mitigated
  by labelling instrument and unit on every tile; a rate stage in the IR is the
  real fix and is tracked separately, at which point only the aggregation choice
  changes.
- **A host's 45 associated metrics is a wall of tiles** → capped grid with an
  explicit count of what is not shown, plus the sparkline column doing the
  at-a-glance job in the list.
- **Registry coverage is uneven** — outside process/system/container/k8s, most
  semconv metric groups declare no `entity_associations` → the panel is absent
  rather than wrong, and improves for free as the bundled registry and tenants'
  own registries fill in.
- **Regex alternation over many names is a broad predicate** → chunked; and the
  identity pins are the selective part of the scan regardless.
- **One grouped query mixes units in one response** → each series carries its
  `metric.name`, and the registry supplies that metric's unit, so tiles are
  never drawn on a shared axis.

## Open Questions

- Whether the tile cap should be a fixed number or driven by how many metrics
  the entity type actually reports. Answerable after seeing real host pages; it
  changes no requirement and no task.
