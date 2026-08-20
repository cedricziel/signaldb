## Context

See proposal.md — Why. What shapes the approach:

- `GET /api/v1/schema/metrics` already returns, per metric definition, its
  `instrument`, `unit`, `attributes`, and `entity_associations` — the entity
  names that metric measures. Coverage in the bundled OTel 1.43 registry is
  concentrated exactly where the Catalog is blindest: all 13 `process.*` →
  `process`, all 45 `system.*` → `host`, all 13 `container.*` → `container`.
  The endpoint pages at 200 hits and takes a name `prefix`.
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
- A metrics panel on the _breakdown_ drill-in depth. A breakdown row is a
  dimension within the entity, not an entity with its own resource attributes.

## Decisions

### Selection: intersect registry associations with what the window holds

The panel's metric set is `entity_associations` ∩ _observed in window_, not one
or the other alone.

- The registry alone would promise metrics a deployment never emits (semconv
  declares far more than any one host reports).
- Observation alone would sweep in every metric that happens to carry the
  entity's resource attributes — for a host, that is nearly the whole tenant.

The intersection is computed by _asking_ for the associated set and letting the
query answer with what exists, rather than by a separate discovery call — see
the next decision.

**Alternative considered:** a metric-name discovery call
(`/prometheus/api/v1/label_values/__name__`) intersected client-side, then one
query per survivor. Rejected: an extra round trip on the compat surface plus up
to 45 follow-up queries, to learn something the single grouped query below
already tells us.

### Fetch: one grouped IR query per source, regex-alternated over the names

Per source (`metrics`, `metrics_histogram`), one IR document:

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

**Alternative considered:** one IR document per metric. Rejected: 45 concurrent
queries for a host page, each repeating the same scan predicates.

The name list is chunked when the alternation would grow unreasonable, and the
tile grid is capped with an explicit "showing N of M" — a silently truncated
panel would read as "this is everything the host reports".

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
