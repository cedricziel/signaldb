## Why

The Catalog discovers entities from every signal, but measures them only from
traces. An entity type whose telemetry is metrics — a process, a container, a
host — is therefore found, listed, and then shown a detail page with `–` in
all four RED cells and an empty "recent matching spans" table. The data that
would fill that page is already stored and already queryable; nothing on the
page asks for it.

The schema registry already carries the missing link: a metric definition
declares which entity it measures (`entity_associations`), so the mapping does
not have to be invented or hand-maintained. Every `process.*` metric associates
with `process`, every `system.*` metric with `host`, every `container.*` metric
with `container`. Reading that relationship is what turns a structurally blank
page into the entity's actual behavior over the selected window.

## What Changes

- The Catalog entity detail page gains a **Metrics panel**: the metrics the
  registry associates with that entity type, filtered to those with data in
  the selected window, each charted as a small time-series tile pinned to the
  entity's identity values.
- Metric selection is registry-derived, never hardcoded: entity type →
  `entity_associations` → metric names. An entity type the registry associates
  no metrics with shows no panel, and a tenant publishing its own registry gets
  its own entity's metrics on the same terms, with no code change.
- Series are pinned to the entity by its identity dimensions, the same pins the
  page's RED aggregate already uses — a process page shows that `process.pid`'s
  CPU, not every process's.
- The entity **list** gains a sparkline column for entity types that have an
  associated headline metric, so a metrics-only entity type's table stops
  reading as entirely unmeasured.
- Absent data stays distinguishable from unmeasured data, as everywhere else in
  the Catalog: a metric associated but not observed in the window is not
  charted as a flat zero.

Not in scope: changing what the four RED cells show for trace-less entity
types (they keep reading `–`), and any new query surface — the panel goes
through the existing Query IR metric path.

## Capabilities

### New Capabilities

<!-- None: this extends the Catalog's existing detail-page behavior. -->

### Modified Capabilities

- `explore-ui-catalog`: adds requirements for a registry-derived metrics panel
  on the entity detail page, its identity pinning, its observed-only selection,
  and the entity list's sparkline column.

## Impact

- **UI only.** `src/ui/src/features/catalog/` (entity detail page, entity
  table), `src/ui/src/api/` (a metric-discovery call against the existing
  `/api/v1/schema/metrics` endpoint and the existing Query IR metric path).
- No Rust workspace crate changes: `/api/v1/schema/metrics` already returns
  `entity_associations`, and the querier already answers Query IR metric
  queries. No new endpoint, no schema change, no Flight or storage change.
- Docs: `docs/users/explore-ui.md` (the Catalog section describes what an
  entity page shows).
- Not breaking: additive to a UI view; no ingest, query-compat, wire, or
  on-disk surface is touched.
