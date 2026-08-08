## Why

The traces tab has no filtering. You can group the result table by a dimension
and open a trace by id, but there is no way to narrow the set — no equivalent
of the logs tab's field sidebar and filter chips. Finding "the failing spans in
`signaldb-ui`" means grouping, reading, and scrolling.

Two of the three pieces a faceted sidebar needs already work, verified against a
live deployment:

- **Applying a filter.** `/tempo/api/search?q={ resource.service.name =
  "signaldb-ui" }` returns only matching traces.
- **Facet values with counts.** A Query IR `table` query —
  `aggregate by [<field>]` + `order` + `limit` — returns exact counts over the
  whole window: `[["signaldb", 2903], ["signaldb-ui", 980]]`. This is better
  than the logs sidebar, which lists values without counts.

The third piece — enumerating *which* fields are facetable — is a backend gap
filed as #1073: `/api/search/tags` is a hardcoded three-name list and tag-value
lookup returns 501 for every attribute. This change therefore scopes the sidebar
to the fields the backend can enumerate exactly, rather than guessing a field
list from a row-limited result set (the bias this project has just finished
removing from the volume charts).

## What Changes

- A facet sidebar on the traces tab offering the three enumerable fields:
  `service.name`, `span.name`, and `status.code`.
- Each facet expands to its values with **exact full-window counts**, sourced
  from a Query IR `table` aggregate — independent of the trace list's row limit,
  ordered by count, and bounded to a top-N with the remainder disclosed rather
  than silently dropped.
- Selecting a value adds a filter; filters render as removable chips above the
  trace table, compile to a TraceQL selector, and are passed to
  `/tempo/api/search` as `q`.
- Filters travel in the URL as their own parameter, so a filtered trace view is
  shareable and survives back/forward.
- The trace list, the group table, and the span-volume chart all reflect the
  active filters, so the chart continues to describe what the table shows.

Not breaking: no API, wire-format, or storage change. Every endpoint used
already exists and is already in the generated TypeScript client.

Surfaces explicitly scoped out: this is a web-UI change. TraceQL filtering is
already reachable via the HTTP API and the MCP `search_traces` tool; there is no
CLI counterpart to a sidebar.

## Capabilities

### New Capabilities

- `explore-ui-trace-facets`: the traces tab presents facetable fields with exact
  value counts over the selected window, and selecting values narrows the traces
  shown.

### Modified Capabilities

(none)

## Impact

- **src/ui** only: new `src/ui/src/features/traces/TraceFacets.tsx` and a
  TraceQL compiler in `src/ui/src/lib/`, plus wiring and URL state in
  `TracesView.tsx` / `urlState.ts`, and sidebar styles.
- **No Rust crates are touched.**
- Docs: the explore-UI page gains the traces sidebar.

## Out of scope

- Enumerating attribute facets beyond the three built-ins — blocked on #1073.
  When that lands, the sidebar gains fields without changing its shape.
- Free-text TraceQL editing on the traces tab (the logs tab's "edit as text"
  escape hatch has no traces equivalent yet).
- Duration and time-based facets, which are range predicates rather than value
  counts.
