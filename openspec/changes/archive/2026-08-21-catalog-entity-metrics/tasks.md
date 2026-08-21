Each group below is written test-first: the failing test comes before the code
that satisfies it. Groups 1–4 are PR 1 (the detail-page panel); group 5 is PR 2
(the list sparkline), stacked on it. Group 6 closes both out.

## 1. Registry association lookup

The registry cannot be asked which metrics describe an entity, so the join runs
from the data toward the registry — see design.md, Selection.

- [x] 1.1 Add a failing test for discovering the window's observed metric names
      through one IR query (`aggregate by ["metric.name"]`), including the
      empty-window case
- [x] 1.2 Implement that discovery call
- [x] 1.3 Add a failing test for collecting definitions for a set of observed
      names: one prefix search per distinct first name segment, not one per
      metric
- [x] 1.4 Implement it over `GET /api/v1/schema/metrics`
- [x] 1.5 Add a failing test that an entity type carries the registry entity
      name it was derived from, including for a curated type a registry entity
      matched — reversing the id is not safe, since registry names contain
      underscores of their own (`gcp.cloud_run`)
- [x] 1.6 Record that name on the entity type in `deriveEntityTypes`
- [x] 1.7 Add a failing test for filtering definitions to one entity type by
      that name, returning an empty list for an entity type nothing associates
      with, then implement the filter
- [x] 1.8 Add a failing test that the definition lookup is cached across
      time-range changes — a registry does not move — while the observed-name
      discovery is keyed by window, then key both queries accordingly

## 2. Grouped metric query

- [x] 2.1 Add a failing test for building one Query IR document from a metric
      name list plus the entity's identity pins: regex alternation on
      `metric.name`, one `where` per pin, `aggregate by metric.name` with a step
- [x] 2.2 Add a failing test that the aggregation follows the instrument
      (gauge/updowncounter → `avg`, counter → `max`)
- [x] 2.3 Add a failing test that the name list is chunked when the alternation
      exceeds the agreed bound
- [x] 2.4 Implement the document builder against those tests
- [x] 2.5 Add a failing test that the response splits into one series group per
      `metric.name`, and that a metric absent from the response yields no group
      (never a zero-filled one), then implement the split

## 3. The panel

- [x] 3.1 Add a failing test: an entity type with associated metrics renders one
      chart per observed metric, each labelled with its unit and instrument
- [x] 3.2 Add a failing test: an entity type with no associations renders no
      metrics section at all (not an empty one)
- [x] 3.3 Add a failing test: every series is pinned to the entity's identity
      values — assert the pins reach the query
- [x] 3.4 Add a failing test: at the breakdown drill-in depth the panel still
      describes the entity, not the breakdown row
- [x] 3.5 Add a failing test: when the window holds no points for any associated
      metric, the panel says so rather than charting zeroes
- [x] 3.6 Add a failing test: over the tile cap, the panel states how many
      metrics are not shown
- [x] 3.7 Add a failing test that the section names the registry association it
      was discovered through
- [x] 3.8 Implement the panel component against those tests, rendering through
      the shared `VizTooltip` per the frontend-instrumentation rule
- [x] 3.9 Wire the panel into the entity detail page, querying `metrics` and
      `metrics_histogram` and merging the results

## 4. PR 1 close-out

- [x] 4.1 Run `pnpm typecheck`, `pnpm lint`, `pnpm test` in `src/ui` — all green
- [x] 4.2 Update `docs/users/explore-ui.md`'s Catalog section to describe the
      metrics panel and where its selection comes from
- [x] 4.3 Run `/simplify` on the changed code, then commit and open PR 1

## 5. Entity list sparkline (PR 2)

- [x] 5.1 Add a failing test for choosing the column's metric: the first
      associated metric observed for that entity type in the window
- [x] 5.2 Add a failing test that one query serves the whole column — series
      aggregated by the entity type's identity dimensions plus step, indexed
      back onto rows
- [x] 5.3 Add a failing test: an entity type with no associated metric shows no
      sparkline column at all
- [x] 5.4 Add a failing test: a row with no data for the column's metric leaves
      the cell empty rather than drawing a flat line
- [x] 5.5 Add a failing test that the column header names the metric being drawn
- [x] 5.6 Implement the column against those tests
- [x] 5.7 Run typecheck, lint, tests; update the docs' entity-list description;
      run `/simplify`; commit and open PR 2 on top of PR 1

## 6. Verification against real data

- [x] 6.1 Point the UI dev server at a deployment carrying `system.*` and
      `process.*` metrics and confirm a host page charts them, pinned to that
      host
- [x] 6.2 Confirm an entity type with no associations (e.g. a database) still
      renders exactly as it does today
- [x] 6.3 Run the docs-freshness gate after committing, and again after any fix
- [x] 6.4 Sync the delta spec into `openspec/specs/explore-ui-catalog/` and
      archive the change
