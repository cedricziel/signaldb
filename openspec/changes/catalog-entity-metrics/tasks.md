Each group below is written test-first: the failing test comes before the code
that satisfies it. Groups 1–4 are PR 1 (the detail-page panel); group 5 is PR 2
(the list sparkline), stacked on it. Group 6 closes both out.

## 1. Registry association lookup

- [ ] 1.1 Add a failing test for an API function that, given an entity type,
      returns the registry metric definitions associated with it (name,
      instrument, unit) — including the case of an entity type with no
      associations returning an empty list
- [ ] 1.2 Implement it over `GET /api/v1/schema/metrics`, matching the entity
      type back to its registry entity name (the dots-to-underscores mapping
      `deriveEntityTypes` applies), paging the endpoint's 200-hit cap
- [ ] 1.3 Add a failing test that the lookup is cached across time-range
      changes — a registry does not move — then key the query accordingly

## 2. Grouped metric query

- [ ] 2.1 Add a failing test for building one Query IR document from a metric
      name list plus the entity's identity pins: regex alternation on
      `metric.name`, one `where` per pin, `aggregate by metric.name` with a step
- [ ] 2.2 Add a failing test that the aggregation follows the instrument
      (gauge/updowncounter → `avg`, counter → `max`)
- [ ] 2.3 Add a failing test that the name list is chunked when the alternation
      exceeds the agreed bound
- [ ] 2.4 Implement the document builder against those tests
- [ ] 2.5 Add a failing test that the response splits into one series group per
      `metric.name`, and that a metric absent from the response yields no group
      (never a zero-filled one), then implement the split

## 3. The panel

- [ ] 3.1 Add a failing test: an entity type with associated metrics renders one
      chart per observed metric, each labelled with its unit and instrument
- [ ] 3.2 Add a failing test: an entity type with no associations renders no
      metrics section at all (not an empty one)
- [ ] 3.3 Add a failing test: every series is pinned to the entity's identity
      values — assert the pins reach the query
- [ ] 3.4 Add a failing test: at the breakdown drill-in depth the panel still
      describes the entity, not the breakdown row
- [ ] 3.5 Add a failing test: when the window holds no points for any associated
      metric, the panel says so rather than charting zeroes
- [ ] 3.6 Add a failing test: over the tile cap, the panel states how many
      metrics are not shown
- [ ] 3.7 Add a failing test that the section names the registry association it
      was discovered through
- [ ] 3.8 Implement the panel component against those tests, rendering through
      the shared `VizTooltip` per the frontend-instrumentation rule
- [ ] 3.9 Wire the panel into the entity detail page, querying `metrics` and
      `metrics_histogram` and merging the results

## 4. PR 1 close-out

- [ ] 4.1 Run `pnpm typecheck`, `pnpm lint`, `pnpm test` in `src/ui` — all green
- [ ] 4.2 Update `docs/users/explore-ui.md`'s Catalog section to describe the
      metrics panel and where its selection comes from
- [ ] 4.3 Run `/simplify` on the changed code, then commit and open PR 1

## 5. Entity list sparkline (PR 2)

- [ ] 5.1 Add a failing test for choosing the column's metric: the first
      associated metric observed for that entity type in the window
- [ ] 5.2 Add a failing test that one query serves the whole column — series
      aggregated by the entity type's identity dimensions plus step, indexed
      back onto rows
- [ ] 5.3 Add a failing test: an entity type with no associated metric shows no
      sparkline column at all
- [ ] 5.4 Add a failing test: a row with no data for the column's metric leaves
      the cell empty rather than drawing a flat line
- [ ] 5.5 Add a failing test that the column header names the metric being drawn
- [ ] 5.6 Implement the column against those tests
- [ ] 5.7 Run typecheck, lint, tests; update the docs' entity-list description;
      run `/simplify`; commit and open PR 2 on top of PR 1

## 6. Verification against real data

- [ ] 6.1 Point the UI dev server at a deployment carrying `system.*` and
      `process.*` metrics and confirm a host page charts them, pinned to that
      host
- [ ] 6.2 Confirm an entity type with no associations (e.g. a database) still
      renders exactly as it does today
- [ ] 6.3 Run the docs-freshness gate after committing, and again after any fix
- [ ] 6.4 Sync the delta spec into `openspec/specs/explore-ui-catalog/` and
      archive the change
