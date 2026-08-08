## 1. TraceQL compilation

- [ ] 1.1 Write failing tests for a TraceQL selector compiler: scoped field
      names (`resource.`/`span.`/intrinsic), value escaping, multiple filters
      combined with `&&`, and an empty filter set compiling to no query.
- [ ] 1.2 Implement the compiler in `src/ui/src/lib/traceFilters.ts`.

## 2. Filters in URL state

- [ ] 2.1 Write failing tests for a traces filter parameter that round-trips,
      drops unparseable entries, and stays distinct from the logs filters.
- [ ] 2.2 Implement it in `src/ui/src/lib/urlState.ts`.

## 3. Facet data

- [ ] 3.1 Write failing tests for a facet-values adapter: builds the IR `table`
      document (`aggregate by` + `order` + `limit`), reads back
      `(value, count)` pairs, and reports when the list was truncated.
- [ ] 3.2 Implement it over the generated `queryIr` client — no hand-written
      `fetch`.
- [ ] 3.3 Write a failing test that a facet whose only value is the
      unresolved-field placeholder is not offered (guards against #1070's
      silent null group); implement the guard.

## 4. Sidebar component

- [ ] 4.1 Write failing tests for `TraceFacets`: lists the enumerable fields,
      expands to values with counts ordered by frequency, states truncation,
      selects a value, and shows a loading and an empty state.
- [ ] 4.2 Implement `src/ui/src/features/traces/TraceFacets.tsx`.
- [ ] 4.3 Add sidebar and chip styles reusing the logs sidebar's visual
      language.

## 5. Wiring

- [ ] 5.1 Write a failing test that filters reach the Tempo search request as
      `q`, and that removing a chip widens the result; implement the wiring in
      `TracesView.tsx`.
- [ ] 5.2 Write a failing test that the span-volume chart applies the same
      filters; implement it in the trace-volume IR document.

## 6. Verification

- [ ] 6.1 UI suite, lint, and typecheck green via pnpm.
- [ ] 6.2 Check against the live deployment: counts match an independent IR
      query, filtering narrows list and chart together, and the filtered view
      survives a reload.

## 7. Documentation

- [ ] 7.1 Document the traces sidebar on the explore-UI page, routed via the
      `docs` skill, noting that attribute facets await #1073.
