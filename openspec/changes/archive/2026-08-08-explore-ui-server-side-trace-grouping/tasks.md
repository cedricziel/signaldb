## 1. Scoped aggregate in the IR (`common`)

- [x] 1.1 Write failing tests in `src/common/src/query_ir/stage.rs` for the `Agg`
      shape: an optional `where` predicate deserializes, an aggregate without one
      is unchanged, and an unknown key is still rejected (`deny_unknown_fields`)
- [x] 1.2 Add the optional scoping `Predicate` to `Agg`, reusing the existing
      predicate type rather than a parallel grammar
- [x] 1.3 Write failing tests in `src/common/src/query_ir/validate.rs` that a
      scoping predicate is validated through the same resolver as the `where`
      stage: an unresolvable field and an undefined operator are both rejected
      identifying the offending predicate
- [x] 1.4 Implement scope validation in `apply_aggregate`
- [x] 1.5 `cargo test -p common` green

## 2. Lowering the scoped aggregate (`querier`)

- [x] 2.1 Write failing tests in `src/querier/src/query/ir_planner.rs` that a
      scoped aggregate lowers to a filtered DataFusion aggregate, that scoped and
      unscoped aggregates coexist in one stage over a single grouping, and that a
      group with no matching record is returned with zero rather than dropped
- [x] 2.2 Implement the lowering in `agg_expr`
- [x] 2.3 Add a test that a scoped quantile measures only matching records
- [x] 2.4 `cargo test -p querier` green
- [x] 2.5 Add integration coverage in `tests-integration` that a scoped and an
      unscoped count over one grouping return the same group set, differing only
      in the scoped values

## 3. Regenerate the API surface

- [x] 3.1 Confirm the OpenAPI schema is unaffected: `QueryIrRequest.pipeline` is
      `#[schema(value_type = Vec<Object>)]`, so stages are opaque at the HTTP
      boundary and the `Agg` shape is not in the spec. The router needs no code
      change either — it forwards the document verbatim
- [x] 3.2 Confirm no client regeneration is required: the generated TS type is
      `pipeline?: Array<{[key: string]: unknown}>`, so a scoped aggregate needs
      no type change in `src/ui/src/api/gen` or `src/signaldb-sdk`
- [x] 3.3 Scoped-aggregate execution is covered end-to-end by the
      `tests-integration` coverage in 2.5 (full ingest→WAL→writer→Iceberg→
      querier→router path). The `status.code` spelling is settled at the source
      rather than by sampling a deployment: `conversion_traces.rs` maps the OTLP
      status enum to the literal `"Unspecified"`/`"Ok"`/`"Error"`, pinned by a
      unit test, so `STATUS_CODE_ERROR` never reaches storage

## 4. IR document builder for the group table

- [x] 4.1 Write failing tests in `src/ui/src/api/traceGroups.test.ts` for the
      document builder: trace grain emits the root-span `where` stage with the
      `0000000000000000` sentinel, span grain omits it, active facet filters are
      ANDed in via the same `facetField` mapping `traceFacets.ts` uses, `by`
      carries the picked dimensions, the aggregates are the RED set (count,
      error count scoped to the error status, p50, p95), and `limit` is
      `budget + 1`
- [x] 4.2 Add `src/ui/src/api/traceGroups.ts` with the builder and a
      `fetchTraceGroups()` submitting through `runIrQuery` (never raw `fetch` —
      `ui-generated-client-only`)
- [x] 4.3 Write failing tests for envelope decoding: results are read by index
      against the requested `by` order, not by the logical name sent, and the
      column count is asserted (the server answers with physical names —
      `span.name` → `span_name`)
- [x] 4.4 Implement decoding into a `TraceGroupRow` view type, with the error
      status spelling shared with `normalizeStatus()` and a test covering both
      spellings the deployment may return

## 5. RED rendering, sorting and truncation

- [x] 5.1 Write failing tests that a group line renders count, rate, error count
      and percentiles straight from the server's row, performing no aggregation —
      rate is the only derived value (`count / window_seconds`, existing
      `formatRate()`)
- [x] 5.2 Implement the group row rendering
- [x] 5.2b Map the sort control to the IR `order` stage so a sort change
      refetches — the page is the top-N *under the current sort*, so re-sorting
      it locally would rank within the previous sort's selection. Rate orders by
      `n`. Drop the Services column (no `count_distinct`/`array_agg` in the IR);
      the dimension picker accepts any attribute name instead
- [x] 5.2c Skeleton loading states for the group table, the drill-in trace list
      and the single-trace view, holding the table's shape across the refetches
      sorting now causes
- [x] 5.3 Write failing tests for over-fetch truncation: a returned row count
      above the display budget sets a truncated flag and the extra row is not
      rendered; at or below the budget nothing is flagged
- [x] 5.4 Implement truncation detection and the truncation message, which states
      that the list is bounded without claiming a total

## 6. Unresolvable-dimension guard

- [x] 6.1 Write failing tests for the companion window-total aggregate (`by: []`,
      same filters and grain) and for the unresolvable signature: exactly one
      group, `null` label, count equal to the window total
- [x] 6.2 Implement the companion query and the guard, reporting the dimension as
      unavailable instead of rendering a single "(not set)" row holding
      everything (#1070)
- [x] 6.3 Write a test that a resolvable dimension some records lack still
      renders its `null` group alongside the real groups

## 7. Grain toggle and URL state

- [x] 7.1 Write failing tests in `src/ui/src/lib/urlState.test.ts` for the grain
      param: absent parses as traces, an invalid value falls back to traces, and
      the default is omitted when serializing
- [x] 7.2 Add the grain to `ExploreState` in `src/ui/src/lib/urlState.ts`
- [x] 7.3 Add the grain control to the traces toolbar and label the count column
      for the active grain

## 8. Wire up the view and delete the client-side path

- [x] 8.1 Replace the `tempo-search` + `groupTraces()` path in
      `src/ui/src/features/traces/TracesView.tsx` with `fetchTraceGroups()`
- [x] 8.2 Write failing tests in `TracesView.test.tsx` that the rendered table
      reflects the server's aggregates and does not change when the row budget
      changes
- [x] 8.3 Delete `groupTraces()`, `groupDimensions()` and `percentile()` from
      `src/ui/src/lib/traceGroups.ts` and their tests; keep `groupKey`,
      `groupLabel`, `parseGroupBy` and `formatRate`
- [x] 8.4 Add an empty-state message for trace grain with a filter on a field
      carried only by child spans, pointing at the span grain

## 9. Drill-in

- [x] 9.1 Write failing tests for the drill-in document: the group's dimension
      values become `where` equalities, a `null` value compiles to a negated
      `exists`, ordering is by `start_time_unix_nano` desc, and the same grain
      and filters apply
- [x] 9.2 Implement the drill-in `rows` query and render the trace list from the
      fixed eight-column projection
- [x] 9.3 Add a test that the drill-in list states it is bounded while the group
      row keeps reporting the group's full size

## 10. Verification and coverage

- [x] 10.1 `cargo test -p common -p querier` and the `tests-integration` coverage
      from task 2.5 green
- [x] 10.2 `pnpm --filter signaldb-ui test` green, and coverage still meets the
      80% thresholds in `vite.config.ts`
- [x] 10.3 `pnpm --filter signaldb-ui lint`, `pnpm ui:build`, `cargo fmt`,
      `cargo clippy --workspace --all-targets --all-features` and
      `cargo machete --with-metadata` clean
- [x] 10.4 Live verification runs through CI, not a hand-driven local instance:
      the `tests-integration` coverage from 2.5 exercises the scoped aggregate
      across the real ingest→WAL→writer→Iceberg→querier→router path, which is
      what a manual dev-proxy check would have sampled less reliably. hive
      cannot serve as the target — it runs a released image predating the
      scoped aggregate and would reject the document

## 11. Documentation

- [x] 11.1 Update `docs/users/explore-ui.md` for the grain toggle, what a group
      row counts under each grain, and the RED columns (route via the docs skill).
      **Must land with task 8** — until the view is wired up and the grain
      control rendered, none of this is reachable, and `explore-ui.md` is
      `status: living`, so documenting it earlier would describe behavior that
      does not exist. The doc-freshness gate will flag `traceGroups.ts` /
      `urlState.ts` before then; that flag is expected and answered here
- [x] 11.2 Note in the traces section that trace-grain filters match the root
      span only, and that any-span matching awaits `query-structural-traces`
- [x] 11.3 Document the scoped aggregate in the query-IR reference docs, and
      update any skill describing the IR's aggregate grammar

## 12. Surface parity

- [x] 12.1 No SDK change: the CLI submits IR documents whose `pipeline` is
      opaque JSON (`Vec<Object>`), so a scoped aggregate is already expressible
      through `signaldb-sdk` without regeneration
- [x] 12.2 No new HTTP endpoint is added; confirm `POST /api/v1/query` is
      unchanged apart from the regenerated request schema
