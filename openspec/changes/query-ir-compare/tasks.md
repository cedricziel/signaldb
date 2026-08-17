## 1. IR model and validation (common)

- [ ] 1.1 Failing tests in `common/src/query_ir`: `compare` stage parses (`selection`, `fields`/`"*"`, `maxValues`, `sample`), rejected under `irVersion` 1–3, accepted under 4; `comparison` envelope requires a terminal `compare`; stages after `compare` rejected by name; document-level `fields` rejected on `comparison`; bounds (`maxValues` > 200, field list > 500, `sample` < 1000) rejected naming the bound
- [ ] 1.2 Add `Stage::Compare(Compare)`, `ResultEnvelope::Comparison`, `RelationType::Comparison`; bump `MAX_IR_VERSION` to 4 and register the stage in the v4 operator registry; implement legal-stage inference and validation to make 1.1 pass
- [ ] 1.3 Define the `Comparison` response types in `common` (`utoipa::ToSchema` + serde): cohort counts, sample sizes, `statistic`, ordered `fields` (dimension / measure / nominal variants with participation, score, values or buckets, `truncated`, `sampled`), `skipped` list with reasons; unit test JSON round-trip
- [ ] 1.4 Pure ranking helpers in `common`: base-2 Jensen–Shannon divergence, participation JSD, `score` per design D5, sort key `(p_sel > 0, !nominal, score desc, name asc)`, risk ratio / support per value — property tests: JSD ∈ [0,1], identical distributions → 0, disjoint → 1, determinism

## 2. Planner execution (querier)

- [ ] 2.1 Failing planner tests over an in-memory traces fixture (`cargo test -p querier`): heatmap-box selection yields the expected cohort counts; group-vs-rest selection; absent → baseline; empty selection succeeds with zero shares; both tenants isolated
- [ ] 2.2 Lower the `selection` predicate to a `cohort` CASE projection and project promoted logical fields, extract-derived fields, and attribute-map columns; `execute_stream` the filtered plan (design D2)
- [ ] 2.3 Implement `CompareAccumulator` fold: per-row cohort flag, promoted-column values, map `(key, value)` entries attributed to their container; participation and value counts per cohort; field cap with `skipped: field-cap`; nominal degradation past the D4 threshold; legacy JSON-string attribute columns decoded via the existing reconciliation
- [ ] 2.4 Failing tests then implementation for classification (D4): string/bool → dimension, ≤32-distinct int → numerically ordered dimension, other numeric/duration/timestamp → measure, near-unique → nominal without value list
- [ ] 2.5 Failing tests then implementation for measures (D6): deterministic hash-stride reservoir per cohort, quantile bucket edges (≤16, deduped, 1-2-5 snap for durations), shared axis across cohorts, min/max/median, `sampled` flag; determinism test (two runs byte-identical)
- [ ] 2.6 Failing tests then implementation for dimension trimming: union of top-`maxValues` per cohort, baseline-frequency ordering, `truncated` flag; wildcard field set = registry ∪ observed keys ∪ derived fields; `body` skipped as retrieval-only; unresolvable names skipped
- [ ] 2.7 Serialize `Comparison` into a single-row `comparison_json` batch and route it through the IR do_get path like `flamegraph`; add the matched-row hard cap with uniform sampling fallback and a test that the response states it
- [ ] 2.8 Extend the real-querier `do_get` benchmark with a `compare` document over the bench dataset; record baseline numbers in the PR

## 3. HTTP API, OpenAPI, clients (router / sdk / ui gen)

- [ ] 3.1 Failing router test: `POST /api/v1/query` with `result: "comparison"` decodes the querier batch into `QueryIrResponse.comparison`; envelope mismatch surfaces as 400 before dispatch
- [ ] 3.2 Add `comparison: Option<Comparison>` to `QueryIrResponse`, decode `comparison_json`, update the endpoint doc comment listing envelopes
- [ ] 3.3 Regenerate OpenAPI (`cargo xtask openapi`), the Rust SDK (`src/signaldb-sdk`), and the TypeScript client (`src/ui/src/api/gen`); commit generated output; verify the CLI `signaldb query` passes a v4 `compare` document through and prints the envelope (test with a fixture document)
- [ ] 3.4 MCP: extend the `query_ir` tool description with the `compare` stage / `comparison` envelope and add an MCP integration test submitting a compare document (note the #1113 param-stringification quirk; ensure the document is accepted as an object)

## 4. Integration coverage (tests-integration)

- [ ] 4.1 End-to-end test (declared in `tests/main.rs`): ingest seeded traces where one `http.route` dominates the slow tail; `compare` with a duration-box selection ranks `http.route` first with the expected shares, risk ratio, and participation
- [ ] 4.2 End-to-end logs test: `extract`-derived field is comparable; `body` appears in `skipped` as retrieval-only
- [ ] 4.3 Tenant isolation test: identical compare documents from two tenants return only their own data

## 5. UI (src/ui)

- [ ] 5.1 Failing component tests for `ComparePanel`: ranked list order follows `score`, participation shown, `truncated` and `skipped` stated, field-name text filter keeps ranked order
- [ ] 5.2 Implement `features/compare/`: `useComparison` hook building the IR v4 document from the tab's active filters + range via the generated client; `ComparePanel`, `DimensionBars`, `MeasureHistogram` (paired bars / overlaid histogram, two fixed cohort colours) through the shared `VizTooltip`
- [ ] 5.3 Heatmap entry point: drag-to-select overlay on `TraceVolumeHeatmap` emitting duration + time bounds; opens the panel with the box as `selection` and shows both cohort counts (test)
- [ ] 5.4 Group-row entry point: "compare to the rest" action on grouped traces and logs tables emitting `field = value` (test)
- [ ] 5.5 Refinement actions: "only this value" / "exclude this value" / "group by this field" on dimension bars, "below" / "above" on measure buckets, dispatching to the existing filter and grouping state; tests that a filter becomes visible and removable and that grouping switches view
- [ ] 5.6 Carry `selection`, scope, and field filter in URL search params; test that reload reopens and re-runs the comparison

## 6. Docs and skills

- [ ] 6.1 `docs/users/querying-ir.md`: `compare` stage reference (document shape, semantics, bounds, statistic name and formula with citations: Wu & Madden 2013; Bailis et al. 2017; Abuzaid et al. 2018; Roy & Suciu 2014; Lin 1991), `comparison` envelope, examples for heatmap-box and group-vs-rest; add v4 to the version table
- [ ] 6.2 UI docs page for the comparison panel (route via the docs skill), CLI and MCP mentions where the IR surfaces are documented
- [ ] 6.3 Update the `tempo-api` / query-surface skill entries that enumerate IR stages and envelopes; run the doc-freshness gate after committing
- [ ] 6.4 Update `openspec/specs` via archive after merge; run `cargo fmt`, `cargo clippy --workspace --all-targets --all-features`, `cargo machete --with-metadata`, and `/simplify` before each commit
