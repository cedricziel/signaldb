# Tasks

## 1. Make the planner callable

- [x] 1.1 Extract a `plan_document(ctx, doc, resolver, …) -> DataFrame` entry
      point from `IrService`'s existing path, and have `IrService` call it, so
      there is one planner rather than two doors into it. No behaviour change;
      `cargo test -p querier` must pass untouched.
      (The resolver is not actually a parameter — see design.md's D1 note:
      it is built internally from the scanned schema, which is what keeps
      resolution promotion-invariant.)
- [x] 1.2 Make `SchemaResolver` and `SourcePlan` `pub(crate)`, and nothing else
      from `ir_planner`. Exposing helpers instead of one entry point would
      re-create the coupling this change removes (D1).

## 2. Differential harness — before anything moves

- [x] 2.1 Build the corpus: every query in `router_tempo_endpoints.rs`,
      `logql_queries.rs` and `query_parity.rs`, plus the `ql-ir` test corpora.
      Record the count so later additions are visible.
      (`query_parity.rs` contributed nothing — verified before writing the
      harness that it carries CLI/MCP operation-surface parity, not query
      text; see `differential.rs`'s module doc. Corpus: 32 queries — 14
      TraceQL, 3 tags, 8 LogQL log, 7 LogQL metric — plus 6 adversarial
      cases.)
- [x] 2.2 Add adversarial cases: a promoted vs unpromoted attribute, a
      mixed-case label (#1070), an absent value, and an attribute key colliding
      with a physical column name.
- [x] 2.3 Write the harness: for each query, lower via both paths and compare
      the **optimized** logical plans (D2 — the raw expression trees differ
      legitimately and DataFusion normalises them).
- [x] 2.3a Compare **rejections**, not only plans. A query one path refuses and
      the other accepts produces no plan to diff, so it is exactly the case a
      plan-only harness cannot see. Assert the same accept/reject decision and
      the same error class — the 400-vs-501 split `publishable-ql-crates`
      established is user-visible and must survive.
- [x] 2.3b Compare **endpoint responses** for a fixture dataset on the queries
      that reach one. Identical plans are strong evidence and not proof: the
      compat layer assembles Tempo and Loki shapes downstream of the plan, and
      a projection or column-name change would pass a plan diff while altering
      what a client receives.
      (traces: done in §3 — `tests-integration::router_tempo_endpoints::test_search_filters_are_applied_via_ir`
      and the `query::trace::tests::search_via_ir_matches_old_path_for_*`
      unit tests exercise `find_traces_with_tenant` end to end with the
      switch both ways; logs: §4 —
      `tests-integration::logql_queries`'s three `_via_ir` twins exercise
      `query_logs`/`query_metric` end to end through the full HTTP/Loki stack,
      and `differential::query_metric_via_ir_buckets_by_the_callers_step_not_the_range_literal`/
      `query_metric_via_ir_value_column_is_float64_like_the_old_path` pin the
      two schema-parity corrections `query_metric_via_ir` applies — exactly
      the "plan is identical but the assembled response would differ" case
      this task exists to catch, since a plan diff alone can't see a
      post-`plan_document` projection.)
      (Completed per signal in §3/§4, where the switch makes both paths
      reachable from the endpoint.)
- [x] 2.4 Run it and triage every difference. Each is a finding about one of
      the two lowerings; record which was wrong. **Do not proceed past this task
      with an unexplained difference.**
      (Six findings, all triaged in `differential.rs`'s module doc: one fixed
      [ql-ir status/kind casing], five reported/pinned rather than fixed
      — line-filter body-retrievability, unscoped-attribute OR-vs-coalesce
      combining semantics, absent-value `!=` semantics, ungrouped
      range-aggregation default grouping, and a still-open #1070-class bug
      in the old LogQL metric path. See the report to the requesting agent
      for the full detail — several of these are significant and need a
      product/design decision before §3/§4.)
- [x] 2.5 Answer open question 1: does plan comparison hold for aggregates, or
      only filters? If not, define the weaker equivalence (row-level results
      over a fixture) the metric path needs, and say so here.
      (No — see design.md's Open Questions, now answered: row-level
      equivalence over a fixture, implemented in
      `logql_metric_corpus_row_level_equivalence`.)
- [x] 2.6 Answer open question 2: grep for tests asserting `search_filter`'s or
      `logql.rs`'s expression _shape_ (`Debug` output). Rewrite any against
      behaviour before the shape changes under them.
      (Only `search_filter.rs`'s own unit tests do this, and §5 deletes them
      along with the code — see design.md's Open Questions.)

## 3. Traces

- [x] 3.0 Fix the promoted-column gap for scope-qualified fields in
      `ir_planner::SchemaResolver::column_for` (D10): strip the scope before
      `materialized_column_name`, as `Lowering::qualified_attr` does. Failing
      test first; the harness's `adversarial_promoted_attribute_agrees_on_result`
      then moves from a row-level to a plan-level comparison.
- [x] 3.1 **Failing test first**: extend the trace-search integration coverage
      with a query whose result depends on attribute promotion, and confirm it
      passes on the old path — the regression net for 3.3.
- [x] 3.2 Add the `Condition`-to-IR shim for Tempo's `tags` parameter, in the
      querier (D4 — `tags` is an HTTP encoding, not a language, so it does not
      belong in `ql-ir`).
- [x] 3.3 Route trace search through `ql_ir::traceql_to_ir` and
      `plan_document`, behind the per-signal switch (D3), defaulting to the old
      path. The switch governs **the whole trace-search filter**, not `q`
      alone: a request may carry `q`, `tags`, both, or neither, and the two
      contribute conditions to one conjunction. Splitting them across two
      lowerings would produce a filter neither path was tested for. - `q` only → lower the text - `tags` only → lower the conditions via the 3.2 shim - both → one document conjoining them, in the order the old path used - neither → no filter stage, exactly as today
- [x] 3.4 `cargo test -p querier -p tests-integration` green with the switch
      both ways. `test_search_filters_are_applied` must pass unmodified in both
      — including the 400/501 assertions from `publishable-ql-crates`. Add a
      case sending `q` and `tags` together, since 3.3 makes that one document
      and no existing test covers the combination.

## 4. Logs

- [x] 4.0a Make `logs.body` filterable for string operators (D6): `LogicalSchema::core()`
      and its tests, the planner's retrieval-only test (use `span_events`),
      `docs/users/querying-ir.md`, any generated schema listing. Failing test
      first: a `where body contains` document plans and executes. The harness's
      `logql_line_filter_is_rejected_by_the_real_schema` pin flips to agreement.
      (Renamed to `logql_line_filter_agrees_on_optimized_plan`; new planner
      tests `body_is_filterable_for_string_operators` added.)
- [x] 4.0b `ql-ir`: default an ungrouped range aggregation's `by` to the stream
      identity (D7), pinned in the querier against `logs.rs::SERIES_COLUMNS`.
      The harness's ungrouped-aggregation pin flips to agreement.
      (`ql_ir::STREAM_IDENTITY` added and pinned by
      `ql_ir_stream_identity_matches_series_columns` through the real
      `SchemaResolver`; the divergence test renamed to
      `adversarial_ungrouped_range_aggregation_default_grouping_agrees`.)
- [x] 4.0c `ql-ir`: encode Loki's absent-matches semantics for `!=`, `!~`, `=""`
      (D9). The harness's absent-value pin flips to agreement.
      (`=""` has no old-path precedent — see
      `empty_string_equality_matches_an_absent_field_on_the_new_path`'s doc —
      so it is a new-path-only regression test, not an old/new pin.)
- [x] 4.0d File an issue for the #1070 bug still present in `logs.rs::execute_plan`
      (mixed-case attribute grouping on the old metric path); reference it from 4.2.
      (Filed as #1392.)
- [x] 4.1 Route LogQL through `ql_ir::logql_to_ir` for what it covers, behind
      its own switch, **falling back to the old lowering on `Inexpressible`**
      (D5 — a working query must not regress into a 501).
      (`QuerierConfig::logql_via_ir`; `LogsService::query_logs_via_ir`/
      `query_metric_via_ir`, sharing `lower_and_plan_via_ir`'s D5
      classification. Two schema-parity corrections found and fixed along
      the way, documented on `query_metric_via_ir`: the aggregate's `step`
      must be the caller's `params.step`, not `ql_ir`'s range-literal
      default, and `value` must be cast to `Float64` — `ir_planner`'s
      `count` aggregate is `Int64`, which the router's `batches_to_matrix`
      silently reads as `0.0`.)
- [x] 4.2 Record which corpus queries take the fallback. That set is the
      remaining IR expressiveness gap and the input to any successor change.
      (Recorded in design.md's new "The fallback set as of §4" subsection
      under D5.)
- [x] 4.3 `cargo test -p querier -p tests-integration` green with the switch
      both ways.
      (`cargo test -p querier -p ql-ir -p common --lib`: 641+414 passed;
      `cargo test -p tests-integration --test integration logql_queries::`:
      9 passed, including the three new `_via_ir` twins
      (`logql_stream_query_returns_all_lines_for_service_via_ir`,
      `logql_line_filter_narrows_to_matching_lines_via_ir`,
      `logql_metric_query_count_over_time_returns_matrix_via_ir`) exercising
      the full ingest→store→query stack with the switch on.)

## 5. Delete the duplication

- [ ] 5.1 With both switches on and differential evidence green, delete
      `search_filter.rs`'s lowering half. Keep `parse_tags` and `take_value`.
- [ ] 5.2 Delete the portion of `logql.rs`/`logql_metric.rs` that `ql-ir`
      covers. What backs the 4.2 fallback set stays.
- [ ] 5.3 Remove both switches and their config keys (D3 — a rollout switch
      that outlives its rollout is a second untested path).
- [ ] 5.4 Confirm the harness still passes against the remaining fallback path,
      then decide whether to keep it as a permanent regression test or retire
      it with the code it compared. Say which, and why, in the PR.

## 6. Docs and skills

- [ ] 6.1 Update the `architecture` skill: the query path has one lowering, and
      compat surfaces reach it through `ql-ir`.
- [ ] 6.2 Update `docs/architecture/fdap.md` — its DataFusion section says each
      parsed query "is lowered to DataFusion `Expr`s and logical plans
      directly", which stops being how traces and logs work.
- [ ] 6.3 Update the `crate-map` skill entries for the querier modules that
      shrink or disappear.
- [ ] 6.4 Update `docs/contributing/compat-crates.md`: the rule "lowering lives
      in the querier" becomes "lowering targets the IR".
- [ ] 6.5 Run the docs-freshness gate **after committing**, and again after any
      fix (it diffs committed history and cascades code → doc → skill).

## 7. Ship

- [ ] 7.1 Run `/simplify` over the changed code.
- [x] 7.2 File the tracking issue this change lacks; add `Closes #N` to the PR.
      (Filed as #1382; the final PR of the stack carries `Closes #1382`.)
- [ ] 7.3 Split into a stack: §1–2 (seam + harness), §3 (traces), §4 (logs),
      §5 (deletion). Each is independently revertible, which is the point of
      the ordering.
- [ ] 7.4 Open each PR; check for CodeRabbit findings and act on them.
