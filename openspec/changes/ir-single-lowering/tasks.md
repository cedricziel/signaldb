# Tasks

## 1. Make the planner callable

- [ ] 1.1 Extract a `plan_document(ctx, doc, resolver, …) -> DataFrame` entry
      point from `IrService`'s existing path, and have `IrService` call it, so
      there is one planner rather than two doors into it. No behaviour change;
      `cargo test -p querier` must pass untouched.
- [ ] 1.2 Make `SchemaResolver` and `SourcePlan` `pub(crate)`, and nothing else
      from `ir_planner`. Exposing helpers instead of one entry point would
      re-create the coupling this change removes (D1).

## 2. Differential harness — before anything moves

- [ ] 2.1 Build the corpus: every query in `router_tempo_endpoints.rs`,
      `logql_queries.rs` and `query_parity.rs`, plus the `ql-ir` test corpora.
      Record the count so later additions are visible.
- [ ] 2.2 Add adversarial cases: a promoted vs unpromoted attribute, a
      mixed-case label (#1070), an absent value, and an attribute key colliding
      with a physical column name.
- [ ] 2.3 Write the harness: for each query, lower via both paths and compare
      the **optimized** logical plans (D2 — the raw expression trees differ
      legitimately and DataFusion normalises them).
- [ ] 2.4 Run it and triage every difference. Each is a finding about one of
      the two lowerings; record which was wrong. **Do not proceed past this task
      with an unexplained difference.**
- [ ] 2.5 Answer open question 1: does plan comparison hold for aggregates, or
      only filters? If not, define the weaker equivalence (row-level results
      over a fixture) the metric path needs, and say so here.
- [ ] 2.6 Answer open question 2: grep for tests asserting `search_filter`'s or
      `logql.rs`'s expression _shape_ (`Debug` output). Rewrite any against
      behaviour before the shape changes under them.

## 3. Traces

- [ ] 3.1 **Failing test first**: extend the trace-search integration coverage
      with a query whose result depends on attribute promotion, and confirm it
      passes on the old path — the regression net for 3.3.
- [ ] 3.2 Add the `Condition`-to-IR shim for Tempo's `tags` parameter, in the
      querier (D4 — `tags` is an HTTP encoding, not a language, so it does not
      belong in `ql-ir`).
- [ ] 3.3 Route `trace.rs`'s `q` handling through `ql_ir::traceql_to_ir` and
      `plan_document`, behind the per-signal switch (D3), defaulting to the old
      path.
- [ ] 3.4 `cargo test -p querier -p tests-integration` green with the switch
      both ways. `test_search_filters_are_applied` must pass unmodified in both
      — including the 400/501 assertions from `publishable-ql-crates`.

## 4. Logs

- [ ] 4.1 Route LogQL through `ql_ir::logql_to_ir` for what it covers, behind
      its own switch, **falling back to the old lowering on `Inexpressible`**
      (D5 — a working query must not regress into a 501).
- [ ] 4.2 Record which corpus queries take the fallback. That set is the
      remaining IR expressiveness gap and the input to any successor change.
- [ ] 4.3 `cargo test -p querier -p tests-integration` green with the switch
      both ways.

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
- [ ] 7.2 File the tracking issue this change lacks; add `Closes #N` to the PR.
- [ ] 7.3 Split into a stack: §1–2 (seam + harness), §3 (traces), §4 (logs),
      §5 (deletion). Each is independently revertible, which is the point of
      the ordering.
- [ ] 7.4 Open each PR; check for CodeRabbit findings and act on them.
