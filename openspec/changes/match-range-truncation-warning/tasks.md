# Tasks

One PR (under 500 changed lines, excluding regenerated clients). Additive. The
querier and router may be deployed in either order (design D3).

## 1. Trailer report carries match incompleteness (common)

- [x] 1.1 Write failing tests in `common::flight`: a report with
      `matchIncomplete` round-trips through `correlate_report_trailer` /
      `parse_correlate_report_trailer`; a trailer JSON with an unknown member
      still parses (forward compatibility); a pre-change trailer without the
      member parses to `match_incomplete: None`; the legacy
      `{"correlate_truncated":true}` form still maps to `row_limit` only.
      Verify with `cargo test -p common flight::`
- [x] 1.2 Rename `CorrelateReport` to `QueryReport` (keep a
      `pub type CorrelateReport = QueryReport;` alias), add
      `match_incomplete: Option<MatchIncompleteReport { matched, unmatched,
    sample_trace_ids }>` with `#[serde(default)]`, and keep the
      `correlate_report:` prefix. 1.1 passes

## 2. Evaluator counts visibly incomplete traces (querier)

- [ ] 2.1 Write failing tests in `query::structural_match::tests` using the
      existing `span`/`chain`/`batch` fixtures, extended with an end time per
      span:
      (a) a `descendant` match over a trace whose root is out of range
      (dangling parent) is counted as `unmatched`, and as `matched` when a
      lower in-range pair still matches;
      (b) an in-range span ending after `window_end_ns` is counted;
      (c) a fully in-range trace is not counted;
      (d) a relation-free match counts nothing;
      (e) at most 3 sample ids, matched first;
      (f) the witness rows equal the pre-change output for every fixture
      (no result change).
      Verify with `cargo test --profile ci-test -p querier structural_match`
- [ ] 2.2 Return the dangling-parent fact from `evaluate` (from the `index`
      lookup it already does), add the end-time check in `finish_trace`, add
      `window_end_ns` to `Spec`, and keep `end_time_unix_nano` in the exec
      input. Put the shared counters on `StructuralMatchExec`, outside
      `Spec`. 2.1 passes
- [ ] 2.3 Plumb the counters through `lower_match` → the plan outcome → the
      `QueryReport` built in `IrService::query`. Add an `ir_planner` test that
      a v12 `match` document over a straddling fixture yields
      `match_incomplete` in the report.
      Verify with `cargo test --profile ci-test -p querier ir_planner::`

## 3. Router warning, API contract, clients, docs

- [x] 3.1 Write a failing router unit test next to `correlate_warnings`: a
      report with `match_incomplete` produces exactly one
      `match_incomplete_trace` warning whose message carries both counts
      (dropping a zero clause) and the sample ids; a report without it
      produces none. Verify with `cargo test -p router endpoints::query`
- [x] 3.2 Map the report to the warning (design D4) and add
      `match_incomplete_trace` to the `QueryWarning.code` doc comment. 3.1
      passes
- [x] 3.3 Regenerate the OpenAPI spec, the Rust SDK (`src/signaldb-sdk`), and
      the TypeScript client (`src/ui/src/api/gen`). Verify the only diff is
      the `code` description, and that
      `pnpm --filter ./src/ui typecheck && pnpm --filter ./src/ui test`
      passes. The UI's `QueryView` and the CLI's JSON output already show any
      warning, so no UI or CLI code changes (surface parity holds through the
      generated clients)
- [ ] 3.4 Add an integration test in `tests-integration`: ingest a trace whose
      root starts before the query window, submit a v12 `match` with a
      `descendant` relation through `POST /api/v1/query`, and assert the
      `match_incomplete_trace` warning and unchanged rows. Verify with
      `cargo test --profile ci-test -p tests-integration match_incomplete`
- [ ] 3.5 Docs (route via the `docs` skill): in `docs/users/querying-ir.md`,
      extend the "Only spans inside the document's `range` are seen" bullet
      under `match` → Semantics with the warning, its two conditions, and
      "a missing warning does not prove completeness". Add
      `match_incomplete_trace` to the Warnings section with an example.
      Verify that `mkdocs build --strict` (or the repo's docs check) passes
- [ ] 3.6 `cargo fmt`; `cargo clippy -p common -p querier -p router
    --all-targets --all-features -- -D warnings`
