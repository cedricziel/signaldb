## Phase 1 — read side (UI + Query IR)

## 1. Data

- [ ] 1.1 Tests for the pass rule, run status, case classification and tool-trajectory alignment (`features/evals/evalModel.test.ts`).
- [ ] 1.2 `features/evals/evalModel.ts`: pass rule, run status, compare classification, LCS tool diff.
- [ ] 1.3 Tests for the IR documents and decoders (`api/evals.test.ts`, `runIrQuery` mocked).
- [ ] 1.4 `api/evals.ts`: results by evaluator and day, runs, per-run per-case results, evaluators, agent-span coverage, tool spans and latency/tokens by trace id.

## 2. Navigation

- [ ] 2.1 Tests: Evaluate group order, longest-prefix current page, palette entries (`navModel.test.ts`, `AppNav.test.tsx`).
- [ ] 2.2 Evaluate group in `navModel.ts`, icons in `NavIcon.tsx`, routes in `routes.tsx`.
- [ ] 2.3 e2e: `/evals/runs` highlights Runs (`e2e/navigation.spec.ts`).

## 3. Pages

- [ ] 3.1 Page tests for Agents & scores, Runs, Compare, case drilldown and Evaluators, including the empty and judge-error states.
- [ ] 3.2 Agents & scores (`/evals`).
- [ ] 3.3 Runs (`/evals/runs`) with its empty state.
- [ ] 3.4 Compare (`/evals/compare`).
- [ ] 3.5 Case drilldown (`/evals/compare/case`) with the span waterfall and evaluator cards.
- [ ] 3.6 Evaluators (`/evals/evaluators`).
- [ ] 3.7 Storybook `Pages/*` stories (Default + Dark) and design-sync registration for each page.

## 4. Docs

- [ ] 4.1 `docs/users/evaluations.md`: the result record, run attributes, pass rule, example IR documents for each page, and a Python snippet emitting a result.
- [ ] 4.2 `docs/users/explore-ui.md`: the Evaluate section.

## Phase 2 — eval sets and uploads

## 5. Eval sets API

- [ ] 5.1 Catalog migration tests for `eval_sets` / `eval_cases` (`cargo test -p common`).
- [ ] 5.2 Catalog tables and repository in `common`.
- [ ] 5.3 Router handler tests: CRUD, append from traces, JSONL export, tenant isolation, privilege checks (`cargo test -p router`).
- [ ] 5.4 `router/src/endpoints/evalsets.rs` and OpenAPI registration; `cargo xtask generate`.
- [ ] 5.5 Regenerate the Rust SDK (`src/signaldb-sdk`) and the TypeScript client (`src/ui/src/api/gen`).

## 6. Results upload

- [ ] 6.1 Parser tests for JSONL/CSV rows, column errors and run-level rows (`cargo test -p common`).
- [ ] 6.2 Results parser and conversion to `gen_ai.evaluation.result` log records.
- [ ] 6.3 `POST /api/v1/evals/results` handler tests, then the handler writing through the log ingest path; OpenAPI and both clients regenerated.
- [ ] 6.4 Integration test in `tests-integration`: upload → the run is queryable over the IR with the right pass rate.

## 7. Span-event results

- [ ] 7.1 Acceptor test: a `gen_ai.evaluation.result` span event becomes a log record with the span's trace context (`cargo test -p acceptor`).
- [ ] 7.2 Fan-out in the acceptor's trace ingest path.

## 8. Surfaces

- [ ] 8.1 CLI: `signaldb-cli evals sets …` and `evals upload` with `--compare-to` / `--fail-if`, with tests.
- [ ] 8.2 UI: Eval sets list and detail, New eval set dialog, Add traces panel, Upload results dialog, "Save regressed cases as eval set".
- [ ] 8.3 MCP: read tools for runs and comparisons (`mcp-server`), with tests.
- [ ] 8.4 Docs: eval sets, results file format, CI gate; update the `http-api` and `crate-map` skills if their described behaviour changes.
