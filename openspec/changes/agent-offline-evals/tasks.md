## Phase 1 — read side (UI + Query IR)

## 1. Data

- [x] 1.1 Tests for the pass rule, run status, case classification and tool-trajectory alignment (`features/evals/evalModel.test.ts`).
- [x] 1.2 `features/evals/evalModel.ts`: pass rule, run status, compare classification, LCS tool diff.
- [x] 1.3 Tests for the IR documents and decoders (`api/evals.test.ts`, `runIrQuery` mocked).
- [x] 1.4 `api/evals.ts`: results by evaluator and day, runs, per-run per-case results, evaluators, agent-span coverage, tool spans and latency/tokens by trace id.

## 2. Navigation

- [x] 2.1 Tests: Evaluate group order, longest-prefix current page, palette entries (`navModel.test.ts`, `AppNav.test.tsx`).
- [x] 2.2 Evaluate group in `navModel.ts`, icons in `NavIcon.tsx`, routes in `routes.tsx`.
- [x] 2.3 e2e: `/evals/runs` highlights Runs (`e2e/navigation.spec.ts`).

## 3. Pages

- [x] 3.1 Page tests for Agents & scores, Runs, Compare, case drilldown and Evaluators, including the empty and judge-error states.
- [x] 3.2 Agents & scores (`/evals`).
- [x] 3.3 Runs (`/evals/runs`) with its empty state.
- [x] 3.4 Compare (`/evals/compare`).
- [x] 3.5 Case drilldown (`/evals/compare/case`) with the span waterfall and evaluator cards.
- [x] 3.6 Evaluators (`/evals/evaluators`).
- [x] 3.7 Storybook `Pages/*` stories (Default + Dark) and design-sync registration for each page.

## 4. Docs

- [x] 4.1 `docs/users/evaluations.md`: the result record, run attributes, pass rule, example IR documents for each page, and a Python snippet emitting a result.
- [x] 4.2 `docs/users/explore-ui.md`: the Evaluate section.

## Phase 2 — eval sets and uploads

## 5. Eval sets API

- [x] 5.1 Catalog migration tests for `eval_sets` / `eval_cases` (`cargo test -p common`).
- [x] 5.2 Catalog tables and repository in `common`.
- [x] 5.3 Router handler tests: CRUD, append cases, tenant isolation, privilege checks (`cargo test -p router`). Appending from a trace query follows in its own task (5.6).
- [x] 5.4 `router/src/endpoints/eval_sets.rs`, `evals:read`/`evals:write` scopes, and OpenAPI registration; `cargo xtask generate`.
- [x] 5.5 Regenerate the Rust SDK (`src/signaldb-sdk`) and the TypeScript client (`src/ui/src/api/gen`).
- [x] 5.6 Append cases from a trace query (`invoke_agent` spans by filter, run through the Query IR), with tests.

## 6. Results upload

- [x] 6.1 Parser tests for JSONL/CSV rows, column errors and run-level rows (`cargo test -p common`).
- [x] 6.2 Results parser and conversion to `gen_ai.evaluation.result` log records.
- [x] 6.3 `POST /api/v1/evals/results` handler tests, then the handler writing through the log ingest path; OpenAPI and both clients regenerated.
- [x] 6.4 Integration test in `tests-integration`: upload → the run is queryable over the IR with the right pass rate.

## 7. Span-event results

- [x] 7.1 Acceptor test: a `gen_ai.evaluation.result` span event becomes a log record with the span's trace context (`cargo test -p acceptor`).
- [x] 7.2 Fan-out in the acceptor's trace ingest path.

## 8. Surfaces

- [x] 8.1a CLI: eval sets — `signaldb-cli eval-sets list|get|export` and `signaldb-cli admin eval-sets create|replace|delete|append`, with tests.
- [x] 8.1b CLI: `evals upload` with `--compare-to` / `--fail-if`, with tests.
- [ ] 8.2 UI: Eval sets list and detail, New eval set dialog, Add traces panel, Upload results dialog, "Save regressed cases as eval set".
- [x] 8.3a MCP: eval set tools `list_eval_sets`, `get_eval_set`, `create_eval_set`, `replace_eval_set`, `delete_eval_set`, `append_eval_cases` (`mcp-server`), with tests.
- [ ] 8.3b MCP: read tools for runs and comparisons (`mcp-server`), with tests.
- [ ] 8.4 Docs: eval sets, results file format, CI gate; update the `http-api` and `crate-map` skills if their described behaviour changes.
