## Why

Teams building AI agents need to know whether an agent still behaves as
specified before they ship a new version. The usual loop is offline: replay a
fixed eval set on the candidate version, score every case with evaluators
(code checks, LLM judges, classifiers), then compare against the last good
version case by case. Today that loop lives in spreadsheets and CI logs,
disconnected from the traces that explain _why_ a case failed.

SignalDB already stores those traces. OpenTelemetry's GenAI conventions give
agent runs a standard shape (`invoke_agent` → `chat` / `execute_tool` spans)
and define a `gen_ai.evaluation.result` event for a single score, but nothing
in the conventions covers eval sets, runs or baselines. The Claude Design
handoff "Agent evaluations" (`templates/evals/Evals.dc.html`) adds an
Evaluate section to the app for exactly this loop. This change defines the
contract behind it, offline evals first.

## What Changes

- **Wire contract for eval results.** An evaluator result is an OTLP log
  record with `event_name = gen_ai.evaluation.result`, carrying the
  `gen_ai.evaluation.*` attributes and the trace/span context of the span it
  scores. SignalDB-specific attributes (`signaldb.eval.run_id`,
  `signaldb.eval.set`, `signaldb.eval.case_id`, `signaldb.eval.evaluator`)
  group results into offline runs, since semconv has no such layer. No
  ingest change: these are ordinary log records.
- **Derived semantics**, identical in every surface: pass/fail per result,
  evaluator errors (never counted as failures), runs, per-case regressions
  and improvements between two runs, and evaluators discovered from the
  results.
- **Evaluate section in the UI** (new sidebar group): Agents & scores,
  Compare, case drilldown with a span-level waterfall, Runs, Evaluators —
  all Query IR reads over `logs` (results) and `traces` (agent spans).
- **Eval sets** (phase 2): stored, versioned lists of cases (input, expected
  tool trajectory, reference answer), managed over a new HTTP API, the CLI
  and the UI, including building a set from real traces and saving a
  comparison's regressions as a new set.
- **Results upload** (phase 2): `POST` a JSONL/CSV results file (UI dialog,
  `signaldb-cli evals upload` with `--fail-if` gates for CI). The server
  validates the file and writes it through the normal log ingest path, so
  uploaded and OTLP-sent results share one read model.

Not breaking: no change to OTLP ingest, Tempo/LogQL/PromQL, Flight schemas
or the WAL/Iceberg layout. Phase 1 adds no HTTP endpoint.

## Capabilities

### New Capabilities

- `agent-evaluation-results`: the eval-result wire contract and the
  semantics derived from it (pass rule, errors, runs, comparisons,
  evaluators).
- `explore-ui-evaluate`: the Evaluate pages.
- `agent-eval-sets`: eval set storage, API and CLI (phase 2).
- `agent-eval-results-upload`: results file upload, API and CLI (phase 2).

### Modified Capabilities

- `explore-ui-navigation`: a fourth sidebar group, Evaluate, and nested
  paths that keep their own sidebar item current.

## Impact

- **Phase 1 — src/ui only**: `api/evals.ts` (IR documents and decoders),
  `features/evals/*`, `features/shell/navModel.ts` + `NavIcon.tsx`,
  `routes.tsx`, e2e navigation spec, Storybook page stories and
  design-sync registration. No HTTP API, CLI or SDK change: every figure is
  a Query IR read, and the IR already queries `logs` by `event_name` and
  attribute.
- **Phase 2**: `router` (eval set and upload endpoints, OpenAPI), `common`
  (catalog tables for eval sets, results-file parser), `signaldb-sdk`,
  `signaldb-cli` (`evals` commands), `mcp-server` (read tools), the
  regenerated TypeScript client, UI pages for sets and the upload / new-set
  dialogs, docs.
- **Surface parity**: phase 1 is reachable in the UI and through the Query
  IR API/CLI (`signaldb-cli query`) with documented example documents; the
  dedicated `evals` CLI commands arrive with phase 2.
