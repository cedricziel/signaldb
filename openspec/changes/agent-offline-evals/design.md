## Context

Research summary (sources in the change's PR description):

- OTel GenAI conventions (Development stability, now in
  `open-telemetry/semantic-conventions-genai`) define agent spans
  (`invoke_agent {gen_ai.agent.name}` → `chat` / `execute_tool` children,
  with `gen_ai.agent.name|id|version`, `gen_ai.tool.name`,
  `gen_ai.usage.input_tokens|output_tokens`) and one evaluation event,
  `gen_ai.evaluation.result`: `gen_ai.evaluation.name` (required),
  `gen_ai.evaluation.score.value`, `gen_ai.evaluation.score.label`,
  `gen_ai.evaluation.explanation`, `gen_ai.response.id`, `error.type`. It is
  parented to the scored operation's span. There is no dataset, case, run or
  baseline concept.
- Langfuse, LangSmith, Braintrust, Phoenix, Inspect AI, promptfoo and
  DeepEval converge on one model: dataset → case (input, expected,
  reference, metadata) → run/experiment (one app version on one dataset) →
  per-case output + trace → evaluator (code, LLM judge, classifier, human) →
  score (value and/or label + explanation). Runs are compared against a
  baseline in aggregate and per case (regressions / improvements).
- Checking behaviour against a spec means scoring the trajectory (which
  tools, in which order, with valid arguments), not only the final answer;
  scoring individual spans; and treating judge errors as their own signal,
  never as failures.

Current SignalDB state that shapes the approach:

- The Query IR queries `logs` by `event_name` and any record attribute, with
  scoped aggregates (`count` where label = pass) and `in` predicates. It can
  read only the _first_ span event of a given name (the `exception.*`
  special case), so several `gen_ai.evaluation.result` span events on one
  span are not addressable today.
- Trace detail, waterfall geometry (`lib/waterfall.ts`), `KpiCard`,
  `Sparkline`, `EmptyState`, `Dialog`, `FilterChips` and `VizTooltip`
  already exist in `src/ui`.
- FDAP alignment is unaffected in phase 1 (no Arrow/Parquet/Flight schema
  change). Phase 2 stores eval sets in the SQL catalog, not in Iceberg, and
  writes uploaded results as ordinary log records, so there is no Flight v1
  / storage transform, WAL or Iceberg migration either. Any Arrow types the
  upload path builds use the ones re-exported by DataFusion.

## Goals / Non-Goals

**Goals:**

- A user can answer "did version B of my agent get worse than version A on
  the same cases, where, and why" without leaving SignalDB.
- Results arrive over plain OTLP with no SignalDB SDK; a file upload and a
  CI command cover harnesses that don't export OTel.
- One read model: every page is a Query IR read, so the CLI, MCP and
  third-party tools can reproduce any figure.

**Non-Goals:**

- Running agents or judges. SignalDB stores, links and compares results; the
  user's harness replays the eval set and scores it.
- Online evaluation dashboards beyond what falls out of the same pages (the
  "Production" source toggle shows results without a run id; alerting on
  them is out of scope).
- Human annotation queues, pairwise judges, judge calibration workflows.
- Multi-trial statistics (pass@k / pass^k). The contract reserves
  `signaldb.eval.trial` so harnesses can send it now; the pages average
  trials until a follow-up adds the statistics.

## Decisions

### D1. An eval result is a log record, not a span event

The harness emits `gen_ai.evaluation.result` through the OTel Logs API (the
Events API successor): `event_name = gen_ai.evaluation.result`, attributes
as in semconv, and `trace_id` / `span_id` set to the span it scores.

- Offline judges run after the agent span has ended, so `span.add_event`
  on the scored span is usually impossible; a log record with explicit
  trace context is not.
- One record per result keeps several evaluators on one span independent
  rows, which the IR already aggregates. Span events would need an IR
  "unnest events" stage first.

Alternative considered: accept span events too, normalising them at ingest
(acceptor fans each `gen_ai.evaluation.result` span event out into a log
record). Deferred to phase 2 task 7.x; it's additive, since the read model
stays the logs table.

### D2. SignalDB attributes carry the offline layer

| Attribute                                   | Meaning                                                                                                         |
| ------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `signaldb.eval.run_id`                      | groups results into one offline run; absent = production (online) result                                        |
| `signaldb.eval.set`                         | eval set name the run replayed                                                                                  |
| `signaldb.eval.case_id`                     | case within the set; the join key between runs                                                                  |
| `signaldb.eval.evaluator`                   | evaluator implementation and version, e.g. `trajectory-match@2.1.0`                                             |
| `signaldb.eval.trial`                       | optional trial index for multi-trial runs                                                                       |
| `gen_ai.agent.name`, `gen_ai.agent.version` | agent identity on the result itself (falls back to `service.name` / `service.version` of the record's resource) |

Agent identity is copied onto the result so grouping by agent and version is
one IR query over `logs`, with no join against spans.

### D3. Pass rule and errors

A result with `error.type` set is an **evaluator error**: excluded from pass
rates and means, counted separately, never a failure. Otherwise, the label
decides when present: `pass`, `passed`, `true`, `yes`, `correct`, `safe`
pass; `fail`, `failed`, `false`, `no`, `incorrect`, `unsafe` fail
(case-insensitive). With no recognised label, a numeric score of at least
0.5 passes. Any other result is scored but has no pass/fail. The rule lives
in one tested module in the UI now and in `common` when phase 2 needs it
server-side (CLI `--fail-if`).

### D4. Runs are derived, not registered

A run is the set of results sharing `signaldb.eval.run_id`. Its eval set,
agent and version come from the results; its time is the first result's.
Status: **in progress** while the newest result is under 10 minutes old,
then **complete**, or **partial** when it has evaluator errors or results
with no trace context. Uploads (phase 2) produce runs the same way, so the
Runs page never needs a registry.

### D5. Comparing two runs

Compare takes a baseline run and a candidate run (normally the same eval
set). Cases join on `signaldb.eval.case_id`. Per evaluator, a case is
**worse** when it went pass → fail or its mean score dropped by at least
0.05, **better** on the reverse; a case with any worse evaluator is a
**regression** (even if another evaluator improved), otherwise with any
better one an **improvement**, else **unchanged**. A case present only in
the candidate is listed under regressions marked "no baseline" when it
fails, else under unchanged. Latency p95 and tokens per run come from the
`invoke_agent` spans of each run's traces. The tool trajectory column
aligns the ordered `execute_tool` names of the two traces (LCS): skipped,
reordered/repeated and new calls are marked.

### D6. Pages and paths

`/evals` (Agents & scores), `/evals/compare`, `/evals/compare/case`,
`/evals/sets` (phase 2), `/evals/runs`, `/evals/evaluators`. The sidebar
matches the longest path prefix, so every page keeps its own item current;
the case drilldown highlights Compare. Compare and the drilldown keep their
selection in the query string (`baseline`, `candidate`, `case`, `mode`), so
links are shareable.

### D7. Phase 2 storage and API (sketch)

Eval sets live in the SQL catalog (`eval_sets`, `eval_cases`), per tenant
and dataset, following the processors pattern:
`GET|POST /api/v1/eval-sets`, `GET|PUT|DELETE /api/v1/eval-sets/{name}`,
`POST /api/v1/eval-sets/{name}/cases` (append, and later from a trace
query). JSONL export is written by the clients from the set's cases.
New API-key scopes `evals:read` and `evals:write` guard them (and, later,
the upload endpoint). Uploads:
`POST /api/v1/evals/results` with the file and run metadata, validated
synchronously (400 with row errors), then converted to log records and
written through the ingest path. Every endpoint has an explicit operation
id and per-endpoint privilege checks (http-api skill). Eval sets are the
reference shape for those rules (`ApiError` bodies, `_links`), where
processors still lag.

## Risks / Trade-offs

- **Semconv churn** (Development stability): attribute names may change.
  → Names live in one UI module and one `common` module; the docs page
  tracks the semconv version.
- **Harnesses that only emit span events** see nothing in phase 1. → The
  empty state and docs show the log-record form; phase 2 normalises span
  events at ingest.
- **Large runs**: Compare fetches spans for every case's trace (`trace_id
in [...]`). → Bounded by the eval set size (hundreds of cases); pages
  query tool spans only for the cases on screen.
- **Label vocabulary**: custom labels (e.g. `partial`) have no pass/fail.
  → Shown as scored-without-verdict; a per-evaluator label mapping can
  follow once eval sets store evaluator settings.

## Migration Plan

Additive. Phase 1 is UI only and can be reverted by reverting the UI
commits. Phase 2 adds catalog tables through the existing migration
mechanism; rollback drops them without touching telemetry.

## Open Questions

- Should the Production source reuse the same pages long term, or become a
  separate "online evals" view with sampling-aware statistics?
- Expected-trajectory checks: SignalDB could compute ToolTrajectory itself
  from the eval set and the trace. Deferred until eval sets exist.
