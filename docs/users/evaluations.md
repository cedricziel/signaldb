---
audience: user
type: how-to
status: living
sources:
  - src/ui/src/api/evals.ts
  - src/ui/src/features/evals/**
  - src/common/src/evals/**
  - src/router/src/endpoints/evals.rs
  - src/signaldb-cli/src/commands/evals.rs
  - openspec/changes/agent-offline-evals/**
---

# Evaluating AI agents

SignalDB answers one question for teams shipping AI agents: **does the new
version still behave as specified?** You replay a fixed set of test cases
(an _eval set_) on the new version, score every case with your own
evaluators — code checks, LLM judges, classifiers — and send the scores to
SignalDB. The **Evaluate** pages then compare the new version with the last
one case by case, and link every score to the span it judged.

SignalDB doesn't run your agent or your judges. It stores the results next
to the traces of the replay and does the comparing. Offline evaluation
(before release) comes first; results sent from production traffic show up
under the **Production** source on the same pages.

## What to send

Your agent is traced as usual with the OpenTelemetry GenAI conventions: an
`invoke_agent {gen_ai.agent.name}` span with `chat` and
`execute_tool {gen_ai.tool.name}` children.

Each evaluator result is one **OTLP log record** with
`event_name = gen_ai.evaluation.result` (the OTel GenAI evaluation event),
carrying the trace id and span id of the span it scores:

| Attribute                       | Required    | Meaning                                                              |
| ------------------------------- | ----------- | -------------------------------------------------------------------- |
| `gen_ai.evaluation.name`        | yes         | The evaluator, e.g. `Correctness`. This name _is_ the evaluator.     |
| `gen_ai.evaluation.score.value` | one of both | Numeric score, usually 0–1.                                          |
| `gen_ai.evaluation.score.label` | one of both | Verdict label, e.g. `pass`/`fail`, `safe`/`unsafe`.                  |
| `gen_ai.evaluation.explanation` | no          | The judge's reasoning, shown on the case page.                       |
| `error.type`                    | on failure  | Set when the evaluator itself failed (timeout, bad judge output).    |
| `gen_ai.agent.name`             | recommended | The agent. Falls back to the record's `service.name`.                |
| `gen_ai.agent.version`          | recommended | The agent version. Falls back to the record's `service.version`.     |
| `signaldb.eval.run_id`          | offline     | Groups results into one run. Absent means a production result.       |
| `signaldb.eval.set`             | offline     | The eval set the run replayed.                                       |
| `signaldb.eval.case_id`         | offline     | The case within the set — how two runs are matched.                  |
| `signaldb.eval.evaluator`       | recommended | Evaluator implementation and version, e.g. `trajectory-match@2.1.0`. |
| `signaldb.eval.trial`           | no          | Trial index when a case runs several times (averaged for now).       |

The `signaldb.eval.*` attributes are SignalDB's own: the OTel conventions
score single responses and have no notion of runs or eval sets yet.

Why a log record and not a span event? Judges usually run after the agent
span has ended, when events can no longer be added to it; a log record with
explicit trace context can be sent any time, and several evaluators scoring
one span stay separate results.

With the OpenTelemetry Python SDK (a release whose `LogRecord` takes `event_name`):

```python
from opentelemetry._logs import LogRecord, get_logger

logger = get_logger("eval-harness")

def send_result(scored_span_ctx, case_id, name, score=None, label=None, explanation=None):
    logger.emit(LogRecord(
        event_name="gen_ai.evaluation.result",
        trace_id=scored_span_ctx.trace_id,
        span_id=scored_span_ctx.span_id,
        trace_flags=scored_span_ctx.trace_flags,
        attributes={
            "gen_ai.evaluation.name": name,
            **({"gen_ai.evaluation.score.value": score} if score is not None else {}),
            **({"gen_ai.evaluation.score.label": label} if label else {}),
            **({"gen_ai.evaluation.explanation": explanation} if explanation else {}),
            "gen_ai.agent.name": "support-triage",
            "gen_ai.agent.version": AGENT_VERSION,
            "signaldb.eval.run_id": RUN_ID,
            "signaldb.eval.set": "triage-golden-200",
            "signaldb.eval.case_id": case_id,
            "signaldb.eval.evaluator": "trajectory-match@2.1.0",
        },
    ))
```

Export logs to SignalDB's OTLP endpoint as described in
[Sending OTLP](sending-otlp.md). Score the `invoke_agent` span for
whole-run judgements (correctness, trajectory) and individual
`execute_tool` or `chat` spans for per-step checks (valid tool arguments,
toxicity) — the case page shows each result on the span it scored.

## How results are read

- **Pass rule.** A result with `error.type` is an _evaluator error_: left
  out of pass rates and means, never a failure. Otherwise a recognised
  label decides — `pass`, `passed`, `true`, `yes`, `correct`, `safe` pass;
  `fail`, `failed`, `false`, `no`, `incorrect`, `unsafe` fail
  (case-insensitive). Without one, a score of 0.5 or more passes. Any other
  result counts toward the mean but has no verdict.
- **Runs.** A run is every result sharing a `signaldb.eval.run_id`. It is
  _receiving results_ until it has been quiet for 10 minutes, then
  _complete_, or _partial_ when some results errored or carried no trace
  context (they still count toward the run's scores).
- **Comparing runs.** Cases are matched on `signaldb.eval.case_id`. Per
  evaluator, a case got _worse_ when it went from pass to fail or its mean
  dropped by 0.05 or more, and _better_ the other way round. A case is a
  **regression** when any evaluator got worse (even if another improved),
  an **improvement** when any got better, otherwise **unchanged**.

## The pages

- **Agents & scores** (`/evals`) — per agent: how many of its runs are
  scored, the pass rate over all evaluators against the previous window,
  the evaluator that dropped most, evaluator errors, a mean-score line per
  evaluator with version markers, and the evaluators table sorted by the
  biggest drop. The source toggle switches between offline runs (default),
  production results, or both. The page opens on the last 7 days.
- **Runs** (`/evals/runs`) — offline runs with their eval set, version,
  cases, pass rate and status. **Compare ›** opens the run against the
  previous run of the same eval set.
- **Compare** (`/evals/compare?baseline=…&candidate=…`) — per evaluator
  means, deltas, pass rates and how many cases moved, plus latency p95 and
  tokens per run from the `invoke_agent` spans; then the cases, filtered
  to regressions, improvements or unchanged, with the candidate's tool
  calls marked against the baseline's (skipped, reordered or repeated,
  new).
- **Case** (`/evals/compare/case?…&case=…`) — the agent trajectory as a
  timeline with each result on its span, tools the baseline called but
  the candidate skipped as _expected, not called_ rows, the user input and
  answer (from `gen_ai.input.messages` / `gen_ai.output.messages` when
  recorded), and one card per evaluator with its explanation. Switch
  between candidate, baseline and side by side. The breadcrumb reads
  "Evaluate / Compare / _case id_", its Compare crumb leading back.
- **Evaluators** (`/evals/evaluators`) — every evaluator that sent a
  result, with the implementation versions seen, the span type it scores,
  its output kind, and whether it ran offline, in production or both.

## Upload a results file

A harness that doesn't export OpenTelemetry can upload its results as a file
instead. Each row is one evaluator result; SignalDB turns it into the same
`gen_ai.evaluation.result` log record described above, so the run shows up on
the Evaluate pages and in the Query IR exactly as if it had been sent over
OTLP.

The file is JSONL (one JSON object per line) or CSV with a header row:

| Column        | Required | Becomes                                                               |
| ------------- | -------- | --------------------------------------------------------------------- |
| `case_id`     | yes      | `signaldb.eval.case_id` (at most 128 bytes)                           |
| `name`        | yes      | `gen_ai.evaluation.name`: the evaluator                               |
| `score`       | one of   | `gen_ai.evaluation.score.value` (a number)                            |
| `label`       | one of   | `gen_ai.evaluation.score.label`                                       |
| `error`       | one of   | `error.type`: the evaluator itself failed                             |
| `explanation` | no       | `gen_ai.evaluation.explanation`                                       |
| `trace_id`    | no       | the record's trace id (32 hex characters); omit for run-level results |
| `span_id`     | no       | the record's span id (16 hex characters); needs a `trace_id`          |
| `evaluator`   | no       | `signaldb.eval.evaluator`, e.g. `trajectory-match@2.1.0`              |
| `trial`       | no       | `signaldb.eval.trial` (a non-negative integer)                        |

Every row needs a `score`, a `label` or an `error`. An empty CSV cell or a
JSON `null` counts as absent; other columns or keys are ignored. CSV headers
are matched case-insensitively.

The run comes from the request: the agent (`gen_ai.agent.name`, and the
records' `service.name`), its version (`gen_ai.agent.version` and
`service.version`), the eval set name (it needn't exist as a stored
[eval set](eval-sets.md)), and an optional run id — a UUID is generated when
you leave it out. Every record is stamped with the upload time, plus one
nanosecond per row, so rows keep their file order within the run.

The whole file is checked before anything is written. If any row is invalid,
the upload is rejected with a `400` and nothing is stored; `details` lists
every problem (the first 100), each with the file line it starts on (a CSV
header is line 1), the column, and why. A missing required CSV column is one
problem naming the column. Files are capped at 100,000 rows and 32 MiB.

```bash
curl -sS -X POST \
  "$SIGNALDB_URL/api/v1/evals/results?agent=support-triage&version=v1.9.0&set=triage-golden-200" \
  -H "Authorization: Bearer $SIGNALDB_API_KEY" -H "X-Tenant-ID: acme" \
  -H "Content-Type: text/csv" --data-binary @results.csv
```

The format comes from the `format` query parameter (`csv` or `jsonl`) when
given, otherwise from the `Content-Type`: `text/csv`, or
`application/x-ndjson` / `application/jsonl`. The upload needs an API key
with the `evals:write` scope and answers `201` with the run id and a summary
per evaluator:

```json
{
  "run_id": "1f0b7c1e-7c55-4c43-9b8e-3f1a2d6c9e10",
  "agent": "support-triage",
  "version": "v1.9.0",
  "set": "triage-golden-200",
  "rows": 600,
  "cases": 200,
  "span_linked": 596,
  "run_level": 4,
  "evaluators": [
    {
      "name": "Correctness",
      "results": 200,
      "errors": 3,
      "mean": 0.87,
      "pass_rate": 0.91
    }
  ],
  "_links": {
    "query": { "href": "/api/v1/query", "method": "POST" },
    "runs": { "href": "/evals/runs" }
  }
}
```

`mean` leaves out evaluator errors; `pass_rate` is passes / (passes + fails)
under the [pass rule](#how-results-are-read), and `null` when no result has a
verdict. The results appear in queries as soon as the writer commits them
(within seconds).

### Durability and retries

A `201` means the results are durable: the router forwards them to a writer,
which acks only after they are in its write-ahead log. The router keeps no
log of its own, so an upload that fails or times out (a `5xx`, a `504`, a
dropped connection) may or may not have been written.

Retrying it is safe as long as you send the same file with the same run id.
The upload's ingest id is a fingerprint of the tenant, dataset, agent,
version, eval set, run id, format and file bytes, and the writer drops a
batch whose ingest id it has already made durable within its dedup window
(`[writer].ingest_dedup_window`, 1 hour by default), so a retry of an upload
that did land is acknowledged without being written twice. Without a run id,
the server generates a new one per request and a retry is a second run; the
CLI and the MCP tool therefore choose the run id themselves when you leave it
out and name it when an upload fails. Uploading a different file under an
existing run id adds its results to that run.

### From the CLI, and as a CI gate

```bash
export SIGNALDB_URL=https://signaldb.example.com SIGNALDB_API_KEY=sk-... SIGNALDB_TENANT_ID=acme
signaldb-cli evals upload results.jsonl \
  --agent support-triage --version "$GIT_SHA" --set triage-golden-200 \
  --compare-to latest:v1.8.0 \
  --fail-if "Correctness.pass_rate < 0.9" \
  --fail-if "ToolTrajectory.mean < 0.85"
```

Without `--run-id`, the command generates one and prints it before
uploading; if the upload fails without an answer, rerun it with
`--run-id <that id>` (see [Durability and retries](#durability-and-retries)).
The command prints the run id, the per-evaluator summary and a link: to the
Compare page (`/evals/compare?baseline=…&candidate=…`) with `--compare-to`,
or to the Runs page otherwise. `--compare-to` takes a run id, or
`latest:<version>` for the newest other run of the same agent and eval set at
that version in the last 30 days (found with a Query IR read, so the key also
needs `logs:read`). The format comes from the file extension (`.csv`,
`.jsonl`, `.ndjson`) unless `--format csv|jsonl` says otherwise.

Each `--fail-if` is `<evaluator>.mean|pass_rate <op> <number>` with `<`,
`<=`, `>`, `>=`, `==` or `!=`, checked against the upload's summary. The
command exits non-zero, listing each condition that held, when any does; a
condition naming an evaluator the run doesn't have, or a metric it has no
value for, fails too. Conditions are checked for syntax before anything is
uploaded. The run is uploaded either way, so a blocked release still has its
results to compare.

In GitHub Actions:

```yaml
- name: Gate on eval results
  env:
    SIGNALDB_URL: ${{ vars.SIGNALDB_URL }}
    SIGNALDB_API_KEY: ${{ secrets.SIGNALDB_EVALS_KEY }}
    SIGNALDB_TENANT_ID: acme
  run: |
    signaldb-cli evals upload eval-results.jsonl \
      --agent support-triage --version "${{ github.sha }}" --set triage-golden-200 \
      --run-id "gh-${{ github.run_id }}" \
      --compare-to latest:${{ vars.RELEASED_VERSION }} \
      --fail-if "Correctness.pass_rate < 0.9"
```

The MCP server offers the same upload as the `upload_eval_results` tool
(see [MCP](mcp.md)).

## Querying results yourself

Every figure is a [Query IR](querying-ir.md) read over `logs`, so the CLI
and the API can reproduce it. Pass rate and mean per evaluator for one run:

```jsonc
{
  "irVersion": 4,
  "from": "logs",
  "range": { "from": "now-30d", "to": "now" },
  "result": "table",
  "pipeline": [
    {
      "where": {
        "field": "event_name",
        "op": "eq",
        "value": "gen_ai.evaluation.result",
      },
    },
    {
      "where": {
        "field": "signaldb.eval.run_id",
        "op": "eq",
        "value": "run-0927-1004",
      },
    },
    {
      "aggregate": {
        "by": [
          "gen_ai.evaluation.name",
          "gen_ai.evaluation.score.label",
          "error.type",
        ],
        "aggs": [
          { "fn": "count", "as": "n" },
          { "fn": "avg", "of": "gen_ai.evaluation.score.value", "as": "mean" },
        ],
      },
    },
  ],
}
```

```bash
signaldb-cli query --ir --file run-scores.json
```

## Eval sets

Stored eval sets hold a harness's cases (inputs, expected tool trajectories,
reference answers) per tenant and dataset, behind an HTTP API. See
[Eval sets](eval-sets.md).

## Coming next

Eval sets and the Upload results dialog in the UI, and accepting results
sent as span events. See the `agent-offline-evals` OpenSpec change.
