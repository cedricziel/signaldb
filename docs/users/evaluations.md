---
audience: user
type: how-to
status: living
sources:
  - src/ui/src/api/evals.ts
  - src/ui/src/features/evals/**
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
  between candidate, baseline and side by side.
- **Evaluators** (`/evals/evaluators`) — every evaluator that sent a
  result, with the implementation versions seen, the span type it scores,
  its output kind, and whether it ran offline, in production or both.

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

Eval sets in the CLI and UI and built from a trace query, uploading results
as JSONL/CSV with a CI gate (`signaldb-cli evals upload --fail-if`), and
accepting results sent as span events. See the `agent-offline-evals`
OpenSpec change.
