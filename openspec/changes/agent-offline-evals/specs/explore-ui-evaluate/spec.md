## ADDED Requirements

### Requirement: Agents & scores page

The explore UI SHALL serve `/evals`, showing for the selected window
(the last 7 days when the URL names none), agent and source (Offline
evals, the default; Production; Both): the share of the agent's
`invoke_agent` spans whose trace holds at least one result ("Runs
scored", with a warning under 50%), the pass rate over all evaluators
with its change against the previous equal-length window, the evaluator
whose mean dropped most against that window, the evaluator error count,
a mean-score line per evaluator (daily for windows of two days or more,
else hourly) with dashed markers where the agent version changed, and an
evaluators table (scored span operation, evaluator implementation,
results with errors, pass-rate bar, mean, change against the previous
window, trend) sorted by largest drop with regressed rows marked.
Hovering a bucket SHALL show that bucket's version and per-evaluator
means in the shared chart tooltip; clicking it SHALL open Runs for that
bucket's time range.

#### Scenario: Offline is the default source

- **WHEN** a user opens `/evals` with no `source` in the URL
- **THEN** only results with `signaldb.eval.run_id` are counted and
  "Offline evals" is selected

#### Scenario: Judge errors are called out

- **WHEN** Correctness has 412 results with `error.type = timeout` in the
  window
- **THEN** a banner says the Correctness evaluator errored on 412 results,
  which are left out of pass rates and not counted as failures, and the
  Correctness row shows "412 errored"

#### Scenario: No evaluations yet

- **WHEN** the agent has `invoke_agent` spans in the window but no results
- **THEN** the page says how many runs it sent with none scored, shows an
  example `gen_ai.evaluation.result` log record

### Requirement: Runs page

The explore UI SHALL serve `/evals/runs`, listing the offline runs in the
window, newest first, filterable by agent and eval set, with run id, eval
set, version, start time and agent, case count, result count, pass rate,
status
(in progress, complete, partial with its reason) and, for complete or
partial runs, a Compare link that uses the newest earlier run of the same
eval set as baseline. With no runs, the page SHALL explain the replay →
score → send loop and show the OTLP log-record form of a result.

#### Scenario: Compare from a run

- **WHEN** a user clicks Compare on run `run-0927-1004` of
  `triage-golden-200`, and `run-0923-0915` is that set's previous run
- **THEN** the browser opens
  `/evals/compare?baseline=run-0923-0915&candidate=run-0927-1004`

### Requirement: Compare page

The explore UI SHALL serve `/evals/compare`, taking `baseline` and
`candidate` run ids from the URL (defaulting to the two newest runs of the
newest eval set) and pickers to change them. It SHALL show a summary table
(per evaluator: baseline, candidate, delta, pass rates, cases worse,
cases better; then latency p95 and tokens per run) and a cases table
filtered by Regressions (default), Improvements or Unchanged with counts,
showing each case's input, per-evaluator baseline → candidate scores, the
candidate's tool calls marked against the baseline (skipped, reordered or
repeated, new) and a link to the case drilldown. A Copy link button SHALL
copy the page URL.

#### Scenario: Regressions come first

- **WHEN** 23 cases regressed, 9 improved and 168 are unchanged
- **THEN** the filter shows those counts and the table lists the 23
  regressions, largest drop first

#### Scenario: A skipped tool is visible

- **WHEN** the baseline trace called `lookup_order → check_policy →
issue_refund` and the candidate `lookup_order → issue_refund`
- **THEN** the tools column shows `check_policy` struck through as skipped

### Requirement: Case drilldown

The explore UI SHALL serve `/evals/compare/case` with `baseline`,
`candidate`, `case` and `mode` (`candidate`, `baseline` or `side`) in the
URL. It SHALL show the case's trace id (linking to the trace view),
duration, LLM call count, tokens and time; a span waterfall of the agent
trajectory (`invoke_agent`, `chat`, `execute_tool` spans) with each span's
results as pass/fail badges, and, in the candidate view, tool calls made in
the baseline but not in the candidate as dashed "expected, not called"
rows; the user input and each version's answer (read from the
`invoke_agent` span's `gen_ai.input.messages` / `gen_ai.output.messages`
when recorded); and one card per evaluator with its verdict, previous
verdict, explanation and raw fields (`gen_ai.evaluation.name`, score
value and label, evaluator, scored span, run). Side by side SHALL show
both trajectories next to each other. Once eval sets exist, the expected
trajectory and reference answer come from the case instead of the
baseline.

#### Scenario: Switching to side by side

- **WHEN** a user picks "Side by side" on case-117
- **THEN** the URL gains `mode=side` and the baseline and candidate
  trajectories render next to each other

#### Scenario: Scores sit on the span they score

- **WHEN** ToolArgsValid scored an `execute_tool lookup_order` span
- **THEN** that row shows a `ToolArgsValid pass` badge and the
  `invoke_agent` row does not

### Requirement: Evaluators page

The explore UI SHALL serve `/evals/evaluators`, listing the discovered
evaluators (see `agent-evaluation-results`) with first-seen date, versions
seen, scored span operation, output kind, seen in, results in the window
and last result time, and explain that the evaluator name is
`gen_ai.evaluation.name` and that sending `signaldb.eval.evaluator` with a
version separates judge changes from agent changes.

#### Scenario: Evaluator versions

- **WHEN** ToolTrajectory results carry `trajectory-match@2.1.0` and
  `trajectory-match@2.0.3`
- **THEN** its row lists both versions

### Requirement: Every Evaluate figure is a Query IR read

Every figure on the Evaluate pages SHALL come from Query IR requests
through the generated client (`logs` for results, `traces` for agent
spans). Agents & scores SHALL offer "Open in Query", opening the Query
view on the `logs` source filtered to the agent's
`gen_ai.evaluation.result` records.

#### Scenario: Reproducing a figure

- **WHEN** a user clicks "Open in Query" on Agents & scores for
  `support-triage`
- **THEN** the Query view opens on `logs` with the filters
  `event_name = gen_ai.evaluation.result` and
  `gen_ai.agent.name = support-triage`
