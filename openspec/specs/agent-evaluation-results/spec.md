# agent-evaluation-results Specification

## Purpose

Defines how offline and online agent evaluation results are represented (as `gen_ai.evaluation.result` log records with SignalDB run attributes), how pass/fail is decided, and how runs and run comparisons are derived from them.

## Requirements

### Requirement: Eval results are log records

SignalDB SHALL treat every OTLP log record whose `event_name` is
`gen_ai.evaluation.result` as one evaluator result, read from the OTel GenAI
attributes `gen_ai.evaluation.name` (the evaluator), `gen_ai.evaluation.score.value`,
`gen_ai.evaluation.score.label`, `gen_ai.evaluation.explanation` and
`error.type`, and linked to the span it scores through the record's
`trace_id` and `span_id`. Such records SHALL be ingested, stored and queried
like any other log record, with no dedicated endpoint or ingest option.

#### Scenario: A judge scores an agent span after the run

- **WHEN** an eval harness emits a log record with `event_name`
  `gen_ai.evaluation.result`, `gen_ai.evaluation.name = "Correctness"`,
  `gen_ai.evaluation.score.value = 0.2`, `gen_ai.evaluation.score.label =
"fail"`, and the trace and span id of an `invoke_agent` span
- **THEN** a Query IR `rows` query on `logs` filtered on that `event_name`
  returns the result with those attributes and that trace and span id

#### Scenario: Several evaluators score the same span

- **WHEN** three results with different `gen_ai.evaluation.name` values
  carry the same `span_id`
- **THEN** they are three independent results, each counted once when
  aggregating by `gen_ai.evaluation.name`

### Requirement: Offline results carry run attributes

A result SHALL belong to an offline run when it carries
`signaldb.eval.run_id`, and SHALL be a production (online) result
otherwise. Offline results SHOULD carry `signaldb.eval.set`,
`signaldb.eval.case_id` and `signaldb.eval.evaluator` (implementation and
version, e.g. `trajectory-match@2.1.0`), and MAY carry
`signaldb.eval.trial`. The agent SHALL be identified by
`gen_ai.agent.name` and `gen_ai.agent.version` on the result, falling back
to the record's `service.name` and `service.version` resource attributes.

#### Scenario: Version falls back to the resource

- **WHEN** a result has no `gen_ai.agent.version` and its resource has
  `service.version = v1.8.0`
- **THEN** the result is attributed to agent version `v1.8.0`

#### Scenario: Results without a run id are production results

- **WHEN** a classifier scores live traffic without `signaldb.eval.run_id`
- **THEN** its results appear under the Production source and in no run

### Requirement: Pass rule

Every surface SHALL classify a result the same way: a result with
`error.type` set is an evaluator error, excluded from pass rates and mean
scores and never counted as a failure. Otherwise a recognised label decides
(`pass`, `passed`, `true`, `yes`, `correct`, `safe` pass; `fail`, `failed`,
`false`, `no`, `incorrect`, `unsafe` fail; case-insensitive), and without
one a numeric score of at least 0.5 passes and below 0.5 fails. A result
with neither is scored without a verdict.

#### Scenario: A judge timeout is not a failure

- **WHEN** 412 of 4,812 Correctness results have `error.type = timeout`
- **THEN** the Correctness pass rate is computed over the other 4,400
  results and the evaluator shows 412 errors

#### Scenario: Label wins over score

- **WHEN** a result has score 0.33 and label `pass`
- **THEN** it passes

### Requirement: Runs derived from results

A run SHALL be the set of results sharing one `signaldb.eval.run_id`; its
eval set, agent, version and start time SHALL be read from its results. A
run SHALL report its case count (distinct `signaldb.eval.case_id`), pass
rate and status: in progress while its newest result is less than 10
minutes old, otherwise partial when it has evaluator errors or results
without a trace id, otherwise complete.

#### Scenario: A run finishes

- **WHEN** a run's last result arrived 12 minutes ago and every result has
  a trace id and no error
- **THEN** the run is complete

#### Scenario: Results without trace context

- **WHEN** 4 of a run's results have no trace id
- **THEN** the run is partial with "4 unmatched", and those results still
  count toward its scores

### Requirement: Comparing two runs

Comparing a baseline run with a candidate run SHALL join their results on
`signaldb.eval.case_id` and classify, per evaluator, a case as worse when it
went from pass to fail or its mean score dropped by at least 0.05, and
better on the reverse. A case SHALL be a regression when any evaluator got
worse, otherwise an improvement when any got better, otherwise unchanged;
a case with no baseline result SHALL be marked as such. The comparison
SHALL report, per evaluator, both means, the delta, both pass rates, and
how many cases got worse and better, plus latency p95 and mean tokens per
run from each run's `invoke_agent` spans.

#### Scenario: A pass turns into a fail

- **WHEN** case-117 passed Correctness in the baseline and fails it in the
  candidate, and improved on Groundedness
- **THEN** case-117 is a regression

#### Scenario: A small score wobble is unchanged

- **WHEN** case-019's Groundedness goes from 0.95 to 0.96 and every other
  evaluator is identical
- **THEN** case-019 is unchanged

### Requirement: Evaluators are discovered

SignalDB SHALL list every evaluator that has sent at least one result in the
window, keyed by `gen_ai.evaluation.name`, with the `signaldb.eval.evaluator`
versions seen, the operation of the spans it scores
(`gen_ai.operation.name` of the linked spans), its output kind (score,
label, or both), whether it was seen in offline runs, production or both,
its result count and the time of its last result. Nothing SHALL need to be
created for an evaluator to appear.

#### Scenario: A new judge shows up

- **WHEN** the first `Faithfulness` result arrives
- **THEN** `Faithfulness` is listed as an evaluator with one result
