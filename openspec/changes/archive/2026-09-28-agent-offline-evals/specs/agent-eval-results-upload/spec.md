## ADDED Requirements

### Requirement: Results file upload

`POST /api/v1/evals/results` SHALL accept a JSONL or CSV file with one row
per evaluator result — `case_id` and `name` required; `score`, `label`,
`explanation`, `trace_id`, `span_id`, `evaluator`, `error` optional — plus
the run's agent, version, eval set and optional run id (generated when
absent). A file with invalid rows SHALL be rejected as a whole with a 400
listing the row numbers and reasons. A valid file SHALL be written as
`gen_ai.evaluation.result` log records carrying the run attributes, so the
run appears on the Evaluate pages exactly as if sent over OTLP.

#### Scenario: Missing case ids

- **WHEN** a CSV has no `case_id` column
- **THEN** the upload is rejected with a 400 naming the missing column and
  no result is stored

#### Scenario: Run-level rows

- **WHEN** 4 of 1,000 rows have no `trace_id`
- **THEN** all 1,000 results are stored and the run is partial with
  "4 unmatched"

### Requirement: Upload dialog

The Runs page SHALL offer an Upload results dialog with three tabs: Upload
file (drop zone, column mapping with required fields marked, counts of
cases, evaluators, span-linked and run-level rows, agent/version/eval set
fields with the version prefilled from `service.version` of the linked
traces, and a warning for rows without `trace_id`), CLI / CI (the
`signaldb-cli evals upload` command) and Send over OTLP (the log-record
form).

#### Scenario: Case ids matched against the set

- **WHEN** a user uploads a file whose 200 case ids all exist in
  `triage-golden-200`
- **THEN** the dialog says "200 of 200 case IDs match triage-golden-200"

### Requirement: CI gate

`signaldb-cli evals upload <file> --agent --version --set
[--compare-to <run|latest:<version>>] [--fail-if <expr>]` SHALL upload the
file, print a link to the comparison, and exit non-zero when a `--fail-if`
condition (`<evaluator>.mean|pass_rate <op> <number>`) holds for the
uploaded run, using the API key in `SIGNALDB_API_KEY`.

#### Scenario: Blocking a release

- **WHEN** CI runs `--fail-if "ToolTrajectory.mean < 0.85"` and the run's
  ToolTrajectory mean is 0.84
- **THEN** the command exits non-zero and prints the failing condition
