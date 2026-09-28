# agent-eval-sets Specification

## Purpose

Defines stored eval sets — named, per-tenant/dataset lists of test cases a harness replays — their HTTP API, building them from traces, and their UI and CLI surfaces.

## Requirements

### Requirement: Eval sets are stored per tenant and dataset

SignalDB SHALL store named eval sets per tenant and dataset, each with an
agent, a description and an ordered list of cases. A case SHALL have an id
unique within the set, an input, optional expected tool trajectory (an
ordered list of tool names), an optional reference answer, optional tags
and a source (a trace id, `upload` or `hand_written`). Sets SHALL be
managed through `GET|POST /api/v1/eval-sets`,
`GET|PUT|DELETE /api/v1/eval-sets/{name}` and
`POST /api/v1/eval-sets/{name}/cases` (append; cases whose id is already
in the set are reported, not overwritten), each with an explicit OpenAPI
operation id and its own privilege check: reads need the `evals:read`
scope, writes the `evals:write` scope and, for a user session, the
tenant-admin role. Errors use the shared API error envelope.

#### Scenario: Creating a set

- **WHEN** a client with `evals:write` posts a set named
  `refund-edge-cases-40` with 40 cases
- **THEN** the response is 201 with a `Location` of
  `/api/v1/eval-sets/refund-edge-cases-40`, and listing sets shows it
  with 40 cases

#### Scenario: Appending a case that already exists

- **WHEN** a client appends cases `edge-40` and `edge-41` to a set that
  already holds `edge-40`
- **THEN** only `edge-41` is added and the response reports 1 added and 1
  already present

#### Scenario: Read-only keys cannot write

- **WHEN** a key with only `evals:read` tries to delete a set
- **THEN** the response is 403 and the set still exists

#### Scenario: Pulling a set in an eval script

- **WHEN** a harness calls `GET /api/v1/eval-sets/triage-golden-200` with a
  read key for the owning tenant
- **THEN** it receives the set's 200 cases with their inputs, expected
  tools and references

#### Scenario: Another tenant cannot read the set

- **WHEN** a key for a different tenant requests the same set
- **THEN** the response is 404

### Requirement: Building a set from traces

Appending cases SHALL accept a trace query (agent, operation, attribute
filters, window, sample size) and create one case per matching
`invoke_agent` trace not already in the set, taking the input from the
agent span, optionally the called tools as the expected trajectory and
optionally the agent's answer as the reference. The response SHALL report
matches, already-present and added counts.

#### Scenario: Sampling failing runs

- **WHEN** 214 traces of `support-triage` failed Correctness in the last 7
  days, 12 of them are already in the set, and the user samples 50
- **THEN** 50 new cases are added from traces not yet in the set, sourced
  from their trace ids, and the response reports 214 matches, 12 already
  present and 50 added

### Requirement: Eval sets in the UI, CLI and export

The UI SHALL list eval sets (`/evals/sets`: name, agent, cases, built
from, last run, pass rate, updated) and show a set's cases with their last
score (`/evals/sets/{name}`), with a New eval set dialog (from JSONL, from
traces, or empty), an Add traces panel, and Export JSONL (written by the
client from the set's cases). Compare SHALL
offer "Save N regressed cases as eval set". The CLI SHALL offer
`signaldb-cli eval-sets list|get|export` (export writes the cases as JSONL)
and `signaldb-cli admin eval-sets create|replace|delete|append`.

#### Scenario: Saving regressions

- **WHEN** a user clicks "Save 23 regressed cases as eval set" on Compare
- **THEN** a new set holds those 23 cases with their original inputs,
  expected tools and references
