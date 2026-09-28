---
audience: user
type: how-to
status: living
sources:
  - src/common/src/eval_sets/mod.rs
  - src/common/src/eval_sets/store.rs
  - src/router/src/endpoints/eval_sets.rs
  - src/router/src/endpoints/eval_sets/from_traces.rs
  - src/common/src/evals.rs
  - src/signaldb-cli/src/commands/eval_sets.rs
  - src/mcp-server/src/server.rs
  - src/ui/src/api/evalSets.ts
  - src/ui/src/features/evals/**
  - src/common/src/auth/mod.rs
  - openspec/changes/agent-offline-evals/**
---

# Eval sets

An eval set is a named, ordered list of test cases for one AI agent. Your
eval harness pulls a set, runs the agent over each case's input, and reports
the results back as `gen_ai.evaluation.result` records (see
[Evaluating AI agents](evaluations.md)). Keeping the set in SignalDB means
every run of your harness, in CI or on a laptop, tests against the same
cases.

A set belongs to one tenant and one dataset: the tenant and dataset of the
credential that created it (`X-Tenant-ID`, and `X-Dataset-ID` or the
tenant's default dataset). A caller in another tenant or another dataset gets
`404` for it, and deleting the dataset deletes its sets.

## The set and its cases

```json
{
  "name": "refund-edge-cases-40",
  "agent": "support-triage",
  "description": "Refund requests the agent got wrong in March",
  "cases": [
    {
      "id": "edge-01",
      "input": "I was charged twice for order 1182, can I get one back?",
      "expected_tools": ["lookup_order", "issue_refund"],
      "reference": "Refunds the duplicate charge on order 1182.",
      "tags": ["refunds", "duplicate-charge"],
      "source": {
        "kind": "trace",
        "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736"
      }
    },
    {
      "id": "edge-02",
      "input": "Cancel my subscription and refund this month."
    }
  ]
}
```

| Field         | Required | Notes                                                                                           |
| ------------- | -------- | ----------------------------------------------------------------------------------------------- |
| `name`        | yes      | Lowercase letters, digits, `-`, `_` and `.`, starting with a letter or digit; 1-128 characters. |
| `agent`       | yes      | The agent under test (`gen_ai.agent.name`). Must not be empty.                                  |
| `description` | no       |                                                                                                 |
| `cases`       | no       | Kept in the order given. At most 10,000 per set.                                                |

Each case:

| Field            | Required | Notes                                                                                                      |
| ---------------- | -------- | ---------------------------------------------------------------------------------------------------------- |
| `id`             | yes      | 1-128 characters, unique within the set.                                                                   |
| `input`          | yes      | What the agent receives.                                                                                   |
| `expected_tools` | no       | The tool names the agent should call, in order. Empty means the trajectory isn't checked.                  |
| `reference`      | no       | A reference answer, for evaluators that compare against one.                                               |
| `tags`           | no       | Free-form labels.                                                                                          |
| `source`         | no       | `{"kind": "trace", "trace_id": "<32 hex>"}`, `{"kind": "upload"}` or `{"kind": "hand_written"}` (default). |

Trace ids are stored lower-case.

## HTTP API

All paths are under `/api/v1` and take the usual tenant credentials
(`Authorization: Bearer <key>`, `X-Tenant-ID`, optional `X-Dataset-ID`, or a
browser session). Errors use the shared envelope
`{"status": "error", "errorType": "...", "error": "..."}`.

| Method   | Path                                  | Scope                        | Result                                                                        |
| -------- | ------------------------------------- | ---------------------------- | ----------------------------------------------------------------------------- |
| `GET`    | `/eval-sets`                          | `evals:read`                 | Sets in the dataset, without cases, ordered by name                           |
| `POST`   | `/eval-sets`                          | `evals:write`                | `201` with a `Location` header; `409` if the name is taken                    |
| `GET`    | `/eval-sets/{name}`                   | `evals:read`                 | The set with its cases in order                                               |
| `PUT`    | `/eval-sets/{name}`                   | `evals:write`                | Replaces agent, description and every case; never creates                     |
| `DELETE` | `/eval-sets/{name}`                   | `evals:write`                | `204`                                                                         |
| `POST`   | `/eval-sets/{name}/cases`             | `evals:write`                | Appends cases, skipping ids the set already holds                             |
| `POST`   | `/eval-sets/{name}/cases/from-traces` | `evals:write`, `traces:read` | Appends one case per matching agent trace ([below](#build-cases-from-traces)) |

Other statuses: `400` for a body that isn't JSON, `422` for a body that
fails validation (bad name, empty agent, duplicate case ids, a trace id that
isn't 32 hex characters, a `PUT` whose body `name` differs from the path),
`403` without the scope, `404` for a set that doesn't exist in your dataset,
and `413` for a body over 32 MiB. Responses carry `_links` to the set and,
when you may write, to its `replace`, `delete`, `append_cases` and
`append_cases_from_traces` actions.

Create a set:

```bash
curl -sS -X POST "$SIGNALDB/api/v1/eval-sets" \
  -H "Authorization: Bearer $KEY" -H "X-Tenant-ID: acme" \
  -H "Content-Type: application/json" \
  -d @refund-edge-cases-40.json
```

List the sets, then pull one into a harness. Each listed set carries
`case_count` and `sources`, how many of its cases came from each source kind
(`{"trace": 162, "upload": 0, "hand_written": 38}`):

```bash
curl -sS "$SIGNALDB/api/v1/eval-sets" \
  -H "Authorization: Bearer $KEY" -H "X-Tenant-ID: acme"

curl -sS "$SIGNALDB/api/v1/eval-sets/refund-edge-cases-40" \
  -H "Authorization: Bearer $KEY" -H "X-Tenant-ID: acme" \
  | jq -c '.cases[]'
```

To replace a set, PUT the same body to `/api/v1/eval-sets/{name}`.

Append cases. Ids already in the set are reported and left as they are:

```bash
curl -sS -X POST "$SIGNALDB/api/v1/eval-sets/refund-edge-cases-40/cases" \
  -H "Authorization: Bearer $KEY" -H "X-Tenant-ID: acme" \
  -H "Content-Type: application/json" \
  -d '{"cases": [
        {"id": "edge-01", "input": "..."},
        {"id": "edge-41", "input": "Refund the shipping fee only."}
      ]}'
```

```json
{
  "added": 1,
  "already_present": 1,
  "added_ids": ["edge-41"],
  "already_present_ids": ["edge-01"]
}
```

Delete a set:

```bash
curl -sS -X DELETE "$SIGNALDB/api/v1/eval-sets/refund-edge-cases-40" \
  -H "Authorization: Bearer $KEY" -H "X-Tenant-ID: acme"
```

The Rust SDK (`signaldb-sdk`) exposes the same operations as
`list_eval_sets`, `create_eval_set`, `get_eval_set`, `replace_eval_set`,
`delete_eval_set`, `append_eval_cases` and `append_eval_cases_from_traces`.

## Build cases from traces

`POST /api/v1/eval-sets/{name}/cases/from-traces` turns production traces
into cases: one case per matching agent trace the set doesn't hold yet.
Sample the runs of `support-triage` that failed Correctness in the last
7 days:

```bash
curl -sS -X POST "$SIGNALDB/api/v1/eval-sets/refund-edge-cases-40/cases/from-traces" \
  -H "Authorization: Bearer $KEY" -H "X-Tenant-ID: acme" \
  -H "Content-Type: application/json" \
  -d '{
        "range": {"from": "now-7d", "to": "now"},
        "agent": "support-triage",
        "failing_evaluator": "Correctness",
        "sample": 50,
        "expected_tools": true,
        "tags": ["correctness-failures"]
      }'
```

```json
{
  "matches": 214,
  "already_present": 12,
  "added": 50,
  "added_ids": ["trace-4bf92f3577b34da6", "..."]
}
```

`matches` counts the distinct matching traces, `already_present` those the
set already has a case for, and `added_ids` lists the new cases in set
order.

| Field                   | Default         | Notes                                                                                                                                                |
| ----------------------- | --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `range`                 | required        | The window, as in a [Query IR](querying-ir.md) document: RFC3339, `now-7d`, or epoch nanoseconds as a string.                                        |
| `agent`                 | the set's agent | `gen_ai.agent.name` of the agent span. A span without one matches on `service.name`.                                                                 |
| `operation`             | `invoke_agent`  | `gen_ai.operation.name` of the agent span.                                                                                                           |
| `filters`               | none            | Extra Query IR predicates on the agent span, e.g. `{"field": "deployment.environment", "op": "eq", "value": "prod"}`.                                |
| `failing_evaluator`     | none            | Keep only traces with at least one failing `gen_ai.evaluation.result` of this evaluator (`gen_ai.evaluation.name`) in the window. Needs `logs:read`. |
| `sample`                | 50              | How many cases to add, 1-1000.                                                                                                                       |
| `expected_tools`        | `false`         | Set the case's expected tools to the trace's `execute_tool` spans' `gen_ai.tool.name`s, in call order.                                               |
| `reference_from_answer` | `false`         | Set the case's reference to the agent's answer: the last text of the agent span's `gen_ai.output.messages`.                                          |
| `tags`                  | none            | Tags put on every new case.                                                                                                                          |

A result fails by the same rule the Evaluate pages use (see
[Evaluating AI agents](evaluations.md)): a result with `error.type` is an
evaluator error and never a failure; otherwise a `pass`/`fail`-style label
decides; otherwise a score below 0.5 fails.

Each new case:

- `id` is `trace-` plus the first 16 hex digits of the trace id, so running
  the same query again never duplicates a case;
- `input` is the last user text of the agent span's `gen_ai.input.messages`
  (a JSON array of `{role, parts}`), or the attribute as-is when it isn't
  that shape; empty when the span has none;
- `source` is `{"kind": "trace", "trace_id": "..."}`.

Which traces are taken is deterministic: newest agent span first (ties by
trace id), traces already a case source in the set skipped, then the first
`sample`. Unknown body fields are rejected (`422`), as are a `sample` outside
1-1000, an empty `agent`/`operation`/`failing_evaluator`, an unparseable or
empty `range`, and a filter the query engine refuses. The reads are three
Query IR queries run on the server, bounded to 10,000 matching traces,
50,000 evaluator results and 100,000 spans; `matches` never exceeds 10,000.

## In the Explore UI

The **Eval sets** page (`/evals/sets`, in the Evaluate group) lists the
dataset's sets: name and description, agent, case count, what the cases
were built from (the list's `sources` counts: "traces 162 · hand-written
38", "JSONL upload", or "saved from Compare" for a set whose description is
the one Compare writes, "Cases that regressed in …"),
the newest run of the set in the last 30 days with its version, date and
pass rate (a [Query IR](querying-ir.md) read; "never run" otherwise), and
when the set last changed. **New eval set…** asks for a name and agent and
starts the set from:

- **Upload JSONL** — a file of cases in the [case format](#the-set-and-its-cases).
  The dialog previews the first rows, counts cases without a reference
  answer, and lists every line it can't read; it won't create the set until
  the file reads cleanly. Cases without a `source` are stored as
  `{"kind": "upload"}`.
- **Real traces** — creates the set, then
  [builds cases from traces](#build-cases-from-traces) with the agent,
  window (24 hours, 7 days or 30 days), optional failing evaluator, sample
  size and the two checkboxes (expected tools from the tools called, on by
  default; the agent's answer as the reference, off).
- **Empty** — just the name and agent.

A set's page (`/evals/sets/{name}`) shows its cases (id, input, expected
tools, reference, source — a trace link for cases built from traces — and
the case's score in the set's newest run: _pass_ when every evaluator
passed, _N failing_ when any failed, _P of E_ when some evaluators reached
no verdict), 50 at a time. **Export JSONL** downloads the cases in the same
format `signaldb-cli eval-sets export` writes. **Add traces…** opens a panel
that [builds cases from traces](#build-cases-from-traces); the endpoint has
no dry run, so the panel shows the matches, already-present and added
counts it returned. The strip under the header links the two newest runs of
the set in Compare, and the **Runs** tab opens Runs filtered to the set.
**Settings** shows the set's fields and deletes it. Buttons that write are
disabled when the credential may not (the set's `_links` carry no write
actions).

On **Compare**, **Save N regressed cases as eval set** creates a set (named
`regressions-MMDD` after the candidate run's day, editable) holding the
regressed cases' inputs, expected tools and references, copied by case id
from the eval set the runs replayed; each case's source is the candidate's
trace when it has one, and every case is tagged `saved-from-compare`. The
button is disabled, with the reason, when the runs name no eval set or that
set isn't in the dataset's list; the set's cases are read when the dialog
opens, and the dialog says so if that read fails.

## CLI

Reads use `signaldb-cli eval-sets`, writes `signaldb-cli admin eval-sets`.
Both authenticate with a tenant API key (`--api-key`, `--tenant-id`,
optional `--dataset-id`, or the `SIGNALDB_*` environment), not the instance
admin key.

```bash
signaldb-cli eval-sets list                      # NAME AGENT CASES (per source) UPDATED DESCRIPTION
signaldb-cli eval-sets get refund-edge-cases-40  # the set, then one row per case
signaldb-cli eval-sets export refund-edge-cases-40 > cases.jsonl

signaldb-cli admin eval-sets create --file refund-edge-cases-40.json
signaldb-cli admin eval-sets replace refund-edge-cases-40 --file refund-edge-cases-40.json
signaldb-cli admin eval-sets append refund-edge-cases-40 --file new-cases.jsonl
signaldb-cli admin eval-sets add-traces refund-edge-cases-40 --range 7d \
  --failing-evaluator Correctness --sample 50 --expected-tools --tag correctness-failures
signaldb-cli admin eval-sets delete refund-edge-cases-40
```

`list` and `get` print tables; add `--json` for the raw response. `export`
writes the set's cases to stdout as JSONL, one case object per line, in
order: the same shape `append` reads.

`create` and `replace` take the set as a YAML or JSON file (`.json` is read
as JSON, anything else as YAML) in the shape shown
[above](#the-set-and-its-cases). For `replace`, a file without a `name`
takes the one on the command line, and a file naming a different set is
refused. `append` takes a JSONL file of cases or a JSON `{"cases": [...]}`
object, and prints how many cases were added and how many were already
present.

`add-traces` [builds cases from traces](#build-cases-from-traces). `--range`
is how far back to look, ending now (`7d`, `24h`; default `7d`). The other
flags mirror the body fields: `--agent`, `--operation`,
`--failing-evaluator`, `--sample`, `--expected-tools`,
`--reference-from-answer`, `--tag` (repeatable) and `--filter` (a Query IR
predicate as JSON, repeatable).

## MCP tools

`list_eval_sets` and `get_eval_set` (`evals:read`), and `create_eval_set`,
`replace_eval_set`, `delete_eval_set`, `append_eval_cases` and
`append_eval_cases_from_traces` (`evals:write`; the last also needs
`traces:read`, plus `logs:read` with `failing_evaluator`). The from-traces
tool takes the body fields [above](#build-cases-from-traces), with the
window as `from` (default `now-7d`) and `to` (default `now`). Each takes the `tenant` argument and a required `dataset`:
one MCP session may span several datasets, so there is no implicit default.
`get_eval_set` returns one page of cases (`offset`, default 0; `limit`,
default 200, at most 1000) with the set header plus `total_cases`, `offset`,
`returned` and `has_more`; page through a large set by raising `offset`. See
[the MCP tool catalogue](mcp.md#what-it-exposes).

## Scopes

| Scope         | Grants                                                   |
| ------------- | -------------------------------------------------------- |
| `evals:read`  | Listing and reading eval sets                            |
| `evals:write` | Creating, replacing, deleting and appending to eval sets |

Building cases from traces also reads signal data: it needs `traces:read`,
and `logs:read` with `failing_evaluator`. A harness that only pulls sets
needs a key with `evals:read`. Keys that
predate scopes are unrestricted. In a browser session, every tenant role can
read sets and only tenant admins (and instance admins) can change them.
`evals:read` is part of the default OAuth grant; `evals:write` is never
grantable through OAuth. See [Authentication](authentication.md).
