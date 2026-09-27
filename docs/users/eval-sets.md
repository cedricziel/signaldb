---
audience: user
type: how-to
status: living
sources:
  - src/common/src/eval_sets/mod.rs
  - src/common/src/eval_sets/store.rs
  - src/router/src/endpoints/eval_sets.rs
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

| Method   | Path                      | Scope         | Result                                                     |
| -------- | ------------------------- | ------------- | ---------------------------------------------------------- |
| `GET`    | `/eval-sets`              | `evals:read`  | Sets in the dataset, without cases, ordered by name        |
| `POST`   | `/eval-sets`              | `evals:write` | `201` with a `Location` header; `409` if the name is taken |
| `GET`    | `/eval-sets/{name}`       | `evals:read`  | The set with its cases in order                            |
| `PUT`    | `/eval-sets/{name}`       | `evals:write` | Replaces agent, description and every case; never creates  |
| `DELETE` | `/eval-sets/{name}`       | `evals:write` | `204`                                                      |
| `POST`   | `/eval-sets/{name}/cases` | `evals:write` | Appends cases, skipping ids the set already holds          |

Other statuses: `400` for a body that isn't JSON, `422` for a body that
fails validation (bad name, empty agent, duplicate case ids, a trace id that
isn't 32 hex characters, a `PUT` whose body `name` differs from the path),
`403` without the scope, `404` for a set that doesn't exist in your dataset,
and `413` for a body over 32 MiB. Responses carry `_links` to the set and,
when you may write, to its `replace`, `delete` and `append_cases` actions.

Create a set:

```bash
curl -sS -X POST "$SIGNALDB/api/v1/eval-sets" \
  -H "Authorization: Bearer $KEY" -H "X-Tenant-ID: acme" \
  -H "Content-Type: application/json" \
  -d @refund-edge-cases-40.json
```

List the sets, then pull one into a harness:

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
        {"id": "edge-40", "input": "..."},
        {"id": "edge-41", "input": "Refund the shipping fee only."}
      ]}'
```

```json
{
  "added": 1,
  "already_present": 1,
  "added_ids": ["edge-41"],
  "already_present_ids": ["edge-40"]
}
```

Delete a set:

```bash
curl -sS -X DELETE "$SIGNALDB/api/v1/eval-sets/refund-edge-cases-40" \
  -H "Authorization: Bearer $KEY" -H "X-Tenant-ID: acme"
```

The Rust SDK (`signaldb-sdk`) exposes the same operations as
`list_eval_sets`, `create_eval_set`, `get_eval_set`, `replace_eval_set`,
`delete_eval_set` and `append_eval_cases`.

## Scopes

| Scope         | Grants                                                   |
| ------------- | -------------------------------------------------------- |
| `evals:read`  | Listing and reading eval sets                            |
| `evals:write` | Creating, replacing, deleting and appending to eval sets |

A harness that only pulls sets needs a key with `evals:read`. Keys that
predate scopes are unrestricted. In a browser session, every tenant role can
read sets and only tenant admins (and instance admins) can change them.
`evals:read` is part of the default OAuth grant; `evals:write` is never
grantable through OAuth. See [Authentication](authentication.md).
