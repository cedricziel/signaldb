---
audience: user
type: reference
status: living
sources:
  - src/router/src/endpoints/query.rs
  - src/common/src/query_ir/**
  - src/querier/src/query/ir_planner.rs
  - src/signaldb-cli/src/commands/query.rs
---

# Query with the native Query IR

SignalDB's native, first-party query surface is a **structured, versioned JSON
query document** — the Query IR — submitted to `POST /api/v1/query`. It sits
alongside the Tempo/LogQL/Prometheus compatibility dialects: those stay for
Grafana and existing clients; the IR is what the SignalDB UI and CLI build
directly, without formulating a dialect string.

This page is the reference for the IR at its foundational scope: **single-signal
queries over `logs`, `traces`, and profile summaries**. Cross-signal correlation,
structural trace matching, and metrics are separate, later capabilities (see
[Roadmap](#roadmap)).

## The endpoint

```
POST /api/v1/query
Authorization: Bearer <api-key>
X-Tenant-ID: <tenant>
X-Dataset-ID: <dataset>   # optional
Content-Type: application/json
```

Authentication and tenant scoping are identical to the other query APIs — the
tenant/dataset come from the authenticated request, never from the document
body. The response is the declared result envelope (see
[Result envelopes](#result-envelopes)).

## The document

```jsonc
{
  "irVersion": 1, // versioned; use 2 for heatmap
  "from": "logs", // a registered source: "logs", "traces", or "profiles"
  "range": { "from": "now-1h", "to": "now" },
  "result": "series", // v1: rows | series | table; v2 adds heatmap
  "fields": ["service.name"], // optional curated projection (rows/table)
  "pipeline": [/* ordered transform stages */],
}
```

- **`from`** selects a _registered signal source_. It is not a fixed enum, so
  later releases can add sources without changing the document shape.
- **`range`** bounds the query in time. `from`/`to` are timestamp literals:
  RFC3339, a relative anchor (`now`, `now-1h`, `now+30m`), or integer
  nanoseconds. Relative anchors are resolved **once**, against the server clock,
  at submission — every stage sees the same absolute window, and the resolved
  window is echoed back in the response for reproducibility.
- **`result`** declares the envelope up front; the server validates it against
  the query's terminal shape and rejects a mismatch before executing.
- **`fields`** is a curated projection of logical field names for `rows`/`table`
  results. Omit it for a bounded server default — the IR never returns every
  physical column.

For `logs`, the default `rows` projection is the OTel LogRecord: `timestamp`
and `observed_timestamp`, `body`, `service_name`, `severity_text` and
`severity_number`, the trace context (`trace_id`, `span_id`, `trace_flags`),
the instrumentation scope (`scope_name`, `scope_version`, `scope_schema_url`),
`resource_schema_url`, and the three attribute containers. The containers stay
**separate** — they are not merged into one bag, because their scopes mean
different things. Each arrives as a JSON object you can index by key.

### Pipeline stages

The `pipeline` is an ordered list of transform stages. Each stage is a
single-key object naming the stage:

| Stage              | Shape                            | Role                                             |
| ------------------ | -------------------------------- | ------------------------------------------------ |
| `where`            | a predicate tree                 | filter                                           |
| `extract`          | `{ parser, as: [{name, type}] }` | derive typed fields from log content (logs only) |
| `aggregate`        | `{ by, aggs, step? }`            | group-reduce; with `step` → a time series        |
| `topk` / `bottomk` | `{ n, of }`                      | rank by a numeric column                         |
| `order`            | `[{ of, dir }]`                  | sort                                             |
| `limit`            | integer                          | bound the row count                              |
| `heatmap` (v2)     | `{x, y, value}`                  | terminal time-by-distribution count aggregate    |

An unknown stage, or a stage illegal for the source (e.g. `extract` on
`traces`), is rejected by name during validation — never silently dropped.

### Predicates

Filtering uses one predicate grammar — comparison leaves composed with
`and`/`or`/`not`:

```jsonc
{
  "and": [
    { "field": "severity_number", "op": "gte", "value": 17 },
    { "field": "deployment.environment", "op": "eq", "value": "prod" },
  ],
}
```

`field` is a **logical, dotted OTel-native name** (`service.name`,
`http.status_code`). You never name a physical column, the attribute blob, or a
storage detail — those are rejected by the resolver's physical-name check.
Operators: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `between`, `contains`,
`regex`, `exists`.

Some logical fields are **retrieval-only**: they can appear in `fields`
projections but are rejected in predicates, `aggregate.by`, `topk.of`,
`bottomk.of`, and `order` keys. The log `body` is retrieval-only today. A
retrieval-only field used in a predicate raises an `UnfilterableField` error.

### Addressing an attribute scope

OTel puts attributes at three scopes, and SignalDB stores each in its own
container: the **resource** (the entity that emitted the telemetry), the
**instrumentation scope** (the library that produced it), and the **record**
itself (the log line or span).

An unqualified name searches all of them:

```jsonc
{ "field": "deployment.environment", "op": "eq", "value": "prod" }
```

That is usually what you want. When a key exists at more than one scope — and
`deployment.environment` on both the resource and the record is common — a
prefix addresses exactly one container:

| Prefix      | Reads                 | Available on |
| ----------- | --------------------- | ------------ |
| `resource.` | resource attributes   | logs, traces |
| `scope.`    | scope attributes      | logs, traces |
| `log.`      | log-record attributes | logs         |
| `span.`     | span attributes       | traces       |
| `profile.`  | profile attributes    | profiles     |

```jsonc
{ "field": "resource.deployment.environment", "op": "eq", "value": "prod" }
```

A physical column wins over a prefix, so `scope.name` is the instrumentation
scope's name (a first-class column), not a key called `name` inside the scope
attributes. To reach a key that literally begins with one of these prefixes,
qualify it: `log.resource.foo` is the key `resource.foo` on the record.

### Structured operands

Aggregate/rank/order operands are structured values, never mini-expression
strings. Each aggregate names its output with `as`, and that name is the only
thing a later stage may reference:

```jsonc
{ "aggregate": { "by": ["service.name"], "aggs": [
  { "fn": "max", "of": "duration", "as": "max_dur" }
]}},
{ "topk": { "n": 10, "of": "max_dur" } }
```

### Scoping an aggregate to a subset

An aggregate may carry an optional `where` predicate scoping which records _it_
consumes. Everything else in the stage is unaffected: the grouping happens once,
and unscoped aggregates in the same stage still see every record in their group.

This is what lets one query report a total beside a measure over part of the
same groups — RED metrics (rate, errors, duration) on a single row per group:

```jsonc
{
  "aggregate": {
    "by": ["service.name"],
    "aggs": [
      { "fn": "count", "as": "requests" },
      {
        "fn": "count",
        "as": "errors",
        "where": { "field": "status.code", "op": "eq", "value": "Error" },
      },
      { "fn": "quantile", "of": "duration", "arg": 0.95, "as": "p95" },
    ],
  },
}
```

The scope uses the same predicate grammar, the same logical field names, and the
same coercion and absent-value rules as a `where` stage — it is validated
identically, so a field or operator `where` would reject is rejected here too.

Two properties worth relying on:

- **A group with no matching record is kept**, reporting `0` (or null for a
  non-count aggregate) rather than disappearing from the result. A `where`
  _stage_ would have dropped it.
- **The group set does not change.** Adding or removing a scope alters only that
  aggregate's values, never which groups come back or how `order`/`topk` rank
  them.

Scoping works on any aggregate function, not just `count` — a scoped `quantile`
computes its percentile over only the records the scope admits.

## Value types, coercion, and absent values

Every logical field has one canonical value type
(`string`/`int64`/`float64`/`bool`/`timestamp_ns`/`duration_ns`/`bytes`). A
literal is coerced to that type at validation — a duration `"500ms"`, a numeric
string `"17"`, an RFC3339 timestamp — and an un-coercible literal is **rejected**,
never silently cast at runtime.

`absent` is a first-class truth value. A comparison against a field that is
absent from a record evaluates to _absent_ (not true, not false) and propagates
through `and`/`or`/`not`. A `where` emits a row only when the predicate is
`true`, so **both `field = x` and `not(field = x)` exclude rows where the field
is absent**. To match or exclude on absence explicitly, use `exists` /
`not(exists)` — the only operators that observe it. This semantics is defined by
the IR, independent of the execution engine.

### Field resolution is promotion-invariant

Fields resolve through the logical schema (`LogicalSchema::core()`, which
declares the canonical client-visible OTel fields independent of the physical
Iceberg layout) and then through the attribute registry to a physical location —
a promoted column or an attribute-map extraction — at plan time. The **result of
a query does not depend on whether a field is currently promoted**; promotion is
pure performance upside. (Until the attribute-registry work lands canonical
attribute types, an unpromoted attribute is typed as a string; a field with no
resolvable type is a defined rejection.)

## Result envelopes

The declared `result` selects one canonical response shape:

```jsonc
// rows   (aggregated = false)
{ "result": "rows",  "window": {...}, "columns": [{name, type}], "rows": [[...]] }
// table  (a grouped aggregate)
{ "result": "table", "window": {...}, "columns": [{name, type}], "rows": [[...]] }
// series (a step aggregate)
{ "result": "series", "window": {...},
  "series": [ { "labels": {...}, "points": [[t_ns, value], ...] } ] }
```

Values follow the value type: timestamps/durations are integer nanoseconds,
bytes are base64, everything else its JSON-native form.

An attribute container is typed `map<string,string>` and arrives as a JSON
object, so you index a key rather than parse a rendering. A `null` cell means
the row carried no such container; `{}` means it carried one holding no
attributes.

## Profile summaries

`profiles` reads one metadata row per stored profile. It supports the same
filtering, aggregation, ranking, ordering, and rows/table/series envelopes as
the other scalar sources. Profile IR requests require the `profiles:read` scope;
the authenticated tenant and dataset still determine the table scanned.

The registered scalar fields are `profile.id`, `timestamp`, `duration`,
`sample.type`, `sample.unit`, `period.type`, `period.unit`, `period`,
`service.name`, `trace.id`, and `span.id`, plus registered profile, scope, and
resource attributes. The default rows projection contains only those scalar
metadata values.

Profile IR deliberately does not expose `samples_json`, `stacktraces_json`, or
attribute payload columns. Use the Pyroscope-compatible APIs for flamegraphs,
diffs, label discovery, profile extraction, and heatmaps; those payload-oriented
operations remain specialized APIs rather than generic Query IR fields.

### Heatmap envelope (IR v2)

Use the terminal `heatmap` stage to count spans by epoch-aligned time and
duration. It is currently available for `traces`; `duration` accepts duration
literals for its bounds.

```json
{
  "irVersion": 2,
  "from": "traces",
  "range": { "from": "now-1h", "to": "now" },
  "result": "heatmap",
  "pipeline": [
    {
      "heatmap": {
        "x": { "step": "1m", "align": "epoch" },
        "y": {
          "of": "duration",
          "bounds": ["1ms", "5ms", "25ms", "100ms", "1s"],
          "overflow": true
        },
        "value": { "fn": "count", "as": "count" }
      }
    }
  ]
}
```

The response has `result: "heatmap"` and a `heatmap` object containing `x`
(`step_ns`, `align`), `y` (`of: "duration"`, `type: "duration_ns"`,
integer-nanosecond `bounds`, `overflow`), and sparse
`{time_bucket_ns, duration_bucket, count}` cells.
Bounds are lower-inclusive and upper-exclusive. Values below the first bound
use bucket zero; values at or above the final bound use the final overflow
bucket. Missing cells inside the declared window are zero. The server accepts
at most 32 y-axis bounds and rejects non-positive steps or non-increasing
bounds before execution.

## Worked example — error-log volume by service (logs → series)

Count error logs per minute, per service, in `prod` over the last hour:

```jsonc
{
  "irVersion": 1,
  "from": "logs",
  "range": { "from": "now-1h", "to": "now" },
  "result": "series",
  "pipeline": [
    {
      "where": {
        "and": [
          { "field": "severity_number", "op": "gte", "value": 17 },
          { "field": "deployment.environment", "op": "eq", "value": "prod" },
        ],
      },
    },
    {
      "aggregate": {
        "by": ["service.name"],
        "aggs": [{ "fn": "count", "as": "n" }],
        "step": "1m",
      },
    },
  ],
}
```

`severity_number` resolves to a column; `deployment.environment`, if unpromoted,
to an attribute extraction — same query, same result either way.

## Submitting a query

- **CLI:** `signaldb-cli query --ir` reads the document from an argument,
  `--file`, or stdin and prints the enveloped result:

  ```bash
  signaldb-cli query --ir --file query.json \
    --url http://localhost:3000 --api-key "$KEY" --tenant-id acme
  # or: cat query.json | signaldb-cli query --ir --tenant-id acme
  ```

  (`--ir` is one of the mutually-exclusive language flags on `query`, alongside
  `--sql`/`--promql`/`--logql`/`--traceql`.)

- **UI:** the Explore view's **Query** tab builds an IR document structurally and
  renders the declared envelope.

- **HTTP:** `POST /api/v1/query` directly (the request/response schemas are in
  the OpenAPI document at `GET /api/v1/openapi.json`).

The first-party UI and CLI consume the endpoint exclusively through their
generated clients (the TypeScript client and Rust SDK), never hand-written HTTP.

## Roadmap

The IR is the base of a dependent stack; each sibling is a separate capability
so it is designed and reviewed on its own risk profile:

- **field discovery** — introspection (signals/fields/values) the builder needs,
  plus live tail + pagination for results.
- **cross-signal correlate** — a `correlate` join stage (the IR becomes a DAG).
- **structural traces** — a `match` stage + a `trace` result envelope.
- **metrics model** — a temporality/histogram-aware metric sub-model.

Also deferred: the compatibility dialects lowering _into_ the IR (one engine),
and full attribute promotion. None of these change the document shape defined
here — that is the point of versioning it from day one.
