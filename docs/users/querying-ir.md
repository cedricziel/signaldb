---
audience: user
type: reference
status: living
sources:
  - src/router/src/endpoints/query.rs
  - src/query-ir/src/**
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
queries over `logs`, `traces`, profile summaries, and metrics**. The `metrics`
source covers the scalar-value case — group/filter a metric by name and
attributes, aggregate, bucket by `step` — the same as every other source. The
`metrics_histogram` source plus the `histogram_quantile` stage cover
percentile-over-buckets. `rate`/`irate`/`increase` and cross-series arithmetic
stay PromQL-only for now. Cross-signal correlation and structural trace
matching are separate, later capabilities (see [Roadmap](#roadmap)).

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
  "from": "logs", // a registered source: "logs", "traces", "profiles", or "metrics"
  "range": { "from": "now-1h", "to": "now" },
  "result": "series", // v1: rows | series | table; v2 adds heatmap; flamegraph is profiles-only
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
`bottomk.of`, and `order` keys. The log `body` and the trace `span_events`
are retrieval-only today. A retrieval-only field used in a predicate raises an
`UnfilterableField` error.

`span_events` on `traces` is the span's whole events list as a JSON string:
`[{"name", "timestamp_unix_nano", "attributes": {...}}, ...]`, `null` for a
span that recorded none. To filter on an exception, use the `exception.*`
fields below instead of the list.

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

The whole bag of one scope is a field too: `log.attributes`, `span.attributes`,
`profile.attributes`, `scope.attributes`, and `resource.attributes` project
the container as a `map<string,string>` (a JSON object in a `rows` result).
They are retrieval-only — filter on the individual keys, not on the bag.

### Exception attributes

An exception can be recorded two different ways depending on the source, and
each needs a different addressing rule:

- **Logs.** Per the
  [exceptions-on-logs](https://opentelemetry.io/docs/specs/semconv/exceptions/exceptions-logs/)
  convention, `exception.type`, `exception.message`, `exception.stacktrace`,
  and `exception.escaped` are ordinary record attributes on the log —
  address them exactly like any other attribute, unqualified or with `log.`.
- **Traces.** Per the
  [exceptions-on-spans](https://opentelemetry.io/docs/specs/semconv/exceptions/exceptions-spans/)
  convention, an exception is a span **event** named `exception`, not a span
  attribute — its `exception.type`/`.message`/`.stacktrace`/`.escaped` live
  inside that event's own attributes. On the `traces` source, these four
  names resolve specially: filtering, grouping, and projecting on
  `exception.type` reads the first `exception` event on each span, not a
  regular span attribute. A span with no `exception` event resolves the field
  to absent (`exists` is false), even if its status is `Error`.

```jsonc
// Traces grouped by exception type — reads each span's `exception` event.
{
  "irVersion": 1,
  "from": "traces",
  "range": { "from": "now-1h", "to": "now" },
  "result": "table",
  "pipeline": [
    { "where": { "field": "exception.type", "op": "exists" } },
    {
      "aggregate": {
        "by": ["exception.type"],
        "aggs": [{ "fn": "count", "as": "count" }],
      },
    },
  ],
}
```

Because a caught-and-logged exception and an exception recorded as a span
event are different data, finding "all exceptions" means querying both
sources and combining the results client-side — there is no single query
that spans both.

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

Two more envelopes are source-scoped rather than available everywhere:
`heatmap` (traces only, see [below](#heatmap-envelope-ir-v2)) and
`flamegraph` (profiles only, see [below](#flamegraph-envelope-profiles-only)).
A fifth, `metadata`, answers a question about the source instead of returning
its records — see [Discovery](#discovery-what-can-i-query).

Values follow the value type: timestamps/durations are integer nanoseconds,
bytes are base64, everything else its JSON-native form.

An attribute container is typed `map<string,string>` and arrives as a JSON
object, so you index a key rather than parse a rendering. A `null` cell means
the row carried no such container; `{}` means it carried one holding no
attributes.

### Warnings

Any envelope may carry a `warnings` array. A warning never changes the
result — it reports something the server suspects you did not intend:

```jsonc
{ "result": "series", "window": {...}, "series": [...],
  "warnings": [ { "code": "unknown_group_by_field",
                  "message": "'statusCode' is not a logical field of 'traces' and no record in the queried window carries an attribute named 'statusCode'; every row was grouped under a null label",
                  "field": "statusCode",
                  "suggestions": ["status.code"] } ] }
```

Branch on `code`, not on `message`. The field is omitted entirely when there
is nothing to report.

`unknown_group_by_field` is raised when an `aggregate.by` field is neither a
logical field of the source nor carried by any record in the window, so every
row landed in one group labelled `null`. It is a warning rather than a
rejection because an unpromoted attribute cannot be enumerated while planning:
grouping by a real attribute that is simply absent from a short window is a
legitimate query, and would otherwise fail a quiet dashboard panel.

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
attribute payload columns as selectable/filterable fields — no query can
address the raw payload directly, on any envelope. Retrieving the actual
profile payload goes through the `flamegraph` envelope below instead, which
returns it aggregated and bounded rather than as raw storage JSON. Use the
Pyroscope-compatible APIs for diffs, label discovery, profile extraction, and
heatmaps — those remain specialized APIs.

The `samples_json`/`stacktraces_json` columns are, however, ordinary columns
in the underlying Iceberg table: raw SQL against `profiles` (see
[querying with SQL](querying-sql.md)) can select them directly. That's a
different surface with different guarantees — no curated projection, no
bounded default — not a gap in the IR.

### Flamegraph envelope (profiles only)

Declare `"result": "flamegraph"` on a `profiles` query to retrieve an actual
profile payload — the same aggregation `/pyroscope/render` produces, bounded
and structured rather than raw `samples_json`/`stacktraces_json`. A pipeline
before it may contain only `from`/`where`; every other stage (`aggregate`,
`topk`/`bottomk`, `order`, `extract`) is rejected, because the flamegraph
aggregation is itself the terminal computation, not one this envelope
composes with. Filtering to one `profile.id` returns that profile's own
flamegraph; a broader filter (service, sample type, time range) aggregates
across every matching profile, same as an equivalent Pyroscope selector/range
would.

```json
{
  "irVersion": 1,
  "from": "profiles",
  "range": { "from": "now-1h", "to": "now" },
  "result": "flamegraph",
  "pipeline": [
    { "where": { "field": "service.name", "op": "eq", "value": "checkout" } }
  ]
}
```

The response carries the Pyroscope flamebearer shape plus a truncation flag:

```jsonc
{
  "result": "flamegraph",
  "window": { "start_ns": 0, "end_ns": 0 },
  "flamegraph": {
    "names": ["total", "main", "handle_request"],
    "levels": [
      [0, 100, 0, 0],
      [0, 100, 30, 1, 0, 70, 70, 2],
    ],
    "total": 100,
    "max_self": 70,
    "truncated": false,
  },
}
```

`levels` is one entry per call-stack depth; each level is a flat sequence of
`[offset_delta, total, self, name_index]` quadruples, `offset_delta` measured
from the end of the previous block on the same level. `truncated: true` means
more than 1,000 profile rows matched — a row-count cap, not a response-size
one — and the flamegraph was aggregated over only the first 1,000 of them;
narrow the query to see the rest. `fields` is not valid on a `flamegraph`
result, same as `series`.

## Metrics

`metrics` scans `metrics_gauge` and `metrics_sum` (unioned) — a scalar
`value` per point, filtered/grouped/aggregated the same way as any other
source. The two tables need not agree on the physical shape of their
columns — not on type, order, or count. A dataset whose `metrics_sum`
predates the typed-attribute change (attribute containers stored as JSON
strings) while `metrics_gauge` stores maps, or whose tables carry their
columns in different positions, is reconciled before the union, so both
filtering and grouping by any resource attribute (`host.name`,
`k8s.pod.name`, …) work across both. `metrics_histogram` is a separate source (see [Histograms](#histograms))
since its row shape — a whole bucketed histogram per point, not a scalar
value — has no equivalent in the generic `where`/`aggregate` pipeline.

The registered scalar fields are `timestamp` (the data point's time),
`metric.name`, `metric.value`, and `service.name`, plus resource attributes
(`resource.*`). `metric.value` has its own logical name rather than reusing
the physical `value` column directly — a document names a _logical_ field,
never storage, even where the spellings would otherwise coincide.

Every scalar source registers its primary time column as a logical field —
`timestamp` on `logs`, `metrics`, `metrics_histogram`, and `profiles`,
`start_time_unix_nano` on `traces` — so a cross-signal "last seen" aggregate
such as `{"fn": "max", "of": "timestamp", "as": "last"}` has the same shape
on every source, and `timestamp` can be filtered, ordered, and selected like
any other field.

```json
{
  "irVersion": 1,
  "from": "metrics",
  "range": { "from": "now-1h", "to": "now" },
  "result": "series",
  "pipeline": [
    {
      "where": {
        "field": "metric.name",
        "op": "eq",
        "value": "signaldb.wal.entries_processed"
      }
    },
    {
      "aggregate": {
        "by": ["service.name"],
        "aggs": [{ "fn": "sum", "of": "metric.value", "as": "v" }],
        "step": "1m"
      }
    }
  ]
}
```

This is what makes an OTel-native dotted metric name — like
`signaldb.wal.entries_processed`, SignalDB's own self-monitoring naming —
queryable at all: PromQL's grammar can't lex a dot in a bare metric-name
identifier, so the same query over `/prometheus/api/v1/query_range` 400s
before it reaches the querier. The IR's field resolution has no such
restriction. `rate`/`irate`/`increase` and cross-series arithmetic stay
PromQL-only until they have an IR pipeline-stage equivalent — the explore
UI's Metrics tab keeps its PromQL escape hatch for those.

## Histograms

The `metrics_histogram` source scans one row per OTLP histogram data point —
`count`, `sum`, `min`, `max`, and the classic-histogram `bucket_counts`/
`explicit_bounds` arrays — not a scalar value, so it's a separate source from
`metrics`. It exposes `timestamp`, `metric.name`, and `service.name` (plus
resource attributes) for filtering and grouping; the bucket columns themselves are not
addressable in a `where` or `by` — they only feed the `histogram_quantile`
stage below.

A `histogram_quantile` stage (IR v3+) interpolates a percentile from those
buckets, following the same linear-interpolation-within-bucket algorithm as
Prometheus's `histogram_quantile()` — and, since it shares its implementation
with SignalDB's PromQL `histogram_quantile()`, the two return identical
values for the same query. It always produces a `series` result, grouped by
`metric.name` plus any extra `by` labels, bucketed by `step`:

```json
{
  "irVersion": 3,
  "from": "metrics_histogram",
  "range": { "from": "now-1h", "to": "now" },
  "result": "series",
  "pipeline": [
    {
      "where": {
        "field": "metric.name",
        "op": "eq",
        "value": "http.server.duration"
      }
    },
    {
      "histogram_quantile": {
        "q": 0.95,
        "by": ["service.name"],
        "step": "1m",
        "as": "p95"
      }
    }
  ]
}
```

- **`q`** — the quantile, in `[0, 1]`.
- **`by`** — extra grouping labels beyond the implicit `metric.name` (merging
  bucket data across different metrics is meaningless, since each metric
  carries its own bucket bounds — so `metric.name` can't be added explicitly
  to `by`, it's already there).
- **`step`** — the time-bucket width.
- **`mode`** — `"rate"` (default) or `"instant"`. `rate` takes each series'
  last-minus-first bucket-count delta within a step bucket, clamped to ≥ 0 (a
  decrease means a counter reset) — the right mode for OTel's cumulative
  temporality, which is what most histogram instrumentation emits. `instant`
  sums bucket counts across points sharing a step bucket instead — the right
  mode for delta temporality, or a series with at most one point per bucket.
- **`as`** — the output value column name.

This is deliberately a distinct stage from the `aggregate` stage's
`fn: "quantile"` (`{"fn": "quantile", "of": "some.numeric.field", "arg": 0.95,
"as": "p95"}`), which estimates a percentile over independent scalar values
via `approx_percentile_cont` — a completely different algorithm, for a
completely different source shape. Neither is a substitute for the other:
`histogram_quantile` needs pre-bucketed histogram data; `aggregate`'s
`quantile` needs raw numeric samples.

`histogram_fraction()` (the CDF-inverse of `histogram_quantile()`) and
`rate`/`irate`/`increase` over `metrics_histogram` have no IR stage yet — stay
on PromQL for those (see [Roadmap](#roadmap)).

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

## Discovery — what can I query?

A structured query builder needs to know what it can build on. The `describe`
stage answers that, and it is deliberately cheap: the answer comes from the
canonical field catalog, your tenant's schema registries, and the statistics
the compactor maintains — **not** from reading your signal data. A `describe`
document never reaches a querier, so a field picker keeps working while query
execution is busy.

`describe` is terminal, pairs with the `metadata` result envelope, and requires
`irVersion` 4.

### Which fields can I filter on?

```jsonc
POST /api/v1/query
{ "irVersion": 4, "from": "logs",
  "range": { "from": "now-1h", "to": "now" },
  "result": "metadata",
  "pipeline": [ { "describe": { "target": "fields" } } ] }
```

```jsonc
{ "result": "metadata", "window": {...},
  "metadata": {
    "kind": "fields",
    "fields": [
      { "name": "service.name", "type": "string", "level": "resource",
        "filterable": true, "origin": "declared" },
      { "name": "body", "type": "any_value", "filterable": false,
        "origin": "declared" },
      { "name": "http.route", "type": "string", "filterable": true,
        "origin": "registry", "coverage": 0.82,
        "cardinality": { "estimate": 42, "at_least": false },
        "brief": "The matched route template." }
    ],
    "truncated": false,
    "cost": { "mode": "metadata", "window_scoped": false, "sampled": false,
              "as_of": "2026-08-17 09:31:00" } } }
```

Every field is a logical name you can put straight into a predicate. Physical
column names and promotion state never appear — promotion changes performance,
never which names are valid.

`origin` says which tier the item came from:

| `origin`   | meaning                                                                                         |
| ---------- | ----------------------------------------------------------------------------------------------- |
| `declared` | the canonical logical schema declares it (always valid)                                         |
| `registry` | statistics observed it and a schema registry defines it, so it carries a type and a description |
| `observed` | statistics observed it and nothing defines it — treated as a string                             |

`coverage` (the fraction of records carrying the field) and `cardinality` (an
approximate distinct-value count; `at_least` means the collector hit its cap)
appear only where statistics exist. Absent means unknown — never a zero that
could be mistaken for a measurement. Fields come back declared-first, then by
coverage descending, so the first screen is the fields most records carry.

### What values does a field take?

```jsonc
{
  "irVersion": 4,
  "from": "traces",
  "range": { "from": "now-6h", "to": "now" },
  "result": "metadata",
  "pipeline": [
    { "describe": { "target": "values", "field": "span.kind", "limit": 50 } },
  ],
}
```

Values are answered in tiers:

1. **A declared value set** — a registry enumeration, or one SignalDB itself
   writes (`span.kind`, `status.code`). Exact, free, and `approximate: false`.
2. **A maintained value sketch** — the most frequent values with their counts,
   recorded by the compactor's analyzer while it was already reading the data
   for compaction. Still free (no data is read to answer you), but bounded and
   therefore `approximate: true`, with `cost.as_of` giving its age. Values come
   back with `origin: "statistics"`.
3. **Nothing covers it.** The response returns no values, `cost.mode: "none"`,
   and a `hint` naming the query that _would_ compute the answer by reading
   data. It does not scan behind your back.
4. **You asked for the data-derived answer** with `"sample": true`. SignalDB
   then runs exactly the aggregation the hint names — bounded by your window
   and `limit` — and reports `cost.mode: "sampled_scan"` with
   `window_scoped: true` and `sampled: true`. Values come back with counts and
   `origin: "sampled"`.

A field can land in tier 3 for two different reasons, and both are honest
rather than empty: the analyzer has not run over this tenant's data yet, or the
field has more distinct values than the analyzer tracks (a request id, a URL
with an id in it). In the second case a partial list would be a confident wrong
answer — the top of a list that was never ranked — so no sketch is kept at all.

### Reading the cost

Every discovery response carries a `cost` object, because an answer's price and
its trustworthiness are part of the answer:

| field           | meaning                                                                                                                                                               |
| --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `mode`          | `metadata` (no data read), `sampled_scan` (data read, on request), `none` (not answered)                                                                              |
| `window_scoped` | whether your `range` narrowed the answer. The maintained statistics carry no time dimension, so a metadata-tier answer says `false` rather than pretending it did     |
| `sampled`       | whether the answer is sampled and therefore possibly incomplete                                                                                                       |
| `approximate`   | whether the answer is a bounded sketch of the most frequent values rather than the exact set. A declared value set is exact; a statistics- or scan-derived one is not |
| `as_of`         | how recent the statistics behind it are. `null` means none exist yet — on a tenant whose compactor has not run, `describe: fields` returns the declared fields only   |

**`mode` and `approximate` are independent, and the combination that matters is
`mode: "metadata"` with `approximate: true`.** That is a sketch answer: it cost
nothing (no data was read) _and_ it is not the exact value set — the most
frequent values, bounded, as of `as_of`. Cheap does not imply exact here. Read
the two fields together:

| `mode`         | `approximate` | what you have                                                                  |
| -------------- | ------------- | ------------------------------------------------------------------------------ |
| `metadata`     | `false`       | a declared value set — free and complete                                       |
| `metadata`     | `true`        | a maintained sketch — free, bounded, and dated; suggest it, do not count on it |
| `sampled_scan` | `true`        | a bounded read of your window, run because you asked                           |
| `none`         | `false`       | no answer, with a `hint` naming the query that would produce one               |

### What discovery deliberately does not do

**It is not predicate-scoped.** A `where` stage before `describe` is rejected,
with an error naming the query that computes the scoped answer instead —
because unconditional statistics cannot be filtered, and quietly ignoring your
predicate (or quietly scanning) would both be worse than saying so:

```jsonc
{ "irVersion": 4, "from": "traces", "range": {...}, "result": "table",
  "pipeline": [
    { "where": { "field": "service.name", "op": "eq", "value": "checkout" } },
    { "aggregate": { "by": ["http.route"], "aggs": [{ "fn": "count", "as": "n" }] } },
    { "topk": { "of": "n", "n": 100 } } ] }
```

That reads data, is bounded like any query, and you asked for it.

### Which sources can I query?

"Which sources exist" is the one question with no source to name, so it is a
`GET` rather than a document:

```
GET /api/v1/query/sources
```

```jsonc
{ "result": "metadata", "window": {...},
  "metadata": { "kind": "sources",
                "sources": [ { "name": "logs", "available": true },
                             { "name": "traces", "available": true },
                             { "name": "profiles", "available": false } ],
                "truncated": false,
                "cost": { "mode": "metadata", "window_scoped": false,
                          "sampled": false } } }
```

A registered signal with a table but no data is `available` and simply returns
nothing — consistent with every other query surface, where a signal with no
data is an empty result, never an error.

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
  `--sql`/`--promql`/`--logql`/`--traceql`/`--trace-id`.)

- **UI:** the Explore view's **Query** tab builds an IR document structurally and
  renders the declared envelope.

- **HTTP:** `POST /api/v1/query` directly (the request/response schemas are in
  the OpenAPI document at `GET /api/v1/openapi.json`).

The first-party UI and CLI consume the endpoint exclusively through their
generated clients (the TypeScript client and Rust SDK), never hand-written HTTP.

## Roadmap

The IR is the base of a dependent stack; each sibling is a separate capability
so it is designed and reviewed on its own risk profile:

- **live tail** — streaming new matching records over the same document
  (part of the streaming epic), and **pagination** for walking a large result.
  Field discovery itself has landed: see
  [Discovery](#discovery-what-can-i-query).
- **cross-signal correlate** — a `correlate` join stage (the IR becomes a DAG).
- **structural traces** — a `match` stage + a `trace` result envelope.
- **metrics: counters and rates** — a `rate`/`irate`/`increase` stage
  equivalent (counter delta over a window) and cross-series arithmetic
  (formulas). The scalar-value case (gauge/sum, plain aggregation) and
  histogram quantiles already work today — see above.

Also deferred: the compatibility dialects lowering _into_ the IR (one engine),
and full attribute promotion. None of these change the document shape defined
here — that is the point of versioning it from day one.
