---
audience: user
type: reference
status: living
sources:
  - src/router/src/endpoints/query.rs
  - src/query-ir/src/**
  - src/querier/src/query/ir_planner.rs
  - src/querier/src/query/metric_series/**
  - src/querier/src/query/graph.rs
  - src/common/src/profile/aggregation.rs
  - src/signaldb-cli/src/commands/query.rs
  - src/signaldb-cli/src/commands/services.rs
---

# Query with the native Query IR

SignalDB's native, first-party query surface is a **structured, versioned JSON
query document** — the Query IR — submitted to `POST /api/v1/query`. It sits
alongside the Tempo/LogQL/Prometheus compatibility dialects: those stay for
Grafana and existing clients; the IR is what the SignalDB UI and CLI build
directly, without formulating a dialect string.

This page is the reference for the IR at its foundational scope: **single-signal
queries over `logs`, `traces`, profile summaries, and metrics**. The `metrics`
source holds every metric type — group/filter a metric by name, type and
attributes, aggregate, bucket by `step` — the same as every other source. The
`histogram_quantile` and `histogram_fraction` stages cover percentiles and
fractions over buckets, and the `rate`/`increase`/`irate`/`*_over_time`
per-series range functions cover counter rates and windowed reductions (see
[Counter rate](#counter-rate-rateincrease-v6) and
[More range functions](#more-range-functions-across-and-window-v7)).
Arithmetic across several queries' results — formulas — is a separate
multi-query document shape (see
[Formulas](#formulas-cross-query-arithmetic-d5)). A `correlate` stage joins
each span to its parent within `traces` (v8), or one source to another signal
(v11) (see
[Correlate: joining across relations](#correlate-joining-across-relations-v8-v11));
a `match` stage finds traces by span structure — parent/child,
descendant, ancestor and sibling relations (v12) (see
[Structural matching](#structural-matching-the-match-stage-ir-v12)).

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
  "irVersion": 1, // 1 to 13; declare the lowest version that carries every feature you use (see Pipeline stages)
  "from": "logs", // a registered source: "logs", "traces", "profiles", "metrics", or "exemplars"
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
  window is echoed back in the response for reproducibility. A `from` after
  `to` is rejected with a 400.
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

| Stage              | Shape                            | Role                                              |
| ------------------ | -------------------------------- | ------------------------------------------------- |
| `where`            | a predicate tree                 | filter                                            |
| `extract`          | `{ parser, as: [{name, type}] }` | derive typed fields from log content (logs only)  |
| `aggregate`        | `{ by, aggs, step? }`            | group-reduce; with `step` → a time series         |
| `topk` / `bottomk` | `{ n, of }`                      | rank by a numeric column                          |
| `order`            | `[{ of, dir }]`                  | sort                                              |
| `limit`            | integer                          | bound the row count                               |
| `heatmap` (v2)     | `{x, y, value}`                  | terminal time-by-distribution count aggregate     |
| `histogram_quantile` (v3) | `{ q, by?, step, mode?, … }` | quantile from histogram buckets (`metrics` only)  |
| `histogram_fraction` (v10) | `{ lower, upper, … }`    | fraction of observations in `(lower, upper]` (`metrics` only) |
| `describe` (v4)    | `{ target, field?, limit?, … }`  | terminal; field and source discovery (`metadata`) |
| `correlate` (v8, v11) | `{ to, on?, kind, window?, … }` | join to the parent span, or to another signal     |
| `sample` (v10)     | `{ fn, window?, lookback?, … }`  | a metric point stream → a Series (`metrics` only) |
| `reduce` (v10)     | `{ fn, by? / without?, arg? }`   | Series → Series: fold series into groups          |
| `map` (v10)        | `{ fn, args? }`                  | apply a function to every Series value            |
| `labels` (v10)     | `{ replace }` / `{ join }`       | rewrite a label of every series                   |
| `filter` (v10)     | `{ op, value, bool? }`           | keep (or 1/0-flag) values that compare true       |
| `binop` (v10)      | `{ op, right, … }`               | arithmetic, comparison or set operator with `right` |
| `absent` (v10)     | `{ labels? }`                    | a series of 1 where the input has no series       |
| `over_time` (v10)  | `{ fn, window, step? }`          | subquery: re-window a Series at its own step      |
| `sort` (v10)       | `"asc"` / `"desc"`               | order an instant Series by value                  |
| `scalar` (v10)     | `{}`                             | a Series → a Scalar                               |
| `vector` (v10)     | `{}`                             | a Scalar → a Series                               |
| `match` (v12)      | `{ spansets, relations? }`       | structural trace match (`traces` only, first stage) |

With `step`, an `aggregate` on the `metrics` source is evaluated at instants
`t = from + k·step` (`t ≤ to`), each reading the left-open window
`(t - step, t]`, and each point is labelled `t`. Range functions and
`histogram_quantile` use the same instants, so a formula over any of them
joins on matching timestamps. Samples after the last instant are not
counted. On every other source, `step` buckets are epoch-aligned
`[t, t + step)` and labelled by their start `t`.

What each `irVersion` unlocks (the server supports 1 to 13; the source of
truth is `src/query-ir/src/version.rs`):

| Version | Adds |
| ------- | ---- |
| 1 | `where`, `extract`, `aggregate`, `topk`/`bottomk`, `order`, `limit`; the `rows`, `series` and `table` results, and `flamegraph` (`profiles` only) |
| 2 | `heatmap` |
| 3 | `histogram_quantile` |
| 4 | `describe` and the `metadata` envelope |
| 5 | aggregate `divisor`; `stddev`, `stdvar`, `first`, `last` |
| 6 | `rate`, `increase` |
| 7 | `irate`, the `*_over_time` aggregates, aggregate `across` and `window` |
| 8 | `correlate` to the parent span; the `graph` envelope |
| 9 | `count_distinct` |
| 10 | the metric Series algebra: `sample`, `reduce`, `map`, `labels`, `filter`, `binop`, `absent`, `over_time`, `sort`, `scalar`, `vector`, `histogram_fraction`, histogram `window`, `lookback` and `per_series`, the `scalar` envelope, document `step`/`constant`, the `time`/`constant` sources |
| 11 | `correlate` to another signal source |
| 12 | `match` and the `trace` envelope |
| 13 | the `flamegraph` envelope's `baseline` (a differential flamegraph) |

Every earlier document keeps its exact meaning; a document using a feature
while declaring a lower version is rejected naming the version it needs, never
silently upgraded.

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
`bottomk.of`, and `order` keys. The trace `span_events` is retrieval-only
today. A retrieval-only field used in a predicate raises an
`UnfilterableField` error.

The log `body` is filterable for string operators (`contains`, `regex`, `eq`,
`ne`, `exists`) — it resolves to a string value like any other string field,
so ordered and numeric operators get no special allowance for it. A
predicate, `order`/`rank` key, `aggregate.by`, or aggregate operand on `body`
compares against the same decoded string value a `rows` result's `body`
field shows, never the raw JSON-encoded storage form.

Every field — attribute or column — has a **canonical type**, and a
comparison operator's literal must match it: `eq`/`ne`/`gt`/`gte`/`lt`/`lte`/
`between`/`in` against an int64 field accepts an integer literal; against a
float64 field an integer literal widens to float, but a fractional literal
against an int64 field is **rejected at validation**, as is a non-numeric
string literal against a numeric field. `contains` and `regex` only work on
a string field — used against a non-string field they are rejected, never
silently stringified. An attribute with no recorded canonical type (nothing
observed it yet) resolves as a string.

`span_events` on `traces` is the span's whole events list as a JSON string:
`[{"name", "timestamp_unix_nano", "attributes": {...}}, ...]`, `null` for a
span that recorded none. To filter on an exception, use the `exception.*`
fields below instead of the list.

### Addressing an attribute scope

OTel puts attributes at three scopes, and SignalDB stores each in its own
container: the **resource** (the entity that emitted the telemetry), the
**instrumentation scope** (the library that produced it), and the **record**
itself (the log line or span).

An unqualified name resolves to the **most specific level that recorded the
key** — record, then scope, then resource:

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
| `point.`    | data-point attributes | metrics      |

```jsonc
{ "field": "resource.deployment.environment", "op": "eq", "value": "prod" }
```

`resource.identity` is a SignalDB-defined field, not an OTel attribute: a
stable digest of the record's resource attribute set (32 lowercase hex
characters), the same value for every record that shares the same resource.
It is available on `logs`, `traces`, `metrics`, and `profiles`, filterable
and usable in `aggregate.by` like any other field, and `null` on rows
written before the column existed.

A physical column wins over a prefix, so `scope.name` is the instrumentation
scope's name (a first-class column), not a key called `name` inside the scope
attributes. To reach a key that literally begins with one of these prefixes,
qualify it: `log.resource.foo` is the key `resource.foo` on the record.
On `metrics`, `point.metric.name` is the data-point attribute `metric.name`,
the spelling a metric Series' labels use for a colliding point attribute.

The whole bag of one scope is a field too: `log.attributes`, `span.attributes`,
`profile.attributes`, `scope.attributes`, and `resource.attributes` project
the container as a JSON object in a `rows` result, one entry per key
**in its originally sent value and type** — including a value whose type
doesn't match the key's canonical type, an array or key-value list, and
bytes, none of which are individually filterable. A single filterable key
(`deployment.environment`, above) always reads its one canonical-typed
value; the raw bag is the only place an off-type or structured value
surfaces. They are retrieval-only — filter on the individual keys, not on
the bag.

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

### Span events and links

On the `traces` source, a span's events and links are filterable in any
`where` predicate:

| Field                       | Value                                     |
| --------------------------- | ----------------------------------------- |
| `events.name`               | an event's name                           |
| `events.attributes.<key>`   | an event attribute                        |
| `links.trace_id`            | a linked span's trace id                  |
| `links.span_id`             | a linked span's span id                   |
| `links.attributes.<key>`    | a link attribute                          |

A leaf is **existential**: it holds when _any_ event or link of the span
satisfies it. Attribute values compare by their JSON text (`3`, `false`).
Supported operators are `eq`, `in`, `contains`, `regex` and `exists` (`exists`
on `events.name` means the span has at least one event). `ne`, `gt`, `gte`,
`lt`, `lte` and `between` are rejected with a 400, because "any element is not
X" is rarely what was meant: wrap `eq` in `not` for "no element equals X". A
span with no events or links, or unparsable stored JSON, never matches a leaf
(so `not` matches it). These fields are filter-only; they cannot be projected,
grouped or ordered.

These names are reserved on traces: a span attribute literally named
`events.name` is addressed as `span.events.name`. Each leaf matches
independently, so `events.name eq retry and events.attributes.attempt eq 3` can
be satisfied by two different events. Link ids compare as lowercase hex (the
value you send is lowercased, except for `regex`). The fields are not available
through correlate scopes: `parent.` and `<target>.` prefixes are rejected. An
event or link that cannot be parsed is skipped; the others still count.

```jsonc
// Spans with a retry event but no exception event.
{ "and": [
  { "field": "events.name", "op": "eq", "value": "retry" },
  { "not": { "field": "events.name", "op": "eq", "value": "exception" } }
] }
```

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

### Aggregate functions

| `fn`                  | `of` | `arg`   | Since  | Output           |
| --------------------- | ---- | ------- | ------ | ---------------- |
| `count`               | —    | —       | v1     | integer          |
| `sum` / `min` / `max` | yes  | —       | v1     | the field's type |
| `avg`                 | yes  | —       | v1     | float            |
| `quantile`            | yes  | `[0,1]` | v1     | float            |
| `stddev` / `stdvar`   | yes  | —       | **v5** | float            |
| `first` / `last`      | yes  | —       | **v5** | the field's type |
| `count_distinct`      | yes  | —       | **v9** | integer          |

`first` and `last` order by the source's own time column, so they mean
earliest and latest — not whichever row the scan happened to produce first.

`sum`/`avg`/`quantile`/`stddev`/`stdvar` require a numeric (`int64` or
`float64`) `of` field — an aggregate over a `string` field, whether a real
column (`service.name`) or an attribute whose canonical type was recorded as
`string`, is rejected at validation, not silently coerced. An attribute with
no recorded canonical type yet resolves as `string` and is rejected the same
way; it starts aggregating once an observed value establishes a numeric
canonical type for it (see
[Field resolution is promotion-invariant](#field-resolution-is-promotion-invariant)).

`count_distinct` is an **approximate** distinct count: it lowers to
DataFusion's `approx_distinct` (a HyperLogLog sketch), so its memory cost
does not grow with the number of distinct values, at the price of roughly
1-2% error — fine for a dashboard counting sessions or users, not for a
billing count. It accepts `string`/`int64`/`bool`/`timestamp` `of` fields;
`float64` is rejected at validation, naming the field and its type, the same
way a non-numeric field is rejected for `sum`/`avg` — `approx_distinct` itself
rejects floating point, and equality over a float is rarely what a distinct
count means anyway. A record with no value for the field is not counted:

```jsonc
{
  "aggregate": {
    "by": ["service.name"],
    "aggs": [{ "fn": "count_distinct", "of": "session.id", "as": "sessions" }],
  },
}
```

Scoped the same way as any other aggregate (see
[Scoping an aggregate to a subset](#scoping-an-aggregate-to-a-subset)), to
count only sessions that hit a particular condition — sessions with at least
one exception, say:

```jsonc
{
  "aggregate": {
    "by": ["service.name"],
    "aggs": [
      { "fn": "count_distinct", "of": "session.id", "as": "sessions" },
      {
        "fn": "count_distinct",
        "of": "session.id",
        "as": "sessions_with_exception",
        "where": { "field": "event_name", "op": "eq", "value": "exception" },
      },
    ],
  },
}
```

### Reporting a rate: `divisor` (v5)

An aggregate may carry an optional `divisor`, which divides its value by that
scalar. That is all a rate is — a count over a window, divided by the window:

```jsonc
{
  "aggregate": {
    "by": ["service.name"],
    "step": "1m",
    "aggs": [{ "fn": "count", "as": "errors_per_second", "divisor": 60 }],
  },
}
```

It is named for the operation rather than `per_seconds` because dividing an
aggregate by a scalar is not inherently about time. The divisor must be
greater than zero; a divided aggregate is always a float, even when the
function it divided returns an integer.

`divisor` composes with [scoping](#scoping-an-aggregate-to-a-subset): an
aggregate may narrow which records it consumes _and_ report the result per
unit, which is how you ask for the error rate rather than the overall rate.

### Counter rate: `rate`/`increase` (v6)

`rate` and `increase` are aggregate functions for a monotonic counter (a
metrics `sum` with cumulative temporality), legal only with `step` set and
only on the `metrics` source:

```jsonc
{
  "aggregate": {
    "by": ["metric.name", "service.name"],
    "aggs": [
      { "fn": "rate", "of": "metric.value", "as": "requests_per_second" },
    ],
    "step": "30s",
  },
}
```

Both are computed per **individual series** (one series per distinct
metric, resource, scope and point-attribute identity) — not per `by` group:
two series sharing a `by` value never take a delta across each other, even
though the `by` grouping still folds their (independently computed) values
together in the output. Each series is evaluated at instants
`t = from + k·step` over the left-open window `(t - window, t]`, and the
output timestamp is the evaluation instant `t`, not an epoch-aligned bucket
start. The point's OTLP temporality is honoured: cumulative points are
differenced against the series' previous point (a drop, or a new
`start_timestamp`, is a counter reset contributing the later value counted
from zero), delta points are summed — PromQL's rule, without extrapolation.
`increase` is the summed delta over the window; `rate` divides that by the
window width in seconds. A window holding a single point yields no sample.
`rate`, `increase` and `irate` over a gauge or a non-monotonic sum are
rejected with a 400, as is a range needing more than 11,000 evaluation
instants. Both always produce a `Float64` series.

A `step` aggregate still allows exactly one aggregate output, so `rate`/
`increase` cannot share a stage with another aggregate function.

### More range functions, `across`, and `window` (v7)

`rate`/`increase` belong to a wider family of **per-series range
functions** — every one legal only with `step` set and only on the
`metrics` source, computed per individual series exactly
as `rate`/`increase` are:

- `irate` — instantaneous per-second rate from the **last two** samples in
  the window, counter-reset aware like `rate`, but reacting to the most
  recent pair rather than averaging over the whole window (PromQL's
  `irate()`).
- `avg_over_time`, `min_over_time`, `max_over_time`, `sum_over_time`,
  `count_over_time` — the corresponding reduction over the raw values seen in
  the window, no counter-reset logic (these apply to gauges as much as
  counters).

Two more fields on the aggregate, both `irVersion` 7:

- **`across`** — the reducer that folds each `by` group's per-series values
  into one value per step: `sum` (default — the `rate`/`increase`
  behaviour), `avg`, `min`, `max`, or `count`. This is what `avg by
(service.name) (rate(...))` needs: `by: ["service.name"], aggs: [{ "fn":
"rate", ..., "across": "avg" }]`.
- **`window`** — the lookback window each evaluation instant `t` is computed
  over, independent of `step`: the left-open `(t - window, t]`. Defaults to
  `step`. A `window` narrower than `step` is legal — PromQL allows the same,
  and it simply means samples in the gap between windows are never counted.

```jsonc
{
  "aggregate": {
    "by": ["service.name"],
    "aggs": [
      {
        "fn": "rate",
        "of": "metric.value",
        "as": "requests_per_second",
        "across": "avg",
        "window": "5m",
      },
    ],
    "step": "1m",
  },
}
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
Iceberg layout) and then through the attribute type authority to a physical
location at plan time. An attribute's home is its typed map at each level it
was sent at; a promoted column (`attr_<level>_<key>`) is only a copy of one
level's home. A typed attribute reads `coalesce(promoted, home)` per level in
record → scope → resource order, and a promoted column is used only when it
exists with the canonical type. The **result set and result types of a query
do not depend on whether a field is currently promoted**; a test holds this
for every scalar canonical type, filters, aggregations, and a key sent at two
levels. Promotion is pure performance upside: `=`, `!=`, `<`, `<=`, `>`, `>=`
filters on a promoted attribute let the engine skip row groups using the
column's statistics, which works where the key is present in every row of a
row group. `in` and `between` do not use this rewrite. An attribute's canonical
type is picked, in order: a config pin, else a semantic-convention type hint
(from the resource/scope `schema_url`'s semconv registry), else the type of
the first value ever observed for it — and never changes once established.
An attribute never observed yet resolves as a string, and a field with no
resolvable type at all is a defined rejection. A correlate target's fields
(`<target>.x`) resolve the same way, including when the target signal has no
table yet: they keep their canonical type and read null. Every typed-attribute
reference in `where`, `aggregate`, ordering and `fields`, a correlate target's
included, counts as demand toward promotion for its own signal (`parent.x`
counts toward traces), once per document however often it is referenced.

Every table is in the typed attribute layout — each attribute container
(`log_attributes`, `span_attributes`, `resource_attributes`, ...) is stored as
one typed map per canonical type plus a binary residue, not a single
`Map<Utf8,Utf8>` or JSON string. This was a one-shot cutover: an operator
upgrading across it lost pre-cutover data in tables that were still in the
legacy layout (see `docs/operations/table-provisioning.md`), but from a
query's perspective there is no coexistence to reason about — every table a
query can see today is typed.

## Result envelopes

The declared `result` selects one canonical response shape:

```jsonc
// rows   (aggregated = false)
{ "result": "rows",  "window": {...}, "columns": [{name, type}], "rows": [[...]] }
// table  (a grouped aggregate)
{ "result": "table", "window": {...}, "columns": [{name, type}], "rows": [[...]] }
// series (an aggregate with `step`, or a metric Series from `sample`)
{ "result": "series", "window": {...}, "step_ns": 60000000000,
  "series": [ { "labels": {...}, "points": [[t_ns, value], ...] } ] }
```

A few more envelopes are source-scoped rather than available everywhere:
`heatmap` (traces only, see [below](#heatmap-envelope-ir-v2)),
`flamegraph` (profiles only, see [below](#flamegraph-envelope-profiles-only)),
and `graph` (traces only, IR v8+, see [below](#graph-envelope-traces-only-ir-v8)).
`trace` (traces only, IR v12+, see [below](#trace-envelope-traces-only-ir-v12))
returns the rows grouped per trace. A further envelope, `metadata`, answers a question about the source instead of returning
its records — see [Discovery](#discovery-what-can-i-query).

`scalar` (IR v10+) is one value per evaluation instant with no labels — the
result of a `scalar` stage or of the `time`/`constant` pseudo-sources:

```jsonc
{ "result": "scalar", "window": {...}, "step_ns": 60000000000,
  "points": [[t_ns, value], ...] }
```

JSON has no NaN or infinity, so a `NaN`, `+Inf` or `-Inf` value in a
`series` or `scalar` envelope is `null` (e.g. `scalar` over zero or several
series). See [Metric Series](#metric-series-ir-v10).

Values follow the value type: timestamps/durations are integer nanoseconds,
bytes are base64, everything else its JSON-native form.

A single attribute field's value follows its own canonical value type — an
`int64`-canonical key comes back as a JSON number, not a numeric string. The
whole-bag field (`log.attributes`, and its siblings) arrives as a JSON
object with each key in its original sent type, so you index a key rather
than parse a rendering. A `null` cell means the row carried no such
container; `{}` means it carried one holding no attributes.

### Trace envelope (`traces` only, IR v12+)

`"result": "trace"` returns the rows of a `traces` query grouped by trace, so a
client gets one entry per trace instead of a flat span list. The pipeline is
any non-aggregated one over `traces`; if `fields` projects columns it must
include `trace_id`. Without `fields` the default `traces` projection applies.

```jsonc
{ "result": "trace", "window": {...},
  "columns": [{name, type}],
  "traces": [
    { "trace_id": "5b8efff798038103d269b633813fc60c",
      "spans": [ { "trace_id": "5b8e…", "span_id": "eee19b7ec3c1b174", "span_name": "GET /", … },
                 … ] },
    …
  ] }
```

Traces appear in order of their first row, and each trace's `spans` keep the
result order (so an `order` stage decides both). Each span is an object keyed
by result column name, with the same cell encoding as `rows`; `columns` gives
each column's type, and `rows` is absent. A `limit` bounds spans, not traces,
so any trace in the result may be incomplete. An empty result is
`"traces": []`.

The envelope is rejected on any source other than `traces`, after an
`aggregate`, and below `irVersion` 12.

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

`match_incomplete_trace` is raised when a
[`match`](#structural-matching-the-match-stage-ir-v12) stage with a relation
evaluated traces that the `range` visibly cut (see its Semantics). The
message counts the matched traces that may be missing witness spans and the
unmatched traces that may have matched over a wider range, and names up to
three example trace ids:

```jsonc
{ "code": "match_incomplete_trace",
  "message": "1 matched trace may be missing witness spans and 2 traces did not match but may match over a wider range: a span's parent is not in the queried range (it started before the range or was not ingested) or a span ends after the range (its children may start after it). Widen `range` to see whole traces. Examples: 0102…, 0a0b…, 0c0d…" }
```

## Graph envelope (`traces` only, IR v8+)

`"result": "graph"` declares a service dependency graph — nodes and edges
built from the `traces` source — rather than a row-shaped result. It requires
`irVersion` 8 or later and is only legal for `from: "traces"`; the pipeline
composes with `where` only, since the graph is assembled from fixed internal
pipelines server-side rather than a client-composed one.

Three optional top-level fields, siblings of `result`, scope the graph:

```jsonc
{
  "irVersion": 8,
  "from": "traces",
  "range": { "from": "now-1h", "to": "now" },
  "result": "graph",
  "focus": "checkout", // restrict to this service's neighbourhood
  "depth": 2, // hops from focus, 1-3, default 1; requires focus
  "pipeline": [],
}
```

- `focus` + `depth` restrict the graph to the nodes within `depth` hops of
  `focus` in either direction; `depth` is only legal alongside `focus`.
- `trace_id` restricts the graph to the services and calls observed in one
  trace; it is mutually exclusive with `focus`.
- With neither, the graph covers every service seen in the window.

`where` stages filter the spans that count as callers and callees. A
dependency whose spans are filtered out still counts as instrumented, so a
call into it never turns into an external node.

### Response

The response carries a `graph` field:

```jsonc
{
  "result": "graph",
  "window": { "start_ns": 1700000000000000000, "end_ns": 1700003600000000000 },
  "graph": {
    "nodes": [
      {
        "id": "service:checkout",
        "name": "checkout",
        "kind": "service",
        "request_rate": 0.4,
        "error_rate": 0.25,
        "p95_ns": 20000000,
      },
      {
        "id": "service:frontend",
        "name": "frontend",
        "kind": "service",
        "request_rate": 0.4,
        "error_rate": 0.0,
        "p95_ns": 12000000,
      },
      {
        "id": "external:database:orders-db",
        "name": "orders-db",
        "kind": "external",
        "dependency_kind": "database",
      },
    ],
    "edges": [
      {
        "source": "service:frontend",
        "target": "service:checkout",
        "count": 4,
        "rate": 0.4,
        "error_rate": 0.25,
        "p95_ns": 20000000,
      },
      {
        "source": "service:checkout",
        "target": "external:database:orders-db",
        "count": 4,
        "rate": 0.4,
        "error_rate": 0.0,
        "p95_ns": 3000000,
      },
    ],
    "dropped_nodes": 0,
  },
}
```

- **Node ids.** Every node has an `id` separate from its display `name`:
  `service:<name>` for a service and `external:<kind>:<name>` for an
  external dependency. Edge `source` and `target` are node ids, so a
  database called `orders` and a service called `orders` stay two nodes.
- **Edges between services.** An edge `A → B` counts the server and consumer
  spans of `B` whose parent span belongs to `A`. `count` is the number of such
  calls. `rate` is `count` divided by the window length in seconds.
  `error_rate` is the share of calls with error status, from 0 to 1. `p95_ns`
  is the 95th-percentile call duration.
- **Service nodes.** `request_rate`, `error_rate` and `p95_ns` come from the
  service's own server and consumer spans. A service that only makes calls
  (for example a frontend with no server spans) has a node without metrics.
- **External nodes.** A client or producer span with no server or consumer
  child in the window becomes an edge to an `external` node. The node is named
  after the first attribute present out of `db.namespace`,
  `messaging.destination.name`, `rpc.service`, `server.address` and
  `peer.service`. If none is present, the dependency is scoped to its
  caller: id `external:<kind>:unnamed:<caller>`, name `unnamed <kind>`. Two
  services with unnamed HTTP calls therefore get two separate nodes, not one
  shared `http` node.
  `dependency_kind` is `database`, `messaging`, `rpc`, `http` or `other`,
  taken from `db.system.name`, `messaging.system`, `rpc.system` or
  `http.request.method`.
- **Node cap.** The graph holds at most `[querier].graph_max_nodes` nodes
  (default 200). Past the cap it keeps the `focus` node, then the nodes with
  the most traffic, where traffic is the sum of call counts on a node's edges.
  Edges to dropped nodes are removed as well. `dropped_nodes` gives the count,
  and the response carries a `graph_node_limit` warning.
- An unknown `focus` returns an empty graph, not an error.
- **Row bound.** The service-edge query uses a `correlate` join, and the
  external-edge query is an anti-join whose two inputs (client/producer spans
  and server/consumer spans) are capped the same way. Both are bounded by
  `[querier].correlate_max_rows`. If any of them reaches the cap, the graph
  is built from the truncated rows and the response carries a
  `correlate_row_limit` warning. When the callee side is truncated, some
  instrumented calls can show up as external edges.

### Window edges

The graph is built from spans that start inside the window, which has two
effects at the edges:

- **Window start.** A call whose caller span started before the window has no
  parent to join, so the call is missing from its edge.
- **Window end.** If a call is still in flight when the window ends, its callee
  span starts after the window. The call then shows up as an edge to an
  external node rather than to the callee service.

A wider window reduces both effects.

### CLI

`signaldb-cli services map` renders the graph envelope directly, without
composing an IR document by hand:

```bash
signaldb-cli services map --service checkout --depth 2 --format table
signaldb-cli services map --trace-id abc123 --format dot | dot -Tsvg > map.svg
signaldb-cli services map --format mermaid
signaldb-cli services map --format json   # the graph envelope unchanged
```

`--service` restricts to a neighbourhood (`--depth`, 1-3, only applies
alongside it) and `--trace-id` restricts to one trace; the two are mutually
exclusive. `--from`/`--to` set the window (`now-1h`/`now` by default). The
default `table` format lists one row per edge — source, target, calls/s,
error %, p95 — sorted by call rate, with external targets marked `(external)`.
`dot` and `mermaid` render the same graph as a Graphviz digraph or a Mermaid
`flowchart LR` for pasting elsewhere. Warnings (the node cap, a truncated
join) print to stderr; an empty graph prints nothing to stdout, a note to
stderr, and exits `0`.

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
returns it aggregated and bounded rather than as raw storage JSON, or, with a
`baseline` window, as a differential flamegraph of two windows.

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
    "locations": [null, { "file": "src/main.rs", "line": 12 }, null],
  },
}
```

`levels` is one entry per call-stack depth; each level is a flat sequence of
`[offset_delta, total, self, name_index]` quadruples, `offset_delta` measured
from the end of the previous block on the same level. `truncated: true` means
more than 1,000 profile rows matched — a row-count cap, not a response-size
one — and the flamegraph was aggregated over only the newest 1,000 of them
(by `timestamp`); narrow the query to see the rest. `fields` is not valid on a `flamegraph`
result, same as `series`. `locations` is parallel to `names`: `{file, line}` for the
first frame seen under that name when the profiler recorded a source file,
`null` otherwise — the Explore UI uses it to offer
[View source](explore-ui.md#view-source-github) on profile frames.

#### Comparing two windows: `baseline` (v13)

A document-level `baseline` range turns the flamegraph into a differential
one: the same `where` stages are read over `baseline` and over `range`, each
side capped at 1,000 profile rows, and merged by call stack. It is valid only
with `"result": "flamegraph"` and needs `irVersion` 13.

```json
{
  "irVersion": 13,
  "from": "profiles",
  "range": { "from": "now-1h", "to": "now" },
  "baseline": { "from": "now-25h", "to": "now-24h" },
  "result": "flamegraph",
  "pipeline": [
    { "where": { "field": "service.name", "op": "eq", "value": "checkout" } }
  ]
}
```

The response is the flamegraph envelope with two changes. Each level is a
sequence of `[offset_delta_baseline, total_baseline, self_baseline,
offset_delta, total, self, name_index]` septuples (Pyroscope's "double"
flamebearer), and two totals are added: `baseline_total` (the `baseline`
window) and `comparison_total` (the `range` window). `total` is their sum,
`max_self` the largest self value on either side, and `truncated` is `true`
when either side hit the cap. A stack present on one side only carries zeros
for the other. `window` echoes `range`.

The two sides are not normalized: a baseline window twice as long as `range`
contributes roughly twice the samples, so compare windows of equal length or
read the per-side totals. A baseline query costs two flamegraph queries, run
one after the other.

## Metrics

`metrics` reads one row per data point across every metric type — gauge,
sum, histogram, exponential histogram and summary. The metric type is a
field, not a separate source: filter or group by `metric.type` to pick the
types you want. An unfiltered `metrics` query returns every type, with
`metric.value` null on the histogram, exponential-histogram and summary rows.
The `histogram_quantile` stage reads the histogram rows (see
[Histograms](#histograms)). The `metrics_histogram` source of earlier
releases is gone; use `metrics` with a `metric.type` filter instead.

| Field                    | Type    | Meaning                                                                   |
| ------------------------ | ------- | ------------------------------------------------------------------------- |
| `timestamp`              | time    | the data point's time                                                     |
| `metric.name`            | string  | the metric name                                                           |
| `metric.type`            | string  | `gauge`, `sum`, `histogram`, `exponential_histogram` or `summary`         |
| `metric.value`           | float64 | the gauge/sum point; null on other types                                  |
| `metric.temporality`     | int64   | the OTLP aggregation temporality as stored (1 delta, 2 cumulative)        |
| `metric.monotonic`       | bool    | whether a sum is monotonic; null on other types                           |
| `metric.count`           | int64   | observation count (histogram types, summary)                              |
| `metric.sum`             | float64 | sum of observations (histogram types, summary)                            |
| `metric.min`/`.max`      | float64 | observed extremes (histogram types)                                       |
| `metric.explicit_bounds` | list    | a histogram's bucket bounds; retrieval-only                               |
| `metric.bucket_counts`   | list    | a histogram's per-bucket counts; retrieval-only                           |
| `metric.quantiles`       | list    | a summary's quantiles as stored (e.g. `[0.5, 0.99]`); retrieval-only      |
| `metric.quantile_values` | list    | the summary's value at each of those quantiles, as stored; retrieval-only |
| `service.name`           | string  | the emitting service                                                      |

Resource attributes (`resource.*`) and point attributes resolve the same way
as on other sources. `metric.value` has its own logical name rather than
reusing the physical `value` column directly — a document names a _logical_
field, never storage, even where the spellings would otherwise coincide. The
list fields can be selected in `fields` but not filtered, grouped or ordered
on. The default `rows` projection includes `metric_type`.

Every scalar source registers its primary time column as a logical field —
`timestamp` on `logs`, `metrics`, and `profiles`,
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
queryable without a rewrite: PromQL's grammar can't lex a dot in a bare
metric-name identifier, so the Prometheus endpoints quote it (`{"a.b"}`)
before lowering. The IR's field resolution has no such restriction. `rate`/`increase`/`irate`/`*_over_time` over `metrics` are
aggregate functions (see
[Counter rate](#counter-rate-rateincrease-v6) and
[More range functions](#more-range-functions-across-and-window-v7));
cross-series arithmetic is the [`binop`](#binop) stage of a metric Series.

## Metric Series (IR v10)

`irVersion` 10 evaluates metrics the way Prometheus does: at the instants
`t = from + k·step` of the range (`step` on the stage, else the document's
`step`), per series, into a **Series**. A range may span at most 11,000
steps (11,001 instants) of its output step, as in Prometheus; more is a 400. The instants an `over_time` reads its input at do not count toward
that limit, but no grid may hold more than 1,000,000 instants.

A dataset with no `metrics` table yet reads as an empty one, so it still
answers what PromQL answers from nothing: `sum(x) or vector(0)` is `{}` = 0,
`absent(x)` is 1 and `scalar(x)` NaN at every instant.

### `sample`

`sample` reads the `metrics` point stream (after any `where`) and evaluates
one function per series at every instant:

```json
{
  "irVersion": 10,
  "from": "metrics",
  "range": { "from": "now-1h", "to": "now" },
  "step": "1m",
  "result": "series",
  "pipeline": [
    {
      "where": {
        "field": "metric.name",
        "op": "eq",
        "value": "http.server.requests"
      }
    },
    { "sample": { "fn": "rate", "window": "5m" } }
  ]
}
```

| Operand    | Meaning                                                                              |
| ---------- | ------------------------------------------------------------------------------------ |
| `fn`       | `latest`, or one of the range functions below                                        |
| `window`   | range functions: read the points in `(t − window, t]`                                |
| `lookback` | `latest`: the newest point in `(t − lookback, t]` (default `5m`)                     |
| `of`       | `metric.value` (default), or `metric.count` / `metric.sum` of a histogram or summary |
| `step`     | the evaluation step, overriding the document's                                       |
| `offset`   | shift every read window back by this duration (`>= 0`)                               |
| `at`       | read at this one timestamp and repeat its value at every instant                     |
| `arg`      | `quantile_over_time`: the quantile in `[0, 1]`                                       |

The range functions are `rate`, `increase`, `irate`, `delta`, `idelta`,
`deriv`, `resets`, `changes`, and `avg_`, `min_`, `max_`, `sum_`, `count_`,
`last_`, `stddev_`, `stdvar_`, `present_` and `quantile_over_time`, with
PromQL's semantics.

`rate`, `increase` and `irate` reject gauges and non-monotonic sums with a
400 naming `delta`/`deriv` instead. A cumulative histogram's `metric.count` and `metric.sum` sample as counters. A series with
no point in an instant's window has no value there. A point flagged
`NO_RECORDED_VALUE` (OTLP's staleness marker) is skipped by range functions,
and `latest` has no value at an instant whose newest point is one (a marker
and a recorded point at the same timestamp: the marker counts as newest).

### Labels

Each series of a Series carries the full label set of its OTLP identity,
with string values:

- `metric.name`: kept by `latest` and `last_over_time`, dropped by every
  other function (the value is no longer the metric), as PromQL drops
  `__name__`.
- `service.name`, and every other resource attribute as `resource.<key>`.
- `otel.scope.name` / `otel.scope.version`: the instrumentation scope.
- Point attributes under their own key, or as `point.<key>` when the key
  would collide with the labels above (`point.metric.name` is the point
  attribute `metric.name`).

Structured values (arrays, maps) are compact JSON; an empty value is absent.
Stored series whose label sets are equal (a gauge and a sum of one name, an
attribute stored as the integer `200` and as the string `"200"`, an empty
vs a missing scope version) are one series, as in Prometheus: `sample`
evaluates their points together. Two series whose label sets only become
equal once a function drops `metric.name` (two metrics' `rate`, say) cannot
be told apart, and the query is a 400 "several series share the label set
…"; narrow the stream to one metric with a `where` first.

### Scalars: `scalar`, `vector`, `time`, `constant`

A **Scalar** is one value per instant with no labels, returned as the
[`scalar` envelope](#result-envelopes). `scalar` turns a Series from `sample`
into one: at each instant the value of its only series, `NaN` when it has
none or several. `scalar` over any other Series (an aggregate with `step`,
say) is not supported yet (501). `vector` turns a Scalar back into a Series
of one series with no labels.

Two pseudo-sources produce a Scalar without reading data; both need the
document `step`:

```jsonc
{ "irVersion": 10, "from": "time", "range": {...}, "step": "1m", "result": "scalar" }
{ "irVersion": 10, "from": "constant", "constant": 2.5, "range": {...}, "step": "1m", "result": "scalar" }
```

`time` is each instant in seconds since the epoch; `constant` is the given
value at every instant.

### Series algebra

These stages take a Series from `sample`, `histogram_quantile` or
`histogram_fraction` (labelled by its `by` labels, or with `per_series` by
each series' labels, at its `step`) or `vector` and return a Series, with
PromQL's semantics: they are what a PromQL expression lowers to. Over a
Series from an `aggregate` with `step` they are not supported yet (501).

#### `reduce`

PromQL's aggregation operators: fold the series into groups at every
instant.

```json
{ "reduce": { "fn": "sum", "by": ["service.name"] } }
{ "reduce": { "fn": "topk", "arg": 3, "without": ["instance"] } }
```

| Operand   | Meaning                                                                                                         |
| --------- | --------------------------------------------------------------------------------------------------------------- |
| `fn`      | `sum`, `avg`, `min`, `max`, `count`, `group`, `stddev`, `stdvar`, `quantile`, `topk`, `bottomk`, `count_values` |
| `by`      | group by exactly these labels (`metric.name` only when listed)                                                  |
| `without` | group by every label but these and `metric.name`                                                                |
| `arg`     | `topk`/`bottomk`: k, truncated (below 1 selects nothing); `quantile`: the quantile in `[0, 1]`                  |
| `label`   | `count_values`: the label each distinct value is written to                                                     |

With neither `by` nor `without` every series is one group with no labels.
The result is labelled by the group, except `topk`/`bottomk`, which keep the
k largest (smallest) series of each group with all their labels (`NaN` ranks
last; ties by label set). `min`/`max` ignore `NaN` unless every value is
`NaN`; `stddev`/`stdvar` are the population deviation/variance; `quantile`
interpolates linearly between the closest ranks, `NaN` ranking lowest (as
in Prometheus, so `quantile(1, …)` of `1`, `2`, `NaN` is `2`). `count_values` counts the
series per distinct value, the value written to `label` as Prometheus prints
it (`1`, `0.5`, `+Inf`, `NaN`).

#### `labels`

Rewrite one label of every series, as PromQL's `label_replace` and
`label_join` do. The metric name is kept.

```jsonc
{ "labels": { "replace": { "dst": "class", "replacement": "${1}xx", "src": "code", "regex": "(\\d).." } } }
{ "labels": { "join": { "dst": "hostport", "separator": ":", "src": ["host", "port"] } } }
```

`replace` sets `dst` to the expanded `replacement` when `regex` matches the
**whole** value of `src` (an absent label reads as `""`, and so does an empty
`src`: `{"dst": "d", "replacement": "v", "src": "", "regex": ""}` adds a
constant label); otherwise the series is unchanged. `join` sets `dst` to the
values of `src` (absent as `""`) joined by `separator`. Either way an empty
result removes `dst`. Two series rewritten to one label set at one instant
are a 400 (`vector cannot contain metrics with the same labelset`), as in
Prometheus; so are two series that dropping `metric.name` (a `sample`
function such as `rate`, an `over_time`, a `map`, a `bool` `filter`, an
arithmetic `binop`) leaves with one label set.

#### `map`

Apply a function to every value: `abs`, `ceil`, `floor`, `round` (optional
`args: [to_nearest]`, rounding half up), `sqrt`, `exp`, `ln`, `log2`,
`log10`, `sgn`, `clamp` (`args: [min, max]`; `min > max` yields no series),
`clamp_min` / `clamp_max` (`args: [bound]`), `timestamp`, and the UTC
calendar functions `day_of_month`, `day_of_week` (0 = Sunday), `day_of_year`,
`days_in_month`, `hour`, `minute`, `month`, `year`, which read the value as
seconds since the epoch.

```json
{ "map": { "fn": "clamp", "args": [0, 100] } }
```

Math follows IEEE as in Prometheus: `ln(0)` is `-Inf`, `ln(-1)` and `sqrt(-1)`
are `NaN`, and `NaN` stays `NaN` through every function (a calendar function
of `NaN` or `±Inf` is `NaN`). A Series loses its `metric.name`. The math
functions also apply to a Scalar. `timestamp` is the evaluation instant in
seconds; Prometheus returns a raw selector's sample timestamp instead.

#### `filter`

Compare every value with a number (`eq`, `ne`, `gt`, `ge`, `lt`, `le`): keep
the values that compare true, with their `metric.name`, or with `"bool": true`
replace every value by `1`/`0` and drop `metric.name`.

```json
{ "filter": { "op": "gt", "value": 0.5 } }
```

`NaN` compares false to everything but `ne`, as in Prometheus.

#### `binop`

Combine the pipeline (the left operand, a Series or a Scalar) with `right`:

```json
{ "binop": { "op": "mul", "right": 100 } }
```

`op` is an arithmetic operator (`add`, `sub`, `mul`, `div`, `mod`, `pow`,
`atan2`), a comparison (`eq`, `ne`, `gt`, `ge`, `lt`, `le`) or a set operator
(`and`, `or`, `unless`); `"reverse": true` evaluates `right op left`
(`10 - v`). Arithmetic follows IEEE (`x / 0` is `±Inf`) and drops
`metric.name`. A comparison filters a Series, keeping the Series' own values
and names where it holds, or with `"bool": true` yields `1`/`0` and drops
`metric.name`; between two scalars it needs `bool`.

`right` may also be a sub-document — `from` (the document's own source or a
pseudo-source), `pipeline`, and `constant` — which inherits the document's
`irVersion`, range and `step` and must yield a Series or a Scalar at the
left operand's step (a different one is a 400):

```json
{
  "binop": {
    "op": "div",
    "right": {
      "from": "metrics",
      "pipeline": [
        { "where": { "field": "metric.name", "op": "eq", "value": "limit" } },
        { "sample": { "fn": "latest" } }
      ]
    },
    "on": ["service.name"],
    "group": { "side": "left", "include": ["resource.region"] }
  }
}
```

A Scalar operand applies to every series. Two Series match series by label
set, as PromQL's vector matching does: by default on every label but
`metric.name`, with `ignoring` on every label but `metric.name` and the listed
ones, with `on` on exactly the listed ones. Without `group` each match must be
one-to-one; `group: {"side": "left"}` (`"right"`) lets several series of that
side match one series of the other, the result keeping the many side's labels
plus the `include` labels copied from the one side. Several series on both
sides of one match (many-to-many), or two results with one label set, is a 400. `and` keeps the left series with a match on the right, `unless` those
without one, and `or` all left series plus the right series without a left
match; set operators allow any number of series per match and keep labels,
names and values.

#### `absent`

`{ "absent": { "labels": { "job": "api" } } }`: at every instant where the
input has no series, one series labelled `labels` (default none; an empty
value is no label) with value `1`; nothing where it has any. PromQL's
`absent(sel)` passes the selector's equality matchers as `labels` by
Prometheus's rule: a label matched more than once is left out.

#### `over_time`

A subquery: re-window a Series evaluated at its own step. At each instant `t`
of its `step` (default the document's), `fn` reads each series' values at the
input's instants in `(t − window, t]`:

```json
[
  { "sample": { "fn": "rate", "window": "5m", "step": "1m" } },
  { "over_time": { "fn": "max", "window": "1h" } }
]
```

`fn` is `avg`, `min`, `max`, `sum`, `count`, `last`, `stddev`, `stdvar`,
`present`, `quantile` (with `arg`), `delta`, `deriv`, `changes` or `resets`,
with the `*_over_time` semantics of `sample`, reading the values as a gauge;
`metric.name` survives `last` only. The stages before an `over_time` are
evaluated from `window` before the range start on, at the multiples of
their own step since the epoch (as Prometheus aligns a subquery's instants,
whatever the query start). Its `step` may not be finer than its input's.

#### `sort`

`{ "sort": "asc" }` or `"desc"`. As the last stage of a document whose range
is one instant, it orders the series by value, `NaN` last either way (ties by
label set), as PromQL's `sort`/`sort_desc` order an instant query. Anywhere
else it changes nothing: series are returned in label-set order, as
Prometheus returns a range query.

## Histograms

A histogram row of the `metrics` source carries a whole OTLP histogram data
point — `count`, `sum`, `min`, `max`, and the classic-histogram
`bucket_counts`/`explicit_bounds` lists — not a scalar value. The bucket lists
are retrieval-only: they are not addressable in a `where` or `by`, and they
feed the `histogram_quantile` stage below.

A `histogram_quantile` stage (IR v3+) runs on the `metrics` source, reads only
its histogram rows, and interpolates a percentile from their buckets, following the same linear-interpolation-within-bucket algorithm as
Prometheus's `histogram_quantile()` — and, since it shares its implementation
with SignalDB's PromQL `histogram_quantile()`, the two return identical
values for the same query. It always produces a `series` result with one
series per metric plus `by` group, evaluated at instants `t = from + k·step`
and labelled `t`:

```json
{
  "irVersion": 3,
  "from": "metrics",
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
- **`by`** — the output labels. Buckets are merged per metric (each metric
  carries its own bounds, so different metrics never merge) and per `by`
  group; the output carries the `by` fields only, not `metric.name`, so a
  later stage cannot read it.
- **`step`** — the evaluation step.
- **`mode`** — `"rate"` (default) or `"instant"`. `rate` differences each
  series against itself over `(t - window, t]` (cumulative points against the
  series' previous point, with drops and `start_timestamp` changes as resets;
  delta points summed) and merges the per-series increases — the shape of
  `histogram_quantile(q, rate(x[w]))`. `instant` reads each series' latest
  point in `(t - lookback, t]` and merges those.
- **`window`** — rate mode's window, default `step`.
- **`lookback`** — instant mode's lookback, default `step`. Both are executed;
  neither is rejected as unsupported.
- **`as`** — the output value column name.
- **`per_series`** (IR v10) — evaluate each series on its own instead of
  merging, the shape of PromQL's `histogram_quantile(q, rate(x[w]))` without
  a `sum by (le)`. The result is a Series (`bucket`, `__labels`, `value`)
  with one series per input series, labelled by its label set less
  `metric.name`; Series stages can follow it. Two series left with one label
  set once the name is dropped are a 400 ("same labelset"). It cannot be
  combined with `by`.

This is deliberately a distinct stage from the `aggregate` stage's
`fn: "quantile"` (`{"fn": "quantile", "of": "some.numeric.field", "arg": 0.95,
"as": "p95"}`), which estimates a percentile over independent scalar values
via `approx_percentile_cont` — a completely different algorithm, for a
completely different source shape. Neither is a substitute for the other:
`histogram_quantile` needs pre-bucketed histogram data; `aggregate`'s
`quantile` needs raw numeric samples.

The stage reads explicit-bucket and **exponential** histogram rows; gauge
and sum rows are ignored, as Prometheus ignores non-histogram series. A
**summary** carries precomputed quantiles, not buckets, so a summary point in
the queried range is a 400 (`<function> is not supported on summary metrics`,
for `histogram_quantile` and `histogram_fraction` alike), never an empty or
invented value, and it fails the whole query even when histograms are
selected beside it. A summary with no points in the range stays empty. Read a
summary from `metric.quantiles`/`metric.quantile_values` instead, or narrow
the selection with `metric.name`. Histogram rows that cannot be interpolated
(unsorted bounds, a count list that doesn't match the bounds, negative
counts) are skipped.
Explicit histograms with different bounds merge over the union of their
bounds; exponential histograms merge by the OTel rule. More than 11,000
evaluation instants is a 400.
Filter by `metric.name` (or `metric.type`) to keep the stage on histograms.

### `histogram_fraction` (IR v10)

`histogram_fraction` is `histogram_quantile`'s sibling: the estimated
fraction of the merged histogram's observations in `(lower, upper]`, that is
cumulative(`upper`) − cumulative(`lower`) over the total count, the IR form of
PromQL's `histogram_fraction(lower, upper, …)`. A bound inside a bucket is
interpolated (linearly for explicit buckets, on a log scale for exponential
ones), so the result is an estimate. It takes `lower` and `upper` (finite,
`lower <= upper`; equal bounds give 0) in place of `q`, and otherwise the same
`by`, `step`, `mode`, `window`, `lookback` and `as` fields, reads the same rows
and evaluates at the same instants, so the two stages over one document
answer from the same merged histograms:

```json
{
  "histogram_fraction": {
    "lower": 0,
    "upper": 0.25,
    "by": ["service.name"],
    "step": "1m",
    "window": "5m",
    "as": "under_250ms"
  }
}
```

The bounds must be finite, so PromQL's `±Inf` idioms need rewriting:

- **At most X** (`histogram_fraction(-Inf, X, …)`): use a `lower` at or below
  every observation, such as `0` for a histogram of positive values like
  latencies. As in Prometheus, an explicit-bucket histogram whose first bound
  is `<= 0` has a first bucket reaching down to `-Inf`, and no finite `lower`
  includes it.
- **More than X** (`histogram_fraction(X, +Inf, …)`): compute
  `1 - fraction(lower, X)` with that same `lower`, for instance as the
  [formula](#formulas-cross-query-arithmetic-d5) `1 - under_x`. This holds
  only when no observation lies at or below that `lower`; a populated first
  bucket reaching down to `-Inf` breaks it, so query such a histogram through
  PromQL, which takes `+Inf` as `upper`. A large finite `upper` does not stand
  in for `+Inf` on an explicit-bucket histogram: the open `+Inf` bucket's
  observations lie above every finite bound, so `(X, 1e300]` leaves them out.

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

## Exemplars

The `exemplars` source reads one row per metric exemplar: a sampled
measurement a metric data point carries together with the trace and span it
was recorded in. It is the way from a metric to the traces behind it.

| Field                          | Type    | Meaning                                                           |
| ------------------------------ | ------- | ----------------------------------------------------------------- |
| `timestamp`                    | time    | when the exemplar was recorded                                    |
| `trace.id` / `span.id`         | string  | the trace and span, hex-encoded the way `traces` stores them      |
| `exemplar.value`               | float64 | the measured value                                                |
| `metric.name` / `metric.type`  | string  | the metric whose data point carries the exemplar                  |
| `series.id`                    | string  | digest of the owning series; the same value as on its metric rows |
| `exemplar.filtered_attributes` | object  | the attributes the SDK filtered out of the point; retrieval-only  |
| `service.name`                 | string  | the emitting service                                              |
| `resource.identity`            | string  | SignalDB's resource identity                                      |

Find the exemplars recorded in one trace:

```json
{
  "irVersion": 1,
  "from": "exemplars",
  "range": { "from": "now-1h", "to": "now" },
  "result": "rows",
  "fields": [
    "timestamp",
    "span.id",
    "exemplar.value",
    "metric.name",
    "metric.type",
    "exemplar.filtered_attributes"
  ],
  "pipeline": [
    {
      "where": {
        "field": "trace.id",
        "op": "eq",
        "value": "4bf92f3577b34da6a3ce929d0e0e4736"
      }
    }
  ]
}
```

Reading exemplars needs the `metrics:read` scope.

## Correlate: joining across relations (v8, v11)

A `correlate` stage joins the current relation to another one by a shared
key. `to: "parent"` (IR v8) joins `traces` to a span's own parent within the
same source. `to: "<signal>"` (IR v11) joins any source to a different
signal — logs, traces, metrics, exemplars, profiles. At most one `correlate`
stage appears per pipeline.

### Joining spans to their parents (v8)

A `correlate` stage with `to: "parent"` joins the current `traces` relation to
the span in the same trace whose `span_id` equals the row's
`parent_span_id` — the building block for "which service called which". It
only ever appears on the `traces` source, and only before an `aggregate`
stage:

```json
{
  "irVersion": 8,
  "from": "traces",
  "range": { "from": "now-1h", "to": "now" },
  "result": "table",
  "pipeline": [
    { "correlate": { "to": "parent", "kind": "inner" } },
    {
      "where": {
        "field": "parent.service.name",
        "op": "ne",
        "value": "service.name"
      }
    },
    {
      "aggregate": {
        "by": ["parent.service.name", "service.name"],
        "aggs": [{ "fn": "count", "as": "calls" }]
      }
    }
  ]
}
```

- **`to`** — `"parent"` joins a span to its own parent within `traces`; this
  meaning, and everything below it, is unchanged by cross-signal correlate.
- **`kind`** — `"inner"` drops a row whose parent isn't in the window (a root
  span, or a parent that started before the window); `"left"` keeps it with
  every `parent.*` field `null`.
- Every field of the parent span is addressable as `parent.<field>`,
  including attribute scopes (`parent.span.<key>`, `parent.resource.<key>`) —
  the same logical names as the unprefixed child side. Later `where` and
  `aggregate` stages accept fields from both sides, as in the caller/callee
  example above.
- Both sides of the join are read from the query's own time range and the
  caller's tenant/dataset only; a parent stored outside either is treated as
  missing, same as a parent genuinely absent from storage.
- The joined row count is capped by a server-side limit
  (`[querier].correlate_max_rows`, default 5,000,000), enforced at the join
  itself — before any later `aggregate`/`where`/`limit` stage — so hitting it
  is never hidden by what those stages do to the row count afterward.
  Reaching it truncates the result to the cap and adds a
  `correlate_row_limit` warning; the query still succeeds rather than
  failing.

### Correlating across signals (v11)

`to` also accepts the name of a registered signal source other than the
pipeline's own `from` — for example a `traces` query correlating `to:
"logs"`. An unregistered name is a validation error, naming the source, not a
parse failure.

```json
{
  "irVersion": 11,
  "from": "traces",
  "range": { "from": "now-1h", "to": "now" },
  "result": "table",
  "pipeline": [
    {
      "correlate": {
        "to": "logs",
        "on": "trace_id",
        "kind": "inner",
        "pipeline": [
          { "where": { "field": "severity", "op": "eq", "value": "ERROR" } }
        ],
        "window": { "before": "5m", "after": "10m" },
        "fanout": 50
      }
    }
  ]
}
```

| Operand    | Type                                  | Default  | Meaning                                                               |
| ---------- | ------------------------------------- | -------- | --------------------------------------------------------------------- |
| `to`       | `"parent"` or a signal source name    | required | join target; a signal name other than `from`                          |
| `on`       | join key name                         | required | the logical key both sides join on; forbidden when `to` is `"parent"` |
| `kind`     | `inner` \| `left` \| `semi` \| `anti` | required | join kind (`semi`/`anti` are signal-target only)                      |
| `pipeline` | array of `where` stages               | `[]`     | filters applied to the target side before the join                    |
| `window`   | `{ before?, after? }` durations       | `0`, `0` | widens the target scan window past the source rows' own time envelope |
| `fanout`   | integer, `1..=10000`                  | `100`    | inner/left only: max target rows kept per source row                  |

**Join keys.** `on` names one of four logical keys, each resolved per source
against `otel-native-logical-schema` rather than the physical column:

| Key                 | Carried by                        | Field name(s)                                                             |
| ------------------- | --------------------------------- | ------------------------------------------------------------------------- |
| `trace_id`          | traces, logs, profiles, exemplars | `trace_id` (`trace.id` on profiles and exemplars)                         |
| `span_id`           | traces, logs, profiles, exemplars | `trace_id` + `span_id` (`trace.id` + `span.id` on profiles and exemplars) |
| `resource_identity` | every signal source               | `resource.identity`                                                       |
| `series_id`         | metrics, exemplars                | `series.id`                                                               |

A key absent from either side is a validation error, and so is a key whose field has no stored column in that source's table (it would otherwise silently match nothing). If the source relation
is already aggregated (a closed schema), every field the key needs must be a
group column of that aggregation, or the query is rejected: "correlate key
'`<key>`' was dropped by a preceding aggregate." A signal-target correlate may
therefore follow `aggregate`/`topk`/`order`/`limit` — unlike `to: "parent"`,
which must still precede any `aggregate` — enabling patterns like "aggregate
by `trace_id` to the slowest traces, then fetch their logs" (see the worked
example below).

**Join kinds and output.** `semi` and `anti` return the source relation
filtered, unchanged in shape: `semi` keeps only source rows with a target
match, `anti` only those without one. `inner` and `left` add the matched
target's fields under a `<target>.` scope (`logs.body`, `logs.severity`, …),
resolved the same way `parent.*` resolves against traces; source fields stay
unprefixed, so same-named fields on both sides never collide. `inner` drops a
source row with no target match; `left` keeps it with every `<target>.*`
field `null`. Every kind keeps the source's row order, so the order a `topk`
or `order` stage produced survives the join. If the target's table doesn't exist yet, `semi`/`inner` return
no rows and `anti`/`left` return the source rows unmatched — the same "no
table yet reads as empty" rule the rest of the IR follows.

**Window.** The target scan window is derived from the source rows' own time
envelope, not clipped to the query's `range`: `[min, max]` of the source
time column (for traces, also widened by `start + duration` when duration is
in the relation). `window.before`/`window.after` extend that envelope
further, past its start and end. A source whose rows are instants (a `metrics` point, a log record) has a narrow envelope, so correlating from it usually needs `window` widened to cover where the target's rows fall. When the target is `traces`, a span matches
if it overlaps the window rather than starting inside it, so the span that
contains a log or exemplar is found even though it started earlier; spans
that started more than 1 hour before the window need `window.before`. Absence (`anti`) and enrichment
(`inner`/`left`) are judged strictly within this window — a target row
outside it does not count as a match, even though it exists — so late-arriving
target data needs `window` to be picked up.

**Bounds**, each reported rather than silently applied:

- **Source rows** — the source pipeline (everything before `correlate`) is
  capped at `[querier].correlate_max_source_rows` (default 10,000). Every
  join kind, including `semi`/`anti`, rejects a source over the cap with an
  HTTP `422` error of type `resource_limit` rather than running unbounded — the
  target scan starts only after the source is known to fit. Retrying does not
  help; narrow the source with `where`, `topk`, or `limit`.
- **Fan-out** — `inner`/`left` only: past `fanout` (default 100, max 10,000)
  target rows for one source row, the kept subset is the earliest by target
  time, ties broken by the remaining target columns, so a capped result is
  reproducible rather than an arbitrary subset. The response carries a
  `correlate_fanout_limit` warning. `semi`/`anti` have no fan-out cap: they
  produce at most one row per source row, and a cap would change whether a
  row counts as a match at all.
- **Joined row count** — the existing `[querier].correlate_max_rows` cap
  (default 5,000,000) still bounds the streamed output of every join kind;
  reaching it adds `correlate_row_limit`, same as the `to: "parent"` join.
- **Window** — every signal-target correlate carries a `correlate_window`
  warning stating the resolved target scan window, so a reader can tell
  absence and enrichment were judged within that window and can widen it with
  `window` if it's too narrow.

**Pushdown.** The join key is pruned as a literal bound on the target scan
only when the target's stored encoding already equals the canonical form
(lowercase for strings, lowercase hex for binary) — for example a `trace_id`
column the writer already stores as lowercase hex. When the stored encoding
differs, the join is still correct, computed by encoding the stored column
before comparing, just without that scan pruning.

**Worked examples.**

Logs for the ten slowest traces in the window:

```json
{
  "irVersion": 11,
  "from": "traces",
  "range": { "from": "now-1h", "to": "now" },
  "result": "table",
  "pipeline": [
    {
      "aggregate": {
        "by": ["trace_id"],
        "aggs": [{ "fn": "max", "of": "duration", "as": "duration" }]
      }
    },
    { "topk": { "n": 10, "of": "duration" } },
    { "correlate": { "to": "logs", "on": "trace_id", "kind": "inner" } }
  ]
}
```

Traces with no error log (`anti`):

```json
{
  "irVersion": 11,
  "from": "traces",
  "range": { "from": "now-1h", "to": "now" },
  "result": "table",
  "pipeline": [
    {
      "correlate": {
        "to": "logs",
        "on": "trace_id",
        "kind": "anti",
        "pipeline": [
          { "where": { "field": "severity", "op": "eq", "value": "ERROR" } }
        ]
      }
    }
  ]
}
```

Traces behind a metric's exemplars, joined on the exemplar's own
`trace.id`/`span.id`:

```json
{
  "irVersion": 11,
  "from": "exemplars",
  "range": { "from": "now-1h", "to": "now" },
  "result": "table",
  "pipeline": [
    { "correlate": { "to": "traces", "on": "span_id", "kind": "inner" } }
  ]
}
```

Metrics enriched with logs from the same resource, where neither side carries
a `trace_id`:

```json
{
  "irVersion": 11,
  "from": "metrics",
  "range": { "from": "now-1h", "to": "now" },
  "result": "table",
  "pipeline": [
    {
      "correlate": {
        "to": "logs",
        "on": "resource_identity",
        "kind": "left"
      }
    }
  ]
}
```

## Structural matching: the `match` stage (IR v12)

`match` finds traces by their shape: traces in which named groups of spans
("span-sets") exist and stand in given parent/child relationships. It returns
the spans that witness the match.

```jsonc
{ "match": {
    "spansets":  { "<name>": <predicate>, … },          // 1..=8 named span-sets
    "relations": [ { "left": "<name>", "op": "child|descendant|ancestor|sibling", "right": "<name>" }, … ]
} }
```

- **`spansets`** — an object of name to predicate. A predicate is any
  [`where` predicate](#predicates) over span fields, including
  [attribute scopes](#addressing-an-attribute-scope) and the span-event and
  span-link fields (`events.name`, `events.attributes.<key>`, `links.trace_id`,
  `links.span_id`, `links.attributes.<key>`). A span belongs to a span-set when
  it satisfies the predicate; one span can belong to several. Names match
  `^[a-z_][a-z0-9_]{0,31}$` and are unique; 1 to 8 span-sets per stage.
  Declaration order is significant (see `spansets` below).
- **`relations`** — optional. Each names two declared span-sets; an undeclared
  name is rejected.

| `op`         | Holds for `left`, `right` when                                     |
| ------------ | ------------------------------------------------------------------ |
| `child`      | a `right` span's parent is a `left` span                           |
| `descendant` | a `right` span is a `left` span's descendant at any depth (not itself) |
| `ancestor`   | a `right` span is an ancestor of a `left` span (`descendant` with the sides swapped) |
| `sibling`    | a `left` span and a *different* `right` span share a non-empty `parent_span_id` |

### Semantics

- A trace matches when every span-set has at least one span **and** every
  relation holds. Each relation is **existential** (some pair of spans
  satisfies it) and relations are **independent**: two relations naming the
  same span-set do not have to be satisfied by the same span.
  `descendant(a, b)` and `descendant(a, c)` match a trace where one `b` sits
  under one `a` and a `c` sits under a different `a`.
- Span-sets that appear in no relation only need to exist in the trace.
- **Only spans inside the document's `range` are seen.** A relation that
  would pass through a span outside the range does not match: a `child` whose
  parent is out of range has no parent, and a `descendant` chain broken by an
  out-of-range span is broken. Widen the `range` to see the whole trace.
  When the stage has a relation and an evaluated trace shows such a cut, the
  response carries a [`match_incomplete_trace`](#warnings) warning and the
  rows stay the same. A trace counts when one of its in-range spans names a
  `parent_span_id` that no in-range span of the trace carries (the parent
  started before the range, or was never ingested), or when an in-range span
  ends strictly after `range.to` (its children may start after the range). A
  trace dropped because a span-set had no in-range span is not counted, and
  clock skew can hide a cut, so a missing warning does not prove the traces
  are whole. Skew also works the other way: with `range.to` at `now`, a span
  whose clock runs ahead can end just after `now` and raise the warning for a
  trace that is complete.
  `sibling` compares `parent_span_id` values, so two spans whose shared parent
  is out of range are still siblings. A parent cycle (corrupt data) is cut
  at the span with the smallest `span_id` in the cycle, which then counts as
  a root, so every `child` pair is also a `descendant` pair.
- Rows that share a `span_id` within a trace (a span delivered twice) are
  treated as **one span**: it matches a span-set if any of its rows does, it
  is never its own sibling, and all of its rows come back when it is a
  witness.
- The result is the **witness rows**: for each matching trace, the spans that
  take part in a relation (as either side), plus every span of a span-set that
  is in no relation. Other spans of the trace are not returned. Use
  `result: "trace"` (below) for one entry per trace.

### The `spansets` column

Each witness row carries a string column `spansets`: the comma-joined names of
the span-sets that row witnesses, in declaration order (`"root,any"`). Later
stages can `where` on it, `order` by it, group by it, or `fields`-project it.
A `match` stage fails validation if the relation already has a `spansets`
column.

### Placement and ordering

- `traces` only (other sources: `source 'logs' does not support match`).
- Must be the **first** pipeline stage, and at most one per pipeline.
- Rows come back ordered by `trace_id`, then span start time. Any following
  stage may reorder, filter, project, `limit` or aggregate them.
- With `"result": "trace"`, each matching trace is one entry in `traces`,
  holding its witness spans (see [Trace envelope](#trace-envelope-traces-only-ir-v12)).
  A `limit` bounds spans, not traces. `match` with the default `rows`
  envelope returns the flat witness rows.

### Example: a `write_parquet_files` with a `create_arrow_writer` child

SignalDB's own writer traces (`_system/_monitoring`) record every Parquet
flush as a `write_parquet_files` root span with a `create_arrow_writer` child:

```json
{
  "irVersion": 12,
  "from": "traces",
  "range": { "from": "now-1h", "to": "now" },
  "result": "trace",
  "pipeline": [
    { "match": {
        "spansets": {
          "flush":  { "field": "span.name", "op": "eq", "value": "write_parquet_files" },
          "writer": { "field": "span.name", "op": "eq", "value": "create_arrow_writer" }
        },
        "relations": [ { "left": "flush", "op": "child", "right": "writer" } ]
    } },
    { "limit": 500 }
  ]
}
```

Each returned trace lists its `write_parquet_files` spans that have a
`create_arrow_writer` child (`spansets: "flush"`) and those
`create_arrow_writer` spans (`spansets: "writer"`). To keep only the
`write_parquet_files` side, add
`{ "where": { "field": "spansets", "op": "eq", "value": "flush" } }`
(`eq` works here because no span can be in both sets; when the span-sets
overlap, a span in both reads `"flush,writer"`, so use `contains` and pick
names that are not substrings of each other).

Span-set predicates can use event fields, e.g. a Flight `DoPut` server span
with a direct child that recorded an `exception` event (on a healthy
deployment this one returns no traces):

```json
{
  "irVersion": 12,
  "from": "traces",
  "range": { "from": "2026-09-30T08:00:00Z", "to": "2026-09-30T09:00:00Z" },
  "result": "trace",
  "pipeline": [
    { "match": {
        "spansets": {
          "put":    { "field": "span.name", "op": "eq", "value": "arrow.flight.protocol.FlightService/DoPut" },
          "failed": { "field": "events.name", "op": "eq", "value": "exception" }
        },
        "relations": [ { "left": "put", "op": "child", "right": "failed" } ]
    } }
  ]
}
```

### Per-trace bounds

`match` buffers one whole trace at a time, and that buffer counts toward the
query's memory pool (`[querier].memory_limit_mb`); a trace that does not fit
fails the same way as one over a bound. Two bounds on a single trace:

| Key                                | Default            | Bounds                                  |
| ---------------------------------- | ------------------ | --------------------------------------- |
| `[querier].match_max_trace_spans`  | 100,000            | spans of one trace inside the range     |
| `[querier].match_max_trace_bytes`  | 67,108,864 (64 MiB)| value bytes of one trace's buffered rows|

A trace over either bound fails the **whole query** with HTTP 422 and
`errorType: "resource_limit"`; the message names the trace and the bound, for
example `match: trace <trace_id> exceeds the span bound of 100000
([querier].match_max_trace_spans): it has at least 100001 spans; a trace is
never evaluated partially, so narrow the range or raise the bound`. The
answer is never truncated and oversized traces are never skipped, so a
successful response is always complete. React by narrowing the `range` (fewer
of that trace's spans fall inside it) or asking the operator to raise the key (set in `[querier]`, see the
`configuration` reference and `signaldb.dist.toml`).

## Formulas: cross-query arithmetic (D5)

A formula computes arithmetic across the `series` results of several named
queries in **one** request — an error ratio, a percentage, a difference —
rather than in the client. Instead of the single-document shape
(`{irVersion, from, range, result, pipeline}`), `POST /api/v1/query` accepts
a **multi-query document**, recognized by its `queries` key:

```jsonc
{
  "queries": {
    "errors": {
      "irVersion": 1,
      "from": "traces",
      "range": { "from": "now-1h", "to": "now" },
      "result": "series",
      "pipeline": [
        { "where": { "field": "status.code", "op": "eq", "value": "Error" } },
        {
          "aggregate": {
            "by": ["service.name"],
            "aggs": [{ "fn": "count", "as": "n" }],
            "step": "1m",
          },
        },
      ],
    },
    "total": {
      "irVersion": 1,
      "from": "traces",
      "range": { "from": "now-1h", "to": "now" },
      "result": "series",
      "pipeline": [
        {
          "aggregate": {
            "by": ["service.name"],
            "aggs": [{ "fn": "count", "as": "n" }],
            "step": "1m",
          },
        },
      ],
    },
  },
  "formulas": [{ "name": "error_ratio", "expr": "errors / total" }],
  "result": "series",
}
```

Every named query must declare `result: "series"` — a formula document has
no other shape. `expr` is `+ - * /` over numeric constants, the request's own
query names, and parentheses, standard precedence, left-associative.

Each inner query executes exactly like a standalone single-document request
(its own Flight ticket, its own `metrics`/`traces`/... source), all under one
server clock stamp; every query's source needs the matching read scope before
any of them run. Once every inner query has returned, each formula is
evaluated by joining its referenced queries' series on an **identical label
set and timestamp**:

- a series present in one operand's result but missing from another's
  contributes nothing to the output — no error, just no series for that
  label set;
- a point whose divisor is zero is dropped from the output series, not an
  error;
- a numeric constant broadcasts across every series it meets (`a * 100`).

The response is an ordinary `series` envelope. Each output series' `labels`
carries the joined labels plus a `formula` key naming which formula produced
it, so a request with several formulas stays distinguishable in one response.

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
      { "name": "body", "type": "any_value", "filterable": true,
        "origin": "declared" },
      { "name": "span_events", "type": "any_value", "filterable": false,
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

| `origin`    | meaning                                                                                                           |
| ----------- | ----------------------------------------------------------------------------------------------------------------- |
| `declared`  | the canonical logical schema declares it (always valid)                                                           |
| `authority` | the type authority committed a canonical type for it at ingest, listed even before statistics observe it          |
| `registry`  | statistics observed it and a schema registry defines it, so it carries a description (its type is still `string`) |
| `observed`  | statistics observed it and nothing defines it — treated as a string                                               |

An attribute's `type` is the canonical type the type authority committed for it
(first-seen, or pinned by config) — the same type a predicate on it is coerced
to, so a key whose first value was an integer is `int64` here. A key with no
authority type is `string`, which is how the planner reads it; a registry
contributes the description and deprecation, never a type. A key committed at
more than one attribute level is listed once per level, qualified the way the
source addresses that level (`resource.region` and `log.region` on logs,
`span.region` on traces, `point.region` on metrics), and carries no `coverage`
or `cardinality` because statistics are kept per key. A key that begins with
its source's own qualifier is listed escaped (`log.log.file.path`), and an
attribute whose name would read a declared field (a scope attribute called
`name`, which `scope.name` resolves to the scope's column) is not listed.

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
                "sources": [ { "name": "exemplars", "available": true },
                             { "name": "logs", "available": true },
                             { "name": "metrics", "available": true },
                             { "name": "profiles", "available": false },
                             { "name": "traces", "available": true } ],
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
  the OpenAPI document at `GET /api/v1/openapi.json`, which types each
  `pipeline` entry as an `IrStage`, so a generated client checks stages
  before sending them).

The first-party UI and CLI consume the endpoint exclusively through their
generated clients (the TypeScript client and Rust SDK), never hand-written HTTP.

## Roadmap

The IR is the base of a dependent stack; each sibling is a separate capability
so it is designed and reviewed on its own risk profile. Still deferred:

- **live tail** — streaming new matching records over the same document
  (part of the streaming epic), and **pagination** for walking a large result.
  Field discovery itself has landed: see
  [Discovery](#discovery-what-can-i-query).
- **typed wire and WAL fidelity** — duplicate attribute keys and key order are
  not preserved today (the wire carries attributes as JSON); keeping them needs a
  typed wire format, a breaking change of its own.
- **a materialized-ancestry fast path for `match`** — structural matching
  evaluates each trace in memory within explicit bounds; precomputed ancestry
  would be an optimization, not a change to the semantics.

`rate`/`increase`/`irate`/`*_over_time` (counter delta and windowed
reductions over a window — see
[Counter rate](#counter-rate-rateincrease-v6) and
[More range functions](#more-range-functions-across-and-window-v7)),
cross-query formulas (see
[Formulas](#formulas-cross-query-arithmetic-d5)), and the `correlate` stage,
both span-to-parent and cross-signal (see
[Correlate: joining across relations](#correlate-joining-across-relations-v8-v11)),
and structural trace matching — the `match` stage with the `trace` envelope (see
[Structural matching](#structural-matching-the-match-stage-ir-v12)), already
work today.

The compatibility dialects already lower _into_ the IR (TraceQL, LogQL and
PromQL through `ql-ir` where the IR can express the query, run by the same
planner), and attribute promotion is
live: a frequently queried attribute key gets a typed `attr_<level>_<key>`
column that changes how much is read, never the result. None of the deferred
items change the document shape defined here — that is the point of
versioning it from day one.
