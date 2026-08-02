## Context

See proposal.md — Why. Current-state facts, verified against the tree:

- **Every signal is already a DataFusion table over Iceberg** (`src/querier/src/flight.rs`).
  The dialects are sugar over what is ultimately SQL, which is what makes a
  single relational IR viable — we unify three query front-ends over one engine,
  not three storage models.
- **Queries reach the querier as JSON Flight tickets** (`src/querier/src/query/mod.rs`:
  `LogQueryParams`, …), each **single-table** and prefixed with
  `tenant_slug:dataset_slug`. Adding the IR adds one ticket prefix
  (`query_ir:{tenant}:{dataset}:{json}`) whose planner builds a full
  `LogicalPlan`. **Scoping invariant:** the router supplies the tenant/dataset
  from the authenticated request context (never from client-controlled document
  body), and the querier rejects any ticket whose tenant/dataset it cannot
  resolve for that context — identical to the existing routed tickets.
- **Attributes are JSON blobs, not columns** (`src/common/src/flight/schema.rs`):
  traces/logs carry `attributes_json` / `resource_json`; only a fixed set are
  physical columns (traces: `trace_id`, `span_id`, `parent_span_id`, `name`,
  `service_name`, `duration_nano`, `status_code`, `is_root`, …; logs:
  `time_unix_nano`, `severity_number`, `body`, `service_name`, …). The attribute
  registry decides column-vs-JSON per field.
- **Metrics are structurally different** — samples live in a `data_json` blob
  with `metric_type ∈ {gauge,sum,histogram,exponential_histogram,summary}` plus
  `aggregation_temporality` and `is_monotonic`, and there is **no `trace_id`** on
  the metric row. This is why metrics are **not** a core source (see
  `query-metrics-model`).
- **A fourth signal, `profiles`, exists** in `schema.rs` (with `trace_id`/
  `span_id`). Hence the source set is a **registry**, not a hardcoded enum.

## Goals / Non-Goals

**Goals**

- A **versioned** JSON IR with a **type system and denotational semantics**
  defined independently of the emitted plan — so the contract is stable across
  DataFusion upgrades and is a sound lowering target for later stages and
  front-ends.
- Single-signal (logs, traces) end-to-end: build in the UI without a dialect
  string, execute via `LogicalPlan`.
- Promotion-invariant field resolution via the registry.

**Non-Goals (this change)**

- `correlate`, `match`, `binop`, metric range-vectors — each is a sibling change.
- Dialects lowering into the IR; all-attribute promotion — later/existing epics.
- A text syntax — JSON only.

## FDAP alignment

IR types and planner use **DataFusion-re-exported** Arrow/Parquet types
(`datafusion::arrow`, `datafusion::logical_expr`, `datafusion::prelude`). No
direct `arrow`/`parquet` imports — the planner builds `LogicalPlan`/`Expr`, so
version drift against the querier's DataFusion is a correctness issue.

## The type system (the contract — this is what lowering must satisfy)

The IR's meaning is defined here, **not** by the LogicalPlan it emits. A plan is
correct iff it evaluates to the denotation below.

### Value types and null semantics

```
   ValueType = string | int64 | float64 | bool | timestamp_ns | duration_ns
             | bytes | array<ValueType>
```

- Each **logical field** has exactly one canonical `ValueType`, owned by the
  attribute registry (never inferred from an incidental physical column).
- **Absent is a third truth value, and it propagates.** A comparison against an
  absent field evaluates to `absent` (not `true`, not `false`); `exists` is the
  only operator that maps an absent field to `true` (and its negation to
  `false`). `absent` propagates through the connectives — Kleene-style, treating
  `absent` like SQL's `unknown` but defined **here** so the result never depends
  on DataFusion's own NULL handling:

  ```
   not(absent) = absent
   absent AND false = false     absent AND true = absent     absent AND absent = absent
   absent OR  true  = true      absent OR  false = absent     absent OR  absent = absent
  ```

  A row is emitted by a `where` only when the top-level predicate evaluates to
  `true`; `absent` (like `false`) does **not** match. Consequently _both_
  `field = x` and `not(field = x)` exclude rows where `field` is absent — the
  earlier draft's claim that these were both "`false`" was inconsistent (ordinary
  negation would flip it); the fix is that they are both `absent`, which the
  emission rule treats as non-matching. To match-or-exclude on absence
  explicitly, use `exists` / `not(exists)`, the only operators that observe it.

- **Coercion** is defined per target `ValueType`: duration literals accept unit
  suffixes (`"500ms"`, `"2s"`) → `duration_ns`; numeric strings → `int64`/
  `float64`; RFC3339/relative (`now-1h`) → `timestamp_ns`. Coercion is total or
  the query is rejected at validation — never a silent runtime cast. **Relative
  timestamp anchoring is resolved once, at a single execution-time boundary** (the
  router stamps the request's server-received clock when it builds the Flight
  ticket), so `now-1h` means one hour before that stamp; only _coercibility_
  (well-formed syntax) is checked at validation. The **resolved absolute bounds
  are carried through the ticket and the plan** — every stage sees identical
  `[t0,t1]` values, not a re-evaluated `now` — and the resolved window is echoed
  in the response for reproducibility and replay.

### Relation types (what flows between stages)

Every stage consumes and produces a typed **relation**:

```
   RelationType =
     | RowSet { source, columns: [{name, ValueType}], grain, aggregated: bool }
     | Series { labels: [name], value: ValueType, step }        // time series
   grain ∈ { event, span, trace }   // what one row denotes
```

`aggregated` is the discriminator that makes the two RowSet-terminal envelopes
distinguishable: a raw scan/filter yields `aggregated=false` → `rows`; a grouped
`aggregate` (no `step`) yields `aggregated=true` → `table`. Without it the
validator could not tell `rows` from `table` (both are RowSets), so envelope
validation would be prose, not a function of the type — this bit is what keeps it
a function of the type.

Legality and envelope-validation are **functions of RelationType**, not prose:

- A stage declares an input RelationType constraint and an output RelationType,
  **including the output columns and grain** so downstream stages can validate
  their references against an inferred schema:
  - `aggregate{ by, aggs }` **no `step`** → `RowSet{ columns = by-fields ∪
aggregate outputs, grain = group, aggregated = true }`. Each aggregate output
    is a named column (see `Agg.as` below); the group fields keep their canonical
    types.
  - `aggregate{ by, aggs, step }` → `Series{ labels = by-fields, value = the
single aggregate output's type, step }`. A `step` aggregate requires exactly
    one aggregate output (the series value); multiple outputs without a `step`
    stay a `table`.
  - `extract` requires `source ∈ {logs}` and adds columns (see _extract field
    resolution_ below).
  - `topk`/`order` require every referenced column/`AggRef` to be present in the
    inferred input schema; a reference to a name the schema does not carry is a
    validation error. After an aggregate has dropped a column, a later stage
    referencing it fails validation (this is also what makes "the join key
    survived the aggregate" checkable in the correlate sibling).
- The planner **infers the RelationType (columns + grain + aggregated) through
  the pipeline**, rejects a stage whose input constraint or column references are
  unmet, and validates the declared envelope against the terminal type —
  `rows`⇔`RowSet{aggregated=false}`, `table`⇔`RowSet{aggregated=true}`,
  `series`⇔`Series`. This is the single mechanism behind "legal-stage per
  source" and "envelope matches terminal stage."

### Structured operands (no embedded strings)

Operand expressions are structured values, never parsed substrings:

```
   Agg   = { fn: "count"|"sum"|"avg"|"min"|"max"|"quantile",
             of?: FieldRef, arg?: number, as: Name }   // `as` names the output column
   Order = { of: FieldRef | AggRef, dir: "asc"|"desc" }
   topk    = { n: int>0, of: AggRef | FieldRef }
   bottomk = { n: int>0, of: AggRef | FieldRef }
   AggRef  = a Name introduced by a prior aggregate's `as`
```

- Every `Agg` carries a required **`as`** name; that name is the output column and
  the only thing `AggRef`, `Order`, and `topk`/`bottomk` may reference. So
  `topk:{of:"max(duration)"}` from the pre-restructure draft is **removed** — the
  aggregate would be `{ fn:"max", of:"duration", as:"max_dur" }` and the rank
  `topk:{ n:10, of:"max_dur" }`.
- **Names are unique within a pipeline.** Two aggregates (or an aggregate and a
  source column) sharing a name is a validation error — no implicit shadowing, so
  every `AggRef` resolves to exactly one column.
- `n` on `topk`/`bottomk` MUST be an integer `> 0`; `0` or negative is rejected at
  validation.

### Extract field resolution (logs)

`extract` introduces **query-local** fields, which coexist with registry-owned
logical fields under one rule:

- An `extract` declares each derived field with a name and a `ValueType` (`extract{
parser, as: [{name, type}] }`); those types come from the extract declaration,
  not the registry (the registry owns _stored_ attributes, not per-query
  derivations).
- **Precedence and collisions:** a derived name that collides with a
  registry-owned logical field, or with an earlier `extract`, is a validation
  error — extract may not silently shadow a registry field. Downstream `where`/
  `aggregate`/`order` resolve a reference to the registry field if one exists,
  else to the in-scope derived field; ambiguity (both) is the rejected collision
  above.
- Coercion of literals compared against a derived field uses the field's
  **declared** `ValueType`, exactly as registry fields use their canonical type.

## Versioning & evolution (a persisted format)

IR documents are stored in dashboards, so the format is a compatibility surface
from commit #1:

- The document carries a top-level **`irVersion`** (integer). The server accepts
  a bounded range and reports the range it supports.
- **Additive-only, deprecate-never-remove.** New stages/ops/fields are additive;
  an op is deprecated (still accepted) before it is ever removed in a future
  major.
- **Reader tolerance vs. physical-addressing guard.** Predicate/stage objects are
  parsed with `deny_unknown_fields` (so a client cannot smuggle a physical
  column or `attributes_json` reference), but the **top-level document** tolerates
  unknown _optional_ envelope-level keys forward-compatibly. The two are
  reconciled by validating structure strictly at the stage level and versioning
  at the document level.
- The **operator/function set is a versioned registry**, not an open enum:
  adding a new agg fn or the deferred `regex` extract parser is a registry +
  version bump (additive), enabling capability negotiation with clients — so the
  deferred `regex` parser has a defined home (a later registry entry), not a
  separate change.
- **`regex` is a DoS surface wherever it appears.** The predicate `regex` **op**
  carries the same catastrophic-backtracking risk as the deferred `regex`
  **extract** parser; both MUST run behind a bounded, timeout-guarded matcher.
  The predicate `regex` op ships in core with that guard; the `regex` extract
  parser is deferred behind the same guard.

## Shared predicate grammar (every `where`)

```
Predicate =
  | { field, op, value }   op ∈ eq ne gt gte lt lte in between contains regex exists
  | { and: [Predicate, …] } | { or: [Predicate, …] } | { not: Predicate }
field = dotted OTel-native name; resolved by the registry to column | json-path.
```

A leaf naming a physical column, `attributes_json`, or any storage detail is
**rejected** (logical-namespace guard).

## Document shape and core stage set (single-signal)

`from` is **not a pipeline stage** — it is a **document-level field** that selects
the source and seeds the initial `RowSet{source}`; the `pipeline` is the ordered
list of _transform_ stages that follow. (An earlier draft listed `from` in the
stage table while the worked example put it at the document root — the document
root is canonical; the stage table below is transforms only.)

```
   Document = { irVersion, from: Source, range, result, fields?, pipeline: [Stage] }
```

| stage (pipeline) | role                 | input → output (RelationType)                 | lowers to               |
| ---------------- | -------------------- | --------------------------------------------- | ----------------------- |
| `where`          | filter               | `RowSet → RowSet`                             | `Filter`                |
| `extract`        | derive fields (logs) | `RowSet{logs} → RowSet{+cols}`                | `Projection`(+UDF)      |
| `aggregate`      | group-reduce         | `RowSet → Series` (step) \| `RowSet → RowSet` | `Aggregate`             |
| `topk`/`bottomk` | rank                 | needs numeric col; preserves type             | windowed `Sort`+`Limit` |
| `order`/`limit`  | row control          | `RowSet → RowSet`                             | `Sort`+`Limit`          |

`extract` v1 parsers: `json` + `logfmt`. `regex` is deferred to a bounded,
timeout-guarded UDF (registry-gated) — a validation/DoS surface.

## Result envelope — declared and validated

Declared `result ∈ {rows, series, table}`; validated against the inferred
terminal RelationType.

**Curated projection is an explicit document field, not an implicit rule.** The
`rows` envelope draws its columns from a document-level **`fields: [FieldRef]`**
(logical names, resolved by the registry like any other reference). If `fields`
is omitted, the server applies a bounded, registry-driven default set — **never
`SELECT *`** (in an all-promoted world a bare `*` is thousands of columns). A
`fields` entry that isn't present in the terminal RowSet is a validation error.
`fields` is meaningful only for `rows`/`table`; declaring it with `series` is
rejected.

The **result payloads** each have one canonical wire shape (defined once in the
OpenAPI schema, so the SDK/TS clients decode a single contract):

```
   rows   : { fields: [{name, ValueType}], rows: [[value…]] }        // aggregated=false
   table  : { columns: [{name, ValueType}], rows: [[value…]] }       // aggregated=true
   series : { series: [{ labels:{…}, points:[[t_ns, value]…] }], step }
```

Value encoding follows `ValueType`: `timestamp_ns`/`duration_ns` as integer
nanoseconds, `bytes` as base64, others as their JSON-native form. The remaining
envelopes arrive with their owning sibling changes, each owned by exactly one:
`trace` → `query-structural-traces`; `scalar` → `query-metrics-model`;
`metadata` → `query-field-discovery`.

```
   RowSet{aggregated=false}  → rows
   RowSet{aggregated=true}   → table
   Series                    → series
```

## Worked lowering — "error-log volume by service, 1m buckets" (logs → series)

> Named "volume" deliberately: this pipeline computes `count` per 1-minute
> bucket, i.e. events-per-bucket, not a `rate()` (which would divide by the
> window and is a `query-metrics-model` concern). The example is a counting
> series, and the title matches.

```json
{
  "irVersion": 1,
  "range": { "from": "now-1h", "to": "now" },
  "result": "series",
  "from": "logs",
  "pipeline": [
    {
      "where": {
        "and": [
          { "field": "severity_number", "op": "gte", "value": 17 },
          { "field": "deployment.environment", "op": "eq", "value": "prod" }
        ]
      }
    },
    {
      "aggregate": {
        "fn": "count",
        "by": ["service.name"],
        "step": "1m",
        "as": "n"
      }
    }
  ]
}
```

`severity_number` resolves to a **column**; `deployment.environment` is not
promoted → **json path**. Emitted plan:

```
Sort: service_name, ts_bucket
└─ Aggregate: groupBy=[service_name, ts_bucket], aggr=[count(*)]
   └─ Projection: service_name,
   │              date_bin('1m', to_timestamp_nanos(time_unix_nano)) AS ts_bucket
   └─ Filter: severity_number >= 17
   │          AND json_get_str(attributes_json,'deployment.environment') = 'prod'
   └─ TableScan: logs   projection=[service_name, severity_number, time_unix_nano, attributes_json]
                        pushed: severity_number>=17, time_unix_nano ∈ [t0,t1]
```

**Promotion payoff:** if `deployment.environment` is later promoted, only the
`Filter` line changes — the `json_get_str(...)` becomes a bare column ref that
pushes into the `TableScan` for bloom/predicate pruning. Same IR, same result
(the denotational spec guarantees it), faster plan, zero client change.

## Forward-compatibility invariants (load-bearing)

- **Logical field namespace only** — no client ever names a physical column,
  `attributes_json`, or storage detail; resolution is always registry-mediated.
- **No `SELECT *` in `rows`** — curated projection.
- **Registry owns field types** — coercion keys off the registry canonical type.
- **The IR is front-end-neutral** — a relational core any front-end (builder,
  dialects, future text language) lowers into; the type system, not the plan,
  is the contract.

## Risks / trade-offs

- **New execution surface in the querier.** Genuinely new plan-construction code;
  confined to lowering + validation, running through existing DataFusion
  execution + Iceberg registration.
- **Type-system-first is more upfront work** than emitting plans directly —
  accepted deliberately: it is what makes the siblings (correlate/match/metrics)
  compose soundly and keeps results stable across DataFusion upgrades.
- **Transitional surface sprawl** — a native surface alongside the dialects until
  they migrate. Accepted: the IR is what they fold into.

## Open decisions carried into siblings (not this change)

- `correlate` fan-out/time-window/keys/encoding → `query-cross-signal-correlate`.
- `match` engine (SQL recursion vs per-trace evaluator vs materialised ancestry)
  and bounded-depth-vs-correctness → `query-structural-traces` (prototype first).
- Metric temporality/histogram model → `query-metrics-model`.
- Streaming/tail + pagination for large results → `query-field-discovery`
  (delivery-side surfaces). Core results stay range- and `limit`-bounded.
