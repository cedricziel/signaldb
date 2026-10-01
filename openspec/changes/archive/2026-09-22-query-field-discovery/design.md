# Design: Query Field Discovery

## Context

See proposal.md — Why. Load-bearing code facts (verified at `b4f8464f`):

- **IR**: `src/common/src/query_ir/` — `Document { ir_version, from, range,
result: ResultEnvelope, fields, pipeline: Vec<Stage> }` (`document.rs`),
  `ResultEnvelope = Rows|Series|Table|Heatmap|Flamegraph` with a doc comment
  reserving `trace`/`scalar`/`metadata` "for their owning sibling changes".
  `MAX_IR_VERSION = 3` (`version.rs`); per-version stage gating is an explicit
  check in `validate()` (`validate.rs:114-133`, the `heatmap`→v2 and
  `histogram_quantile`→v3 precedents).
- **Router**: `src/router/src/endpoints/query.rs` — `query_ir` handler,
  `TenantContextExtractor` + `source_read_scope`, re-serializes the request to a
  `Document`, builds a Flight ticket `query_ir:{tenant}:{dataset}:{json}` and
  calls the querier. `#[utoipa::path]` there, registered in
  `src/router/src/openapi.rs`, snapshot-tested against `api/signaldb-api.json`.
- **The metadata tier already exists and is nearly unused**:
  - `LogicalSchema::core()` (`src/common/src/schema/logical.rs`) — the canonical
    client-visible field catalog: `LogicalField { id{source, level, name},
value_type, filterability, kind, non_native }`, deliberately free of any
    physical realization. In-process, zero I/O.
  - `common::schema_registry::SchemaResolver` — merges the tenant's custom
    registries over the bundled `signaldb` and `otel` semconv registries;
    resolves an attribute key to an `AttributeDef` (type, brief, enum members,
    deprecation). Already backs `/api/v1/schema/*`, the CLI, and the MCP
    `resolve_attribute`/`search_schema` tools.
  - `attribute_stats` (`src/common/src/catalog.rs`) — `(tenant, dataset, signal,
attr_key) → present_rows, total_rows, distinct_estimate, capped,
query_hits, promote_streak`, written by the compactor's read-only analyzer
    (`src/compactor/src/attr_stats.rs`, which already tracks per-key presence
    and a distinct-value set capped at 10 000) and read today by exactly one
    endpoint, `promql.rs::label_stats` — **which reads the catalog directly from
    the router with no querier round-trip**. That is the precedent this design
    generalizes.
- **Every other discovery endpoint scans**: Loki `labels`/`label_values`/
  `detected_fields`, Tempo `search/tags` + `search/tag/{tag}/values` (v1 and
  v2), Prometheus `labels`/`label_values` all build a Flight ticket and let the
  querier read data. `trace-attribute-discovery`'s spec bounds them to a window
  and a sample — a scan, bounded, not a metadata read.
- **The attribute registry (#813) does not exist**: `SchemaResolver` in
  `src/querier/src/query/ir_planner.rs` falls through to
  `Resolved::JsonPath { container, key, value_type: String }` for any name the
  logical schema does not declare. Permissive by design — and it means the
  resolver cannot enumerate.

FDAP constraint: any Arrow/Parquet type used on the sampled path comes from
DataFusion's re-exports. Nothing here touches Flight v1 wire vs v2 storage
schemas, the WAL, or the Iceberg layout — no migration, no rollback story
beyond "revert the endpoint".

## Goals / Non-Goals

**Goals**

- One native discovery surface for every signal, over the logical namespace the
  IR already uses, so a discovered name is directly usable in a predicate.
- The interactive path costs a catalog read, not a query. Typing in a field
  picker must not schedule DataFusion work.
- Provenance and cost are in the response, so a client (and a reviewer) can see
  which tier answered and how stale or approximate it is.
- Forward-compatible with #813 and with value sketches: better data behind the
  same contract, no client change.

**Non-Goals**

- Replacing or extending the compat metadata endpoints (they stay as they are,
  for Grafana).
- Exposing promotion/materialization state or physical column names in any form.
- Exact, window-scoped, predicate-scoped answers. Those are queries; the IR
  already expresses them and they cost what queries cost.
- Caching/TTL infrastructure for discovery responses (catalog reads are cheap;
  revisit if measurement says otherwise).

## Decisions

**D1 — A discovery request is an IR document: `result: "metadata"` with a
terminal `describe` stage, at `irVersion` 4, on the existing
`POST /api/v1/query`.**

Stages are externally tagged (a single-key object naming the stage), so:

```jsonc
{ "irVersion": 4, "from": "logs", "range": {"from":"now-1h","to":"now"},
  "result": "metadata",
  "pipeline": [ { "describe": { "target": "fields" } } ] }

{ "irVersion": 4, "from": "traces", "range": {"from":"now-6h","to":"now"},
  "result": "metadata",
  "pipeline": [ { "describe": { "target": "values", "field": "http.route",
                                "limit": 100 } } ] }
```

Rationale: `query-ir-core` deferred the `metadata` envelope _to this change_, so
the envelope belongs in the document, not in a parallel request type. Reusing
the document gets version negotiation, the source registry, range literals,
tenant scoping, and `source_read_scope` authorization for free, and it keeps one
generated-client method per surface. The pairing rules mirror the existing
`flamegraph` precedent exactly: `describe` is terminal and legal only with
`result: "metadata"`; `metadata` is legal only with a terminal `describe`; only
`from` may precede it.

Alternatives rejected: (a) a separate `POST /api/v1/discover` request type —
duplicates version/range/source/auth handling and orphans the deferred envelope;
(b) reusing/extending the dialect metadata endpoints — forbidden for first-party
consumers and lossy by construction.

**D2 — `irVersion` 4, not a v3 addition.** `MAX_IR_VERSION` 3 → 4, and
`describe`/`metadata` under `irVersion < 4` are rejected with a typed error
naming the required version — never coerced, never executed with the stage
dropped — following the `heatmap`→v2 / `histogram_quantile`→v3 checks in
`validate()`. Stage sets are versioned capability sets in this IR
(`version.rs` says so); a client negotiating v3 must not have to guess whether
this server's v3 includes `describe`. The bump is labelled BREAKING as a
surface-version change (see proposal) even though every existing document keeps
its meaning; the v3-rejection behaviour is pinned by a test rather than left to
prose.

**D3 — The router answers `describe` locally; the document never reaches a
querier.** `query_ir` parses the document (it already does, to re-serialize it),
and when the terminal stage is `describe` it dispatches to the discovery path
instead of building a Flight ticket. That path reads `LogicalSchema::core()`
(in-process), the tenant's `SchemaResolver` (already held by the router for
`/api/v1/schema/*`), and `Catalog::get_attribute_stats(...)` — one indexed
catalog query. This is `label_stats`'s pattern, generalized.

Consequence, deliberate: **discovery availability is decoupled from querier
availability**. A field picker still works while queriers are saturated — which
is precisely when a user is most likely to be building a query.

The querier keeps a defensive rejection: an IR ticket carrying a `describe`
stage returns a clear "not executable" error rather than falling through the
lowering match.

**D4 — Three tiers, merged, with per-item `origin`.**

| tier       | source                             | contributes                                                      |
| ---------- | ---------------------------------- | ---------------------------------------------------------------- |
| `declared` | `LogicalSchema::core()`            | membership, canonical type, attribute level, filterability       |
| `registry` | tenant + bundled schema registries | type, brief, enum members for a key                              |
| `observed` | `attribute_stats`                  | membership for attribute keys, coverage, approximate cardinality |

Merge rule: **membership comes from `declared` ∪ `observed`, never from the
semconv registry alone.** Listing every semconv attribute a registry knows would
bury a tenant's actual fields under thousands of definitions it has never
emitted. The registry _enriches_ (type, brief, enum values) the keys the tenant
actually has, and is what makes an observed key more than an untyped string.
When #813 lands, `attribute_registry` replaces `attribute_stats` as the
membership source for `observed` items — same contract, better freshness and
scope fidelity.

Never emitted: physical column names, `label_*` materialization state,
`promote_streak`, `query_hits`. Promotion is a performance detail; per
`query-ir-core`, a query's result must not depend on it, so discovery must not
report it. (#820 floats a "fast" hint — deferred deliberately with #813, since
today's stats table has no materialization column and inventing one from table
schemas would leak physical layout.)

**D5 — Values: declared value sets are exact and free; everything else is
opt-in, bounded, and priced.** In order:

1. **Registry/intrinsic enumerations** — `span.kind`, `status.code`,
   `severity_text`, and any field whose registry `AttributeDef` carries enum
   members: returned exactly, `origin: "registry"`, `cost.mode: "metadata"`.
2. **Value sketches** — a bounded top-N-with-counts per `(tenant, dataset,
signal, key)` maintained by the compactor analyzer. The analyzer already
   walks every attribute value to compute presence and distinct counts and
   already holds a capped per-key value set; turning that set into a counted
   top-N and persisting it is an extension of a pass that already pays the scan.
   `origin: "statistics"`, `cost.mode: "metadata"`, `cost.asOf` = the sketch's
   `updated_at`. This is the tier that makes "values from statistics, not
   scans" true in general, and it is the last task group — the surface degrades
   to (3) without it.
3. **Nothing, with an explanation** — when no declared set and no sketch covers
   the field, the response returns zero values, `origin: "unavailable"`,
   `cost.mode: "none"`, and a `hint` naming the IR query that would compute the
   answer by scanning. Silence with a reason, never a silent scan.
4. **Explicit read of the data** — only when the request says
   `"sample": true`. The router then runs **the very query the hint in (3)
   names**: an ordinary IR `aggregate by [field] count` + `topk`, bounded by
   the requested window and limit, through the same path as any other query.
   It returns `origin: "sampled"` with counts, and
   `cost { mode: "sampled_scan", sampled: true, windowScoped: true }` beside
   the hint that says what was run. The cost of reading data is thus a client
   decision recorded in the request and reported in the response.

   Running the IR query rather than the compat label/tag-value tickets (the
   first sketch of this decision) keeps one execution path for anything that
   touches data, gives the answer value *counts* for free, inherits the
   query surface's own limits and authorization, and means the fallback is
   literally the query we tell the client to run — no second mechanism whose
   bounds could drift from the documented one.

Alternative rejected: defaulting to the sampled scan and merely _labelling_ it.
That reproduces exactly the property #820 exists to remove — a UI keystroke
scheduling a scan — with better documentation.

**D6 — Predicate-scoped discovery is rejected, not faked.** A `where` before
`describe` cannot be honoured from unconditional statistics: `attribute_stats`
counts every row of a table, not the rows matching a filter. Three options were
considered: silently ignore the predicate (lies), always scan (defeats the
change), or reject with a pointer. We reject, with an error naming the exact
equivalent the IR already expresses:

```jsonc
{ "irVersion": 4, "from": "traces", "range": {...}, "result": "table",
  "pipeline": [ { "where": ... },
                { "aggregate": { "by": ["http.route"],
                                 "aggs": [{"fn":"count","as":"n"}] } },
                { "topk": { "of": "n", "n": 100 } } ] }
```

That is a scan, it is bounded by the same limits as any query, and the client
asked for it explicitly. The `where` slot stays syntactically reserved so a
future sketch- or index-backed scoped answer is additive.

**D7 — Sources are a GET, not a document.** `GET /api/v1/query/sources` returns
the `metadata` envelope with `kind: "sources"`. "Which sources exist" has no
`from`, and `Document.from` is required; contorting the document (a sentinel
source) to fit would be worse than one small GET. Availability per source =
the source's table exists for the tenant/dataset, read from the same catalog the
tenant table listing uses — no scan, and a source with no data is reported as
present-but-empty rather than missing (matching the project's "a signal with no
table returns an empty result, never an error" rule).

**D8 — Cost object on every discovery response.**

```jsonc
"cost": { "mode": "metadata" | "sampled_scan" | "none",
          "window_scoped": false, "sampled": false,
          "as_of": "2026-08-17 09:31:00" | null }
```

The result's `hint` carries the query that produced the answer (on the sampled
path) or the one that would compute it (when nothing covers the field), so
"what did this cost" and "what exactly ran" are answered by the same object.

`windowScoped` is the honest half: the `range` is carried, echoed, and applied
on the sampled path, but `attribute_stats` is a whole-table snapshot with no
time dimension, so a metadata-tier answer reports `windowScoped: false` and
`asOf` rather than pretending the window narrowed anything. A tenant whose
compactor has never run gets `asOf: null` plus a warning on the envelope, not an
empty result presented as fact.

**D9 — Bounds everywhere, with `truncated`.** Fields and values responses are
capped (defaults: 1 000 fields, 200 values; the sampled path inherits the query
surface's own bounds) and set `truncated: true` when the cap bites.
Fields are ordered declared-first then by coverage descending — a field picker's
first screen should be the fields most rows actually carry.

**D10 — Layering.** `common` owns the merge (pure functions over
`LogicalSchema`, registry resolutions, and `Vec<AttributeStatsRecord>`, unit
tested without I/O); the router owns I/O, auth, bounds, and serialization; the
CLI and MCP consume the generated SDK. No discovery logic in the querier.

## Risks / Trade-offs

- **Stats staleness** → a key ingested since the last compaction pass is absent
  from `describe: fields`. Mitigated by `cost.asOf` + declared fields always
  being present, and by the `sample: true` escape hatch; resolved properly by
  #813 (registry rows written transactionally with promotion) and by running the
  analyzer more often. Recorded in the spec as an explicit non-guarantee, so the
  UI can label its picker "as of ..." rather than claim completeness.
- **`attribute_stats` has no attribute level (resource/scope/record)** → observed
  items report `level: null`; declared items keep theirs. Under-informative, not
  wrong. #813's `scope` column fixes it.
- **`attribute_stats` is per `(tenant, dataset, signal)` with no window** →
  double-counted concerns if a dataset is huge and heterogeneous. Accepted: it
  is a suggestion surface, not an analytic result.
- **A `describe` document with `sample: true` can still schedule querier work** →
  bounded by the existing ticket's sample cap, requires the source's read scope
  like any query, subject to the same rate limiting, and visible in the response.
- **Two response shapes on `POST /api/v1/query`** (query envelopes vs. metadata)
  → mitigated by the same discriminator the endpoint already has (`result`), and
  by the generated clients decoding one documented schema per envelope.
- **Version bump to 4 touches shared IR constants** → additive only; every
  existing document keeps its meaning, and the v1/v2/v3 gating tests are the
  regression net.

## Migration Plan

Additive; no data migration. Ship in order: (1) IR envelope + stage + merge
logic in `common`; (2) router endpoints, OpenAPI, regenerated SDK/TS clients;
(3) CLI + MCP; (4) compactor value sketches (the `statistics` tier for values).
Each is independently revertible; stopping after (3) leaves values answered by
tiers 1/3/4 of D5, which is correct, just less useful. Rollback is removing the
route — no client depends on it until (3).

## Open Questions

- Whether the analyzer should keep value sketches for keys above the
  cardinality cap (currently 10 000 distinct) at all, or record only "too many
  values to suggest". Answerable when the sketch task starts; it changes neither
  the contract nor the tasks.
- Whether `describe: fields` should offer a `prefix` filter server-side for very
  wide tenants, or leave filtering to the client. Additive either way.
