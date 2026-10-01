## Context

- Ingest path today (per crate map): OTLP gRPC/HTTP → auth middleware
  (`TenantContext` with `tenant_id`, `dataset_id`, scopes) → per-signal handler
  in `src/acceptor/src/handler/` → `common::flight::conversion::otlp_*_to_arrow`
  → WAL append → forward to writer. There is **no** transform hook; the only
  request rewrite is metric-type partitioning (`metrics_partition.rs`). Schema
  transforms (v1→v2, attribute promotion) run in the writer and are untouched.
- The acceptor already holds the sqlx catalog (through `Authenticator`) and has
  no per-tenant config cache; the authenticator hits the catalog per request.
  `SchemaResolver` (`common::schema_registry`) is the reference for a
  tenant-keyed `DashMap` cache with explicit invalidation.
- Catalog DDL is inline Rust in `Catalog::init`, one SQLite and one Postgres
  branch, `CREATE TABLE IF NOT EXISTS`, tenant-first primary keys.
- API surface is code-first utoipa → `api/signaldb-api.json` → progenitor Rust
  SDK (`src/signaldb-sdk/src/generated.rs`) and `@hey-api/openapi-ts` TS client
  (`src/ui/src/api/gen`). CLI, MCP, and UI consume only the generated clients.
- No Rust OTTL implementation is adoptable (see proposal). Parser crates already
  in `Cargo.lock`: `pest` 2.8 (transitive), `regex` 1.13 (direct).
- FDAP alignment: this change touches no Arrow/Parquet types; the transform
  runs on protobuf structs before conversion. If a later backend compiles
  conditions to DataFusion `Expr`, it must use DataFusion's re-exports.
- Flight v1 wire and v2 storage schemas are unchanged; the WAL sees only
  transformed batches, so there is no WAL/Iceberg migration and nothing to roll
  back on disk. Rolling back the feature = dropping the `processors` table.

## Goals / Non-Goals

**Goals**

- A tenant admin can redact PII and sanitize URLs at ingest with statements
  copied from a Collector `transform` config, per dataset or tenant-wide.
- The transformed payload is the only one ever persisted (WAL, forward, retry).
- Rules are provable before enablement (validate + dry-run with a diff).
- Zero cost for tenants without processors; bounded cost otherwise (compiled
  once, regex size-limited, statement count capped).
- Parity: API, SDK, CLI, MCP, UI, docs.

**Non-Goals**

- Profiles and Prometheus remote-write (no OTTL context defined for them in
  the Collector either; profiles can follow once `profile` context stabilises).
  Metrics processors therefore do **not** apply to `/api/v1/prometheus/write`
  (`PrometheusHandler` builds its own `ExportMetricsServiceRequest`), and
  batches pushed straight to the writer's Flight `do_put` bypass the acceptor
  entirely. Both are documented.
- Dropping items (`filter` processor semantics) — a `drop()` editor would need
  count reconciliation in metrics partitioning; separate change.
- Query-time / read-path redaction, per-user masking.
- Full OTTL: nested map paths, slices, `Cache`, enum symbols, `merge_maps`,
  `flatten`, `ParseJSON`, `Time`/`Duration` converters, `span.events[...]`,
  `spanevent`/`scope` statement contexts.
- Sampling, routing, or any editor with cross-item state.

## Decisions

### D1 — In-house bounded OTTL subset in a new `ottl` crate (pest grammar)

Alternatives: (a) depend on `otel-arrow-dfe-query-engine-languages` — rejected:
only `set(ident, literal)`, pre-1.0 IR, and its evaluators need OTAP batches on
arrow 58 while we are on 59; (b) invent our own syntax — rejected: tenants
already know OTTL and can move Collector configs over verbatim.

`src/ottl` mirrors `src/schema-model`: light deps only (`pest`, `pest_derive`,
`regex`, `thiserror`, `serde`, `sha2`, `opentelemetry-proto`). Public API:

```
pub fn parse(src: &str) -> Result<Statement, ParseError>          // one statement
pub fn compile(signal: Signal, statements: &[String], limits: &Limits)
    -> Result<CompiledProgram, CompileError>                       // Vec<Located<CompileError>> in the API
impl CompiledProgram {
    pub fn apply_traces(&self, req: &mut ExportTraceServiceRequest, mode: ErrorMode) -> ApplyReport;
    pub fn apply_logs(...); pub fn apply_metrics(...);
}
```

`Signal` selects which paths are legal (`span.*` only compiles for traces,
etc.). `Limits` carries `max_statements` (default 200), `max_regex_len`
(2 KiB), and the `regex::RegexBuilder::size_limit` (1 MiB). Every regex is
compiled at `compile` time, never per item.

Paths come in two shapes: keyed (`attributes["k"]`, a single value) and bare
map (`attributes`, `resource.attributes`, `instrumentation_scope.attributes`,
`datapoint.attributes`) — the map editors (`delete_key`, `delete_matching_keys`,
`keep_keys`, `truncate_all`, `limit`, `replace_all_patterns`,
`replace_all_matches`) take a map path. Only the 3-argument
`replace_pattern(target, regex, replacement)` is supported;
`replace_all_patterns(map, "key"|"value", regex, replacement)` requires the
mode string. Replacement strings use `regex`-crate syntax (`$1`, `${1}`); a
`$$` is normalised to `$` at compile time so Collector configs that write
`$$1` (to survive the Collector's `${}` expansion) port verbatim. String
literals accept `\\`, `\"`, `\n`, `\t` escapes. For metrics, bare
`attributes` is the data point's attributes and bare `name` is `metric.name`.

### D2 — Evaluation over `opentelemetry-proto` request structs, before Arrow

The hook sits in each acceptor handler right after decode/auth and before
`otlp_*_to_arrow` (for metrics: before `partition_metrics_by_type`, so a
renamed metric partitions correctly). The evaluator walks
`resource_spans → scope_spans → spans` (and the log/metric equivalents) and
runs the program once per leaf item with a `Context` that exposes
`resource`, `instrumentation_scope`, and the leaf (`span`, `log`, `metric` +
`datapoint`). For metrics the leaf is each data point; `metric.*` paths on a
metric with N points evaluate N times against the same `Metric` (matches the
Collector's `datapoint` context). Resource-level edits therefore run once per
leaf too; `where` conditions make that idempotent in practice, and the
Collector behaves the same way.

Alternative (transform the Arrow batch): rejected — the v1 wire batch stores
attributes as JSON strings, so every rule would parse/serialise JSON per row,
and the metric partitioner needs the protobuf anyway.

### D3 — Data model and ordering

Table `processors` (both dialects):

```
tenant_id TEXT NOT NULL, name TEXT NOT NULL, dataset TEXT NULL,
signal TEXT NOT NULL, enabled BOOLEAN NOT NULL DEFAULT TRUE,
priority INTEGER NOT NULL DEFAULT 100, error_mode TEXT NOT NULL DEFAULT 'ignore',
description TEXT NULL, statements TEXT NOT NULL (JSON array of strings),
created_at, updated_at, PRIMARY KEY (tenant_id, name),
FOREIGN KEY (tenant_id, dataset) REFERENCES datasets(tenant_id, name) ON DELETE CASCADE
```

`dataset` is the dataset **name** — the same value `TenantContext.dataset_id`
carries at ingest, the WAL directory uses, and `api_keys.dataset_ids` stores
(the `datasets.id` UUID is never exposed here). The API/SDK/CLI/UI field is
called `dataset` for the same reason. The composite FK is legal on both
dialects because `datasets` has `UNIQUE (tenant_id, name)`; SQLite enforces it
because the pool enables `PRAGMA foreign_keys`, so a dataset delete cascades on
both dialects with no extra code.

`name` is a slug (`[a-z0-9][a-z0-9-]{0,62}`), unique per tenant, and is the
API identifier (`/api/v1/processors/{name}`). Selection for a request
(tenant T, dataset D, signal S): all rows with `tenant_id = T AND signal = S
AND enabled AND (dataset IS NULL OR dataset = D)`, ordered by
`dataset IS NULL DESC, priority ASC, name ASC`. Tenant-wide rules run first
so a dataset can refine what the tenant baseline left.

### D4 — `error_mode` per processor, default `ignore`

Mirrors the Collector: `ignore` logs the failing statement (rate-limited) and
continues with the next statement/item; `silent` continues without logging;
`propagate` aborts the export with `IngestError::Invalid` (HTTP 400 / gRPC
`InvalidArgument`) — the client retries nothing, matching the "deterministic
failure → reject" rule from issue #926. Runtime errors are type errors
(`Int("abc")`), missing paths in editors that require presence, and regex
replacement template errors; a `where` that references an absent key is
`false`, not an error.

### D5 — `ProcessorRegistry`: lazy per-tenant cache with TTL + in-process invalidation

`common::processors::ProcessorRegistry { catalog, cache: DashMap<tenant_id,
Arc<Entry>>, ttl }` with `Entry { loaded_at, programs: Arc<Vec<CompiledProcessor>>,
reload: tokio::sync::Mutex<()> }`. `for_request(tenant, dataset, signal) ->
Vec<Arc<CompiledProcessor>>` returns the filtered, ordered subset (per tenant
the list is small). An entry older than `ttl` is reloaded **synchronously** on
the next access; the per-entry mutex stops concurrent requests stampeding the
catalog, and if a **reload** (an already-cached tenant) fails the old
programs keep serving and the error is logged. A tenant's **first** load
failing is different: there is no known-good program set to fall back to, so
`for_request` returns an error instead of caching an empty list — an empty
successful load (a tenant with zero processors) and a failed load are never
conflated, since silently proceeding with zero processors on a load failure
would bypass any redaction the tenant configured. Callers (the acceptor
handlers) reject the export in that case rather than ingest unredacted data.
No background task: the cost is one small SELECT per tenant per
interval, in the style of `SchemaResolver`'s DashMap cache (the acceptor
already holds `TenantRateLimiter` and `StorageUsageTracker` the same way).
Writes through the same process call `invalidate(tenant)`; the router's write
handlers must do so. `RouterState` gets a defaulted `processor_registry()`
like `schema_resolver()`, with the concrete cached instance on `RouterAppState`. Cross-process propagation is the TTL
only — accepted: `[processors].reload_interval` default 30s, documented, and
the API response for writes carries `applies_within_seconds`. A tenant with
zero rows caches an empty program, so the common case costs one `DashMap`
lookup per request. Rows that fail to compile (e.g. after a limit change) are
skipped with an error log and surfaced via `GET /processors` as
`status: "invalid"`; they never block ingest.

### D6 — HTTP API (tenant credential, `/api/v1/processors`)

```
GET    /processors                      list (processors:read)
POST   /processors                      create (processors:write) → 201
GET    /processors/{name}               get
PUT    /processors/{name}               replace (full document)
DELETE /processors/{name}               delete → 204
POST   /processors:validate             body: ProcessorSpec → ValidationReport
POST   /processors:test                 body: { processors?: [ProcessorSpec] | null (use stored),
                                                dataset?, signal, payload: OTLP JSON }
                                         → { payload, statements: [{index, matched, errors}] }
```

Errors: 404 unknown name (GET, PUT, DELETE — PUT never upserts), 409 create
on existing name, 422 with
`errors: [{statement, column, message}]` for compile failures, 403 scope. The
`:test` payload is capped at `[processors].test_payload_max_bytes` (1 MiB) and
never touches the WAL. Nested under `/api/v1` next to `/schema`, so it
inherits the tenant auth layer and rate limiting.

`:test` is also 422, with a plain `error` message (no `errors` array), for
two execution-time cases distinct from a compile failure: an inline
processor whose statements run under `error_mode: propagate` and raise a
runtime error, and an inline processor whose own `signal` does not match the
request's `signal` (checked before compiling it). Each `statements` entry
also carries the owning `processor`'s name, since every processor's
statement indices restart at 0 and a multi-processor `:test` call would
otherwise return ambiguous duplicate indices.

### D7 — Scopes

`processors:read` and `processors:write` follow `schema:*` exactly: constants
in `common::auth`, `can_read_processors` (read scope or any session) and
`can_write_processors` (`can_manage_tenant()` and write scope), `READ_SCOPES`
gains the read scope, the shared `API_KEY_SCOPES` vocabulary and UI scope
picker gain both, OAuth grants `processors:read` in the default read set and
rejects `processors:write`.

### D8 — UI

`/processors` under the existing schema/settings pattern: `ProcessorList`
(table: name, signal, dataset, enabled, priority, status, updated), `ProcessorEditor`
(form + one-statement-per-line textarea; on blur calls `:validate` and
annotates lines; "Test" panel takes an OTLP JSON payload — with a per-signal
sample preloaded — and shows a before/after JSON diff plus per-statement match
counts). TanStack Query keys `["processors", tenant]`. Entry in the user
menu next to Schema. Everything goes through the generated client.

### D9 — Observability

Counters `signaldb.processors.statements` (`{outcome=applied|error|skipped}`,
`tenant`, `processor`) and `signaldb.processors.rejected_requests`, plus a
`processors.apply` span (from `common::self_monitoring::spans` job factory,
`skip_all` with bounded fields: tenant, dataset, signal, processor count).
Statement error logs are rate-limited per (tenant, processor) to one per
minute.

## Risks / Trade-offs

- **Cost per item**: a program with R regex statements runs R matches per
  leaf item. Mitigations: compile once, `size_limit`, statement cap, `where`
  guards. Benchmarked in `performance-benchmarking` with a 10-statement
  program; target ≤ 15 % ingest throughput loss on the trace benchmark.
- **Semantic drift from upstream OTTL**: subset is documented with a
  conformance table; unsupported tokens fail validation loudly.
- **TTL staleness across processes**: documented; the UI shows
  "applies within N s".
- **Editing resource attributes per leaf**: N evaluations per resource;
  documented, identical to Collector behaviour.

## Migration Plan

1. `processors` table is created by `Catalog::init` on next boot (idempotent).
2. Feature is inert until a row exists; no config change needed. Rollback:
   deploy the previous binary; the extra table is ignored.
3. `[processors]` config section is optional; defaults documented in
   `signaldb.dist.toml`.

## Open Questions

- Should `dataset = NULL` be allowed to be *overridden off* per dataset
  (an `exclude_datasets` list)? Deferred; a dataset-scoped `set` can undo a
  tenant-wide one in most cases.
- Profiles context: revisit when the Collector defines it.
