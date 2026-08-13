## Context

See proposal.md - Why/What Changes for motivation and scope. Relevant
current-state details not repeated there:

- The existing catalog factory `db_client_span` (`src/common/src/self_monitoring/spans.rs:199`)
  is the pattern to extend: `tracing::info_span!("db.client", otel.kind="client",
db.system.name, db.operation.name, db.namespace, ...)`, named
  `{operation} {namespace}` per DB span-naming precedence. It's a plain
  function returning `Span`, called at the point of the operation and
  `.instrument()`-ed or entered around the async call.
- `execute_query` (`src/querier/src/flight.rs:1243`) takes only
  `ctx: &SessionContext` and the raw SQL string — it has no explicit
  tenant/dataset parameter. Tenant/dataset scoping lives in the
  `SessionContext`'s `default_catalog`/`default_schema`, set earlier by
  `session_for_request` when the per-request context is derived.
  `IrService::query`, `MetricsService::query_metric`, `LogsService::query_logs`,
  and `trace.rs`'s find/search functions are methods on services that are
  constructed per-request and already hold tenant/dataset as fields or
  request parameters (they don't need to recover it from `SessionContext`).
- The literal-field-name pin test (`spans.rs:246`,
  `literal_field_names_match_semconv_constants`) asserts macro field-name
  string literals equal `opentelemetry_semantic_conventions::attribute`
  constants — `DB_SYSTEM_NAME`, `DB_OPERATION_NAME`, `DB_NAMESPACE` are
  already pinned there for the catalog factory; a `DB_QUERY_TEXT` pin needs
  adding for the new factory.
- CI's span-construction guard (`.github/workflows/ci.yml:194-217`) greps
  for `otel.kind` outside `src/common/src/self_monitoring/` and fails the
  build — the new factory has no choice but to live in `spans.rs`.

## Goals / Non-Goals

**Goals:**

- One consistently-shaped CLIENT span per DataFusion query, emitted from
  all five querier execution paths, reusing the existing `db_client_span`
  shape rather than inventing a second pattern.
- Preserve the existing `signaldb.query.plan`/`signaldb.query.execute`
  stage-timing spans and their attributes unchanged in content, only
  changing their parent.
- Keep `db.namespace` as an explicit function parameter at each call site
  (tenant/dataset the caller already has), not something the span factory
  or `SessionContext` recovers indirectly.

**Non-Goals:**

- No per-DataFusion-operator spans or `ExecutionPlan::metrics()` wiring
  (proposal's explicit out-of-scope item).
- No change to what query text is logged via `tracing::info!` — only span
  attributes are in scope.
- No change to the Flight ticket/ `do_get` request/response shape or
  timeout behavior.

## Decisions

**Reuse `db_client_span`, add an optional `query_text` parameter, rather
than adding a parallel `datafusion_query_span` factory.**
The catalog and DataFusion spans differ only in whether they carry
`db.query.text` (catalog operations don't have query text to sanitize;
DataFusion ones do) and possibly the `db.system.name` value. Adding a
`query_text: Option<&str>` parameter to `db_client_span` keeps one factory,
one pin-test suite, one place implementing the DB span-naming rule.
Alternative considered: a separate factory — rejected, since it would
duplicate the naming/status logic in `spans.rs` for a difference of one
field.

**`db.system.name` value: `"datafusion"`.**
DataFusion is not a registered upstream `db.system.name` enum value, but
the semconv `db.system.name` field explicitly permits implementation-defined
values when the query engine isn't in the registry (the same latitude the
project already exercises — `sqlite`/`postgresql` are registered, but the
convention doesn't require every system to be). Using the literal engine
name keeps the value meaningful to someone reading a trace, and matches how
`db_system_name` is already threaded as a plain `&str` rather than an enum.

**`db.operation.name` values are query-surface-specific, not SQL-verb-only.**
Raw SQL: the parsed leading verb (`SELECT`, matching existing SQL
conventions) where cheaply extractable, else a generic `"query"`.
PromQL/LogQL/TraceQL/query-IR: a fixed literal per surface
(`"promql_query"`, `"logql_query"`, `"traceql_query"`, `"query_ir"`) — these
protocols don't have a small enum of "verbs" the way SQL does, and a fixed
per-surface literal keeps cardinality bounded and makes the five query
types distinguishable in aggregate views (e.g. "average CLIENT span
duration grouped by `db.operation.name`").

**`db.namespace` is threaded as an explicit parameter, not derived from
`SessionContext`.**
Each of the five call sites already has the tenant/dataset the request is
scoped to at hand (from the parsed ticket or the service's own fields)
before `SessionContext` derivation happens. Passing it straight through
avoids adding a `SessionContext` inspection helper that would need to parse
`default_catalog`/`default_schema` back out — more code, and a second
source of truth for something already known at the call site.

**Stage spans (`signaldb.query.plan`/`signaldb.query.execute`) become
children of the new CLIENT span via normal span nesting (entering the
CLIENT span, then creating the stage spans as before) — no attribute
changes to the stage spans themselves.**
This satisfies the modified "Query execution stage spans" requirement
(nesting changes, content doesn't) with a one-line change per call site:
wrap the existing stage-span-producing code in `.instrument(db_client_span(...))`
or enter the CLIENT span around the existing block.

**Metrics get the same `db.*` attributes added as separate `KeyValue`s
alongside the existing `query_type` attribute, not a replacement of it.**
`query_type` (the ticket verb) and `db.operation.name` differ slightly in
intent (Flight-protocol dispatch vs. DB-semconv operation) and existing
dashboards/alerts may already key on `query_type`; adding rather than
replacing avoids a breaking change to metric cardinality/labels for
existing consumers.

## Risks / Trade-offs

- [Risk] Five call sites, three of which (`MetricsService`, `LogsService`,
  `trace.rs`) currently have zero span coverage — more surface area to get
  wrong than a single-call-site change → Mitigation: identical
  `db_client_span(...)` call shape at each site, covered by the pin tests
  from `db_catalog_span_semconv.rs`'s pattern extended to the new
  parameter, plus one integration-style test per query surface asserting
  the CLIENT span exists in the exported trace.
- [Risk] `db.query.text` on PromQL/LogQL/TraceQL spans records query
  language text that was never covered by `sanitize_query_text` (built for
  SQL literal patterns) — a PromQL/LogQL/TraceQL literal (e.g. a label
  value) could leak through unsanitized → Mitigation: these three surfaces
  don't currently have any literal-scrubbing needs called out in existing
  specs (their query languages don't have the same free-text-literal
  injection shape SQL does — labels/matchers are structured), but this
  needs explicit review during implementation; if a surface can carry
  free-text literals, extend or scope `sanitize_query_text` to it before
  recording `db.query.text`, rather than recording it unsanitized.
- [Risk] Additional span per query changes self-monitoring export volume
  and cost for the `_system`/`_monitoring` tenant → Mitigation: this is one
  additional span per query (not per-operator), a small, bounded increase
  consistent with the existing catalog-span precedent.

## Migration Plan

Purely additive telemetry change — no schema, wire-format, or API changes.
Roll out as a normal PR; no feature flag needed since new spans are
additive to existing traces and don't change any existing span's identity
except reparenting stage spans (which have no external consumers depending
on their current parent). Rollback is a plain revert.
