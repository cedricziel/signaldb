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

**`db.operation.name` is a fixed, five-value literal per query surface —
never parsed from query text.**
Raw SQL: the fixed literal `"query"` on every call, regardless of the
submitted SQL text. PromQL/LogQL/TraceQL/query-IR: one fixed literal per
surface (`"promql_query"`, `"logql_query"`, `"traceql_query"`,
`"query_ir"`). This differs from the catalog factory's convention (real
CRUD verbs — `"SELECT"`, `"INSERT"` — supplied as literals by the caller at
each call site), because the querier's raw-SQL surface neither validates
nor distinguishes statement types before execution: parsing a leading verb
out of arbitrary client-submitted SQL to label a span would let a client
grow the attribute's value set arbitrarily (also SQL-injection-adjacent —
never derive a span attribute from unvalidated input). Five fixed literals,
none derived from request content, keeps `db.operation.name` exactly as
low-cardinality as the registry enum declares it.
Alternative considered: parse the SQL verb for a friendlier per-verb value
— rejected for the reasons above; if per-verb distinction is ever wanted,
it requires an explicit allowlist-and-reject step (unsupported verbs
rejected before execution), which is out of scope here.

**`db.namespace` is threaded as an explicit parameter, not derived from
`SessionContext`.**
Each of the five call sites already has the tenant/dataset the request is
scoped to at hand (from the parsed ticket or the service's own fields)
before `SessionContext` derivation happens. Passing it straight through
avoids adding a `SessionContext` inspection helper that would need to parse
`default_catalog`/`default_schema` back out — more code, and a second
source of truth for something already known at the call site.

**Stage spans stay exactly the two that exist today
(`signaldb.query.plan`/`signaldb.query.execute`) and become children of
the new CLIENT span via normal span nesting — no new stage spans are
added, and no attribute changes to the existing two.**
The pre-existing "Query execution stage spans" requirement text (inherited
unmodified from before this change) names four stages — planning,
table/Iceberg scan, execution, result encoding — but the implementation
has only ever produced two spans: `signaldb.query.plan` wraps
`ctx.sql()`/`plan()` (planning), and `signaldb.query.execute` wraps
`.collect()` (which covers scan, execution, and result encoding together,
since DataFusion's `DataFrame::collect` doesn't expose those as separable
async boundaries at the `DataFrame` API level this codebase uses). This
change does not add scan/encoding spans — see Non-Goals — so the spec delta
narrows the requirement's wording to name the two spans that actually
exist, rather than perpetuating a four-stage description nothing
implements. Splitting scan/execution/encoding into separate spans would
require operating on `ExecutionPlan` directly, which is the same
instrumentation depth as `ExecutionPlan::metrics()` — explicitly deferred.

**Metrics carry a fixed, explicit attribute allowlist — `db.system.name`
and `db.operation.name` only — added alongside the existing `query_type`
attribute, never `db.namespace` or `db.query.text`.**
`db.query.text` is excluded outright: unbounded, and a metric attribute
(unlike a span attribute) is a label copied verbatim into every metric
data point's identity — recording free text there multiplies storage and
risks retaining sanitized-but-still-sensitive content in aggregate form.
`db.namespace` (tenant/dataset) is also excluded: no existing
`self_monitoring` metric in this codebase carries a tenant- or
dataset-scoped label today (checked directly — `app_metrics.rs` has no
`tenant_id`/`dataset_id` attribute on any instrument), and introducing one
here would make per-query metrics scale with tenant count for the first
time, an architectural change bigger than this proposal's scope. Per-tenant
query cost is already visible on the CLIENT _span_ (which does carry
`db.namespace`) via trace queries; the metric only needs the
low-cardinality, unconditionally-safe subset. `query_type` (the ticket
verb) is kept unchanged since `db.operation.name` differs slightly in
intent (Flight-protocol dispatch vs. DB-semconv operation) and existing
dashboards/alerts may already key on it.

**Sanitization of recorded query text is mandatory and unconditional for
every literal-bearing surface (SQL, PromQL, LogQL, TraceQL), and every
attribute that records query text — `db.query.text` on the new CLIENT span
and the pre-existing `signaldb.query.text` on the SQL stage span alike —
reuses one sanitized value per query, never a second, independently-derived
copy.**
`sanitize_query_text` already exists and is proven for SQL. For
PromQL/LogQL/TraceQL, this change adds an equivalent sanitizer per surface
(scoped to what each grammar can carry as a literal — e.g. quoted label
matcher values) _before_ any query text is recorded anywhere, including
`tracing::info!` logging. If a surface's literal shape can't be safely
sanitized with reasonable effort, that surface's `db.query.text` is
omitted entirely rather than recording unsanitized text — recording
nothing is always the safe default over recording something unscrubbed.
Task 8.1 (see tasks.md) is a blocking implementation-and-test requirement,
not an optional follow-up review.

## Risks / Trade-offs

- [Risk] Five call sites, three of which (`MetricsService`, `LogsService`,
  `trace.rs`) currently have zero span coverage — more surface area to get
  wrong than a single-call-site change → Mitigation: identical
  `db_client_span(...)` call shape at each site, covered by the pin tests
  from `db_catalog_span_semconv.rs`'s pattern extended to the new
  parameter, plus one integration-style test per query surface asserting
  the CLIENT span exists in the exported trace.
- [Risk] A PromQL/LogQL/TraceQL sanitizer that's incomplete for its
  grammar could let a literal through unscrubbed despite the mandatory
  policy above → Mitigation: per-surface unit tests asserting known literal
  shapes (quoted label values, string matchers) are stripped, and the
  omit-rather-than-leak fallback keeps an incomplete sanitizer failing
  safe instead of failing open.
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
