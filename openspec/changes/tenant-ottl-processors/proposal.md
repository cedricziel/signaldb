## Why

Telemetry arriving at SignalDB often carries data a tenant must not keep:
e-mail addresses and user ids in span attributes, session tokens and signed
query strings in `url.full`, credit-card fragments in log bodies. Today the only
way to strip them is to run an OpenTelemetry Collector in front of every
producer, which most tenants of a hosted SignalDB do not control. The
Collector's `transform` processor already gives operators a vocabulary for this
— OTTL (OpenTelemetry Transformation Language) statements such as
`replace_pattern(attributes["url.full"], "\\?.*$", "")` or
`set(attributes["user.email"], SHA256(attributes["user.email"]))` — and the
tenants who need redaction already know it. SignalDB should let a tenant admin
declare such statements per tenant and per dataset, apply them at ingest before
anything is made durable, and manage them from the API, the CLI, MCP, and the
Explore UI.

## What Changes

- **New `telemetry-processors` capability**: a tenant-scoped, ordered set of
  *processors*. Each processor binds a name, a signal (`traces`, `logs`,
  `metrics`), an optional dataset name (unset = every dataset of the tenant), an
  `enabled` flag, a `priority`, an `error_mode`, and an ordered list of OTTL
  statements. The acceptor applies every enabled processor matching the
  request's tenant, dataset, and signal — tenant-wide first, then
  dataset-scoped, each group by ascending priority then name — to the decoded
  OTLP request **before** conversion to Arrow and before the WAL append, so the
  redacted form is the only form ever persisted, forwarded, or retried.
- **Bounded OTTL subset, implemented in-house** (new crate `src/ottl`; no Rust
  OTTL implementation exists — the only candidate, otel-arrow's
  `query-engine-languages`, parses `set(ident, literal)` only and pins an
  incompatible Arrow line): statements `editor(args) [where condition]`;
  context-qualified paths (`resource.attributes["k"]`,
  `instrumentation_scope.*`, `span.name|kind|status.code|status.message|attributes`,
  `log.body|severity_text|severity_number|attributes`,
  `metric.name|description|unit`, `datapoint.attributes`; bare `attributes[...]`
  and `body`/`name` resolve to the signal's leaf item); literals, `nil`,
  comparison and boolean operators; editors `set`, `delete_key`,
  `delete_matching_keys`, `keep_keys`, `truncate_all`, `limit`,
  `replace_pattern`, `replace_all_patterns`, `replace_match`,
  `replace_all_matches`; converters `IsMatch`, `IsString`, `Concat`, `String`,
  `Int`, `Double`, `Len`, `SHA256`, `Substring`, `ToLowerCase`, `ToUpperCase`,
  `Truncate`. Everything else is a validation error naming the token, never a
  silent no-op. Regexes use the `regex` crate syntax with a compiled-size limit.
- **Validation and dry-run**: `POST /api/v1/processors:validate` parses and
  compiles a processor without storing it and returns positional errors;
  `POST /api/v1/processors:test` applies a stored-or-inline processor set to an
  OTLP JSON payload and returns the transformed payload plus per-statement
  match counts, so a tenant can prove a redaction rule before enabling it.
- **Storage and propagation**: processors live in the catalog (`processors`
  table, both dialects). Every service that applies them holds a
  `ProcessorRegistry` that lazily loads a tenant's compiled processors and
  refreshes them on a TTL (`[processors].reload_interval`, default 30s) and on
  in-process writes, so a change made through the router reaches a separate
  acceptor process within one interval without a restart.
- **Access control**: two new scopes, `processors:read` (list/get/validate/test)
  and `processors:write` (create/replace/delete), enforced like
  `schema:read|write`; `processors:read` joins the OAuth-grantable read set,
  `processors:write` requires tenant admin and is not OAuth-grantable. Both are
  selectable on every key-management surface.
- **Surface parity**: SDK, CLI (`signaldb-cli processors …` and
  `signaldb-cli admin processors …`), MCP tools, and an Explore UI page
  (`/processors`) with a list, an editor with live validation, and a "test
  against sample payload" panel that diffs before/after.
- **Observability**: self-monitoring counters for statements applied, statements
  errored, and requests rejected by `error_mode: propagate`, labelled by tenant
  and processor name; a tracing span per processor set application.
- Out of scope in this change (explicit non-goals in design.md): profiles and
  Prometheus remote-write ingest (metrics processors do not run for
  `/api/v1/prometheus/write`); dropping whole spans/logs/points; query-time
  application; nested map/slice paths; `Cache`, enum symbols, `merge_maps`,
  `flatten`, `ParseJSON`; per-statement contexts other than the signal's own.

Not BREAKING: no OTLP wire, Tempo/LogQL/PromQL, Flight, WAL, or Iceberg layout
change. A tenant with no processors sees byte-identical ingest behaviour.

## Capabilities

### New Capabilities

- `telemetry-processors`: per-tenant, per-dataset, per-signal OTTL processors —
  the language subset, ordering and scoping rules, ingest-time application
  semantics and error modes, catalog storage, cache/refresh, validation and
  dry-run endpoints, HTTP API, and access control.
- `explore-ui-processors`: the `/processors` page — list, editor with
  validation, and the dry-run panel.

### Modified Capabilities

- `cli-command-surface`: gains `processors` (read) and `admin processors`
  (write) command groups.
- `mcp-tool-surface`: gains processor list/get/validate/test and
  create/replace/delete tools.
- `api-key-management`: `processors:read` / `processors:write` join the scope
  vocabulary on every surface.
- `mcp-oauth`: `processors:read` joins the read scopes; `processors:write` is not
  OAuth-grantable.
- `user-menu`: the user menu gains a Processors link.

## Impact

- **ottl (new crate)**: pest grammar, AST, compiler (regex precompilation,
  path validation per signal), evaluator over `opentelemetry-proto` 0.32
  request types, unit + conformance tests.
- **common**: `processors` catalog table and CRUD, `ProcessorRegistry`
  (compiled cache + TTL refresh), `[processors]` config section,
  `processors:read|write` scopes and `TenantContext::can_read_processors /
  can_write_processors`, self-monitoring metrics.
- **acceptor**: apply the registry in the trace, log, and metric handlers
  between decode and `otlp_*_to_arrow`; `AcceptorResources` gains the registry.
- **router / signaldb-api**: `/api/v1/processors` CRUD + `:validate` + `:test`;
  OpenAPI, generated Rust SDK, and generated TS client regenerated.
- **signaldb-sdk / signaldb-cli / mcp-server**: new methods, commands, tools.
- **ui**: `/processors` route, editor, dry-run diff, user-menu entry, scope
  picker entries.
- **docs**: `docs/users/processors.md`, `docs/operations/…` config note,
  `multi-tenancy` and `configuration` skills.
- No changes to writer, querier, compactor, WAL format, or Iceberg layout.
