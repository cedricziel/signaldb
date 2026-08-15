## Context

See proposal.md — Why. Relevant current state:

- Weaver is already a pinned CI dependency (`otel/weaver:v0.25.1`) for
  `registry check` on `otel/registry/` and for `live-check` of self-monitoring
  telemetry; `common::self_monitoring::SEMCONV_SCHEMA_URL` pins semconv
  `1.43.0`. Nothing consumes the conventions at runtime.
- The catalog (`src/common/src/catalog.rs`, SQLite or Postgres via sqlx) already
  holds tenant-scoped configuration tables (`tenants`, `datasets`, `api_keys`,
  `attribute_stats`) with the "config-sourced rows are read-only" pattern in the
  admin API. `attribute_stats` is the _observed_ side of attribute knowledge; this
  change adds the _semantic_ side and deliberately does not join them yet.
- Discovery surfaces are dialect-specific and name-only (Tempo tags static,
  Loki labels/`detected_fields`, Prom labels, MCP `discover_attributes`
  forwarding those). The stub change `query-field-discovery` is where the
  observed ⊕ semantic join will land; it will consume this registry.
- The UI renders attribute keys as bare strings in `FieldSidebar`, `FilterChips`
  (`<datalist>`), span/log detail `<dl>` tables, and a static `FACET_FIELDS`
  list. The UI reaches the API only through the generated SDK
  (`ui-enforce-sdk-only-http`).
- Semconv v1.43.0 model: 241 YAML files, 1.7 MB, ~900 attributes, 64 entities,
  532 metric groups (~260 with `entity_associations`), plus span/event groups.
  Attributes have `brief`/`note`/`type`/`examples`/`stability`/`deprecated`;
  groups have `display_name`; entities reference attributes with
  `role: identifying|descriptive`; metrics carry `metric_name`, `instrument`,
  `unit`, `entity_associations`.
- FDAP constraint (stated for completeness): no Arrow/Parquet types are touched
  by this change; if any are ever needed use DataFusion's re-exports. No Flight
  v1/v2 transform, WAL, or Iceberg change is involved — nothing to migrate or
  roll back on disk.

## Goals / Non-Goals

**Goals:**

- One data model for bundled, custom, and (later) remote registries, keyed by
  `namespace@version`, stored/served in the Weaver semantic-convention model.
- A single Rust parser + subset validator that is exercised against the entire
  vendored upstream model in tests, so custom-registry validation is proven on
  ~900 real definitions.
- Resolved lookups (attribute / entity / metric, exact + prefix) that are cheap
  enough to call per rendered label from the UI and per tool call from MCP.
- Bundled `otel` cannot drift from the self-monitoring pin.

**Non-Goals:**

- Joining semantics with observed data (cardinality, presence, values) —
  `query-field-discovery`.
- Persisting or inferring entities from ingested resources / `entity_refs` —
  a follow-up change on top of this registry (#263).
- Resolving `span`/`event` groups or running telemetry lint against tenant data.
- Remote registry fetching/refresh (model field reserved only).
- Item-level CRUD (edit one attribute in place); document-level only.
- Multiple concurrent versions of `otel`, per-tenant `otel` version selection.

## Decisions

### D1 — Vendor the raw semconv _model_ (YAML), not a pre-resolved JSON

Vendor `open-telemetry/semantic-conventions@v<pin>/model/**` into
`vendor/otel-semconv/<version>/model/` via an xtask (`cargo xtask vendor-semconv`)
that clones the tag, copies `model/`, and writes a `VERSION` file. A unit test
asserts `VERSION` == the version in `SEMCONV_SCHEMA_URL`; CI runs it.

Why: the same parser then handles upstream, `otel/registry/signaldb.yaml`,
uploaded custom files, and future remote registries — one code path, and the
upstream corpus becomes the parser's conformance suite. A resolved JSON would
need Weaver at build time (Docker in CI only, not on dev machines) and would be
a second format to maintain.

Alternatives: `weaver registry resolve` JSON checked in (rejected: second
format, Weaver-at-build); git submodule (rejected: the existing
`opentelemetry-proto` submodule already trips fresh worktrees; and we want a
subset copy, not the whole repo).

### D2 — Load bundled registries at build time via `build.rs` into an embedded blob

`common` gets a `build.rs` that parses `vendor/otel-semconv/<v>/model` and
`otel/registry/` with the shared parser (in a small `schema-model` crate both
`build.rs` and `common` depend on) and emits a compact serialized snapshot
(`bincode`/`postcard`) included with `include_bytes!`. Startup deserializes it
into the in-memory registry index (sub-10 ms). Bundled registries never touch
the catalog DB.

Why: no runtime file access (Docker images, `signaldb <service>` dispatch), fast
startup, parse errors in vendored YAML fail the build rather than the process.
Alternative: `include_dir!` the YAML and parse at startup (simpler, ~50–150 ms
per process; acceptable fallback if `build.rs` proves awkward — decide during
task 1, spec unaffected).

### D3 — Own parser + validator; Weaver stays an offline tool

Implement `schema-model`: serde types for the Weaver semconv model subset
(manifest; groups of type `attribute_group`, `entity`, `metric`; attributes with
`id`/`ref`, `type` incl. enum + `template[...]`, `brief`, `note`, `examples`,
`stability`, `deprecated` (both the legacy string form and the structured
`{reason, renamed_to}` form), `requirement_level`, `role`; `extends`;
`entity_associations`; group `display_name`). Unknown group types and unknown
fields round-trip as opaque JSON so an uploaded file is stored losslessly.
YAML via `serde_norway` (already in the graph).

Validation (subset, per spec): id uniqueness, ref/extends resolution against
own registry then declared dependencies (bundled `otel`, `signaldb`), type
vocabulary, role vocabulary, metric mandatory fields, `entity_associations`
targets resolve, extension cannot add identifying attributes, reserved
namespaces. Errors carry a JSON-pointer-like path (`groups[3].attributes[1].ref`).

Why not `weaver_semconv`/`weaver_resolver` crates: heavy dependency tree tuned
to the CLI, publish cadence unclear, and we need per-tenant in-process
validation on the request path. Weaver keeps validating our _own_ registry in
CI (`registry check`) — and a CI test runs `weaver registry check` on the sample
custom registries used in tests, so our validator's accept-set stays a subset
of Weaver's.

### D4 — Storage: document blob + flattened lookup tables in the catalog

New catalog tables (both SQLite and Postgres DDL, following the existing
`CREATE TABLE IF NOT EXISTS` bootstrap):

```
schema_registries   (tenant_id, namespace, version, source, schema_url,
                     document JSON, created_at, updated_at,
                     PK (tenant_id, namespace, version))
schema_attributes   (tenant_id, namespace, version, attr_key, group_id,
                     display_name, brief, note, type, stability,
                     deprecated JSON?, examples JSON, PK (…, attr_key))
schema_entities     (tenant_id, namespace, version, entity_name, group_id,
                     brief, stability, identifying JSON[], descriptive JSON[])
schema_metrics      (tenant_id, namespace, version, metric_name, group_id,
                     brief, instrument, unit, stability, attributes JSON[],
                     entity_associations JSON[])
```

The document is the source of truth (returned verbatim on GET); the flattened
rows are derived on write inside one transaction (replace = delete rows for
`(tenant, ns, version)` + reinsert; the spec's atomicity scenario). Bundled
registries live only in the embedded index; the resolver merges the tenant's
DB rows with the bundled index in memory.

Why not only-blob + parse on read: prefix search and per-key lookup at UI
frequency need indexed rows. Why not only-rows: lossless round-trip of the
uploaded document (unknown groups/fields) requires keeping it.

### D5 — Resolver: in-memory bundled index + per-tenant cached custom index

`SchemaResolver { bundled: Arc<Index>, custom: DashMap<TenantId, Arc<Index>> }`.
Custom index is loaded lazily from the catalog and invalidated on write (single
router process today; multi-router invalidation rides on the same
`updated_at`-polling pattern the tenant registry uses — task, not design).
Lookup: `resolve_attribute(tenant, key) -> Vec<AttributeHit>` ordered custom
(by namespace, then version desc) → `signaldb` → `otel`; same for entity and
metric. Entity hits are enriched with `metrics_associated` by reverse index;
attribute hits with `entity_roles` by reverse index. Prefix search is a
BTreeMap range on the key column per index, merged and de-duplicated by
`(namespace, key)`, capped at 200.

### D6 — HTTP surface (router), one resource family

```
GET    /api/v1/schema/registries                       list (bundled + tenant custom, with counts)
POST   /api/v1/schema/registries                       create custom (JSON or YAML body)
POST   /api/v1/schema/registries:validate              validate a document, store nothing
GET    /api/v1/schema/registries/{ns}/{version}        document
PUT    /api/v1/schema/registries/{ns}/{version}        replace custom (409 if bundled)
DELETE /api/v1/schema/registries/{ns}/{version}        delete custom (409 if bundled)
GET    /api/v1/schema/attributes?prefix=&limit=        search
GET    /api/v1/schema/attributes/{key}                 resolve
GET    /api/v1/schema/entities?prefix=&limit=
GET    /api/v1/schema/entities/{name}
GET    /api/v1/schema/metrics?prefix=&limit=
GET    /api/v1/schema/metrics/{name}
```

Tenant scoping via the existing request context (`X-Tenant-ID`/API key).
Authorization adds two scopes to `common::auth`: `SCHEMA_READ_SCOPE =
"schema:read"`, `SCHEMA_WRITE_SCOPE = "schema:write"`, with
`TenantContext::can_read_schema()` / `can_write_schema()` following the
`can_read`/`can_ingest` shape (explicit scopes → must contain; `None` →
unrestricted; sessions → any role reads, Admin/instance-admin writes). The
management API's key-creation allow-list becomes `INGEST_SCOPES ∪
SCHEMA_SCOPES`; the ApiKeys UI, CLI `admin api-key create --scope`, and MCP
admin toolset expose both. `READ_SCOPES` (OAuth default grant / consent) gains
`schema:read`; `schema:write` stays outside it so `granted_read_scopes` rejects
it. Read endpoints require `schema:read`; mutations and `:validate` require
`schema:write`; bundled registries additionally 409 on mutation. utoipa-annotated → OpenAPI → regenerated Rust SDK + TS client (parity gate
enforces coverage). Response envelope for resolve: `{ key, primary, hits: [...]}`
where each hit carries `namespace, version, source` + the definition.

### D7 — Clients

- SDK: `schema().registries()/registry(ns, v)/create/replace/delete`,
  `resolve_attribute/entity/metric`, `search_*`.
- CLI: `signaldb schema registry list|get`, `schema attribute get|search`,
  `schema entity get|search`, `schema metric get|search`; `signaldb admin schema
create|replace|delete --file`. YAML/JSON accepted for `--file`.
- MCP: `list_schema_registries`, `resolve_attribute`, `resolve_entity`,
  `resolve_metric`, `search_schema` (prefix, kind); admin toolset gains
  `create/replace/delete_schema_registry`. Tool descriptions steer LLMs to
  resolve before querying.

### D8 — UI: `useSemantics()` hook with a session-level LRU

A small hook batching resolve calls (debounced, de-duplicated by key, cached for
the session, keyed by tenant) behind the generated client. Rendering sites
(`FieldSidebar`, `FilterChips` autocomplete, `spanAttributes` detail table,
log detail, `TraceFacets` headers) render the raw key synchronously and attach
title/brief/namespace tag/deprecation marker when resolution lands. Batch
endpoint: `GET /schema/attributes?keys=a,b,c` (same handler as resolve, list
form) to avoid N requests per span — added to D6.

### D9 — UI: `/schema` hub (Conventions | Storage), Weaver-source editor

Routes: `/schema` → hub with tabs; `/schema/conventions` (registry list +
global lookup), `/schema/conventions/:ns/:version` (browser: search box, three
sections, definition pane), `/schema/conventions/:ns/:version/{attributes|
entities|metrics}/:name` (deep-linkable definition), `/schema/storage` (the
existing `SchemaExplorer`, unchanged; `/schema` keeps working for it via the
tab). Feature folder `src/ui/src/features/schema/`; the existing
`management/SchemaExplorer*` moves under it as the Storage tab.

Editor: a plain textarea/code editor over the raw document (YAML or JSON as
uploaded — no structured form editor in v0), Validate calls
`registries:validate` and renders per-path errors and counts; Save calls
create/replace; a client-side diff of flattened definitions (from the validate
response vs. the stored registry's flattened view) shows added/changed/removed
before Save. Mutations gated on `is_tenant_admin` (hidden otherwise) and on
`source != bundled`. Everything goes through the generated client.

Later consumers wired to the same routes: attribute tooltips (D8) link to the
definition page; `/catalog`'s hardcoded `entityTypes.ts` becomes derivable from
`GET /schema/entities` (out of scope here, noted so the route shape fits).

## Risks / Trade-offs

- [Vendored 1.7 MB YAML + build.rs adds build time to `common`] → parse is
  ~100 ms; `build.rs` reruns only when `vendor/` or `otel/registry/` change
  (`rerun-if-changed`).
- [Our validator accepts something Weaver rejects, or vice versa] → conformance
  test parses the whole upstream corpus with zero errors; CI runs
  `weaver registry check` on the sample custom registries; divergences are bugs
  with a repro file.
- [Semconv `deprecated` field has two shapes across versions] → parser accepts
  both; test fixtures cover each.
- [Namespaced precedence hides an upstream deprecation because a tenant
  re-described a deprecated key] → hits are never dropped; UI/MCP show the
  deprecation marker if _any_ hit is deprecated.
- [Per-tenant custom index in router memory grows with many tenants] → lazily
  loaded, LRU-evicted; a registry document is capped (e.g. 2 MB) at upload.
- [Group `display_name` is missing on many upstream groups] → title falls back
  to a humanized namespace prefix (`k8s.pod` → "K8s Pod"); brief always exists.
- [Moving the storage explorer under a tab changes a route users may have
  bookmarked] → `/schema` still renders the hub with Storage selectable and
  `/schema/storage` deep-links to it; no redirect needed.
- [Wire keys on the Loki path are underscore-flattened (`k8s_pod_uid`)] → UI
  resolves the dotted logical key; the Loki-label→dotted mapping is #819's job
  and out of scope here; until then those labels resolve only when the dotted
  form is available.

## Migration Plan

- Additive catalog tables created by the existing bootstrap; no data migration.
- Deploy router/common together (embedded index lives in `common`). Rollback =
  previous image; the new tables are inert to older code.
- UI ships behind no flag: raw-key fallback means an older API simply yields
  no enrichment.

## Open Questions

- Postcard vs bincode vs plain JSON for the embedded snapshot — pick the smallest
  dependency footprint at task time; behavior identical.
- Whether `signaldb admin schema` should also accept a directory of YAML files
  (Weaver's multi-file layout) and merge into one document client-side — nice
  for parity with Weaver users; can be added later without spec change.
