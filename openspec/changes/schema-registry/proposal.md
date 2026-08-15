## Why

SignalDB stores OTel telemetry but knows nothing about what the keys _mean_: the
UI, MCP tools, and CLI show `k8s.pod.uid` or `http.response.status_code` as bare
strings, discovery endpoints return names only, and OTLP `Resource.entity_refs`
is dropped on ingest. The OpenTelemetry semantic conventions — which we already
consume through Weaver to validate our own self-monitoring telemetry — carry
exactly the missing knowledge: attribute briefs, types, enum members, stability
and deprecation, ~64 entity types with their identifying attributes, and metric
definitions with `entity_associations`. A schema registry that vendors those
conventions, lets tenants add their own, and resolves any wire key / entity type
/ metric name to its definitions is the foundation every later semantic feature
(field discovery, entity explorer, telemetry lint, LLM-friendly MCP discovery)
builds on. Do it first so the consumers can be thin.

## What Changes

- **New `schema-registry` capability**: namespaced, versioned semantic-convention
  registries stored and served by SignalDB.
  - A registry is identified by `namespace@version` (`otel@1.43.0`, `acme@0.3.0`).
    Namespaces remove collision: two registries may describe the same wire key.
  - **Bundled, read-only registries**: `otel` (the OpenTelemetry semantic
    conventions, vendored into the repo at the version pinned for self-monitoring)
    and `signaldb` (`otel/registry/`). Bundled registries are visible to every
    tenant and reject mutation.
  - **Custom registries**: tenant-scoped, full CRUD via the HTTP API (document
    level: create / replace / delete a whole registry), authored in the OTel
    Weaver semantic-convention YAML/JSON model so existing Weaver users can upload
    their files unchanged. Custom registries may `ref`/`extends` definitions from
    `otel`/`signaldb`. Validation is a Rust subset validator (unique ids,
    resolvable refs, valid types/roles/instruments/units, entity association
    targets exist).
  - **Remote registries** are reserved in the model (`source: remote`) but not
    implemented in this change.
  - Supported group types in v1: `attribute_group`, `entity`, `metric`
    (including `entity_associations`). Other group types are accepted and stored
    but not resolved.
- **Resolved lookup surface**: read endpoints that answer, for the calling
  tenant, "what is known about wire key X / entity type Y / metric name Z" —
  merging every visible registry, tagging each hit with its namespace and
  version, ordered by precedence (tenant custom → `signaldb` → `otel`), and
  including deprecation/rename hints and entity relationships (attribute
  ↔ entity roles, metric → entity associations).
- **Access control**: two new API-key scopes, `schema:read` (list/get
  registries, resolve/search) and `schema:write` (create/replace/validate/delete
  custom registries), enforced like the existing `<signal>:read|write` scopes;
  `schema:read` is OAuth-grantable and part of the default read grant,
  `schema:write` is not.
- **Scopes on every key-management surface**: the admin HTTP API used by the CLI
  and MCP creates keys with no scopes today (legacy-unrestricted). Key creation
  on every surface (UI, admin/management API, SDK, CLI, MCP) SHALL take an
  explicit scope set (required, validated against one vocabulary) and optional
  dataset; an existing key's scopes SHALL be updatable without rotating the
  secret.
- **Surface parity**: SDK, CLI (`signaldb schema …`) and MCP tools for the
  registry list/get/lookup and the custom CRUD, per `client-surface-parity`.
- **v0 UX**: wherever the Explore UI renders an attribute key as a label — field
  sidebar, filter chips, span/log detail panels, facet headers — it SHALL resolve
  the key to its semantic title and description from the registry (tooltip /
  secondary text), falling back to the raw key when nothing is registered.
- **Schema management & inspection UX**: the `/schema` route becomes a hub with
  a **Conventions** tab (new) beside the existing **Storage** explorer.
  Conventions lets every tenant user list registries, browse and search a
  registry's attributes / entities / metrics, open entity pages (identifying and
  descriptive attributes, associated metrics, extensions), and run a
  precedence-ordered global lookup; tenant admins additionally create, replace
  (with in-browser validation and diff), and delete custom registries by
  uploading or editing a Weaver-format document.
- Vendoring: `otel` semconv model is checked into the repo (`vendor/` or
  `otel/semconv/`) at the pinned tag and re-generated with an xtask; the pin is
  shared with `common::self_monitoring::SEMCONV_SCHEMA_URL` so the two cannot
  drift.

No OTLP ingest, Tempo/LogQL/PromQL, Flight, or on-disk changes. Not BREAKING.

## Capabilities

### New Capabilities

- `schema-registry`: namespaced, versioned semantic-convention registries —
  bundled read-only (`otel`, `signaldb`), tenant custom CRUD in Weaver format,
  remote reserved; validation; storage in the catalog; resolved lookup by wire
  key, entity type, and metric name with namespace-tagged, precedence-ordered
  results; entity roles and metric→entity associations.
- `explore-ui-semantic-labels`: the Explore UI resolves attribute keys shown as
  labels to their registry title and description, with raw-key fallback.
- `explore-ui-schema-registry`: the `/schema` Conventions hub — registry list,
  browse/search, entity pages, global lookup, and tenant-admin management of
  custom registries (upload/edit, validate, replace, delete).

### Modified Capabilities

- `cli-command-surface`: the command taxonomy gains a `schema` group (registry
  list/get/lookup; custom registry create/replace/delete under `admin schema`).
- `mcp-tool-surface`: MCP tools SHALL additionally cover schema-registry lookup
  and custom-registry management.
- `api-key-management`: API keys can be created with `schema:read` /
  `schema:write` scopes; scopes are required and selectable on every
  key-management surface (admin API, CLI, MCP too); existing keys' scopes can be
  updated.
- `mcp-oauth`: `schema:read` joins the read scopes (default grant, consent);
  `schema:write` is not OAuth-grantable.

## Impact

- **common**: schema-registry model, Weaver-format parser + subset validator,
  bundled registry loading (vendored semconv + `otel/registry/`), catalog tables
  for custom registries and flattened lookup indexes.
- **router / signaldb-api**: admin `POST /tenants/{id}/api-keys` gains
  `scopes` (required) + `dataset_id`; new `PATCH …/api-keys/{key_id}` (scopes,
  dataset) on admin and management APIs; one shared scope vocabulary.
- **router**: `/api/v1/schema/*` endpoints (list/get, custom CRUD, resolved
  lookup); OpenAPI + generated SDK/TS client regenerated.
- **signaldb-sdk / signaldb-cli / mcp-server**: new methods, `schema` commands,
  and MCP tools.
- **ui**: semantic label resolution in field sidebar, filter chips, span/log
  detail, facet headers; `/schema` hub (Conventions tab: registries, browser,
  entity pages, lookup, custom-registry editor; existing storage explorer moves
  under the Storage tab).
- **xtask / repo**: vendored OTel semconv model at the pinned tag with a
  regeneration task; CI check that the vendored version equals the
  self-monitoring pin.
- No changes to acceptor, writer, querier, compactor, WAL, or Iceberg layout.
