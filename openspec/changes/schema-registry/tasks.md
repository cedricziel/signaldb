## 1. Vendor the OpenTelemetry semantic conventions

- [x] 1.1 Add `cargo xtask vendor-semconv` that clones
      `open-telemetry/semantic-conventions` at the tag derived from
      `SEMCONV_SCHEMA_URL`, copies `model/**` into
      `vendor/otel-semconv/<version>/model/`, and writes `VERSION`
- [x] 1.2 Run it for 1.43.0 and commit the vendored model; add a
      `deny.toml`/license note for the vendored CC/Apache content
- [x] 1.3 Failing test in `common`: `vendor/otel-semconv/VERSION` equals the
      version in `SEMCONV_SCHEMA_URL`; wire the check into CI

## 2. Weaver-model parser and validator (`schema-model` crate)

- [x] 2.1 Create `src/schema-model` (no workspace-heavy deps: serde,
      serde_json, serde_norway, thiserror); write failing conformance test that
      parses every file under the vendored model with zero errors
- [x] 2.2 Implement serde types for manifest + groups (`attribute_group`,
      `entity`, `metric`; opaque passthrough for other types/unknown fields),
      attributes (`id`/`ref`, `type` incl. enum + `template[...]`, brief, note,
      examples, stability, both `deprecated` shapes, requirement_level, role),
      `extends`, `entity_associations`, `display_name`; make 2.1 pass
- [x] 2.3 Failing tests for the resolver: `ref`/`extends` resolution across a
      registry and its dependencies; flattening into attribute/entity/metric
      records with reverse indexes (attribute→entity roles, entity→metrics)
- [x] 2.4 Implement resolution + flattening; make 2.3 pass; assert against
      known upstream facts (64 entities, `k8s.pod.cpu.time` → `k8s.pod`,
      `http.status_code` deprecated → `http.response.status_code`)
- [x] 2.5 Failing tests for the validator: duplicate ids, dangling ref, invalid
      type/role, metric missing instrument/unit, association to unknown
      entity, extension adding an identifying attribute, reserved namespace;
      error paths name the group/attribute
- [x] 2.6 Implement the validator; make 2.5 pass
- [x] 2.7 CI job: `weaver registry check` on the sample custom registries in
      `src/schema-model/tests/fixtures/` (accept-set ⊆ Weaver)

## 3. Bundled registries in `common`

- [ ] 3.1 Failing test: `SchemaResolver::bundled()` lists `otel@1.43.0` and
      `signaldb@<version>` with `source: bundled` and resolves `k8s.pod.uid`,
      `k8s.pod`, `k8s.pod.cpu.time`, `signaldb.tenant.id`
- [ ] 3.2 `common/build.rs` (or `include_dir!` fallback per design D2) parses
      vendored model + `otel/registry/` into an embedded snapshot; load it into
      the bundled index at startup; make 3.1 pass
- [ ] 3.3 Failing test: bundled index rejects mutation (`ReadOnlyRegistry`)

## 4. Custom registries in the catalog

- [ ] 4.1 Failing catalog tests (SQLite + Postgres via existing test harness):
      create/get/replace/delete `schema_registries` with flattened rows,
      replace is transactional, tenant isolation, reserved namespaces refused
- [ ] 4.2 Add DDL for `schema_registries`, `schema_attributes`,
      `schema_entities`, `schema_metrics` to catalog bootstrap (both dialects)
      and implement the repository; make 4.1 pass
- [ ] 4.3 Failing tests for `SchemaResolver` with a tenant: precedence custom →
      signaldb → otel, alternatives never dropped, prefix search cap, empty
      result for unknown key, cache invalidation on write
- [ ] 4.4 Implement per-tenant lazy index + invalidation; make 4.3 pass

## 5. HTTP API (router)

- [x] 5.0 Failing tests in `common::auth`: `can_read_schema`/`can_write_schema`
      for explicit scopes, legacy `None`, Viewer/Member/Admin sessions,
      instance admin; add `schema:read`/`schema:write` constants and
      `schema:read` to `READ_SCOPES`; implement
- [x] 5.0b Failing tests: one shared `API_KEY_SCOPES` vocabulary +
      `validate_scopes()`; management and admin APIs accept `schema:read`/
      `schema:write` on key creation and list them; unknown scope → 422; empty
      scopes → 422; OAuth `granted_read_scopes` grants `schema:read` by default
      and rejects a `schema:write`-only request; implement
- [x] 5.0c Failing tests: admin `POST /tenants/{id}/api-keys` takes required
      `scopes` + optional `dataset_id`; `PATCH …/api-keys/{key_id}` on admin
      and management APIs updates scopes/dataset, rejects revoked keys, and the
      next request with the key reflects the change; catalog
      `update_api_key_scopes`; implement

- [ ] 5.1 Failing router tests for `/api/v1/schema/registries` (list, create
      JSON+YAML, get, replace, delete; 409 on bundled; 422 with error paths;
      403 for non-admin session, for keys lacking `schema:write`, and for
      ingest-only keys on read endpoints; tenant scoping)
- [ ] 5.1b Failing router tests for `registries:validate` (errors with paths;
      counts; nothing stored) and list counts; implement
- [ ] 5.2 Failing router tests for resolve/search endpoints (attributes,
      entities, metrics; `?keys=` batch form; `?prefix=&limit=`)
- [ ] 5.3 Implement handlers with utoipa annotations; make 5.1–5.2 pass
- [ ] 5.4 Update the OpenAPI document; `cargo xtask generate` to regenerate the
      Rust SDK (`src/signaldb-sdk`) and TS client (`src/ui/src/api/gen`); make
      the parity gate pass

## 6. SDK, CLI, MCP

- [ ] 6.1 Failing SDK tests for `schema()` methods; implement over the
      regenerated client
- [x] 6.1a Sweep every key-creation call site for the now-required `scopes`
      (no compat shim): CLI `admin api-key create` + TUI admin client
      (`src/signaldb-cli/src/tui/client/admin.rs`, `components/admin/`),
      MCP admin toolset, `tests-integration` and router/session tests that
      mint keys, `docs/users/authentication.md`, `docs/users/explore-ui.md`,
      `docs/architecture/overview.md`, skills `multi-tenancy`, `tempo-api`,
      `signaldb-observe`; release note marks the admin API body change
      BREAKING
- [x] 6.1b Failing tests: ApiKeys UI scope picker groups Ingestion and Schema
      scopes with descriptions and offers Edit scopes on live keys; CLI
      `admin api-key create --scope … --dataset …` (scope required) and
      `admin api-key update`; MCP admin `create_api_key` takes `scopes`/
      `dataset_id` and `update_api_key_scopes` exists; key listings show scopes
      on all surfaces; implement
- [ ] 6.2 Failing CLI tests: `signaldb schema registry list|get`,
      `schema attribute|entity|metric get|search`,
      `admin schema create|replace|delete --file` (YAML and JSON); implement
- [ ] 6.3 Failing MCP tests: tools `list_schema_registries`,
      `resolve_attribute`, `resolve_entity`, `resolve_metric`,
      `search_schema`, admin `create/replace/delete_schema_registry`; result
      shape equals HTTP; implement (tool descriptions steer resolve-before-query)
- [ ] 6.4 tests-integration: end-to-end — create custom registry via CLI,
      resolve via HTTP and MCP, precedence and alternatives verified

## 7. Explore UI semantic labels

- [ ] 7.1 Failing hook tests: `useSemantics()` batches/de-dupes keys, caches
      per session/tenant, degrades to raw key on error
- [ ] 7.2 Implement the hook over the generated client (batch `?keys=`)
- [ ] 7.3 Failing component tests: span/log detail rows show key + brief +
      title + namespace tag; unregistered key unchanged; deprecated marker with
      replacement; tenant definition primary with otel alternative
- [ ] 7.4 Wire `spanAttributes` detail table, log detail panel, `FieldSidebar`,
      `TraceFacets` headers
- [ ] 7.5 Failing test + implement: `FilterChips` autocomplete merges registry
      prefix search (with briefs) and observed labels
- [ ] 7.6 Verify in the running app (`/run`) against `_system/_monitoring` data

## 8. Schema hub UI (management & inspection)

- [ ] 8.1 Failing route tests: `/schema` renders hub with Conventions tab for any
      tenant user and Storage tab only for instance admins; `/schema/storage`
      deep-links the existing explorer; back button returns to prior view
- [ ] 8.2 Move `management/SchemaExplorer*` under `features/schema/` as the
      Storage tab; add hub shell and tabs; make 8.1 pass
- [ ] 8.3 Failing tests: registry list shows namespace/version/source/counts,
      read-only marker on bundled; global lookup shows precedence-ordered hits
      with primary marker; implement over the generated client
- [ ] 8.4 Failing tests: registry browser (search filters attributes / entities /
      metrics; definition pane per kind; alternatives linked; deep-link URLs for
      definitions); implement
- [ ] 8.5 Failing tests: entity page (identifying/descriptive roles, associated
      metrics, extended-by); implement
- [ ] 8.6 Failing tests: custom-registry editor — upload YAML/JSON or paste,
      Validate renders per-path errors and counts, Save blocked until valid,
      diff summary vs stored, save-as-new-version, Delete with confirmation,
      actions hidden for non-admins and for bundled registries; implement
- [ ] 8.7 Link attribute tooltips (7.x) to the definition pages; verify in the
      running app (`/run`)

## 9. Docs, skills, housekeeping

- [ ] 9.1 Docs (route via the docs skill): users guide "Schema registry —
      custom conventions", operations note on vendored semconv + pin, API
      reference regenerated
- [ ] 9.2 Update skills: `cli-command-surface`-touching `dev-workflow` /
      `crate-map` (new `schema-model` crate, `vendor/otel-semconv`), `docs`
      routing if needed
- [ ] 9.3 `cargo machete`, `cargo deny check`, `make lint && make format`,
      `/simplify`; validate specs (`openspec validate --strict`)
