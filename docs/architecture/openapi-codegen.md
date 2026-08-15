---
audience: contributor
type: explanation
status: living
sources:
  - src/router/src/openapi.rs
  - src/router/src/endpoints/admin.rs
  - src/router/src/endpoints/management.rs
  - src/signaldb-api/src/**
  - xtask/src/main.rs
  - src/ui/openapi-ts.config.ts
  - api/signaldb-api.json
---

# Code-First OpenAPI & Client Generation

SignalDB's HTTP admin, tenant-management, and trace-query API is **code-first**:
the Rust handlers and their data types are the single source of truth, and the
OpenAPI spec plus every client are generated from them. Nothing is hand-authored
downstream of the code, so the spec cannot drift from what the router actually
serves.

```mermaid
flowchart LR
    A["#[utoipa::path] handlers<br/>#[derive(ToSchema)] DTOs"] --> B["ApiDoc<br/>(router::openapi)"]
    B -->|golden test| C["api/signaldb-api.json<br/>(OpenAPI 3.1)"]
    B -->|served at| S["/api/v1/openapi.json"]
    C -->|progenitor<br/>+ 3.1→3.0 downconvert| D["signaldb-sdk<br/>(Rust client, CLI)"]
    C -->|@hey-api/openapi-ts| E["src/ui/src/api/gen<br/>(TS client, UI)"]
```

## Source of truth

- **DTOs** live in [`signaldb-api`](../../src/signaldb-api/src/schemas.rs) as
  hand-written structs deriving `utoipa::ToSchema` (admin surface) and, for the
  session-authenticated management surface, in
  `src/router/src/endpoints/management.rs`. Field names and serde attributes
  define the JSON wire format; `ToSchema` makes each struct an OpenAPI
  component.
- **Operations** are declared with `#[utoipa::path(...)]` on the handlers in
  `endpoints/admin.rs` (`/api/v1/admin/...`, including the API-key
  `POST`/`PATCH` bodies with their required `scopes`), `endpoints/management.rs`
  (`/api/v1/manage/...`), `endpoints/tempo.rs` (the Tempo-compatible trace
  query endpoints under `/tempo/api/...`, whose DTOs live in `tempo-api`),
  `endpoints/query.rs` (the native Query IR endpoint `POST /api/v1/query`, whose
  request/response DTOs are defined in that module), the PromQL/LogQL
  instant and range query endpoints plus their label-discovery endpoints in
  `endpoints/promql.rs` (`/prometheus/api/v1/query{,_range}`, `/labels`,
  `/label/{name}/values`) and `endpoints/logql.rs`
  (`/loki/api/v1/query{,_range}`, `/labels`, `/label/{name}/values`), the operational-control endpoints in
  `endpoints/ops.rs` (`/api/v1/ops/compact{,/status,/dry-run}`, admin-authenticated,
  proxied to the compactor's Flight `do_action` surface), `endpoints/oauth.rs`
  (the session-authed OAuth consent surface the explore-UI consumes —
  `GET /oauth/consent/context` and `POST /oauth/authorize/decision`), and
  `endpoints/schema.rs` (the schema registry: `/api/v1/schema/registries`
  CRUD + `:validate`, and attribute/entity/metric resolution and prefix search
  under `/api/v1/schema/{attributes,entities,metrics}`; its resolved-definition
  DTOs derive `ToSchema` in `common::schema_registry` and `schema-model`, and
  the raw registry document is typed as an opaque object). Paths are absolute; operationIds on the
  management handlers are prefixed `manage_*` and their colliding component
  schemas aliased `Manage*` (via `#[schema(as = ...)]`) so admin and manage
  names don't clash. The PromQL/LogQL handlers set explicit `operation_id`s
  (`promql_query`, `logql_query`, …) because their bare handler names collide.
- `src/router/src/openapi.rs` assembles everything into the `ApiDoc`
  (`#[derive(OpenApi)]`) — info, `servers`, the `bearerAuth` security scheme,
  tags, the path list, and the component schemas.

The router serves this document live at `/api/v1/openapi.json`
(`openapi_document()`), so the served spec is always exactly the code.

Cross-cutting response headers that apply to every operation — the
`Server-Timing`/`traceresponse` trace-context headers the shared middleware
adds — are documented once in `info.description`, not repeated as per-response
header schemas on each path.

## The spec artifact and its golden test

`api/signaldb-api.json` is the committed OpenAPI 3.1 document. A golden test in
`router::openapi` regenerates it from `ApiDoc` and fails if the checked-in file
drifts:

```bash
# Refresh the committed spec after changing annotations:
UPDATE_OPENAPI=1 cargo test -p router openapi_spec_is_up_to_date
# CI runs the same test without the env var, so a stale spec fails the build.
```

## Downstream clients

`cargo xtask generate` reads `api/signaldb-api.json` and regenerates both
clients; `cargo xtask check` verifies they are current (used in CI):

- **Rust SDK** (`signaldb-sdk`, consumed by `signaldb-cli`) via progenitor.
  progenitor parses through the `openapiv3` crate, which only understands
  OpenAPI 3.0, so xtask downconverts _its input only_ (`downconvert_nullable_types`
  in `xtask/src/main.rs`), handling both 3.1 nullable encodings utoipa emits:
  nullable `type` arrays (`["string","null"]`) become `type` + `nullable:
true`; `"oneOf": [{"type": "null"}, X]` (emitted for `Option<T>` where `T`
  is a `$ref` — a struct or enum DTO) has the null branch dropped and `X`'s
  fields merged onto the parent object plus `nullable: true`. The IR version
  is pinned to `3.0.3`. The served spec and `signaldb-api.json` stay 3.1.
- **TypeScript client** (`src/ui/src/api/gen`, consumed by the web UI) via
  `@hey-api/openapi-ts` (config in `src/ui/openapi-ts.config.ts`), which
  consumes 3.1 directly. In `check` mode xtask regenerates into a temp directory
  and compares, so it never mutates the tree.

xtask also owns one non-OpenAPI generation task: `cargo xtask vendor-semconv`
copies the OpenTelemetry semantic-conventions `model/` tree at the version
pinned by `common::self_monitoring::SEMCONV_SCHEMA_URL` into
`vendor/otel-semconv/` (the source of the bundled `otel` schema registry). It
is run by hand when the pin is bumped; a `common` test fails if the vendored
`VERSION` and the pin disagree.

Because the annotated paths are absolute, generated client URLs are absolute
too — the CLI's admin client and the UI client are both configured with the
router root as their base URL.

## Adding or changing an endpoint

1. Add/adjust the `ToSchema` DTOs and the `#[utoipa::path]` annotation on the
   handler; register new paths/schemas in `router::openapi::ApiDoc`.
2. `UPDATE_OPENAPI=1 cargo test -p router openapi_spec_is_up_to_date` to refresh
   `api/signaldb-api.json`.
3. `cargo xtask generate` to regenerate the Rust and TypeScript clients.
4. Consume the endpoint through the generated clients — the UI must not issue
   raw HTTP against the API (see the DoD in `openspec/config.yaml`).
5. Commit the code, the spec, and the regenerated clients together.

CI enforces all of this: the golden test gates spec-vs-code in the Test Suite
job, and the `codegen` job runs `cargo xtask check` to gate the clients.

## Known gaps

- **A nullable `$ref` (struct or enum) used to break the Rust SDK
  generator.** `Option<T>` where `T` derives `ToSchema` makes utoipa emit
  `"oneOf": [{"type": "null"}, {"$ref": "..."}]`, which progenitor's
  schema-to-Rust generator panicked on (`not yet implemented: invalid type:
null`, in `to_schema.rs`) — the panic was in client generation, so
  `cargo xtask check`'s spec-golden-test analog passed even though codegen
  itself couldn't complete. `downconvert_nullable_types` in `xtask/src/main.rs`
  now flattens this `oneOf` shape before handing the spec to progenitor (see
  above), so `Option<T>` for a `$ref` DTO works like any other optional
  field — no per-field workaround needed. `ManageLogicalField::level` in
  `endpoints/management.rs` still converts `AttributeLevel` to a plain
  `Option<String>` at the handler boundary, but that's no longer required to
  avoid this panic; it predates the fix and can be revisited independently.
- The Tempo (trace), Loki (LogQL), and Prometheus (PromQL) instant/range query
  endpoints are all annotated. Tempo responses are **typed** (`SearchResult`,
  `Trace`, …). The PromQL and LogQL responses, however, are declared with a
  **loose `serde_json::Value` body** rather than typed schemas: their
  `[timestamp, "value"]` tuple sample shapes need extra schema handling, so the
  generated clients see an opaque JSON value and pass the native Loki/Prometheus
  response through unchanged. Tightening those two response schemas is a
  follow-up (epic #620, Phase A). The Loki/Prometheus label-discovery endpoints
  (`labels`, `label_values`) share the same loose-body treatment — they return
  a flat `{status, data}` shape simple enough that a typed schema adds no value.
- The polymorphic Tempo attribute `Value` (a serde-tagged union of
  string/int/bool/double) serializes as an untyped object in the schema, so the
  generated clients see it as an arbitrary JSON value rather than a typed enum.
- The Pyroscope-compatible query endpoints (`/pyroscope/...`,
  `/api/profiles/...`) are not yet annotated, so they are absent from the
  code-first spec. Annotating them requires `ToSchema` on the `pyroscope-api`
  types and is tracked as a follow-up.
