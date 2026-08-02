## Context

Retroactive design record for the code-first OpenAPI pipeline (PR #856). The
goal: one source of truth for the admin/management HTTP API, a published spec
that cannot drift from the code, and generated clients for both first-party
consumers (CLI, UI).

## Pipeline

```
#[utoipa::path] handlers + #[derive(ToSchema)] DTOs
   → ApiDoc (router::openapi) → api/signaldb-api.json (golden-tested, 3.1)
        │                                   ├→ signaldb-sdk  (progenitor, Rust → CLI)
        └→ served at /api/v1/openapi.json   └→ src/ui/src/api/gen (@hey-api, TS → UI)
```

## Decisions

- **Tool: utoipa (code-first) over hand-authored spec.** Annotate handlers and
  DTOs; assemble via `#[derive(OpenApi)]`. The router serves
  `openapi::openapi_document()` directly, so the served spec is always the code.
- **Handlers stay generic** over `RouterState`. A spike confirmed
  `#[utoipa::path]` compiles on generic handlers, so no concrete refactor was
  needed. (A separate, orthogonal rename — `InMemoryStateImpl` → `RouterAppState`
  — corrected a misleading type name.)
- **Emission via a golden test**, not a build step. `router::openapi`'s test
  regenerates `api/signaldb-api.json` from `ApiDoc` and fails on drift
  (`UPDATE_OPENAPI=1` to refresh). This rides `cargo test` and keeps xtask off
  the datafusion-heavy router dep.
- **Downstream clients from the emitted spec, gated by `cargo xtask check`.**
  - Rust SDK via progenitor. progenitor parses through the `openapiv3` crate
    (OpenAPI 3.0), but utoipa emits 3.1, so xtask **downconverts progenitor's
    input only**: nullable `type` arrays (`["string","null"]`) → `type` +
    `nullable: true`, version pinned to `3.0.3`. The served spec and
    `signaldb-api.json` stay 3.1; `@hey-api` consumes 3.1 natively.
  - TS client via `@hey-api/openapi-ts`; `check` regenerates into a temp dir and
    compares, so it never mutates the tree.
- **Absolute paths.** Annotated paths are absolute (`/api/v1/admin/...`), so
  generated client URLs are absolute; the CLI's admin client and the UI client
  use the router root as base URL.
- **Admin/manage disambiguation.** The two surfaces share handler/type names;
  management operationIds are prefixed `manage_*` and colliding component
  schemas aliased `Manage*` via `#[schema(as = ...)]`. Rust names and JSON wire
  format are unchanged.
- **Auth documented globally.** `bearerAuth` is required globally via a
  `Modify` addon so both surfaces are marked authenticated; admin handlers also
  restate it per-path.

## FDAP / storage constraints

- **FDAP version alignment:** not applicable — this change touches the HTTP
  admin/management surface, not Arrow/Parquet/DataFusion types.
- **Flight v1 wire vs v2 storage transforms; WAL/Iceberg migration + rollback:**
  not applicable — no Flight, WAL, or Iceberg schema/layout change. The only
  schema transform is the OpenAPI 3.1→3.0 downconvert of progenitor's input,
  which is a code-generation detail with no runtime effect and is trivially
  reversible (revert the xtask function).

## Known gaps / follow-up

- Pyroscope query endpoints (`/pyroscope/...`, `/api/profiles/...`) are not yet
  annotated, so they are absent from the code-first spec (the old hand-authored
  spec documented them). Annotating them needs `ToSchema` on `pyroscope-api`
  types.
