> Retroactive capture: the work described here was implemented and merged in
> PR #856 (squash `e34fbfb`) ahead of this record. This change documents the
> capability and its design so the specs reflect reality; its tasks are marked
> complete.

## Why

SignalDB's admin (`/api/v1/admin/*`) and tenant-management (`/api/v1/manage/*`)
HTTP APIs had no reliable contract. The admin surface was described by a
hand-authored `api/admin-api.yaml` that generators turned into a Rust SDK and
types — but the spec was maintained by hand, so it could drift from the
handlers. Worse, the management surface the web UI actually calls
(`/api/v1/manage/*`, including memberships) was **not in any spec at all**, and
the UI issued raw `fetch` against it. There was no single source of truth, no
guarantee the published spec matched the served API, and no generated client
for the UI.

## What Changes

- Invert the pipeline to **code-first**: the annotated Rust handlers and their
  `#[derive(utoipa::ToSchema)]` DTOs are the single source of truth. The router
  assembles them into an `ApiDoc` and serves it live at `/api/v1/openapi.json`,
  and a golden test regenerates `api/signaldb-api.json` from the code and fails
  on drift.
- Bring the previously-undocumented management surface into the spec alongside
  admin, with disambiguated operationIds/schemas (`manage_*` / `Manage*`).
- Generate **both** clients from the emitted spec and gate them in CI: the
  progenitor Rust SDK the CLI consumes, and an `@hey-api/openapi-ts` TypeScript
  client the UI consumes. The UI no longer issues raw HTTP against the API.
- Retire the hand-authored `api/admin-api.yaml` and the typify type-generation
  step; hand-write the `signaldb-api` DTOs.

This is **not** a wire-contract change to the query surfaces: OTLP ingest,
Tempo/LogQL/PromQL APIs, Flight schemas, and the on-disk Iceberg/WAL layout are
unchanged. The admin/management HTTP behavior is unchanged; only its
description (a published, code-derived OpenAPI document) and its client
consumption change. Not BREAKING by the project's criteria.

## Capabilities

### New Capabilities

- `admin-management-api-contract`: SignalDB publishes an OpenAPI document for
  its admin and tenant-management HTTP APIs, derived from the implementation so
  it cannot drift, and exposes those APIs to first-party consumers (CLI, web
  UI) through clients generated from that document.

### Modified Capabilities

<!-- None. This adds a contract/description capability over the existing
     admin & management endpoints; their runtime behavior is unchanged. -->

## Impact

- **router** (`src/router/src/openapi.rs`, `endpoints/admin.rs`,
  `endpoints/management.rs`, `lib.rs`): `#[utoipa::path]` annotations, the
  `ApiDoc`, the golden test, and serving the code-first document at
  `/api/v1/openapi.json`.
- **signaldb-api** (`src/signaldb-api/src/schemas.rs`): hand-written admin DTOs
  deriving `ToSchema`, replacing the typify-generated types.
- **signaldb-sdk** (`src/signaldb-sdk/src/generated.rs`): progenitor Rust
  client regenerated from the emitted spec.
- **signaldb-cli** (`tui/client/admin.rs`, `commands/completions.rs`): admin
  client base URL is the router root (generated URLs are absolute).
- **common** (`src/common/src/catalog.rs`): `MembershipRole` derives `ToSchema`.
- **xtask** (`xtask/src/main.rs`): consumes `api/signaldb-api.json`, generates
  the Rust SDK (with a 3.1→3.0 downconvert for progenitor) and the TypeScript
  client; `check` gates both. typify/schemars/serde_yaml deps dropped.
- **ui** (`src/ui/openapi-ts.config.ts`, `src/api/gen/**`, `src/api/client.ts`,
  `src/api/management.ts`): generated client + config, `management.ts` migrated
  off raw `fetch`.
- **CI** (`.github/workflows/ci.yml`): a `codegen` job runs `cargo xtask check`.
- No dependency-breaking, migration, or on-disk layout changes. No Flight/WAL/
  Iceberg schema changes.
