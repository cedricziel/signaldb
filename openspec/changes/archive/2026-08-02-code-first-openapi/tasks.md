<!-- Retroactive: all tasks were completed in PR #856 (squash e34fbfb) and are
     recorded here as done. -->

## 1. Hand-written DTOs as the schema source (`signaldb-api`)

- [x] 1.1 Replace the typify-generated `signaldb-api/generated.rs` with
      hand-written admin DTOs in `schemas.rs` deriving `utoipa::ToSchema`,
      preserving field names and serde attributes (optional fields keep
      `skip_serializing_if`); drop the unused Pyroscope types.
- [x] 1.2 Retire the typify type-generation step from `xtask` (drop
      typify/schemars deps); keep the admin handlers compiling against the
      hand-written types (`cargo test -p router` green).

## 2. Code-first spec emission (`router`)

- [x] 2.1 Write the golden test (`cargo test -p router
    openapi_spec_is_up_to_date`) that regenerates `api/signaldb-api.json` from
      the `ApiDoc` and fails on drift (`UPDATE_OPENAPI=1` refreshes it).
- [x] 2.2 Annotate the admin handlers (`endpoints/admin.rs`) with
      `#[utoipa::path]` — absolute paths, params, request/response bodies, and
      the quota (`429`) responses on key/dataset creation.
- [x] 2.3 Annotate the management handlers (`endpoints/management.rs`), typing
      the previously-untyped `json!` responses; disambiguate operationIds
      (`manage_*`) and colliding schemas (`Manage*`); declare the `500`
      responses handlers emit. Derive `ToSchema` on `common::MembershipRole`.
- [x] 2.4 Assemble `router::openapi::ApiDoc` (info, servers, tags, global
      `bearerAuth`), expose `openapi_document()`, and serve it at
      `/api/v1/openapi.json`. Make 2.1's golden test pass.

## 3. Downstream clients (`xtask`, `signaldb-sdk`, `ui`)

- [x] 3.1 Point `xtask` at `api/signaldb-api.json`; regenerate the progenitor
      Rust SDK, downconverting 3.1→3.0 for progenitor's input only. Delete the
      hand-authored `api/admin-api.yaml`/`.json`.
- [x] 3.2 Update the CLI admin client base URL to the router root (generated
      URLs are absolute); `cargo test -p signaldb-cli` green.
- [x] 3.3 Add `@hey-api/openapi-ts` to `signaldb-ui` with `openapi-ts.config.ts`;
      wire generation + a temp-dir `check` comparison into `xtask`
      (`cargo xtask check` gates both clients).

## 4. UI consumes the generated client (`ui`)

- [x] 4.1 Rewrite `management.test.ts` to assert behavior against the generated
      client (Request-object assertions; a fresh `Response` per call).
- [x] 4.2 Add `api/client.ts` (same-origin base URL + tenant-header
      interceptor) and rewrite `management.ts` onto the generated `manage_*`
      SDK, preserving the `ApiError`/401 contract; no raw HTTP remains
      (`pnpm --filter signaldb-ui typecheck && lint && test` green).

## 5. Gating & docs

- [x] 5.1 Add a CI `codegen` job running `cargo xtask check` (Rust + pnpm).
- [x] 5.2 Encode the UI/CLI/API-parity + spec/client-regen DoD in
      `openspec/config.yaml`.
- [x] 5.3 Document the pipeline in `docs/architecture/openapi-codegen.md` and
      fix the affected architecture overview / crate-map descriptions.
