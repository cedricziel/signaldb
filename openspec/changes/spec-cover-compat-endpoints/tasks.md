## 1. Loki: schema fidelity and metadata coverage

- [ ] 1.1 `cargo test -p router` baseline (failing/absent-coverage expectation): add or extend `logql.rs` handler tests asserting `labels`/`label_values`/`series`/`detected_fields` response shapes, to lock in behavior before touching annotations.
- [ ] 1.2 Add `ToSchema` (and any needed `#[schema(...)]` attributes for enum variants) to `loki-api`'s `QueryResponse` and related result types.
- [ ] 1.3 Retrofit `logql::query` and `logql::query_range` to declare `body = loki_api::QueryResponse` instead of `serde_json::Value`.
- [ ] 1.4 Add `#[utoipa::path]` to `logql::labels`, `logql::label_values`, `logql::series`, `logql::detected_fields` with real response schemas.
- [ ] 1.5 Register all four in `openapi.rs`'s `paths()` and their DTOs in `components(schemas(...))`.
- [ ] 1.6 `UPDATE_OPENAPI=1 cargo test -p router` to regenerate `api/signaldb-api.json`; review the diff.

## 2. Prometheus: schema fidelity and metadata coverage

- [ ] 2.1 Add or extend `promql.rs` handler tests asserting `labels`/`label_values`/`label_stats` response shapes.
- [ ] 2.2 Add `ToSchema` to `prometheus-api`'s response types (query/query_range, label list, `label_stats`).
- [ ] 2.3 Verify (or retrofit, if it shares Loki's `serde_json::Value` pattern) `promql::query`/`query_range` declare real response schemas.
- [ ] 2.4 Add `#[utoipa::path]` to `promql::labels`, `promql::label_values`, `promql::label_stats`.
- [ ] 2.5 Register all three in `openapi.rs` and their DTOs in `components(schemas(...))`.
- [ ] 2.6 `UPDATE_OPENAPI=1 cargo test -p router`; review the diff.

## 3. Pyroscope: full coverage

- [ ] 3.1 Add or extend `pyroscope.rs` handler tests asserting `render`/`render_diff`/`label_names`/`label_values`/`profile_types`/`profiles_by_trace` response shapes.
- [ ] 3.2 Add `ToSchema` to `pyroscope-api`'s `Flamebearer`, `FlamebearerMetadata`, `Timeline`, `RenderResponse`, `ProfileType`, `LabelsResponse`, and `tempo_api::ProfileSummary` (used by `profiles_by_trace`); document the delta-encoded `levels` field.
- [ ] 3.3 Add `#[utoipa::path]` to `render`, `render_diff`, `label_names`, `label_values`, `profile_types`, `profiles_by_trace` with their real paths (`/pyroscope/render`, `/pyroscope/render-diff`, `/pyroscope/label-names`, `/pyroscope/label-values`, `/pyroscope/profile-types`, `/api/profiles/trace/{trace_id}`).
- [ ] 3.4 Register all six in `openapi.rs` and their DTOs in `components(schemas(...))`.
- [ ] 3.5 `UPDATE_OPENAPI=1 cargo test -p router`; review the diff.

## 4. UI session and whoami

- [ ] 4.1 Add or extend `session.rs` handler tests asserting `POST`/`DELETE /ui/session` and `GET /api/v1/whoami` response shapes (several already exist; confirm coverage of the memberships-pending-selection and error paths).
- [ ] 4.2 Add `ToSchema` to `CreateSessionRequest`, `SessionMembership`, `WhoamiResponse`, `WhoamiTenant`, `WhoamiDataset`, `WhoamiUser`, `WhoamiMembership`.
- [ ] 4.3 Add `#[utoipa::path]` to `create_session`, `delete_session`, `whoami`, with `security(())` overrides on the first two.
- [ ] 4.4 Register all three in `openapi.rs` and their DTOs in `components(schemas(...))`.
- [ ] 4.5 `UPDATE_OPENAPI=1 cargo test -p router`; review the diff.

## 5. Security scheme

- [ ] 5.1 Add the `cookieAuth` (`SecurityScheme::ApiKey(ApiKey::Cookie(...))`, naming the `signaldb_session` cookie) scheme in `SecurityAddon`.
- [ ] 5.2 Change the global default security requirement from `[{bearerAuth: []}]` to `[{bearerAuth: []}, {cookieAuth: []}]` (alternatives).
- [ ] 5.3 `UPDATE_OPENAPI=1 cargo test -p router`; review the full diff (every existing operation's security metadata changes here — confirm nothing else moved).

## 6. Regenerate downstream clients

- [ ] 6.1 `cargo xtask generate` to regenerate `signaldb-sdk` (Rust) and `src/ui/src/api/gen` (TypeScript).
- [ ] 6.2 `cargo build -p signaldb-sdk` and `cargo build --workspace` to confirm the regenerated SDK compiles and nothing downstream broke.
- [ ] 6.3 `pnpm --filter signaldb-ui typecheck` to confirm the regenerated TS client compiles (not yet consumed by application code).
- [ ] 6.4 Commit the regenerated `api/signaldb-api.json`, `signaldb-sdk`, and `src/ui/src/api/gen` output.

## 7. Docs

- [ ] 7.1 Update `docs/contributing/rust.md` or the relevant operations doc if the OpenAPI-generation workflow description needs to mention the query-compat/session surfaces (route via the docs skill).
- [ ] 7.2 Check whether any bundled skill describing the OpenAPI/SDK generation flow needs updating to reflect the newly-covered surfaces.
