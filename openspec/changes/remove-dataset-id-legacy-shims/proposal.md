## Why

`multi-dataset-key-restriction` (#1475, merged) deliberately kept two
backward-compat shims so the rollout to a set-valued `dataset_ids` restriction
wouldn't hard-break existing callers mid-migration: a dual-written legacy
`dataset_id` TEXT column on `api_keys` (D2), and a deprecated, best-effort
`dataset_id` response field on `CreateApiKeyResponse`/`ApiKeyResponse` (D8).
`tasks.md` task 7.2 named removing both as a deliberate, separate follow-up
once every consumer had migrated off the singular field.

That precondition is now met, with one fix folded into this change: CLI, MCP
server, and every other UI surface already read `dataset_ids` only, but
`src/ui/src/features/management/DatasetPicker.tsx` still falls back to the
legacy `dataset_id` field when `dataset_ids` is absent (`restrictionSet`),
with test fixtures in `ApiKeys.test.tsx` exercising that fallback. Once that
one remaining reader is migrated, both shims are dead weight: extra
dual-write cost on every key create/update, a response field every client
must keep tolerating, and a schema column carrying no information
`dataset_ids` doesn't already have.

## What Changes

- Migrate `DatasetPicker.tsx`'s `restrictionSet`/`datasetRestrictionLabel` to
  read `dataset_ids` only, dropping the `dataset_id` fallback; update
  `ApiKeys.test.tsx` fixtures that exercised the fallback to use `dataset_ids`
  instead.
- **BREAKING**: remove the deprecated `dataset_id: Option<String>` field from
  `CreateApiKeyResponse` and `ApiKeyResponse` (`src/signaldb-api/src/
schemas.rs`) and delete `derive_legacy_dataset_id`; update the three call
  sites in `src/router/src/endpoints/admin.rs` and `src/router/src/endpoints/
management.rs`. Regenerate the OpenAPI spec, the Rust SDK
  (`signaldb-sdk`), and the UI TypeScript client from the updated schema (the
  same `cargo xtask generate` step the base change used at each schema
  edit). A client still reading `dataset_id` from a key-management response
  after this change sees the field simply absent (`null`/`undefined`
  depending on client-side JSON handling), not an error — this is a response
  shape change, not a request-validation change, so there is nothing to
  reject.
- Drop the legacy `dataset_id` TEXT column from `api_keys` in
  `src/common/src/catalog.rs` (both the SQLite and Postgres branches of
  `Catalog::init()`), and delete the `DatasetRestrictionUpdate` dual-write
  logic that targets it, including the `project_dataset_id_set`/
  `decode_dataset_fields`-style helpers introduced for the dual-read/
  dual-write migration. See `design.md` for the column-removal mechanism
  (this repo's migrations are additive-only today; a drop needs its own,
  narrowly-scoped approach).
- Update `docs/users/authentication.md`: the "Legacy field removed" section
  moves from "deprecated but present" to "fully removed"; the "Multi-dataset
  rollout" section drops the API-key dual-write rationale, since there is no
  legacy column left to write to (the `[auth].dataset_restriction_rollout_
complete` gate and its OAuth-side rationale are unaffected — OAuth never
  had a legacy column, per D2).

Explicitly out of scope: `oauth_authorization_codes`, `oauth_access_tokens`,
and `oauth_refresh_tokens` never had a legacy single-value `dataset_id`
column (D2 only ever applied to `api_keys`), so there is nothing to remove on
the OAuth side of storage.

## Capabilities

### New Capabilities

- (none)

### Modified Capabilities

- (none) — the main `api-key-management` spec never described the deprecated
  `dataset_id` field or the legacy column; both were transitional
  compatibility shims documented only in `multi-dataset-key-restriction`'s
  own `design.md` (D2, D8), never as required behavior. The dataset
  restriction semantics `api-key-management` does describe (a key can carry
  an optional dataset restriction, updatable without rotation) are unchanged
  by this cleanup — only a temporary wire/storage compatibility mechanism is
  removed. This change sets `skip_specs: true`.

## Impact

- **common**: `catalog.rs` — drop the `api_keys.dataset_id` column
  (SQLite + Postgres branches of `Catalog::init()`); remove the
  `DatasetRestrictionUpdate` variants'/helpers' dual-write to that column;
  `ApiKeyRecord`/`ApiKeyAuthRecord` lose their legacy-column read path.
- **signaldb-api**: `schemas.rs` — remove `dataset_id` from
  `CreateApiKeyResponse`/`ApiKeyResponse` and delete
  `derive_legacy_dataset_id`.
- **router**: `endpoints/admin.rs`, `endpoints/management.rs` — drop the
  three `derive_legacy_dataset_id(...)` call sites and the response-mapping
  fields that used them.
- **signaldb-sdk**: regenerated from the OpenAPI spec (response types lose
  `dataset_id`) — do not hand-edit.
- **src/ui**: `DatasetPicker.tsx` (`restrictionSet`/`datasetRestrictionLabel`),
  `ApiKeys.test.tsx` fixtures, regenerated `api/gen/types.gen.ts`.
- **docs**: `docs/users/authentication.md`.
- Not affected: `signaldb-cli`, `mcp-server` (already `dataset_ids`-only),
  OAuth token tables, `tests-integration` dataset-restriction e2e coverage
  (asserts behavior, not the deprecated field).
