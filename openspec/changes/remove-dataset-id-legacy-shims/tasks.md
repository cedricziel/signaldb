## 1. Storage: drop the legacy `dataset_id` column (common)

- [x] 1.1 Failing tests in `common::catalog` (`cargo test -p common`, SQLite):
      a pool created fresh (no prior schema) never has a `dataset_id` column
      on `api_keys` (`PRAGMA table_info`); a pool seeded to simulate an
      upgrade (create the table with the legacy `dataset_id` column present
      and a row carrying both `dataset_id` and `dataset_ids` values, as the
      pre-this-change schema would) has the column removed after
      `Catalog::init()` runs again, and every other column's data for that
      row (`dataset_ids`, `scopes`, `created_by_user_id`, `created_at`,
      `revoked_at`) is unchanged; running `Catalog::init()` a second time
      against an already-migrated pool is a no-op (no error, column stays
      absent). Same three cases against Postgres via the testcontainer
      suite.
- [x] 1.2 Implement (D1): remove `dataset_id TEXT,` from the literal
      `CREATE TABLE IF NOT EXISTS api_keys` string on both the SQLite
      (`catalog.rs` ~198-232) and Postgres (~561-572) branches; replace the
      SQLite `has_api_key_column("dataset_id")` add-column block (~242-246)
      with a drop guarded the same way: `if has_api_key_column("dataset_id")
{ ALTER TABLE api_keys DROP COLUMN dataset_id }`; replace the Postgres
      `ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS dataset_id TEXT` line
      (~575-577) with `ALTER TABLE api_keys DROP COLUMN IF EXISTS
dataset_id`.
- [x] 1.3 Implement: delete `backfill_api_key_dataset_ids`,
      `pending_api_key_dataset_id_backfill`, and
      `apply_api_key_dataset_id_backfill` (dead code once there is no
      legacy column to backfill from), and their call from `Catalog::init()`
      (~840).
- [x] 1.4 Failing tests: `upsert_scoped_api_key` and `update_api_key_scopes`
      (`DatasetRestrictionUpdate::Set`/`Clear`/`Keep`) no longer write to a
      `dataset_id` column at all (assert by querying `PRAGMA table_info`/
      `information_schema.columns` shows no such column exists after the
      call, and that `dataset_ids` alone reflects the requested state);
      `ApiKeyRecord`/`ApiKeyAuthRecord` construction from a row reads
      `dataset_ids` only.
- [x] 1.5 Implement: remove `ApiKeyRecord::dataset_id`; simplify
      `project_dataset_id_set`/`decode_dataset_fields` (or delete them
      outright if nothing but the legacy projection remains — replace call
      sites with a direct `encode_dataset_ids_json`/JSON-decode of
      `dataset_ids`); remove the legacy-column branch from
      `DatasetRestrictionUpdate`'s SQL (`Set`/`Clear` only ever touch
      `dataset_ids` now); update every construction site of `ApiKeyRecord`
      accordingly. `cargo test -p common` green; `cargo machete
--with-metadata` (a helper going unused entirely should be deleted,
      not left dead).
- [x] 1.6 Code-review fix (CodeRabbit, PR #1480): 1.2's drop ran before
      `dataset_ids` was ensured to exist, and 1.3 deleted the backfill
      entirely — together, a database jumping straight from before
      `multi-dataset-key-restriction` to after this change (skipping any
      boot of the intermediate dual-write code) would drop `dataset_id`
      before anything ever copied its data into `dataset_ids`, silently
      turning every single-dataset-restricted key unrestricted. Failing
      tests first (both dialects):
      `catalog_init_backfills_dataset_ids_before_dropping_legacy_column_with_no_intermediate_boot`
      seeds the pre-#1475 schema (legacy column present, `dataset_ids`
      column absent) with a restricted and an unrestricted row, runs
      `Catalog::init()`, and asserts the restriction survives. Implement:
      reorder so `dataset_ids` is ensured to exist first; before the drop,
      backfill every row where `dataset_id IS NOT NULL AND dataset_ids IS
  NULL` (a per-row `SELECT` then `UPDATE ... WHERE id = ? AND
  dataset_ids IS NULL`, re-checking `IS NULL` at write time so a
      concurrent legitimate `dataset_ids` write from another already-
      upgraded instance wins instead of being clobbered — no compare-and-
      swap on the old value needed, since there is no longer a legacy write
      to race against). See `design.md`'s revised D2.

## 2. Response DTOs, router, and regenerated clients

- [x] 2.1 Failing router tests (`cargo test -p router`): the JSON response
      for creating and listing an API key via the admin API and the
      management API no longer contains a `dataset_id` key at all (not
      `null` — absent), only `dataset_ids`.
- [x] 2.2 Implement: remove `dataset_id: Option<String>` from
      `CreateApiKeyResponse` and `ApiKeyResponse`
      (`src/signaldb-api/src/schemas.rs`); delete
      `derive_legacy_dataset_id`; remove the three call sites in
      `src/router/src/endpoints/admin.rs` (~626, ~769) and
      `src/router/src/endpoints/management.rs` (~507, ~592, ~767) along with
      their now-unused `dataset_ids` intermediate bindings where those
      exist only to feed the deleted call. `UPDATE_OPENAPI=1 cargo test -p
router openapi_spec_is_up_to_date`, then `cargo xtask generate`
      (regenerates the OpenAPI spec, `signaldb-sdk`, and the UI's
      TypeScript client) — run this immediately, not deferred, so task 3
      starts from a client whose generated types no longer declare
      `dataset_id` on these response shapes.
- [x] 2.3 `cargo test -p router` and `cargo test -p signaldb-sdk` green with
      the regenerated types.

## 3. UI: migrate the last `dataset_id` reader

- [x] 3.1 Failing test in `ApiKeys.test.tsx` (or a new
      `DatasetPicker.test.tsx` if one doesn't already isolate this): a key
      object carrying only a legacy-shaped `{ dataset_id: "production" }`
      (no `dataset_ids`) is treated as unrestricted (`datasetRestrictionLabel`
      returns `"unrestricted"`, `restrictionSet` returns `[]`) — the
      opposite of today's fallback — proving the fallback is gone, not just
      untested (`pnpm --filter signaldb-ui test`).
- [x] 3.2 Implement: in `src/ui/src/features/management/DatasetPicker.tsx`,
      drop the `dataset_id` field from the `RestrictedByDataset` interface
      and change `restrictionSet` to `return key.dataset_ids ?? [];`; update
      `ApiKeys.test.tsx` fixtures that currently set `dataset_id` (instead of
      `dataset_ids`) on a key to use `dataset_ids`, since a fixture still
      shaped as legacy would now assert "unrestricted" rather than the
      dataset it originally meant to represent.
- [x] 3.3 `pnpm --filter signaldb-ui lint && pnpm --filter signaldb-ui test`
      green; confirm no other UI file reads `.dataset_id` off an API-key
      response shape (`grep -rn "\.dataset_id\b" src/ui/src`, excluding the
      unrelated per-request/context `dataset_id` usages already audited in
      this change's proposal).

## 4. Docs

- [x] 4.1 Update `docs/users/authentication.md` (route via the docs skill):
      the "Legacy field removed" section changes from "deprecated but
      present" to "removed" — no `dataset_id` field on any API-key response,
      no `dataset_id` column in storage; the "Multi-dataset rollout" section
      drops the API-key dual-write rationale (there is no legacy column left
      to write to), keeping the OAuth-side and
      `dataset_restriction_rollout_complete` rationale intact since neither
      is affected by this change.
- [x] 4.2 Add the deployment-safety note design.md's Risks/Migration Plan
      sections identified during 4.1: this change must ship as a full
      stop-and-restart of every instance sharing the catalog database, never
      a staggered/rolling upgrade — unlike every prior `ADD COLUMN`
      migration in this codebase, `DROP COLUMN` breaks a still-running
      pre-this-change instance outright (its `api_keys` queries reference
      `dataset_id` explicitly), not just semantically. Fold this into
      `docs/users/authentication.md` (near the "Multi-dataset rollout"
      section) and/or `docs/operations/` if there's a release/upgrade-notes
      doc this project already routes such constraints through — check the
      `docs` skill's routing guidance for where an upgrade constraint like
      this belongs.

## 5. Verification

- [ ] 5.1 `cargo fmt`; `cargo clippy --workspace --all-targets
--all-features`; `cargo machete --with-metadata`; `cargo test
--workspace` (SQLite path) plus the Postgres testcontainer suite for
      `common`/`router`.
- [ ] 5.2 `openspec validate remove-dataset-id-legacy-shims --type change
--strict` (if the `openspec` CLI is available in the implementing
      environment).
