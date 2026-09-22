## Context

See `proposal.md` for motivation. The two shims being removed, both from
`multi-dataset-key-restriction` (`openspec/changes/multi-dataset-key-
restriction/design.md` D2, D8):

- **D2 (storage)**: `api_keys.dataset_id TEXT` is a legacy single-value
  column, still present on every row alongside `dataset_ids TEXT` (a
  JSON-array-in-TEXT column, same pattern as `scopes`). Every write through
  `upsert_scoped_api_key`/`update_api_key_scopes` dual-writes both columns
  (`project_dataset_id_set` derives the legacy projection: `Some(ids[0])`
  when the set has exactly one element, else `None`). Every read
  (`ApiKeyRecord`/`ApiKeyAuthRecord` construction, `decode_dataset_fields`)
  dual-reads: `dataset_ids` when non-`NULL`, else derived from the legacy
  `dataset_id` column. A one-time backfill (`backfill_api_key_dataset_ids` /
  `pending_api_key_dataset_id_backfill` / `apply_api_key_dataset_id_backfill`,
  called from `Catalog::init()`) populates `dataset_ids` for any row that
  predates the column, with a compare-and-swap guard against a concurrent
  legacy write. OAuth's three token tables never had a legacy column — D2's
  dual-read/write only ever applied to `api_keys`.
- **D8 (wire)**: `CreateApiKeyResponse`/`ApiKeyResponse` carry a deprecated
  `dataset_id: Option<String>` alongside `dataset_ids`, computed by
  `derive_legacy_dataset_id` with the same one-element-or-`None` projection,
  at the three call sites in `router/src/endpoints/{admin,management}.rs`.

This repo's schema evolution is additive-only today: every existing call
site is either a literal `CREATE TABLE IF NOT EXISTS` (fixed at the schema
version current when the table was introduced) or an idempotent `ALTER
TABLE ADD COLUMN [IF NOT EXISTS]` guarded by a `PRAGMA table_info` existence
check on SQLite (`ensure_sqlite_text_column` / inline `has_api_key_column`
checks) or Postgres's native `ADD COLUMN IF NOT EXISTS`. There is no
migrations framework, no schema-version table, and no precedent anywhere in
`catalog.rs` for removing a column. `Catalog::init()` runs this DDL
unconditionally on every service boot, on both fresh and pre-existing
databases, so whatever replaces the `dataset_id` column handling must stay
idempotent and safe to run against a database at any prior schema state
(including one that predates the column entirely, e.g. a fresh install).

## Goals / Non-Goals

**Goals:**

- Remove `api_keys.dataset_id`, `CreateApiKeyResponse::dataset_id` /
  `ApiKeyResponse::dataset_id`, and all dual-write/dual-read/backfill code
  that exists solely to support them, without a migrations framework and
  without breaking a fresh install or a database already running the
  dual-write schema.
- Regenerate the OpenAPI spec, Rust SDK, and UI TypeScript client from the
  updated response schema, and migrate `DatasetPicker.tsx` off the field
  those types currently still expose.

**Non-Goals:**

- Introducing a migrations framework or schema-version tracking for this
  repo generally — out of scope, and unnecessary for one column drop.
- Any change to `dataset_allowed`, resolution order, the
  `dataset_restriction_rollout_complete` gate, or OAuth token storage — none
  of that touches the legacy `dataset_id` column or field.
- A reversible/staged rollout of the column drop. Per this repo's existing
  convention (breaking changes are acceptable post-1.0; no migration-path
  requirement), this is a one-shot removal, not a phased deprecation.

## Decisions

**D1 — Column removal mechanism: idempotent `DROP COLUMN`, run once at
`Catalog::init()`, replacing the `ADD COLUMN` handling in place.**

- Postgres has native, idempotent syntax:
  `ALTER TABLE api_keys DROP COLUMN IF EXISTS dataset_id`. This replaces the
  existing `ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS dataset_id TEXT`
  line one-for-one, and the column is also removed from the literal `CREATE
TABLE IF NOT EXISTS api_keys (...)` string (so a fresh database never
  creates it).
- SQLite's `ALTER TABLE ... DROP COLUMN` has been supported since 3.35.0
  (2021); this workspace's `sqlx` (`0.8`, `features = ["sqlite", ...]`,
  no `sqlite-unbundled`) links the bundled `libsqlite3-sys` (0.30.1), whose
  vendored amalgamation is far newer than 3.35 — verified as part of task
  1 by running the drop against a real SQLite pool in a test, not just
  reading a version macro. SQLite additionally restricts `DROP COLUMN`
  when the column is part of a `PRIMARY KEY`, a `UNIQUE`/`CHECK` constraint,
  an index, or a generated-column expression; `dataset_id` is none of
  these (`api_keys`' only constraints/indexes are on `id`, `key_hash`,
  `(tenant_id, name)`, and `tenant_id` — confirmed by reading every
  `CREATE INDEX ... api_keys` and the table's own constraint list), so the
  restriction doesn't apply here. Mirroring the existing
  `has_api_key_column` check: `if has_api_key_column("dataset_id") { ALTER
TABLE api_keys DROP COLUMN dataset_id }`, and the column is removed from
  the literal `CREATE TABLE IF NOT EXISTS` string the same way as Postgres.
  This keeps the operation idempotent (a second boot finds the column
  already gone and does nothing) and safe against a fresh database (the
  `CREATE TABLE` never introduces the column, so the guard is simply
  false and skipped).
- Alternative considered: leave the column in place, physically unused
  (stop reading/writing it, but never `DROP`). Rejected — the proposal's
  whole point is removing dead schema, and an orphaned nullable TEXT column
  costs nothing to actually drop given the constraint check above; keeping
  it around would just be technical debt with an established shims-are-fine
  precedent in this repo, not a real deployment risk to avoid.

**D2 — A minimal backfill still runs immediately before the drop; it is not
a no-op step.**

The original version of this decision assumed `dataset_ids` was already
guaranteed authoritative for every row by the time this change's code ever
runs, on the theory that `multi-dataset-key-restriction`'s dual-write and
one-time backfill (`backfill_api_key_dataset_ids`) would already have synced
every row. That assumption does not hold in general: this repo has no
discrete release process, and nothing prevents a database from jumping
straight from a schema that predates `multi-dataset-key-restriction`
entirely to this change's code in one deployment, skipping any boot of the
intermediate dual-write code — the exact case a caught-in-review regression
test (`catalog_init_backfills_dataset_ids_before_dropping_legacy_column_with_no_intermediate_boot`,
both dialects) now covers. In that case `dataset_ids` was never populated
for a pre-existing single-dataset-restricted row, and the original
drop-first ordering would have silently turned it unrestricted — the exact
security regression the base change went out of its way to rule out.

The fix: after `dataset_ids` is ensured to exist (not before — the backfill
needs somewhere to write to) and before `dataset_id` is dropped, backfill
every row where `dataset_id IS NOT NULL AND dataset_ids IS NULL` into
`dataset_ids`, then drop the column. The backfill's `UPDATE` re-checks
`dataset_ids IS NULL` at write time (not only at the preceding `SELECT`),
which is sufficient — without the elaborate compare-and-swap the base
change's now-deleted `backfill_api_key_dataset_ids` needed — because there
is no longer a _legacy_ write this code could race against (dual-write to
`dataset_id` is gone); the only remaining race is a concurrent _legitimate_
`dataset_ids` write from another service instance already running this
code, and the `IS NULL` guard alone makes such a write win over a stale
backfill instead of being clobbered by it.

**D3 — Order of implementation: storage → response DTOs → regen → UI,
matching the base change's own sequencing.**

Catalog/storage changes land first (task 1), then the response-DTO and
router changes plus an immediate `cargo xtask generate` regen (task 2) —
same reasoning `multi-dataset-key-restriction` task 2.2 used: regenerate
immediately so later tasks start from a client that already has the new
shape, rather than deferring regen to the end. `DatasetPicker.tsx`'s
fallback removal (task 3) comes last, once the regenerated types no longer
even declare `dataset_id` on the relevant response types, so the UI fix and
its test-fixture update are working against the final shape.

## Risks / Trade-offs

- **[Risk] `DROP COLUMN` is irreversible; a hand-rolled `ALTER TABLE ...
DROP COLUMN` bug corrupts the `api_keys` table.** → Mitigation: task 1's
  failing-tests-first pass exercises the drop against a real SQLite pool
  that already has the column (simulating an upgrade) and asserts every
  other column and every existing row's data survives untouched; the
  Postgres testcontainer suite does the same. Both are existing test
  infrastructure patterns in `common::catalog`, not new tooling.
- **[Risk] Rollback to a pre-this-change binary after the column is
  dropped.** → Mitigation: not a real risk given this repo's own
  additive-migration pattern — the older binary's `ensure_sqlite_text_column`
  /`ADD COLUMN IF NOT EXISTS` logic simply re-adds `dataset_id` as `NULL`
  on every row and resumes dual-writing it going forward; since dual-read
  always prefers `dataset_ids` first, and `dataset_ids` was never touched
  by this change, enforcement is unaffected. No data is lost by a rollback
  because the legacy column never carried information `dataset_ids` lacked
  (D2 above).
- **[Risk] A caller outside this repo (an external integration not audited
  here) still reads the deprecated `dataset_id` response field.** →
  Mitigation: this is the accepted, explicitly-labeled BREAKING part of the
  change (see proposal.md); `docs/users/authentication.md`'s "Legacy field
  removed" section documents it as a hard cutover, matching how
  `multi-dataset-key-restriction` documented the analogous request-side
  break for `dataset_id` → `dataset_ids`.
- **[Risk] A rolling upgrade with a pre-this-change node still serving
  traffic after another node has already run `DROP COLUMN` breaks the old
  node outright, not just semantically.** This is a materially different
  hazard than anything the base change had to reason about:
  `multi-dataset-key-restriction`'s `ADD COLUMN` migrations are safe under a
  rolling upgrade because old code's `INSERT`/`SELECT` statements name their
  columns explicitly and simply never mention the new one — a genuinely
  additive change is invisible to code that doesn't know about it yet. A
  _drop_ is the opposite: every pre-this-change binary's `api_keys`
  `INSERT`/`SELECT` statements explicitly name `dataset_id` in their column
  list (see `upsert_scoped_api_key`, `get_api_key`, `list_api_keys`,
  `authenticate_from_database`'s key lookup as they exist before this
  change). The moment any one instance in a multi-instance deployment runs
  `Catalog::init()` with this change's code and the `DROP COLUMN` executes,
  every other instance still running pre-this-change code gets a SQL error
  (`no such column: dataset_id` / equivalent Postgres error) on its very
  next API-key create, update, list, or auth lookup — not a stale read, an
  outright failure — for as long as that instance keeps running the old
  binary. → Mitigation: this repo has no live-migration/version-negotiation
  mechanism to gate the drop automatically (the same gap
  `multi-dataset-key-restriction`'s `dataset_restriction_rollout_complete`
  flag papered over for its own, milder, ADD-COLUMN-era concern), so this is
  addressed operationally rather than in code: this change must be deployed
  as a full stop-and-restart of every instance sharing the catalog database,
  never as a staggered/rolling upgrade where old and new binaries serve
  traffic concurrently. See "Migration Plan" below. This matches how this
  project is actually operated today (memory: hive deployments are
  stop/redeploy, not rolling blue-green), so the constraint costs nothing in
  practice, but it is a real constraint and is called out explicitly rather
  than silently assumed.

## Migration Plan

Deploy this change as a full stop-and-restart of every service instance that
shares the catalog database (acceptor, router, writer, querier, compactor,
mcp, or the monolithic `signaldb` binary) — not a staggered/rolling upgrade.
This is required by the risk above: unlike every prior additive migration in
`catalog.rs`, dropping `dataset_id` is not safe to run while any
pre-this-change instance is still serving `api_keys` traffic. There is no
in-place rollback once any instance has run the drop (see the rollback risk
above for why rolling back to an older binary afterward is still safe on its
own — the hazard is _concurrent_ mixed versions, not a clean sequential
rollback after every instance is already stopped).
