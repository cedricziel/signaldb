## 1. Storage + shared enforcement (common)

- [ ] 1.1 Failing tests in `common::catalog`: creating an API key with
      `dataset_ids: Some(vec!["a", "b"])` round-trips; `dataset_ids:
      Some(vec![])` is rejected at the catalog layer with a clear error on
      create; `dataset_ids: Some(vec!["production", "production"])` (a
      duplicate name within the same set) is rejected at the catalog layer
      rather than silently deduplicated — a duplicate that collapsed
      unnoticed to one element would make a restricted key look
      unrestricted to the legacy single-`dataset_id` projection (D1a); an
      existing key created before this change (legacy
      `dataset_id` column populated) reads back as `dataset_ids:
      Some(vec![<value>])`; the `DatasetRestrictionUpdate` tri-state
      (`Keep`/`Clear`/`Set`) is exercised directly against
      `update_api_key_scopes` — `Keep` leaves both `dataset_id` and
      `dataset_ids` untouched, `Clear` writes `NULL` to both columns (not
      just `dataset_ids` — this is the specific case a naive
      `COALESCE`-based update can't express and the first draft missed),
      `Set(["a"])` writes the derived single value to `dataset_id` too,
      `Set(["a","b"])` writes `dataset_id = NULL`; critically,
      `Set(vec![])` and `Set(vec!["a", "a"])` (duplicate) are rejected by
      `update_api_key_scopes` with the same error as the create path —
      the empty/duplicate validation is one shared function called from
      both `upsert_scoped_api_key` and `update_api_key_scopes`, not
      duplicated logic that the update path could independently forget;
      both SQLite and Postgres
      branches (`cargo test -p common`, plus the Postgres testcontainer
      suite)
- [ ] 1.2 Implement: add the `dataset_ids` column on `api_keys` (SQLite
      `catalog.rs:198-232`, Postgres `525-546`), following the `scopes`
      JSON-array-in-TEXT pattern (D1) — including how `scopes` is populated
      today: encode with `serde_json::to_string(&[dataset_id])` in Rust and
      bind the result as a parameter, never a SQL-side JSON constructor
      (e.g. `json_array(...)`), which this codebase does not use anywhere
      and which is not portable across the SQLite/Postgres backfill
      branches (D2); dual-read (`dataset_ids` if non-NULL, else derive from
      `dataset_id`) and dual-write (every write sets both columns per D2's
      projection) rather than a one-shot migration off
      `dataset_id`; the idempotent backfill only ever touches a row neither
      column has been written to since this change; define
      `enum DatasetRestrictionUpdate { Keep, Clear, Set(Vec<String>) }` in
      `common::catalog` and change `update_api_key_scopes`'s signature to
      take it in place of `dataset_id: Option<&str>`, replacing the
      `COALESCE`-based SQL with a branch per variant (D2b) — the `Set`
      branch validates its `Vec<String>` (reject empty, reject duplicates)
      through the same helper `upsert_scoped_api_key`'s create path calls,
      so the two entry points can't drift into checking different rules;
      update `ApiKeyRecord`/`ApiKeyAuthRecord`, `upsert_scoped_api_key`,
      `validate_api_key`; the backfill's read-then-write is made
      compare-and-swap safe against a concurrent legacy write: the
      `UPDATE ... SET dataset_ids = ?` statement includes `AND dataset_id =
      ?` bound to the exact value the backfill's `SELECT` just read (in
      addition to the existing `dataset_ids IS NULL` guard), so a
      concurrent old-code write to `dataset_id` between the read and the
      write makes the backfill's `UPDATE` match zero rows instead of
      persisting a `dataset_ids` value derived from data that is already
      stale — the row is picked up correctly by the next boot's backfill
      pass instead; a deterministic regression test proves this
      compare-and-swap guard, on both SQLite and Postgres: run the
      backfill's `SELECT`, then — before its `UPDATE` executes — perform a
      legacy `dataset_id` write against the same row through the old
      `COALESCE`-based write path (simulating the old-code race), then let
      the backfill's `UPDATE` proceed and assert it affects zero rows and
      leaves `dataset_ids` NULL; then run the backfill pass a second time
      and assert `dataset_ids` now reflects the *new* `dataset_id` value —
      proving the race resolves itself on the next pass rather than
      wedging the row in an inconsistent state
- [ ] 1.3 Failing tests: `dataset_allowed(None, "x")` is true;
      `dataset_allowed(Some(&["a","b"]), "a")` is true,
      `dataset_allowed(Some(&["a","b"]), "c")` is false; a resolution-order
      test proves a request with no explicit dataset and a single-element
      restriction resolves to that element (matching today's
      `dataset_id.or(...)` behavior exactly), and the same with a
      two-element restriction is rejected rather than falling through to
      the tenant default (D4) (`cargo test -p common`)
- [ ] 1.4 Implement `dataset_allowed` and the D4 resolution order in
      `common::auth` (D3); replace the inline check in
      `Authenticator::authenticate_from_database`
      (`authenticator.rs:391-397`); `TenantContext.api_key_dataset_id` →
      `api_key_dataset_ids: Option<Vec<String>>`, `with_api_key_restrictions`
      signature updated. This field rename fans out beyond
      `authenticator.rs`: fix every other `TenantContext` construction site
      and `with_api_key_restrictions(_, None)` call —
      `acceptor/src/lib.rs`, `acceptor/src/handler/prometheus_handler.rs`,
      each `acceptor/src/services/otlp_*_service.rs`,
      `router/src/read_scope.rs`, `router/src/endpoints/query.rs`,
      `router/src/endpoints/discovery.rs` — all pass `None` today and need
      no new logic, only the renamed field/type, but must compile and their
      existing tests must still pass
- [ ] 1.5 Failing tests: an OAuth-authenticated request whose token carries a
      dataset restriction is denied for a dataset outside it and allowed for
      one inside it; a token with no restriction (including every token
      issued before this change) reaches every dataset; a token restricted
      to two datasets with no explicit request dataset is rejected, same as
      the API-key case in 1.3 (`cargo test -p common`)
- [ ] 1.6 Implement: add `dataset_ids` column to `oauth_authorization_codes`,
      `oauth_access_tokens`, `oauth_refresh_tokens` (SQLite `catalog.rs:
      406-482`, Postgres `710-782`); thread through
      `create_authorization_code`/`create_access_token`/
      `create_refresh_token`; call `dataset_allowed` from
      `authenticate_oauth_token` (D3); the refresh-grant path reads
      `dataset_ids` from the presented `oauth_refresh_tokens` row, not from
      any access token (D6) — write the test for this before implementing,
      since it's the one place the first draft of this design was wrong

## 2. Router: admin + management APIs

- [ ] 2.1 Failing router tests (`cargo test -p router`): create/update an API
      key via the admin API with `dataset_ids: ["a", "b"]`; the key
      authenticates against dataset `a` and `b` but is refused for `c`;
      `dataset_ids: []` is rejected with a validation error on both create
      and update; a `dataset_ids` entry naming a dataset that does not
      belong to the target tenant is rejected and no key is created/updated
      (validate every element, not just that the list is non-empty);
      omitting `dataset_ids` creates an unrestricted key on create, leaves
      the restriction unchanged on update; `clear_dataset_restriction: true`
      together with a non-empty `dataset_ids` in the same update request is
      rejected as a contradictory combination; `clear_dataset_restriction:
      true` alone clears a restriction back to unrestricted; same set of
      cases through the management API; a key carrying `tenant:manage` and
      a non-empty `dataset_ids` is refused by every management-API endpoint
      (D9) — assert this against at least `manage_create_api_key` and
      `manage_delete_dataset`, not just one; a create/update request body
      carrying the legacy `dataset_id` field (instead of `dataset_ids`) is
      rejected with a validation error naming the field and pointing at
      `dataset_ids`, on both the admin and management API — it is never
      silently dropped, since a dropped restriction would create an
      unrestricted key when the caller asked for a restricted one; with
      `[auth].dataset_restriction_rollout_complete` at its default `false`,
      a create/update naming two or more datasets is rejected with an
      error naming the config key, while a single-dataset restriction,
      clearing, and an unrestricted key are all still accepted; the same
      two-or-more-datasets request succeeds once the test harness sets the
      config key to `true`
- [ ] 2.2 Implement: `endpoints/admin.rs` (586-620 create, 656-737 update,
      752-761 response mapping), `endpoints/management.rs` (403-433 create
      DTOs, 509-536 create handler, 573-580 update DTO, 603-668 update
      handler, and `authorize_tenant`/`can_manage` at 61-80 refusing a
      dataset-restricted principal per D9), the new
      `[auth].dataset_restriction_rollout_complete` config key (`common`
      config struct, defaults → TOML → env precedence per existing
      pattern) read by both handlers to gate a two-or-more-element
      `dataset_ids` before any persistence, `signaldb-api/src/schemas.rs`
      DTOs (`CreateApiKeyRequest`, `UpdateApiKeyRequest` gain
      `dataset_ids: Option<Vec<String>>` and, on the update DTO,
      `clear_dataset_restriction: bool`, and both request DTOs get
      `#[serde(deny_unknown_fields)]` (or an equivalent explicit rejection)
      so a request still sending the removed `dataset_id` field fails
      deserialization naming that field, per D8/proposal's breaking-change
      section, rather than silently ignoring it; `CreateApiKeyResponse`,
      `ApiKeyResponse` gain `dataset_ids` and keep a deprecated
      `dataset_id: Option<String>` derived per D8, 86-142); utoipa
      descriptions marking `dataset_id` deprecated on responses and absent
      from requests; `UPDATE_OPENAPI=1 cargo test -p router
      openapi_spec_is_up_to_date` + `cargo xtask generate` (regenerates the
      Rust SDK and the UI's TypeScript client) — run this regen immediately
      after this task's schema changes, not deferred to the end, so task 3
      starts from a client that already has the shape task 2 introduced

## 3. Router: OAuth consent + tokens

- [ ] 3.1 Failing router tests (`cargo test -p router`):
      `GET /oauth/consent/context` includes each tenant's dataset list;
      `POST /oauth/authorize/decision` accepts `dataset_ids: null`/omitted
      (unrestricted) and a non-empty `dataset_ids` naming only datasets the
      chosen tenant has, and rejects both `dataset_ids: []` and a dataset
      name the chosen tenant doesn't have; the issued authorization code,
      then the access and refresh tokens after exchange, carry the chosen
      set; a decision omitting `dataset_ids` entirely (simulating a client
      built before this change) is accepted and yields an unrestricted
      grant — the `#[serde(default)]` case; refreshing a token preserves its
      dataset set on **both** of the tokens the refresh grant mints — the
      new access token and the new replacement refresh token — by reading
      it from the `oauth_refresh_tokens` row used for the refresh, not from
      any access token (D6) — construct the test so the original access
      token is already gone/expired when refresh happens, so a wrong
      implementation that tries to read it fails loudly instead of
      coincidentally passing, and assert the dataset set on the new refresh
      token explicitly (not just the new access token), since a fix that
      only propagates to the access token would pass a test that checks
      only that token; with `[auth].dataset_restriction_rollout_complete`
      at its default `false`, a consent decision naming *any* non-empty
      `dataset_ids` is rejected with an error naming the config key — the
      stricter OAuth rule, since OAuth has no legacy column at all — while
      omitting `dataset_ids` ("all datasets") is still accepted; the same
      restricted decision succeeds once the config key is set to `true`
- [ ] 3.2 Implement: `ConsentContextResponse`/`ConsentTenant` gain a
      `datasets: Vec<ConsentDataset>` field per tenant (`oauth.rs:605-621`,
      handler 640-715); `ConsentDecision` gains
      `dataset_ids: Option<Vec<String>>` with `#[serde(default)]`
      (`oauth.rs:395-417`), validated against the chosen tenant's datasets
      and rejecting an empty array, and — before that validation — checked
      against `[auth].dataset_restriction_rollout_complete` (same config
      key task 2.2 introduces), rejecting any non-empty selection while
      the key is `false`; `create_authorization_code`/token
      issuance persist the set; the refresh-grant handler reads the set
      from the presented `oauth_refresh_tokens` row and writes it onto both
      the new access token and the new replacement refresh token it mints
      (D6); immediately regenerate the OpenAPI
      spec, Rust SDK, and UI TypeScript client (same command as 2.2) — this
      task changes `#[utoipa::path]`-annotated types a second time and the
      golden `openapi_spec_is_up_to_date` test goes red again until it's
      re-run

## 4. CLI

- [ ] 4.1 Failing test: `admin api-key create acme --name ci --dataset
      production --dataset staging` and `tenant api-key create --dataset
      production --dataset staging` are accepted; `update` accepts the same
      repeated flag to replace a restriction, and a separate
      `--clear-dataset-restriction` flag (with no `--dataset` in the same
      invocation) clears one; passing both `--dataset` and
      `--clear-dataset-restriction` on the same `update` invocation is a
      CLI-level argument error, not merely a server-side rejection; omitting
      both leaves the restriction unchanged
- [ ] 4.2 Implement: `signaldb-cli/src/commands/api_key.rs` — `--dataset`
      becomes a repeatable flag (`Vec<String>`) on `Create`/`Update`, a new
      `--clear-dataset-restriction` boolean flag on `Update` only, both
      wired to the regenerated SDK's `dataset_ids`/`clear_dataset_restriction`
      fields (30-32, 44-46, 82, 104) — critically, `Update` must send
      `dataset_ids: None` (not `Some(vec![])`) when `--dataset` was not
      passed at all, distinguishing "flag absent" from "flag passed with
      zero values" (clap: `Vec` is empty in both cases, so this has to be
      tracked separately, e.g. `Option<Vec<String>>` from
      `ArgAction::Append` with no default); key listing prints the set (or
      "unrestricted")

## 5. MCP

- [ ] 5.1 Failing tests then implement (`cargo test -p mcp-server`):
      `CreateApiKeyParams`, `TenantCreateApiKeyParams` take
      `dataset_ids: Option<Vec<String>>`; `UpdateApiKeyScopesParams`,
      `TenantUpdateApiKeyParams` take both `dataset_ids: Option<Vec<String>>`
      and `clear_dataset_restriction: bool`, and the tool handler rejects
      the combination of a non-empty `dataset_ids` with
      `clear_dataset_restriction: true` before making any router request; a
      mock-router test proves the fields are forwarded correctly; tool
      descriptions mention the set restriction and the clear flag
- [ ] 5.2 Failing tests then implement: `discover_datasets` and
      `tenant_list_tables` filter their dataset/table listing to the
      caller's `api_key_dataset_ids`/OAuth restriction when one is present,
      unchanged when absent (D10); a test with a dataset-restricted
      credential proves an unlisted dataset never appears in either tool's
      result

## 6. UI

- [ ] 6.1 Failing test then implement: the API-key creation/update form's
      dataset selector becomes a multi-select with an explicit "clear
      restriction" control distinct from "select no datasets" (`pnpm
      --filter signaldb-ui test`); the key list shows the set (or
      "unrestricted")
- [ ] 6.2 Failing test then implement: the OAuth consent page shows an
      explicit "all datasets" (default) vs. "only these datasets" choice
      under the selected tenant (D5); the checklist only renders, and the
      submit control is only enabled, in the "only these" state with at
      least one box checked; choosing a different tenant resets the
      dataset choice back to "all datasets" for the newly selected tenant

## 7. Integration + docs

- [ ] 7.1 tests-integration e2e: an API key restricted to `[production]`
      queries `production` successfully and is refused for `staging`; the
      same for an OAuth token issued with a dataset restriction through the
      full DCR flow, including a refresh that preserves the restriction
      after the original access token is discarded; an unrestricted key/token
      (both legacy and newly created with no `dataset_ids`) reaches every
      dataset — confirming no regression for existing credentials; a key
      restricted to two datasets with no `X-Dataset-ID` header is rejected
      rather than silently resolving to the tenant default (D4); a key
      carrying `tenant:manage` and a dataset restriction is refused by the
      management API end-to-end (D9); an OAuth session for a tenant-admin
      user, authorized with a non-empty dataset restriction (`tenant:manage`
      itself is never grantable through OAuth consent — this exercises the
      *role*-based `can_manage` path a tenant-admin session reaches
      management through, which D9 closes the same way), is likewise
      refused by the management API end-to-end, proving D9 covers both the
      scope-carrying API-key case and the role-carrying OAuth case;
      `discover_datasets` for a restricted
      credential never lists a dataset outside its restriction, and
      `tenant_list_tables` for the same restricted credential neither
      exposes nor accepts `staging` (D10 covers both call sites, not only
      `discover_datasets`); `GET /api/v1/whoami` for a credential restricted
      to `[production]` excludes `staging` from its dataset list,
      exercising the same D10 filter on its other named call site;
      an OAuth access token restricted to `[production]` is presented directly
      (bearer token, no MCP involved) against a Tempo/Loki/Prometheus
      compat endpoint with `X-Dataset-ID: staging` and is refused — proving
      the restriction is enforced in the shared `Authenticator` on the
      acceptor/router HTTP path, not only through the MCP tool wrapper
- [ ] 7.2 Docs (route via the docs skill): `docs/users/authentication.md`
      (dataset-set restriction, both API keys and OAuth, the
      omit/`[]`-is-invalid/explicit-clear contract, and the mixed-version
      rollout constraint from D2 — stated per credential type, since they
      differ: an API key is only unsafe mid-rollout once it names *more
      than one* dataset, because a single-element restriction still has a
      legacy `dataset_id` column an old node can fall back to, while an
      OAuth token has no such legacy column at all, so *any* non-empty
      OAuth dataset restriction is unsafe until every authenticating node
      runs the new code);
      `docs/users/mcp.md` (`dataset_ids`/`clear_dataset_restriction` on the
      key-management tools, `discover_datasets`/`tenant_list_tables`
      filtering); update the `multi-tenancy` skill (`Optional dataset the
      key is restricted to` → set semantics); file a follow-up task (not
      part of this change) to drop the deprecated `dataset_id` response
      field (D8) and the legacy `dataset_id` column (D2) once callers have
      migrated
- [ ] 7.3 `cargo fmt`, `cargo clippy --workspace --all-targets
      --all-features`, `cargo machete --with-metadata`; `pnpm --filter
      signaldb-ui lint && test`; `openspec validate
      multi-dataset-key-restriction --type change --strict` (if the
      `openspec` CLI is available in the implementing environment)
