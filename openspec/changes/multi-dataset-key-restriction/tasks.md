## 1. Storage + shared enforcement (common)

- [ ] 1.1 Failing tests in `common::catalog`: creating an API key with
      `dataset_ids: Some(vec!["a", "b"])` round-trips; `Some(vec![])` is
      rejected at the catalog layer with a clear error; an existing key
      created before this change (legacy `dataset_id` column populated) reads
      back as `dataset_ids: Some(vec![<value>])` after the backfill; both
      SQLite and Postgres branches (`cargo test -p common`, plus the Postgres
      testcontainer suite)
- [ ] 1.2 Implement: rename/add the `dataset_ids` column on `api_keys`
      (SQLite `catalog.rs:198-232`, Postgres `525-546`), following the
      `scopes` JSON-array-in-TEXT pattern (D1/D2); backfill existing
      `dataset_id` values; update `ApiKeyRecord`/`ApiKeyAuthRecord`,
      `upsert_scoped_api_key`, `update_api_key_scopes`, `validate_api_key`
- [ ] 1.3 Failing tests: `dataset_allowed(None, "x")` is true;
      `dataset_allowed(Some(&["a","b"]), "a")` is true,
      `dataset_allowed(Some(&["a","b"]), "c")` is false (`cargo test -p
      common`)
- [ ] 1.4 Implement `dataset_allowed` in `common::auth` (D3); replace the
      inline check in `Authenticator::authenticate_from_database`
      (`authenticator.rs:391-397`); `TenantContext.api_key_dataset_id` →
      `api_key_dataset_ids: Option<Vec<String>>`, `with_api_key_restrictions`
      signature updated, every call site fixed
- [ ] 1.5 Failing tests: an OAuth-authenticated request whose token carries a
      dataset restriction is denied for a dataset outside it and allowed for
      one inside it; a token with no restriction (including every token
      issued before this change) reaches every dataset (`cargo test -p
      common`)
- [ ] 1.6 Implement: add `dataset_ids` column to `oauth_authorization_codes`,
      `oauth_access_tokens`, `oauth_refresh_tokens` (SQLite `catalog.rs:
      406-482`, Postgres `710-782`); thread through
      `create_authorization_code`/`create_access_token`/
      `create_refresh_token`; call `dataset_allowed` from
      `authenticate_oauth_token` (D3)

## 2. Router: admin + management APIs

- [ ] 2.1 Failing router tests (`cargo test -p router`): create/update an API
      key via the admin API with `dataset_ids: ["a", "b"]`; the key
      authenticates against dataset `a` and `b` but is refused for `c`;
      `dataset_ids: []` is rejected with a validation error; omitting
      `dataset_ids` creates an unrestricted key; same set of cases through
      the management API
- [ ] 2.2 Implement: `endpoints/admin.rs` (586-620 create, 656-737 update,
      752-761 response mapping), `endpoints/management.rs` (403-433 create
      DTOs, 509-536 create handler, 573-580 update DTO, 603-668 update
      handler), `signaldb-api/src/schemas.rs` DTOs (`CreateApiKeyRequest`,
      `UpdateApiKeyRequest`, `CreateApiKeyResponse`, `ApiKeyResponse`,
      86-142); utoipa descriptions; `UPDATE_OPENAPI=1 cargo test -p router
      openapi_spec_is_up_to_date` + `cargo xtask generate` (regenerates the
      Rust SDK and the UI's TypeScript client)

## 3. Router: OAuth consent + tokens

- [ ] 3.1 Failing router tests (`cargo test -p router`):
      `GET /oauth/consent/context` includes each tenant's dataset list;
      `POST /oauth/authorize/decision` accepts `dataset_ids` and rejects one
      naming a dataset the chosen tenant doesn't have; the issued
      authorization code, then the access and refresh tokens after exchange,
      carry the chosen set; omitting `dataset_ids` (or sending an empty
      array, meaning "leave every box unchecked") yields an unrestricted
      token, matching today's behavior; refreshing a token preserves its
      dataset set unchanged
- [ ] 3.2 Implement: `ConsentContextResponse`/`ConsentTenant` gain a
      `datasets: Vec<ConsentDataset>` field per tenant (`oauth.rs:605-621`,
      handler 640-715); `ConsentDecision` gains `dataset_ids: Vec<String>`
      (`oauth.rs:395-417`) validated against the chosen tenant's datasets;
      `create_authorization_code`/token issuance/refresh (D6) persist and
      propagate the set

## 4. CLI

- [ ] 4.1 Failing test: `admin api-key create acme --name ci --dataset
      production --dataset staging` and `tenant api-key create --dataset
      production --dataset staging` are accepted; `update` accepts the same
      repeated flag
- [ ] 4.2 Implement: `signaldb-cli/src/commands/api_key.rs` — `--dataset`
      becomes a repeatable flag (`Vec<String>`) on `Create`/`Update`, wired
      to the regenerated SDK's `dataset_ids` field (30-32, 44-46, 82, 104);
      key listing prints the set

## 5. MCP

- [ ] 5.1 Failing tests then implement (`cargo test -p mcp-server`):
      `CreateApiKeyParams`, `UpdateApiKeyScopesParams`,
      `TenantCreateApiKeyParams`, `TenantUpdateApiKeyParams` take
      `dataset_ids: Option<Vec<String>>`; a mock-router test proves the field
      is forwarded as a JSON array; tool descriptions mention the set
      restriction

## 6. UI

- [ ] 6.1 Failing test then implement: the API-key creation/update form's
      dataset selector becomes a multi-select (`pnpm --filter signaldb-ui
      test`); the key list shows the set (or "unrestricted")
- [ ] 6.2 Failing test then implement: the OAuth consent page shows a
      dataset checklist under the selected tenant (D5 — starts fully
      unchecked), submits the chosen set with the decision

## 7. Integration + docs

- [ ] 7.1 tests-integration e2e: an API key restricted to `[production]`
      queries `production` successfully and is refused for `staging`; the
      same for an OAuth token issued with a dataset restriction through the
      full DCR flow; an unrestricted key/token (both legacy and newly
      created with no `dataset_ids`) reaches every dataset — confirming no
      regression for existing credentials
- [ ] 7.2 Docs (route via the docs skill): `docs/users/authentication.md`
      (dataset-set restriction, both API keys and OAuth), `docs/users/mcp.md`
      (`dataset_ids` on the key-management tools); update the
      `multi-tenancy` skill (`Optional dataset the key is restricted to` →
      set semantics)
- [ ] 7.3 `cargo fmt`, `cargo clippy --workspace --all-targets
      --all-features`, `cargo machete --with-metadata`; `pnpm --filter
      signaldb-ui lint && test`; `openspec validate
      multi-dataset-key-restriction --type change --strict` (if the
      `openspec` CLI is available in the implementing environment)
