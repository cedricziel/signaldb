## 1. IR stage and validation (query-ir)

- [x] 1.1 Write failing tests in `src/query-ir` for: parsing the `correlate` stage; rejection under IR version 7; rejection on non-`traces` sources, twice in one pipeline, and after `aggregate`; `parent.` field resolution including attribute scopes; unknown `parent.` field error. Verify with `cargo test -p query-ir` (tests fail).
- [x] 1.2 Add the `correlate` stage, bump `MAX_IR_VERSION` to 8, register the feature, extend relation typing and the field resolver with the `parent.` scope. Verify `cargo test -p query-ir` passes.

## 2. Lowering and bounds (querier)

- [x] 2.1 Write failing querier tests over an in-memory traces table: inner join pairs child/parent; left join keeps roots with null parent fields; parent outside window is missing; `aggregate count() by parent.service_name, service_name` gives per-pair counts; row cap produces a truncation warning. Verify with `cargo test -p querier` (tests fail).
- [x] 2.2 Lower `correlate` to a DataFusion hash join with the time-range filter on both scans, add `[querier].correlate_max_rows` (default 5,000,000) and the truncation warning. Verify `cargo test -p querier` passes. `correlate_max_rows` is threaded from config via `IrService::with_correlate_max_rows` (set from `QuerierConfig` at both Flight-service construction sites); `parent.`-scoped attribute-container references (`parent.span.<key>`, `parent.resource.<key>`) are lowered against the parent side the same way the child side lowers them. The truncation warning is ground truth from the querier: `lower_correlate` requests `correlate_max_rows + 1`, eagerly collects and trims the join's own output before any later stage can shrink or hide the row count, and stamps `common::flight::CORRELATE_TRUNCATED_METADATA_KEY` onto the result's Arrow schema metadata — the router reads it back rather than sniffing the document/row-count coincidence.
- [x] 2.3 Add a `tests-integration` test that ingests OTLP traces for two tenants across three services and checks caller/callee pair counts and tenant isolation through `POST /api/v1/query`. Verify it passes.

## 3. API contract and clients

- [x] 3.1 Add the `correlate` stage schema to the OpenAPI document and refresh `api/signaldb-api.json` with `UPDATE_OPENAPI=1 cargo test -p router openapi_spec_is_up_to_date`; verify the test passes without the env var. **No change needed**: `QueryIrRequest.pipeline` is already `Vec<Object>` (opaque JSON) at the OpenAPI boundary — `openapi_spec_is_up_to_date` passes unmodified.
- [x] 3.2 Regenerate the Rust SDK (`src/signaldb-sdk`) and the TypeScript client (`src/ui/src/api/gen`) with `cargo xtask generate`; verify `cargo xtask check` and `pnpm --filter signaldb-ui typecheck` pass. **No change needed** — the OpenAPI contract is unchanged (see 3.1), so there is nothing to regenerate.

## 4. Surfaces

- [x] 4.1 CLI: verify `signaldb-cli query --ir` runs a version-8 `correlate` document end to end, with a CLI test covering it.
- [x] 4.2 MCP: update the `query_ir` tool description and the `query-ir` skill with a `correlate` example; verify the MCP skill snapshot test passes.
- [x] 4.3 UI: no new UI in this change; the `service-map` change consumes the stage.

## 5. Docs and follow-ups

- [x] 5.1 Via the docs skill, add a "Joining spans to their parents (v8)" section to `docs/users/querying-ir.md` with the caller/callee example, and update its Roadmap entry. Verify the docs frontmatter check passes.
- [x] 5.2 Update `openspec/changes/query-cross-signal-correlate/proposal.md` to state it extends the version-8 `correlate` stage. Verify `openspec validate --all` passes.
