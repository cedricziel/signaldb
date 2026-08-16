## 1. Router: typed contract

- [ ] 1.1 Failing router tests: the OpenAPI document declares `pyroscope_render`, `pyroscope_render_diff`, `pyroscope_label_names`, `pyroscope_label_values`, `pyroscope_profile_types`, `profiles_by_trace`; the route drift guard passes with the six routes in `KNOWN_ROUTES` and off `ALLOWLISTED_ROUTES`; each handler's JSON output equals the serialization of its new typed response struct for a fixture (`cargo test -p router`)
- [ ] 1.2 Implement: `IntoParams` on `RenderParams`/`DiscoveryParams`, typed response structs with `ToSchema`, `utoipa::path` on the six handlers (tag `profiles`, 401/403/429), register in `openapi.rs`
- [ ] 1.3 `UPDATE_OPENAPI=1 cargo test -p router openapi_spec_is_up_to_date`; `cargo xtask generate`; commit generated files

## 2. CLI

- [ ] 2.1 Failing clap-tree test: `profiles {types,labels,label-values,render,diff,by-trace}` exist with the flags from the spec
- [ ] 2.2 Implement `commands/profiles.rs` over the SDK; native JSON output; unit tests for argument mapping

## 3. MCP

- [ ] 3.1 Failing tests (mock router): `discover_profile_types`, `search_profiles` (payload cap + truncated), `compare_profiles`, `profiles_for_trace` return SDK shapes; `discover_attributes(signal="profiles")` with/without `tag`; annotations read-only
- [ ] 3.2 Implement the tools and the `profiles` signal

## 4. UI

- [ ] 4.1 Failing test then implement: `src/ui/src/api/pyroscope.ts` uses the generated Pyroscope operations (no raw `fetch`/`retryingFetch` call remains in it, or the module is removed and callers use the generated client); existing profile view tests green

## 5. Parity + e2e

- [ ] 5.1 Map the six operations in `tests-integration/tests/query_parity.rs` (CLI + MCP); the check is green with no new exclusions
- [ ] 5.2 tests-integration e2e: ingest a CPU profile; `signaldb profiles types` and the MCP `discover_profile_types` tool both list it; `profiles render` returns a flame graph

## 6. Docs, skills, hygiene

- [ ] 6.1 Docs (route via the docs skill): `docs/users/profiles.md` (contract-backed API table; CLI and MCP usage), `docs/users/mcp.md` tool catalogue, `docs/architecture/openapi-codegen.md` if it lists the allowlist
- [ ] 6.2 Update the `tempo-api` skill (implemented endpoints table) and `multi-tenancy`/`dev-workflow` if they list CLI groups
- [ ] 6.3 `cargo fmt`, clippy on touched crates, `cargo machete --with-metadata`; UI lint/test; `openspec validate pyroscope-openapi-parity --type change --strict`
