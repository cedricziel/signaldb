## 1. Querier discovery

- [x] 1.1 Failing tests (`cargo test -p querier`) on an in-memory traces table (map-attribute and JSON-attribute fixtures): `get_tags` returns resource keys, span keys, and intrinsics, scoped; window excludes older spans; empty table → intrinsics only; `get_tag_values` for a map attribute, a dedicated column (`service.name`), intrinsics `status`/`kind` enums, unknown tag → empty; values sorted and bounded by the sample
- [x] 1.2 Implement `get_tags` / `get_tag_values` reusing `time_window`, `attr_context_of`, `attr_documents`, `LABEL_SCAN_LIMIT` (lift shared helpers to `query/attrs.rs` if needed)
- [x] 1.3 Failing tests then implement Flight tickets `trace_tags:` and `trace_tag_values:` (`TicketRequest`, parsing, dispatch, query-stage span like `label_names`)

## 2. Router Tempo endpoints

- [x] 2.1 Failing router tests: `/tempo/api/search/tags` and `/api/v2/search/tags?scope=` return querier-backed names (mock Flight); `/tag/{tag}/values` for a map attribute returns values; unknown tag → `200 []`; scoped names resolve; `start`/`end` forwarded; default window is 1 h
- [x] 2.2 Implement: `search_tags`/`search_tags_v2` take state + tenant + params and call the ticket; `tag_values_for` calls the ticket for every tag; delete `RESOURCE_TAGS`, `INTRINSIC_TAGS`, the `501` arm, and `distinct_column_values`/`distinct_values_sql` if now unused; utoipa: drop `501`, document params
- [x] 2.3 `cargo xtask generate` — regenerate OpenAPI, SDK, TS client; golden test green

## 3. Surface parity

- [x] 3.1 tests-integration e2e: ingest spans with `deployment.environment.name` (resource) and `http.route` (span); assert both appear via `/api/search/tags` and v2 scopes, values via both value endpoints. Deviation: MCP `discover_attributes` and the `signaldb discover attributes` CLI command are not separately re-tested here — both call the same Tempo tag endpoints through the generated SDK with zero code changes (verified: `cargo build -p signaldb-cli`/`-p mcp-server` green against the regenerated SDK), and MCP is another in-flight change's area per the conflict note.
- [x] 3.2 UI: failing component test then implement traces-tab filter-key suggestions from `searchTags` merged with registry hits (`mergeLabelSuggestions`). Deviation: wired into the traces tab's "group by attribute" free-text input (the one place that already accepted an arbitrary attribute key), not a new filter-add control — `FACET_FIELDS`/`compileTraceQL` deliberately curate which fields are filterable and drop the rest (tested), so a value-suggestion step (`searchTagValues` on key selection) has no filter-with-value input to attach to; not added.
- [ ] 3.3 Grafana smoke (manual, documented in the PR): TraceQL autocomplete on the built-in Tempo datasource lists the custom keys — not performed, no live Grafana instance in this environment.

## 4. Docs, skills, hygiene

- [x] 4.1 Docs (route via the docs skill): `docs/users/tempo-api-reference.md` tags endpoints — real data, window default, scope, sampling caveat; MCP/CLI discovery docs lose the "traces are hardcoded" caveat (mcp.md already described `traces` as the default signal with no stale caveat to remove)
- [x] 4.2 Update the `tempo-api` skill (implemented endpoints table: tags no longer stub/501)
- [x] 4.3 `cargo fmt`, clippy, machete; `pnpm --filter signaldb-ui lint && test`; `openspec validate tempo-tag-discovery --type change --strict`; close #1073
