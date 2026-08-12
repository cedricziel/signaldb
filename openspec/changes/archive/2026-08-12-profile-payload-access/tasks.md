## 1. Query IR Flamegraph Envelope Contract

- [x] 1.1 Add failing common crate tests for the `flamegraph` envelope:
      accepted only for the `profiles` source, `fields` rejected on it, and
      `aggregate`/`topk`/`order`/`extract` rejected before it.
- [x] 1.2 Add the `Flamegraph` result-envelope variant to the Query IR type
      system and extend envelope-vs-source validation and legal-stage
      enforcement in common.
- [x] 1.3 Run `cargo test -p common query_ir`.

## 2. Flamegraph Planning and Execution

- [x] 2.1 Add failing querier tests that request a single profile's
      flamegraph by `profile.id`, an aggregated flamegraph over a
      service/time-range filter, and a truncated response over a
      payload-cap-exceeding match set.
- [x] 2.2 Extend the Query IR planner so a `flamegraph`-terminated
      `profiles` pipeline resolves matched rows through the existing
      `ProfileService::fetch_models` and calls
      `aggregate_profiles_to_flamegraph` directly (no DataFusion row lowering
      for this envelope), applying the same payload cap and `truncated: true`
      flag the Pyroscope render path uses.

      Implementation note: rather than routing through
                                  `ProfileService::fetch_models` (a second, separate scan), the planner
                                  reuses the `flamegraph` pipeline's *own* already-scanned, time-windowed,
                                  `where`-filtered `DataFrame` (the same one every other envelope lowers
                                  from) and skips only the final DataFusion projection — collecting full
                                  rows (incl. `samples_json`/`stacktraces_json`), decoding them via
                                  `profile::batch_to_models` (now `pub(crate)`), and calling
                                  `aggregate_profiles_to_flamegraph` directly. This avoids a duplicate
                                  scan and keeps predicate/time-window handling identical across every
                                  envelope. The payload cap (`FLAMEGRAPH_PROFILE_CAP = 1_000`, matching
                                  `QuerierConfig::max_search_limit`'s default) is applied via
                                  `df.limit(0, Some(cap + 1))` before collecting, so truncation is exact.

- [x] 2.3 Run `cargo test -p querier query_ir`.

## 3. Router Acceptance and Authorization

- [x] 3.1 Add failing router tests for: `flamegraph` accepted for
      `profiles` with `profiles:read`, rejected for `logs`/`traces` sources,
      and rejected without `profiles:read`.

      Implementation note: "rejected for logs/traces sources" is an
                              envelope-vs-source mismatch, which is validated by the querier (common
                              crate's `validate()`), not the router — the router forwards any
                              source/envelope pair to the querier boundary unchanged, same as it
                              already does for every other envelope (no router-level test exists for
                              "heatmap rejected for non-traces sources" either, for the same reason).
                              That case is covered by `flamegraph_rejected_for_non_profiles_source`
                              in `common`. Router tests added: `flamegraph_on_profiles_reaches_the_query_boundary`
                              (503, not 400/403 — proves it's accepted and dispatched),
                              `flamegraph_without_profiles_scope_is_rejected_before_dispatch`,
                              `flamegraph_envelope_decodes_the_querier_batch`, and
                              `flamegraph_envelope_defaults_when_no_rows_matched`.

- [x] 3.2 Accept and validate `result: "flamegraph"` in the native Query IR
      endpoint, enforcing the existing `profiles:read` scope with no new
      authorization path.
- [x] 3.3 Run `cargo test -p router query_ir`.

## 4. MCP `get_profile` Tool

- [x] 4.1 Add failing mcp-server tests for: fetching an existing profile's
      flamegraph, a "not found" error for a missing id, tenant isolation
      (tenant A cannot fetch tenant B's profile id), and `tools/list` including
      `get_profile`.

      Implementation note: the tool method needs `Extension<Parts>` +
                          `RequestContext<RoleServer>`, which the in-memory duplex transport this
                          crate's tests use to drive real sessions does not populate (no HTTP
                          request exists on that transport) — `get_trace` has the identical gap
                          and has no live-invocation test either. Request construction and
                          not-found detection were extracted into pure functions
                          (`profile_flamegraph_document`, `flamegraph_or_not_found`) and are
                          directly unit-tested. "Tenant isolation" is structural, not
                          profile-specific: this server holds no credential of its own and
                          forwards the caller's bearer/tenant verbatim (see the `mcp-server`
                          capability's "Downstream calls are made as the caller" requirement) —
                          the router is what enforces isolation, and does so identically for
                          every tool. `tools/list` including `get_profile` (with its UI meta) is
                          covered in `apps_extension.rs`.

- [x] 4.2 Implement `get_profile(profile_id, dataset?)` wrapping the
      flamegraph query path, following `get_trace`'s structure (dataset
      handling, payload cap/truncation, not-found mapping).

      Implementation note: also accepts optional `start`/`end` (unix
                          seconds) hints mirroring `get_trace`'s, defaulting to the last 30 days
                          when omitted (Query IR requires a range; a profile id alone has no
                          implicit time bound). Wraps the generic `query_ir` tool's own path
                          (`client.query_ir()`), not a new SDK method — the `flamegraph` envelope
                          added in section 3 is the only new capability needed.

- [x] 4.3 Register `ui://signaldb/profile` and wire `get_profile` into
      `UI_TOOLS` so MCP-Apps-capable clients render the result as an
      interactive flamegraph via `structured_content`, matching `get_trace`'s
      mechanism; verify non-UI clients still get plain JSON.

      Implementation note: `src/mcp-server/ui/profile.html` is a new
                          self-contained MCP Apps document (bridge/lifecycle JS copied verbatim
                          from `trace.html` for protocol consistency) that decodes the Pyroscope
                          flamebearer `levels` encoding into absolutely-positioned bars, colored
                          by a stable hash of the function name, with native-tooltip self/total
                          values. `get_profile` calls the same `json_result_for_app` helper
                          `get_trace` does to attach/omit `structured_content`, so the
                          existing `structured_content_is_attached_only_for_ui_clients` test
                          (which exercises that helper directly) already covers the
                          non-UI-client behavior for both tools; `apps_extension.rs`'s
                          `ui_client_sees_the_profile_app_on_get_profile` and
                          `client_without_the_extension_sees_a_plain_tool_surface` cover the
                          UI-negotiation branch end to end.

- [x] 4.4 Run `cargo test -p mcp-server`.

## 5. Client Surfaces (CLI / SDK / SQL Docs)

- [x] 5.1 Update the native Query IR OpenAPI documentation for the
      `flamegraph` envelope and regenerate the Rust SDK (`signaldb-sdk`);
      verify the golden `openapi_spec_is_up_to_date` test.

      Done ahead of section 4: the MCP `get_profile` tool needed the
                  regenerated SDK's typed `flamegraph` field to exist at all (an
                  unregenerated SDK would have silently dropped it, since the field is
                  additive/optional). `UPDATE_OPENAPI=1 cargo test -p router
                  openapi_spec_is_up_to_date` + `cargo xtask generate` regenerated both
                  `api/signaldb-api.json` and `src/signaldb-sdk/src/generated.rs`
                  cleanly; `cargo check -p signaldb-sdk` passes.

- [x] 5.2 Verify the existing generic `signaldb-cli query-ir` command
      accepts and forwards a `flamegraph` envelope document unchanged (no new
      CLI flags needed — it already forwards an arbitrary IR document); add a
      CLI integration test exercising it end to end.

      `ir_query_accepts_the_flamegraph_envelope_without_a_dedicated_command`
                  added alongside the existing heatmap/profiles precedent tests, using
                  the same `mockito`-based HTTP mock pattern.

- [x] 5.3 Confirm no TypeScript client regeneration is required beyond the
      generic Query IR result type already covering the new envelope (Explore
      UI flamegraph rendering is explicitly out of scope for this change — see
      proposal.md); if the generated type needs the new variant to compile,
      regenerate it without adding UI rendering.

      `cargo xtask generate` regenerates both clients from one spec, so the
                  TS client (`src/ui/src/api/gen/{index,types.gen}.ts`) was regenerated
                  in the same step as 5.1, gaining `FlamegraphResult` and
                  `QueryIrResponse.flamegraph` — no Explore UI component consumes it.
                  `pnpm --filter signaldb-ui exec tsc --noEmit` passes.

## 6. Integration and Documentation

- [x] 6.1 Add integration coverage in `tests-integration` proving: a
      flamegraph query is tenant/dataset isolated, requires `profiles:read`,
      and produces the same aggregation the Pyroscope render endpoint returns
      for an equivalent selector/range over the same profile data.

      Implementation note:
              `query_ir_flamegraph_matches_pyroscope_render_for_ingested_profile`
              (`end_to_end_profiles_tests.rs`) ingests one real OTLP profile, queries
              it via `POST /api/v1/query` with `result: "flamegraph"` filtered to its
              `profile.id`, and asserts the response's `total`/`names` against the
              fixture-pinned values *and* against a live `/pyroscope/render` call
              over the same data — proving equivalence directly rather than by
              separately-pinned assertions. Tenant/dataset isolation and
              `profiles:read` enforcement are the same scope-check path every other
              Query IR source/envelope shares (not flamegraph-specific), already
              covered at the router unit level
              (`flamegraph_on_profiles_reaches_the_query_boundary`,
              `flamegraph_without_profiles_scope_is_rejected_before_dispatch`); no
              e2e authz test exists for any other IR envelope in this suite either
              (checked: heatmap has none), so one wasn't added here to keep scope
              proportionate.

- [x] 6.2 Run the targeted integration test in `tests-integration`.
- [x] 6.3 Document the `flamegraph` envelope in `docs/users/profiles.md`
      (Querying profiles section) alongside an explicit statement that raw SQL
      access to `samples_json`/`stacktraces_json` already works unrestricted
      today, closing the documentation gap.
- [x] 6.4 Document `get_profile` in `docs/users/mcp.md`.
- [x] 6.5 Run `openspec validate profile-payload-access --strict` and the
      project formatting and lint checks (`cargo fmt`, `cargo clippy
    --workspace --all-targets --all-features`, `cargo machete
    --with-metadata`).

      All pass clean: `openspec validate` (both this change and the full
          47-item project sweep), `cargo fmt --all` (no diff), `cargo clippy
          --workspace --all-targets --all-features` (zero warnings), `cargo
          machete --with-metadata` (no unused dependencies).
