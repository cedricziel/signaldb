## 1. Phase 0 — SDK query keystone

- [x] 1.1 Annotate the existing PromQL/LogQL/TraceQL compat endpoints in the
      router into the code-first OpenAPI surface (request params + a loose
      response schema; no behavior/response-shape change)
- [x] 1.2 Run `cargo xtask generate`; verify `signaldb-sdk` gains
      `query_promql`/`query_logql`/`query_traceql` and the TS client in
      `src/ui/src/api/gen` regenerates
- [x] 1.3 Write failing tests in `signaldb-sdk` for a hand-written `query_sql`
      method that sends SQL over the router Flight transport and returns Arrow
      rows (`cargo test -p signaldb-sdk`)
- [x] 1.4 Implement the hand-written Flight `query_sql` module (SQL ticket, Arrow
      `RecordBatch` stream decode) using the workspace DataFusion/`arrow-flight`
      re-exports; keep `generated.rs` untouched
- [x] 1.5 Unify exposure in `lib.rs` (single `Client` over the generated HTTP
      client + the hand-written `query_sql`); document the boundary
- [x] 1.6 Add a `signaldb-sdk`/`tests-integration` test running each of the four
      query methods against a live router and asserting native shapes (rows for
      SQL, native JSON for the three)

## 2. Phase 1 — CLI as a pure SDK consumer

- [x] 2.1 Write failing test asserting the CLI crate contains no direct
      `FlightServiceClient`/raw-HTTP construction against a SignalDB service
      (`cargo test -p signaldb-cli`)
- [x] 2.2 Write failing tests for the single `query` command: a required,
      mutually-exclusive language flag (`--sql`/`--promql`/`--logql`/
      `--traceql`); missing/duplicate flag → usage error, non-zero exit
- [x] 2.3 Write failing tests for native per-language output: `--sql` gives
      `table`/`csv`/`ndjson` rows; `--promql`/`--logql`/`--traceql` pass through
      the native Prometheus/Loki/Tempo JSON
- [x] 2.4 Write failing tests for deterministic exit codes (success `0`,
      empty-result `0`, error non-zero with diagnostics on stderr)
- [x] 2.5 Implement `query --<lang>` dispatch through the SDK; replace
      `commands/query.rs` Flight usage with SDK calls
- [x] 2.6 Replace `tui/client/flight.rs` Flight usage with SDK query calls
- [x] 2.7 Re-taxonomize the command tree: move top-level `tenant`/`api-key`/
      `dataset` under `admin`; keep `tui`/`completions`/user bootstrap
- [x] 2.8 Write failing test for endpoint/credential resolution precedence
      (flag > env > config); implement resolution

## 3. Phase 1 — MCP query parity

- [x] 3.1 Write failing tests for MCP query tools (one per language) returning the
      SDK's native shapes (`cargo test -p mcp-server`)
- [x] 3.2 Implement the MCP query tools over the SDK query methods
- [x] 3.3 Write failing test that the same query in the same language via MCP and
      via the CLI yield equivalent data (`cargo test -p tests-integration`)

## 4. Phase 1 — Parity enforcement

- [x] 4.1 Write the failing three-way parity test in `tests-integration`:
      enumerate the SDK public capability surface, assert each has a CLI
      verb/flag and an MCP tool (covering query + admin)
- [x] 4.2 Make the parity test pass; wire it into CI

## 5. Phase 2 — Operational control API (router proxy)

- [x] 5.1 Integration tests for `/api/v1/ops/*` (compaction): admin-auth
      rejection and 503 when no compactor is registered (`tests-integration`
      `ops_endpoints`)
- [x] 5.2 Implement the `/api/v1/ops/compact{,/status,/dry-run}` handlers
      forwarding to the compactor's Flight `do_action` surface
- [x] 5.3 Annotate the ops endpoints in the code-first OpenAPI surface
- [x] 5.4 Run `cargo xtask generate`; verify `ops_compact*` methods appear in
      `signaldb-sdk` and the TypeScript client in `src/ui/src/api/gen`
- [x] 5.5 Ops-proxy behavior covered by `ops_endpoints`; the compactor's own
      `do_action` semantics are covered by the `compactor` crate. (Retention/
      snapshot/orphan control is deferred — needs compactor `do_action`
      commands.)

## 6. Phase 2 — CLI + MCP ops parity

- [x] 6.1 Write failing tests for CLI `ops <verb>` verbs over the SDK
      (`cargo test -p signaldb-cli`)
- [x] 6.2 Implement CLI `ops` verbs
- [x] 6.3 Write failing test for the MCP ops tool(s) (`cargo test -p mcp-server`)
- [x] 6.4 Implement MCP ops tool(s)
- [x] 6.5 Extend the parity test (4.1) to cover ops; keep CI green

## 7. Docs & skills

- [x] 7.1 Update `docs/architecture/overview.md` and `docs/users/
authentication.md` for the new CLI taxonomy and `query`/`ops` verbs
      (route via the docs skill)
- [x] 7.2 Update the `multi-tenancy` skill's CLI references to the `admin ...`
      command form
- [x] 7.3 Add a CLI changelog entry noting the BREAKING top-level → `admin` move

## 8. Validation

- [x] 8.1 `cargo fmt`, `cargo clippy --workspace --all-targets --all-features`,
      `cargo machete --with-metadata`
- [x] 8.2 `openspec validate sdk-parity-cli-mcp --strict`
