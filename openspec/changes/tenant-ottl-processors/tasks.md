## 1. `ottl` crate — parser, compiler, evaluator

- [ ] 1.1 Create `src/ottl` (pest, pest_derive, regex, sha2, thiserror, serde,
      opentelemetry-proto, workspace-hack); add to workspace members and
      default-members; `ottl.workspace = true` alias
- [ ] 1.2 Failing parser tests: every grammar construct in D1 parses to the
      expected AST; unsupported editor/converter, bad arity, unterminated
      string, and unknown path each fail with column + token
- [ ] 1.3 Implement `ottl.pest` grammar + AST + `parse`; make 1.2 pass
- [ ] 1.4 Failing compile tests: per-signal path legality, regex
      precompilation, `Limits` (statement cap, regex length, size_limit),
      errors carry statement index
- [ ] 1.5 Implement `compile`; make 1.4 pass
- [ ] 1.6 Failing evaluator tests over `opentelemetry-proto` requests for
      traces, logs, metrics (every data-point type): each editor and converter,
      `where` semantics (absent key = false), resource/scope paths, ordering,
      `ErrorMode` behaviour, `ApplyReport` counts
- [ ] 1.7 Implement `Context` + `apply_traces/logs/metrics`; make 1.6 pass
- [ ] 1.8 Conformance table `src/ottl/README.md` (supported vs upstream OTTL)

## 2. Catalog, config, scopes (`common`)

- [ ] 2.1 Failing catalog tests (SQLite + Postgres): CRUD on `processors`,
      tenant isolation, unique name, `dataset` (name) must exist,
      `delete_dataset_for_tenant` cascades processor rows on both dialects,
      `select_for_request` ordering (tenant-wide first, priority, name)
- [ ] 2.2 DDL in both `Catalog::init` branches + `impl Catalog` CRUD; make 2.1
      pass
- [ ] 2.3 Failing tests for `[processors]` config (`reload_interval`,
      `test_payload_max_bytes`, `max_statements`, `max_regex_len`) with
      defaults and env override; add section + `signaldb.dist.toml` entry
- [ ] 2.4 Failing tests for `processors:read|write` scopes:
      `can_read_processors`/`can_write_processors`, `READ_SCOPES`,
      `API_KEY_SCOPES`, OAuth default grant includes read and rejects write
- [ ] 2.5 Implement scopes (`READ_SCOPES` → 6, `API_KEY_SCOPES` → 13, router
      `oauth.rs` grant tests get processor twins, doc-comment vocab in
      `signaldb-api/src/schemas.rs` and `mcp-server/src/server.rs`, CLI TUI
      scope hint in `tui/components/admin/api_keys.rs`); make 2.4 pass
- [ ] 2.6 Failing tests for `ProcessorRegistry`: lazy load, empty tenant
      cached, TTL refresh, `invalidate`, invalid rows skipped and reported,
      `for_request` filtering
- [ ] 2.7 Implement `common::processors::ProcessorRegistry`; make 2.6 pass
- [ ] 2.8 Failing test that applying a program records the counters; implement
      self-monitoring counters + `processors.apply` span factory

## 3. Acceptor application

- [ ] 3.1 Failing acceptor tests: trace/log/metric handlers transform before
      WAL append (inspect WAL entry), metric rename partitions by new name,
      `propagate` rejects with `Invalid` and writes nothing, no-processor
      tenant byte-identical
- [ ] 3.2 Add registry to `AcceptorResources`, apply in the three handlers
      (gRPC and HTTP share them); make 3.1 pass
- [ ] 3.3 Integration test in `tests-integration` (reuse the OTLP-HTTP export
      + Query IR helpers from `tests/query_ir_e2e.rs`): create processor via
      API, export, query shows redacted value only

## 4. HTTP API (router)

- [ ] 4.1 Failing router tests: list/create/get/replace/delete, 404/409/422
      with positional errors, 403 for missing scopes / member session, tenant
      scoping, `applies_within_seconds`
- [ ] 4.2 Implement `endpoints/processors.rs` + defaulted
      `RouterState::processor_registry()` with the cached instance on
      `RouterAppState`; write handlers call `invalidate(tenant)`; nest under
      `/api/v1/processors`; PUT on unknown name is 404; make 4.1 pass
- [ ] 4.3 Failing tests for `:validate` and `:test` (inline and stored
      processors, payload cap 413, no WAL write, per-statement counts)
- [ ] 4.4 Implement (router gains an `opentelemetry-proto` dependency; decode
      the JSON payload with `serde_json::from_slice::<Export*Request>` as the
      acceptor HTTP path does); make 4.3 pass
- [ ] 4.5 utoipa paths/schemas in `openapi.rs` and all seven routes in
      `KNOWN_ROUTES` (the drift-guard test); `applies_within_seconds` in the
      write DTOs; `UPDATE_OPENAPI=1 cargo test -p router`; `cargo xtask generate` (Rust SDK + TS client); SDK tests in
      `src/signaldb-sdk/tests/processor_methods.rs`

## 5. CLI

- [ ] 5.1 Failing CLI tests for `processors list|get|validate|test` and `admin
      processors create|replace|delete` (file and flag input)
- [ ] 5.2 Implement `commands/processors.rs`; wire into `commands/mod.rs`; make
      5.1 pass

## 6. MCP

- [ ] 6.1 Failing tests in `src/mcp-server/tests/processor_tools.rs`: seven
      tools listed, tenant scope check, read vs write scope enforcement
- [ ] 6.2 Implement tools; make 6.1 pass

## 7. UI

- [ ] 7.1 Failing component tests: `ProcessorList` renders fields and
      read-only mode; `ProcessorEditor` annotates invalid lines and disables
      Save; test panel renders diff and counts
- [ ] 7.2 Implement `features/processors/` (routes, api.ts over generated
      client, list, editor, test panel, samples per signal), user-menu entry,
      scope-picker entries (`SCOPE_GROUPS` and the scope union in
      `src/ui/src/api/management.ts`); make 7.1 pass; `typecheck`, `lint`,
      `test`

## 8. Docs, skills, benchmark

- [ ] 8.1 `docs/users/processors.md` (how-to, conformance table link, PII and
      URL examples), `mkdocs.yml` entry, config note in
      `docs/operations` per docs skill
- [ ] 8.2 Update `multi-tenancy`, `configuration`, and `crate-map` skills
- [ ] 8.3 Benchmark: 10-statement trace program in the ingest benchmark;
      record result in the docs page
