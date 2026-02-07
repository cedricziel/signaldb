# SignalDB Terminal UI

## TL;DR

> **Quick Summary**: Build an all-in-one terminal UI for SignalDB by extending `signaldb-cli` with a `tui` subcommand. The TUI connects via Flight SQL for all data access (traces, logs, metrics, system status) and HTTP admin API for tenant management. Prerequisite: implement self-monitoring where services use the OpenTelemetry SDK to emit their own telemetry via OTLP to themselves (dogfooding), processed through the normal Acceptor → WAL → Writer → Iceberg pipeline under a dedicated `_system` tenant.
> 
> **Deliverables**:
> - Self-monitoring infrastructure: services emit WAL/pool/health/resource metrics into SignalDB
> - TUI framework: ratatui + crossterm integrated into signaldb-cli
> - 5 tabs: Dashboard, Traces, Logs, Metrics, Admin
> - Permission-aware UI: admin vs tenant key auto-detection
> - TDD test suite with snapshot testing
> 
> **Estimated Effort**: XL
> **Parallel Execution**: YES - 3 waves
> **Critical Path**: Self-monitoring → TUI skeleton → Tab implementations → Integration

---

## Context

### Original Request
Design a terminal UI for SignalDB that serves SREs/operators, developers, and end users with permission-aware feature visibility.

### Interview Summary
**Key Discussions**:
- **Purpose**: All-in-one TUI — operations dashboard + query explorer + admin console
- **Users**: SREs monitoring clusters, developers debugging locally, end users querying data
- **Location**: Extend `signaldb-cli` crate with `tui` subcommand (not a new crate)
- **Data Access**: Flight SQL as universal interface — even for system status data
- **Self-Monitoring via OTLP (dogfooding)**: Services use OpenTelemetry SDK to emit metrics/logs/traces about themselves via OTLP to the Acceptor. Processed through normal write path under a `_system` tenant. Zero new write-path code needed.
- **Framework**: ratatui + crossterm, tab-based navigation, terminal-native 16 ANSI colors
- **Auth**: API key from signaldb.toml or env var, auto-detect admin vs tenant permission level
- **Admin**: Full CRUD for tenants, datasets, API keys via HTTP admin API
- **Dashboard**: 5 panels — Service Health, WAL Status, Pool Stats, Tenant Overview, System Resources
- **Viewers**: Trace waterfall, log browsing (Flight SQL), metric sparklines, JSON detail pane
- **Refresh**: Configurable, default 5s
- **Tests**: TDD approach with ratatui TestBackend + insta snapshots
- **Deferred to V2**: Plugin system, multi-cluster, remote TUI support
- **Out of scope**: Mouse support, export/save to files

**Research Findings**:
- signaldb-cli has clap 4.5, reqwest HTTP client, Flight client, progenitor SDK — all reusable
- No TUI dependencies or test infrastructure exist in signaldb-cli today
- ratatui Component Pattern recommended for multi-tab apps (over TEA)
- Async pattern: `tokio::select!` with tick/render/crossterm event streams
- `tui-tree-widget` for JSON viewer, `Canvas` widget for trace waterfall
- `insta` crate for snapshot testing of rendered TUI widgets
- Internal data (WAL stats, pool stats) exists in code but no HTTP endpoints — self-monitoring via OTLP solves this
- `signal-producer` crate already demonstrates the full OTel SDK → OTLP pattern (traces, logs, metrics)
- OTel crates already in workspace: `opentelemetry 0.31.0`, `opentelemetry_sdk 0.31.0`, `opentelemetry-otlp 0.31.0`
- Self-monitoring approach: services are OTLP clients to themselves, data flows through normal Acceptor → WAL → Writer → Iceberg pipeline

### Metis Review
**Identified Gaps** (addressed):
- Dashboard panels need APIs that don't exist → Resolved: self-monitoring via OTLP dogfooding + Flight SQL queries
- Metrics/Logs API stubs → Resolved: Flight SQL for logs, metrics via self-monitoring
- Plugin/multi-cluster scope creep → Resolved: deferred to V2
- Crate bloat from TUI deps → Resolved: user chose always-included (no feature gate)
- Permission detection flow → Resolved: try admin endpoint, fall back to tenant mode
- TraceQL search bar → Resolved: use Tempo search API params, not TraceQL parsing

---

## Work Objectives

### Core Objective
Enable interactive terminal-based monitoring, querying, and administration of SignalDB through a unified TUI that leverages self-monitoring and Flight SQL for all data access.

### Concrete Deliverables
- Self-monitoring module in `src/common/` that instruments services via OpenTelemetry SDK, exporting metrics/logs via OTLP to the Acceptor under a `_system` tenant
- `signaldb-cli tui` subcommand launching a full-screen ratatui application
- 5 tabs: Dashboard, Traces, Logs, Metrics, Admin
- Permission-aware view system (admin features gated behind key type detection)
- TDD test suite covering state management, rendering, and API client layer

### Definition of Done
- [ ] `cargo build -p signaldb-cli` succeeds with TUI code included
- [ ] `cargo test -p signaldb-cli` passes all TUI tests
- [ ] `cargo clippy -p signaldb-cli --all-targets -- -D warnings` is clean
- [ ] `cargo machete --with-metadata` shows no unused dependencies
- [ ] `signaldb-cli tui --help` displays usage information
- [ ] TUI launches, shows tab bar, connects to configured SignalDB instance
- [ ] All 5 tabs render data from Flight SQL / admin API
- [ ] Permission detection auto-hides admin tab for non-admin keys

### Must Have
- Self-monitoring: services emit WAL/pool/health/resource metrics via OTLP to themselves (dogfooding), stored under `_system` tenant
- Flight SQL client in TUI for all data queries (traces, logs, metrics, system status)
- HTTP admin API client in TUI for tenant/dataset/key CRUD
- Tab-based navigation with keyboard shortcuts
- Configurable refresh rate (default 5s)
- Graceful degradation when services are unreachable
- Confirmation dialogs for destructive admin operations (delete tenant, revoke key)
- Terminal-native 16 ANSI colors

### Must NOT Have (Guardrails)
- No mouse support — keyboard-only navigation
- No export/save query results to files
- No plugin/extension system (V2)
- No multi-cluster view (V2)
- No remote TUI support (V2)
- No live log tailing/streaming (V1 uses Flight SQL browse)
- No TraceQL parser in TUI — use Tempo search API parameters
- No `color-eyre` — project uses `anyhow` for error handling, stay consistent
- No `sysinfo` in TUI crate — system resources come from self-monitoring via Flight SQL
- No changes to existing CLI command behavior — `tui` subcommand is purely additive
- No interactive prompts for auth — read from config/env only

---

## Verification Strategy

> **UNIVERSAL RULE: ZERO HUMAN INTERVENTION**
>
> ALL tasks in this plan MUST be verifiable WITHOUT any human action.

### Test Decision
- **Infrastructure exists**: NO (signaldb-cli has zero dev-dependencies and zero tests today)
- **Automated tests**: YES (TDD — red-green-refactor)
- **Framework**: Standard `#[tokio::test]` + `ratatui::backend::TestBackend` + `insta` for snapshots

### TDD Workflow

Each TODO follows RED-GREEN-REFACTOR:

**Task Structure:**
1. **RED**: Write failing test first
   - Test file location as specified per task
   - Test command: `cargo test -p signaldb-cli`
   - Expected: FAIL (test exists, implementation doesn't)
2. **GREEN**: Implement minimum code to pass
   - Command: `cargo test -p signaldb-cli`
   - Expected: PASS
3. **REFACTOR**: Clean up while keeping green
   - Command: `cargo test -p signaldb-cli`
   - Expected: PASS (still)

### Agent-Executed QA Scenarios (MANDATORY — ALL tasks)

**Verification Tool by Deliverable Type:**

| Type | Tool | How Agent Verifies |
|------|------|-------------------|
| Rust library code | Bash (cargo test) | Run tests, check compilation, clippy |
| TUI rendering | Bash (cargo test) | Snapshot tests via TestBackend + insta |
| TUI interaction | interactive_bash (tmux) | Launch TUI, send keystrokes, validate output |
| API integration | Bash (cargo test) | Mock-based tests for HTTP and Flight clients |
| Self-monitoring | Bash (cargo test + Flight SQL query) | Verify metrics appear in tables |

---

## Execution Strategy

### Parallel Execution Waves

```
Wave 1 (Start Immediately):
├── Task 1: Self-monitoring infrastructure (common crate)
├── Task 2: TUI dependencies & project scaffolding
└── Task 3: Test infrastructure setup

Wave 2 (After Wave 1):
├── Task 4: TUI event loop & terminal management
├── Task 5: Flight SQL client for TUI
├── Task 6: HTTP admin API client for TUI
└── Task 7: Permission detection & app state

Wave 3 (After Wave 2):
├── Task 8: Tab navigation shell & status bar
├── Task 9: Dashboard tab (self-monitoring data)
├── Task 10: Traces tab (search + waterfall)
├── Task 11: Logs tab (Flight SQL browse)
├── Task 12: Metrics tab (sparklines)
└── Task 13: Admin tab (full CRUD)

Wave 4 (After Wave 3):
└── Task 14: Integration, polish, and final verification

Critical Path: Task 1 → Task 5 → Task 9 (self-monitoring → Flight SQL client → Dashboard)
Critical Path: Task 2 → Task 4 → Task 8 → Tasks 9-13 (scaffolding → event loop → tabs)
```

### Dependency Matrix

| Task | Depends On | Blocks | Can Parallelize With |
|------|------------|--------|---------------------|
| 1 | None | 9, 11, 12 | 2, 3 |
| 2 | None | 4, 5, 6, 7 | 1, 3 |
| 3 | None | 4, 5, 6, 7, 8-13 | 1, 2 |
| 4 | 2, 3 | 8 | 5, 6, 7 |
| 5 | 2, 3 | 9, 10, 11, 12 | 4, 6, 7 |
| 6 | 2, 3 | 13 | 4, 5, 7 |
| 7 | 2, 3 | 8 | 4, 5, 6 |
| 8 | 4, 7 | 9-13 | None |
| 9 | 1, 5, 8 | 14 | 10, 11, 12, 13 |
| 10 | 5, 8 | 14 | 9, 11, 12, 13 |
| 11 | 1, 5, 8 | 14 | 9, 10, 12, 13 |
| 12 | 1, 5, 8 | 14 | 9, 10, 11, 13 |
| 13 | 6, 8 | 14 | 9, 10, 11, 12 |
| 14 | 9-13 | None | None (final) |

### Agent Dispatch Summary

| Wave | Tasks | Recommended Category |
|------|-------|---------------------|
| 1 | 1, 2, 3 | quick/unspecified-low (scaffolding + config) |
| 2 | 4, 5, 6, 7 | unspecified-high (async architecture, client design) |
| 3 | 8-13 | visual-engineering (TUI components) + unspecified-high (data integration) |
| 4 | 14 | deep (integration testing, edge cases) |

---

## TODOs

- [x] 1. Self-Monitoring Infrastructure via OTLP (Dogfooding)

  **What to do**:
  - Create a self-monitoring module in `src/common/src/self_monitoring/` that services use to instrument themselves via the OpenTelemetry SDK and export telemetry via OTLP
  - **Architecture**: Services act as OTLP clients to the Acceptor — their own telemetry flows through the normal Acceptor → WAL → Writer → Iceberg pipeline. SignalDB monitors itself using its own ingestion stack.
  - Replicate the `signal-producer` pattern (`src/signal-producer/src/main.rs:165-202`) for OTel SDK initialization:
    - `init_telemetry(endpoint, service_name) -> Result<Telemetry>` function in common crate
    - Configure OTLP exporter with `SpanExporter`, `LogExporter`, `MetricExporter` using `.with_tonic()` transport
    - `Resource` with `service.name`, `service.version`, `service.instance.id` attributes
    - Batch exporters for traces/logs, periodic exporter for metrics
    - Graceful shutdown: `force_flush()` + `shutdown()` on all providers
  - **Dedicated `_system` tenant**: Configure self-monitoring to send data with:
    - Header `X-Tenant-ID: _system`
    - Header `X-Dataset-ID: _monitoring`
    - Auth: use the admin API key or a dedicated internal key (configurable)
    - Ensure `_system` tenant is auto-created on first startup (or bootstrapped in config)
  - **Configuration**: Add self-monitoring config section to `signaldb.toml`:
    ```toml
    [self_monitoring]
    enabled = true
    endpoint = "http://localhost:4317"  # OTLP gRPC endpoint (acceptor)
    interval = "15s"                     # Metric export interval
    tenant_id = "_system"
    dataset_id = "_monitoring"
    ```
  - **Metric instruments** to create per service (following `signal-producer` pattern):
    - WAL stats (gauge/observable): `signaldb.wal.unprocessed_entries`, `signaldb.wal.active_writers`, `signaldb.wal.segment_count`
    - Flight pool stats (gauge/observable): `signaldb.flight.pool_current_connections`, `signaldb.flight.pool_max_connections`
    - Service health (gauge): `signaldb.service.uptime_seconds`, `signaldb.service.healthy` (1/0)
    - System resources (gauge/observable): `signaldb.system.cpu_usage_percent`, `signaldb.system.memory_used_bytes`, `signaldb.system.memory_total_bytes`
    - Request counters: `signaldb.requests.total`, `signaldb.requests.errors`
    - Use `f64_observable_gauge` with callbacks (like `signal-producer`'s CPU gauge) for stats that require periodic sampling
  - **System resources**: Use `sysinfo` crate in common crate for CPU/memory collection via observable gauge callbacks
  - **Integrate into each service's `main.rs`**:
    - Call `init_telemetry()` during startup (after config load, before service start)
    - Pass metric instruments to service components that report stats
    - Call shutdown on graceful termination (alongside existing ctrl+c handler)
    - If self-monitoring is disabled or fails to connect, log a warning and continue (non-blocking)
  - **OTel dependencies**: Add to each service crate's `Cargo.toml`:
    ```toml
    opentelemetry = { workspace = true, features = ["trace", "metrics", "logs"] }
    opentelemetry_sdk = { workspace = true, features = ["trace", "metrics", "logs"] }
    opentelemetry-otlp = { workspace = true, features = ["grpc-tonic", "trace", "metrics", "logs"] }
    opentelemetry-semantic-conventions.workspace = true
    ```
    Note: These crates are already at workspace level (v0.31.0). Also add `sysinfo` to `common` Cargo.toml.

  **Must NOT do**:
  - Do not create bespoke HTTP endpoints or custom write paths — use OTLP exclusively
  - Do not add `sysinfo` to service crates — only to `common` (exposed via the self-monitoring module)
  - Do not block service startup if self-monitoring fails to connect (log warning, continue)
  - Do not write directly to WAL or Iceberg — let OTLP flow through the Acceptor like any other client
  - Do not create a separate binary for self-monitoring — it's integrated into each service

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Requires understanding of OTel SDK configuration, OTLP export patterns, service lifecycle, async background tasks, and how data flows through the Acceptor pipeline
  - **Skills**: [`architecture`, `rust-patterns`, `configuration`]
    - `architecture`: Understand the write path (Acceptor → WAL → Writer → Iceberg) to ensure self-monitoring data flows correctly
    - `rust-patterns`: Follow project error handling (thiserror/anyhow), async patterns, graceful shutdown
    - `configuration`: Add `[self_monitoring]` section to signaldb.toml configuration
  - **Skills Evaluated but Omitted**:
    - `flight-schemas`: Self-monitoring doesn't define new schemas — data uses standard OTLP-to-Arrow conversion
    - `storage-layout`: Data flows through normal pipeline, no direct storage interaction

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 2, 3)
  - **Blocks**: Tasks 9, 11, 12 (Dashboard, Logs, Metrics tabs need self-monitoring data)
  - **Blocked By**: None (can start immediately)

  **References**:

  **Pattern References** (existing code to follow — PRIMARY):
  - `src/signal-producer/src/main.rs:165-202` — **PRIMARY REFERENCE**: Complete `init_telemetry()` function with SpanExporter, LogExporter, MetricExporter setup via `.with_tonic()` — replicate this pattern
  - `src/signal-producer/src/main.rs:204-231` — Metric instrument creation: counters, histograms, observable gauges with callbacks
  - `src/signal-producer/src/main.rs:409-433` — Metric recording pattern: `.add()`, `.record()`, callback-based gauges
  - `src/signal-producer/src/main.rs:139-159` — Graceful shutdown: `force_flush()` + `shutdown()` on all providers
  - `src/signal-producer/Cargo.toml` — OpenTelemetry dependency declarations with feature flags

  **Metric Data Sources** (where to read stats for observable gauges):
  - `src/acceptor/src/handler/wal_manager.rs:177` — `wal_count()` for WAL instance counts
  - `src/writer/src/processor.rs:304` — `get_stats()` returning `ProcessorStats` with `active_writers`
  - `src/common/src/flight/transport.rs:327` — `pool_stats()` returning `(current, max)` for connection pool
  - `src/router/src/discovery.rs:234` — `is_healthy()` and `flight_pool_stats()` for discovery status
  - `src/common/src/wal/mod.rs:775` — `get_unprocessed_entries()` for WAL backlog

  **Service Integration Points** (where to add init_telemetry() calls):
  - `src/acceptor/src/main.rs` — After config load, before service start
  - `src/writer/src/main.rs` — After config load, before service start
  - `src/querier/src/main.rs` — After config load, before service start
  - `src/router/src/main.rs` — After config load, before service start
  - `src/signaldb-bin/src/main.rs` — After config load, before monolithic service start

  **Configuration References**:
  - `src/common/src/config/mod.rs` — Configuration struct to add `[self_monitoring]` section

  **Test References**:
  - `src/router/src/endpoints/admin.rs` (bottom of file) — `#[tokio::test]` pattern
  - `src/common/` test modules — Unit test patterns for common crate code

  **Acceptance Criteria**:

  - [ ] `cargo test -p common` passes with new self-monitoring module tests
  - [ ] Self-monitoring module compiles: `cargo build -p common`
  - [ ] `init_telemetry()` function creates OTLP exporters for traces, logs, and metrics
  - [ ] Each service (acceptor, writer, querier, router, signaldb-bin) calls `init_telemetry()` at startup
  - [ ] `[self_monitoring]` config section parsed from signaldb.toml
  - [ ] Self-monitoring gracefully degrades when Acceptor is unreachable (warning log, no crash)
  - [ ] `cargo build` succeeds for all workspace members
  - [ ] `cargo clippy --workspace --all-targets -- -D warnings` is clean

  **Agent-Executed QA Scenarios:**

  ```
  Scenario: Self-monitoring module compiles and tests pass
    Tool: Bash
    Preconditions: None
    Steps:
      1. cargo build -p common
      2. Assert: exit code 0
      3. cargo test -p common self_monitoring
      4. Assert: all tests pass, exit code 0
    Expected Result: Module compiles, unit tests pass
    Evidence: cargo test output captured

  Scenario: All services build with self-monitoring integration
    Tool: Bash
    Preconditions: Self-monitoring module exists in common
    Steps:
      1. cargo build --workspace
      2. Assert: exit code 0
      3. cargo clippy --workspace --all-targets -- -D warnings
      4. Assert: exit code 0
    Expected Result: No compilation or lint errors
    Evidence: Build and clippy output captured

  Scenario: Configuration parsing includes self_monitoring section
    Tool: Bash (cargo test)
    Preconditions: None
    Steps:
      1. Run config parsing test with TOML containing [self_monitoring] section
      2. Assert: enabled=true, endpoint parsed, interval parsed, tenant_id="_system"
      3. Run config parsing test WITHOUT [self_monitoring] section
      4. Assert: defaults applied (enabled=false or sensible default)
    Expected Result: Config section parsed with defaults
    Evidence: Test output captured

  Scenario: Self-monitoring graceful degradation
    Tool: Bash (cargo test)
    Preconditions: No OTLP endpoint available
    Steps:
      1. Call init_telemetry() with unreachable endpoint "http://localhost:99999"
      2. Assert: Returns Ok (not Err) — initialization succeeds
      3. Assert: Warning logged about unreachable endpoint
      4. Assert: Service continues to function normally
    Expected Result: Non-blocking failure, service unaffected
    Evidence: Test output captured
  ```

  **Commit**: YES
  - Message: `feat(common): add OTLP-based self-monitoring infrastructure (dogfooding)`
  - Files: `src/common/src/self_monitoring/`, `src/common/Cargo.toml`, `src/common/src/config/`, service `main.rs` files
  - Pre-commit: `cargo test -p common && cargo clippy --workspace --all-targets -- -D warnings`

---

- [x] 2. TUI Dependencies & Project Scaffolding

  **What to do**:
  - Add ratatui and crossterm to workspace `Cargo.toml` dependencies
  - Add ratatui, crossterm to `signaldb-cli/Cargo.toml` dependencies
  - Add `tui-tree-widget` for JSON viewer
  - Create TUI module structure inside signaldb-cli:
    ```
    src/signaldb-cli/src/
    ├── tui/
    │   ├── mod.rs          # TUI module root, public API
    │   ├── app.rs          # App struct, component orchestration
    │   ├── event.rs        # Event enum (Key, Tick, Render, Data)
    │   ├── action.rs       # Action enum for message passing
    │   ├── terminal.rs     # Terminal setup/teardown, Tui struct
    │   ├── state.rs        # AppState (data + UI state)
    │   ├── client/
    │   │   ├── mod.rs      # Client module root
    │   │   ├── flight.rs   # Flight SQL client wrapper
    │   │   └── admin.rs    # HTTP admin API client wrapper
    │   ├── components/
    │   │   ├── mod.rs      # Component trait definition
    │   │   ├── tabs.rs     # Tab bar component
    │   │   ├── status_bar.rs   # Bottom status bar
    │   │   ├── dashboard/  # Dashboard tab components
    │   │   ├── traces/     # Traces tab components
    │   │   ├── logs/       # Logs tab components
    │   │   ├── metrics/    # Metrics tab components
    │   │   └── admin/      # Admin tab components
    │   └── widgets/
    │       ├── mod.rs      # Custom widgets
    │       ├── waterfall.rs    # Trace span waterfall
    │       ├── json_viewer.rs  # JSON tree viewer
    │       └── sparkline.rs    # Sparkline wrapper
    ```
  - Add `tui` subcommand to signaldb-cli's clap `Commands` enum with args:
    - `--url` / `SIGNALDB_URL` (default: `http://localhost:3000`)
    - `--flight-url` / `SIGNALDB_FLIGHT_URL` (default: `http://localhost:50053`)
    - `--api-key` / `SIGNALDB_API_KEY`
    - `--admin-key` / `SIGNALDB_ADMIN_KEY`
    - `--refresh-rate` / `SIGNALDB_TUI_REFRESH_RATE` (default: `5s`)
    - `--tenant-id` / `SIGNALDB_TENANT_ID`
    - `--dataset-id` / `SIGNALDB_DATASET_ID`
  - Create stub `mod.rs` files for each module with minimal placeholder code
  - Ensure `signaldb-cli tui --help` works

  **Must NOT do**:
  - Do not implement any TUI logic yet — just scaffolding and stub files
  - Do not add `color-eyre` — use `anyhow`
  - Do not add `sysinfo` to signaldb-cli

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: Scaffolding task — creating files, adding deps, no complex logic
  - **Skills**: [`crate-map`, `rust-patterns`]
    - `crate-map`: Understand workspace structure and Cargo.toml conventions
    - `rust-patterns`: Follow project dependency management and module patterns
  - **Skills Evaluated but Omitted**:
    - `architecture`: Not needed for scaffolding
    - `flight-schemas`: Not relevant to dependency setup

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1, 3)
  - **Blocks**: Tasks 4, 5, 6, 7 (all Wave 2 tasks need scaffolding in place)
  - **Blocked By**: None (can start immediately)

  **References**:

  **Pattern References**:
  - `src/signaldb-cli/Cargo.toml` — Current dependencies to extend
  - `src/signaldb-cli/src/commands/mod.rs:1-54` — Existing CLI struct and Commands enum to add `tui` variant
  - `src/signaldb-cli/src/commands/query.rs:1-20` — Pattern for Flight URL and auth args
  - `Cargo.toml` (workspace root) — Workspace dependency declarations

  **External References**:
  - ratatui component template: `https://github.com/ratatui/templates/tree/main/component` — Module structure reference
  - ratatui async template: `https://github.com/ratatui-org/ratatui-async-template` — Async event loop structure

  **Acceptance Criteria**:

  - [ ] `cargo build -p signaldb-cli` succeeds
  - [ ] `cargo run -p signaldb-cli -- tui --help` shows TUI subcommand help with all args
  - [ ] Module structure exists with stub files (all `mod.rs` files compile)
  - [ ] ratatui, crossterm, tui-tree-widget appear in Cargo.toml

  **Agent-Executed QA Scenarios:**

  ```
  Scenario: TUI subcommand appears in help
    Tool: Bash
    Preconditions: None
    Steps:
      1. cargo run -p signaldb-cli -- --help
      2. Assert: output contains "tui"
      3. cargo run -p signaldb-cli -- tui --help
      4. Assert: output contains "--url", "--flight-url", "--api-key", "--refresh-rate"
    Expected Result: TUI subcommand registered with all expected arguments
    Evidence: Help output captured

  Scenario: Build succeeds with new dependencies
    Tool: Bash
    Preconditions: None
    Steps:
      1. cargo build -p signaldb-cli
      2. Assert: exit code 0
      3. cargo clippy -p signaldb-cli --all-targets -- -D warnings
      4. Assert: exit code 0
    Expected Result: Clean build with no warnings
    Evidence: Build output captured
  ```

  **Commit**: YES
  - Message: `feat(cli): scaffold TUI module structure and add ratatui dependencies`
  - Files: `Cargo.toml`, `src/signaldb-cli/Cargo.toml`, `src/signaldb-cli/src/tui/`
  - Pre-commit: `cargo build -p signaldb-cli && cargo clippy -p signaldb-cli --all-targets -- -D warnings`

---

- [x] 3. Test Infrastructure Setup

  **What to do**:
  - Add dev-dependencies to `signaldb-cli/Cargo.toml`:
    - `insta` (snapshot testing for ratatui)
    - `mockito` (HTTP mocking for admin API tests)
    - `tokio-test` (from workspace)
    - `tempfile` (from workspace)
  - Create test directory structure:
    ```
    src/signaldb-cli/src/tui/
    ├── tests/          # or inline #[cfg(test)] modules
    ```
  - Create a test helper module `src/signaldb-cli/src/tui/test_helpers.rs` with:
    - `create_test_terminal()` → `Terminal<TestBackend>` with standard 120x40 size
    - `create_test_app_state()` → `AppState` with mock data
    - `assert_buffer_contains(terminal, expected_text)` → helper for buffer assertions
  - Write a single "canary" test that creates a TestBackend terminal and renders an empty frame
  - Verify: `cargo test -p signaldb-cli` passes with the canary test

  **Must NOT do**:
  - Do not write tests for unimplemented components — just the infrastructure
  - Do not add integration test dependencies that require running services

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: Dev-dependency setup and minimal test scaffold
  - **Skills**: [`rust-patterns`]
    - `rust-patterns`: Follow project testing patterns (tokio::test, module structure)
  - **Skills Evaluated but Omitted**:
    - `crate-map`: Not needed for test setup
    - `dev-workflow`: Test infra is straightforward

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1, 2)
  - **Blocks**: All subsequent tasks (TDD requires test infra)
  - **Blocked By**: None (can start immediately)

  **References**:

  **Pattern References**:
  - `src/router/src/endpoints/admin.rs` (bottom of file) — `#[tokio::test]` pattern with in-memory state
  - `tests-integration/Cargo.toml` — Example dev-dependency declarations

  **External References**:
  - ratatui testing docs: `https://ratatui.rs/recipes/testing/snapshots/` — insta + TestBackend setup
  - insta docs: `https://insta.rs/` — Snapshot testing setup

  **Acceptance Criteria**:

  - [ ] `cargo test -p signaldb-cli` passes with at least 1 canary test
  - [ ] `insta`, `mockito` appear in `[dev-dependencies]`
  - [ ] Test helper module provides `create_test_terminal()` function

  **Agent-Executed QA Scenarios:**

  ```
  Scenario: Test infrastructure works
    Tool: Bash
    Preconditions: Task 2 scaffolding complete
    Steps:
      1. cargo test -p signaldb-cli -- tui
      2. Assert: at least 1 test passes
      3. Assert: exit code 0
    Expected Result: Canary test passes
    Evidence: Test output captured
  ```

  **Commit**: YES (groups with Task 2)
  - Message: `test(cli): add TUI test infrastructure with TestBackend and insta`
  - Files: `src/signaldb-cli/Cargo.toml`, test helper files
  - Pre-commit: `cargo test -p signaldb-cli`

---

- [x] 4. TUI Event Loop & Terminal Management

  **What to do**:
  - Implement `src/signaldb-cli/src/tui/terminal.rs`:
    - `Tui` struct wrapping `Terminal<CrosstermBackend<Stdout>>`
    - `init()`: enter alternate screen, enable raw mode, set panic hook to restore terminal
    - `exit()`: leave alternate screen, disable raw mode, show cursor
    - Restore terminal on panic (critical — broken terminal is terrible UX)
  - Implement `src/signaldb-cli/src/tui/event.rs`:
    - `Event` enum: `Key(KeyEvent)`, `Tick`, `Render`, `Data(DataEvent)`, `Error(String)`
    - `DataEvent` enum: variants for each data type the TUI receives from background tasks
    - `EventHandler` struct using `tokio::select!`:
      - Crossterm event stream (key presses) via `crossterm::event::EventStream`
      - Tick interval (configurable, default 5s) for data refresh
      - Render interval (fixed ~30fps) for UI repaints
      - `mpsc::Receiver<DataEvent>` for background task results
    - Proper cancellation via `CancellationToken`
  - Implement `src/signaldb-cli/src/tui/action.rs`:
    - `Action` enum: `Quit`, `SwitchTab(usize)`, `Refresh`, `ScrollUp`, `ScrollDown`, `Select`, `Back`, `Search(String)`, `Confirm`, `Cancel`, etc.
  - Implement the main run loop in `src/signaldb-cli/src/tui/app.rs`:
    - `App::run()` method that loops: receive event → map to action → update state → render
    - Clean shutdown on `Action::Quit` or Ctrl+C
  - Wire into CLI: `Commands::Tui` variant calls `App::run()` from `main.rs`

  **Must NOT do**:
  - Do not implement tab rendering or data fetching yet — just the event loop shell
  - Do not use `color-eyre` for panic hook — use `std::panic::set_hook` with terminal restore

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Async architecture with tokio::select!, event streams, and cancellation requires careful design
  - **Skills**: [`rust-patterns`]
    - `rust-patterns`: Async code patterns, error handling, CancellationToken usage
  - **Skills Evaluated but Omitted**:
    - `architecture`: Event loop is TUI-specific, not SignalDB architecture

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 5, 6, 7)
  - **Blocks**: Task 8 (tab navigation needs event loop)
  - **Blocked By**: Tasks 2, 3

  **References**:

  **Pattern References**:
  - `src/signaldb-cli/src/commands/mod.rs:55-101` — Entry point pattern to follow for `tui` subcommand
  - `src/signaldb-cli/src/main.rs` — `#[tokio::main]` async main

  **External References**:
  - ratatui async counter tutorial: `https://ratatui.rs/tutorials/counter-async-app/` — tokio::select! event loop
  - ratatui component template: `https://github.com/ratatui/templates/tree/main/component` — App/Tui struct separation
  - crossterm event-stream: `https://docs.rs/crossterm/latest/crossterm/event/struct.EventStream.html`

  **Acceptance Criteria**:

  - [ ] TDD: Tests for event handler (key mapping), action dispatch, terminal init/exit
  - [ ] `cargo test -p signaldb-cli` passes all event loop tests
  - [ ] TUI launches and shows a blank screen with terminal properly initialized
  - [ ] Pressing `q` or `Ctrl+C` exits cleanly (terminal restored)

  **Agent-Executed QA Scenarios:**

  ```
  Scenario: TUI launches and exits cleanly
    Tool: interactive_bash (tmux)
    Preconditions: signaldb-cli built
    Steps:
      1. tmux new-session: cargo run -p signaldb-cli -- tui --url http://localhost:3000
      2. Wait for: alternate screen mode entered (timeout: 10s)
      3. Send keys: "q"
      4. Assert: process exits with code 0
      5. Assert: terminal is restored (normal mode, cursor visible)
    Expected Result: TUI starts, responds to quit key, terminal intact
    Evidence: Terminal output captured

  Scenario: Ctrl+C exits cleanly
    Tool: interactive_bash (tmux)
    Preconditions: signaldb-cli built
    Steps:
      1. tmux new-session: cargo run -p signaldb-cli -- tui --url http://localhost:3000
      2. Wait for: alternate screen mode entered (timeout: 10s)
      3. Send keys: Ctrl+C
      4. Assert: process exits with code 0
      5. Assert: terminal is restored
    Expected Result: Graceful shutdown on Ctrl+C
    Evidence: Terminal output captured
  ```

  **Commit**: YES
  - Message: `feat(cli): implement TUI event loop with async terminal management`
  - Files: `src/signaldb-cli/src/tui/terminal.rs`, `event.rs`, `action.rs`, `app.rs`
  - Pre-commit: `cargo test -p signaldb-cli`

---

- [x] 5. Flight SQL Client for TUI

  **What to do**:
  - Implement `src/signaldb-cli/src/tui/client/flight.rs`:
    - `FlightSqlClient` struct wrapping `arrow_flight::FlightServiceClient`
    - Constructor: takes `flight_url`, optional `api_key`, optional `tenant_id`, `dataset_id`
    - Methods for each data query:
      - `query_sql(sql: &str) -> Result<Vec<RecordBatch>>` — generic SQL query
      - `search_traces(params: TraceSearchParams) -> Result<Vec<TraceResult>>` — Tempo search via Flight
      - `get_trace(trace_id: &str) -> Result<TraceDetail>` — single trace lookup
      - `query_logs(sql: &str) -> Result<Vec<RecordBatch>>` — log table query
      - `query_metrics(sql: &str) -> Result<Vec<RecordBatch>>` — metrics table query
      - `query_system_metrics(sql: &str) -> Result<Vec<RecordBatch>>` — self-monitoring data query
    - Auth metadata injection (Bearer token, X-Tenant-ID, X-Dataset-ID headers)
    - Connection timeout (10s), request timeout (30s)
    - Error handling: connection refused → `ConnectionError`, auth failure → `AuthError`, query error → `QueryError`
  - Define data model types in `src/signaldb-cli/src/tui/client/models.rs`:
    - `TraceSearchParams`, `TraceResult`, `TraceDetail`, `SpanInfo`
    - `SystemMetric`, `ServiceHealth`, `WalStatus`, `PoolStats`
    - Arrow RecordBatch → domain type conversion functions
  - Reuse existing Flight connection pattern from `src/signaldb-cli/src/commands/query.rs`

  **Must NOT do**:
  - Do not implement caching — keep client stateless
  - Do not implement retry logic — let the TUI handle retry via refresh

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Flight client with auth, Arrow type conversion, and proper error handling is non-trivial
  - **Skills**: [`flight-schemas`, `rust-patterns`]
    - `flight-schemas`: Understand Flight schema wire format, RecordBatch structure, metadata
    - `rust-patterns`: Error handling (thiserror for client errors), async patterns
  - **Skills Evaluated but Omitted**:
    - `architecture`: Client is a consumer, not part of service architecture
    - `storage-layout`: Client doesn't interact with storage directly

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 4, 6, 7)
  - **Blocks**: Tasks 9, 10, 11, 12 (all data tabs need Flight client)
  - **Blocked By**: Tasks 2, 3

  **References**:

  **Pattern References**:
  - `src/signaldb-cli/src/commands/query.rs:56-109` — Existing Flight client setup with auth metadata injection — **primary reference**, reuse this pattern
  - `src/common/src/flight/transport.rs` — Flight transport layer used by services

  **API/Type References**:
  - `src/common/src/flight/schema.rs` — Flight schema definitions for traces/logs/metrics
  - `src/router/src/endpoints/flight.rs` — Flight service query handling (what the client will call)

  **Test References**:
  - `src/common/src/flight/integration_tests.rs` — Flight protocol testing patterns

  **Acceptance Criteria**:

  - [ ] TDD: Unit tests with mock Flight server for each client method
  - [ ] `cargo test -p signaldb-cli` passes all Flight client tests
  - [ ] Auth headers correctly injected (test with mock assertions)
  - [ ] Connection errors return typed `ConnectionError`, not panics
  - [ ] RecordBatch → domain type conversions tested

  **Agent-Executed QA Scenarios:**

  ```
  Scenario: Flight client handles connection refused gracefully
    Tool: Bash (cargo test)
    Preconditions: No Flight server running
    Steps:
      1. Run test that creates FlightSqlClient with url "http://localhost:99999"
      2. Call query_sql("SELECT 1")
      3. Assert: Returns Err(ConnectionError)
      4. Assert: No panic
    Expected Result: Typed error returned, not a panic
    Evidence: Test output captured

  Scenario: Flight client injects auth metadata correctly
    Tool: Bash (cargo test)
    Preconditions: Mock Flight server set up in test
    Steps:
      1. Create client with api_key="test-key", tenant_id="acme"
      2. Call query_sql("SELECT 1")
      3. Assert: mock received "authorization: Bearer test-key" header
      4. Assert: mock received "x-tenant-id: acme" header
    Expected Result: Auth metadata present in Flight request
    Evidence: Test output captured
  ```

  **Commit**: YES
  - Message: `feat(cli): add Flight SQL client for TUI data access`
  - Files: `src/signaldb-cli/src/tui/client/flight.rs`, `models.rs`
  - Pre-commit: `cargo test -p signaldb-cli`

---

- [x] 6. HTTP Admin API Client for TUI

  **What to do**:
  - Implement `src/signaldb-cli/src/tui/client/admin.rs`:
    - `AdminClient` struct wrapping `signaldb_sdk::Client` (progenitor-generated)
    - Reuse existing SDK client construction pattern from `commands/mod.rs`
    - Methods:
      - `list_tenants() -> Result<Vec<Tenant>>`
      - `create_tenant(req: CreateTenantRequest) -> Result<Tenant>`
      - `get_tenant(id: &str) -> Result<Tenant>`
      - `update_tenant(id: &str, req: UpdateTenantRequest) -> Result<Tenant>`
      - `delete_tenant(id: &str) -> Result<()>`
      - `list_api_keys(tenant_id: &str) -> Result<Vec<ApiKey>>`
      - `create_api_key(tenant_id: &str, req: CreateApiKeyRequest) -> Result<ApiKey>`
      - `revoke_api_key(tenant_id: &str, key_id: &str) -> Result<()>`
      - `list_datasets(tenant_id: &str) -> Result<Vec<Dataset>>`
      - `create_dataset(tenant_id: &str, req: CreateDatasetRequest) -> Result<Dataset>`
      - `delete_dataset(tenant_id: &str, dataset_id: &str) -> Result<()>`
    - Error handling: 401 → `Unauthorized`, 404 → `NotFound`, 409 → `Conflict`, network → `ConnectionError`
  - Define request/response types if not already in SDK

  **Must NOT do**:
  - Do not build a new HTTP client — reuse the progenitor SDK
  - Do not implement pagination (if API doesn't support it yet)

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: Wrapper around existing SDK client, straightforward
  - **Skills**: [`rust-patterns`, `multi-tenancy`]
    - `rust-patterns`: Error handling patterns
    - `multi-tenancy`: Understand tenant/dataset/API key model
  - **Skills Evaluated but Omitted**:
    - `tempo-api`: Admin API is not Tempo-related

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 4, 5, 7)
  - **Blocks**: Task 13 (Admin tab)
  - **Blocked By**: Tasks 2, 3

  **References**:

  **Pattern References**:
  - `src/signaldb-cli/src/commands/mod.rs:55-101` — Admin key resolution and SDK client construction — **primary reference**
  - `src/signaldb-cli/src/commands/tenant.rs` — Existing tenant CRUD using SDK client
  - `src/signaldb-cli/src/commands/api_key.rs` — Existing API key operations
  - `src/signaldb-cli/src/commands/dataset.rs` — Existing dataset operations

  **API/Type References**:
  - `src/router/src/endpoints/admin.rs:18-155` — Admin API endpoint definitions (what the client calls)

  **Acceptance Criteria**:

  - [ ] TDD: Unit tests with mockito HTTP mock for each admin method
  - [ ] `cargo test -p signaldb-cli` passes all admin client tests
  - [ ] 401 responses return `Unauthorized` error type
  - [ ] SDK client correctly constructed with Bearer token

  **Agent-Executed QA Scenarios:**

  ```
  Scenario: Admin client lists tenants successfully
    Tool: Bash (cargo test)
    Preconditions: mockito HTTP server returns tenant list JSON
    Steps:
      1. Create AdminClient with mock server URL and test admin key
      2. Call list_tenants()
      3. Assert: Returns Ok with expected tenant data
      4. Assert: mock received correct Authorization header
    Expected Result: Tenants deserialized correctly
    Evidence: Test output captured

  Scenario: Admin client handles 401 unauthorized
    Tool: Bash (cargo test)
    Preconditions: mockito returns 401 for all requests
    Steps:
      1. Create AdminClient with invalid key
      2. Call list_tenants()
      3. Assert: Returns Err(Unauthorized)
    Expected Result: Typed auth error, not panic
    Evidence: Test output captured
  ```

  **Commit**: YES (groups with Task 5)
  - Message: `feat(cli): add HTTP admin API client for TUI`
  - Files: `src/signaldb-cli/src/tui/client/admin.rs`
  - Pre-commit: `cargo test -p signaldb-cli`

---

- [x] 7. Permission Detection & App State

  **What to do**:
  - Implement `src/signaldb-cli/src/tui/state.rs`:
    - `AppState` struct:
      - `permission: Permission` — `Admin` or `Tenant { tenant_id, dataset_id }`
      - `active_tab: Tab` — enum: Dashboard, Traces, Logs, Metrics, Admin
      - `available_tabs: Vec<Tab>` — filtered by permission (Admin tab only for admin keys)
      - `connection_status: ConnectionStatus` — Connected, Disconnected, Connecting
      - `last_error: Option<String>` — latest error for status bar
      - `refresh_rate: Duration` — configurable
      - Per-tab state structs (initially empty, filled in by tab tasks)
    - `Permission` enum: `Admin { admin_key }`, `Tenant { api_key, tenant_id, dataset_id }`
    - `Tab` enum: `Dashboard`, `Traces`, `Logs`, `Metrics`, `Admin`
  - Implement permission detection in `app.rs`:
    - On startup: try `GET /api/v1/admin/tenants` with provided key
    - If 200 → `Permission::Admin`
    - If 401/403 → `Permission::Tenant`
    - If connection refused → show "Connection Failed" with retry
    - Store result in `AppState.permission`
    - Filter `available_tabs` based on permission (hide Admin tab for non-admin)

  **Must NOT do**:
  - Do not create a "whoami" endpoint — use try-and-fail detection
  - Do not prompt user for permission mode — auto-detect only

  **Recommended Agent Profile**:
  - **Category**: `unspecified-low`
    - Reason: State struct definitions and simple HTTP probe logic
  - **Skills**: [`rust-patterns`, `multi-tenancy`]
    - `rust-patterns`: Enum design, state management patterns
    - `multi-tenancy`: Understand admin vs tenant key distinction
  - **Skills Evaluated but Omitted**:
    - `architecture`: State design is TUI-specific

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 4, 5, 6)
  - **Blocks**: Task 8 (tab navigation needs available_tabs)
  - **Blocked By**: Tasks 2, 3

  **References**:

  **Pattern References**:
  - `src/common/src/auth/mod.rs` — Two-tier auth model: admin key vs tenant API key
  - `src/signaldb-cli/src/commands/mod.rs:55-101` — Admin key resolution pattern

  **Acceptance Criteria**:

  - [ ] TDD: Tests for permission detection (admin key → Admin, tenant key → Tenant, no server → error)
  - [ ] TDD: Tests for tab filtering (Admin tab hidden for Tenant permission)
  - [ ] `cargo test -p signaldb-cli` passes all state tests
  - [ ] AppState is `Clone` and testable without side effects

  **Agent-Executed QA Scenarios:**

  ```
  Scenario: Permission detection with admin key
    Tool: Bash (cargo test)
    Preconditions: mockito returns 200 for admin tenant list
    Steps:
      1. Run permission detection with admin key
      2. Assert: result is Permission::Admin
      3. Assert: available_tabs includes Tab::Admin
    Expected Result: Admin permission detected, all tabs available
    Evidence: Test output captured

  Scenario: Permission detection with tenant key
    Tool: Bash (cargo test)
    Preconditions: mockito returns 401 for admin tenant list
    Steps:
      1. Run permission detection with tenant key
      2. Assert: result is Permission::Tenant
      3. Assert: available_tabs does NOT include Tab::Admin
    Expected Result: Tenant permission detected, admin tab hidden
    Evidence: Test output captured
  ```

  **Commit**: YES
  - Message: `feat(cli): implement permission detection and TUI app state`
  - Files: `src/signaldb-cli/src/tui/state.rs`, `app.rs` (detection logic)
  - Pre-commit: `cargo test -p signaldb-cli`

---

- [x] 8. Tab Navigation Shell & Status Bar

  **What to do**:
  - Implement `src/signaldb-cli/src/tui/components/tabs.rs`:
    - Render tab bar at top of screen using `ratatui::widgets::Tabs`
    - Highlight active tab with bold style
    - Show keyboard shortcuts in tab labels: `[1] Dashboard  [2] Traces  [3] Logs  [4] Metrics  [5] Admin`
    - Only show tabs from `AppState.available_tabs` (permission-filtered)
    - Handle key events: `1`-`5` switches tabs, `Tab`/`Shift+Tab` cycles
  - Implement `src/signaldb-cli/src/tui/components/status_bar.rs`:
    - Render status bar at bottom of screen
    - Left: connection status (🟢 Connected / 🔴 Disconnected) + service URL
    - Center: permission level (Admin / Tenant: {tenant_id})
    - Right: last refresh time + refresh rate + keybind hints (`q: Quit  r: Refresh  /: Search`)
    - Show `last_error` in status bar with red styling when present
  - Implement main layout in `app.rs` render function:
    - `Layout::default().direction(Direction::Vertical).constraints([Length(3), Min(0), Length(1)])`
    - Top: tab bar (3 rows)
    - Middle: active tab content (flexible)
    - Bottom: status bar (1 row)
  - Implement Component trait:
    ```rust
    pub trait Component {
        fn handle_key_event(&mut self, key: KeyEvent) -> Option<Action>;
        fn update(&mut self, action: &Action, state: &mut AppState);
        fn render(&self, frame: &mut Frame, area: Rect, state: &AppState);
    }
    ```
  - Wire tab switching: key press → `Action::SwitchTab(n)` → update `AppState.active_tab` → re-render

  **Must NOT do**:
  - Do not implement tab content — render placeholder "Tab content coming soon" text in middle area
  - Do not use mouse events for tab switching

  **Recommended Agent Profile**:
  - **Category**: `visual-engineering`
    - Reason: UI layout, styling, keyboard navigation — visual component work
  - **Skills**: [`rust-patterns`]
    - `rust-patterns`: Trait design, enum matching for key events
  - **Skills Evaluated but Omitted**:
    - `architecture`: UI component, not system architecture
    - `frontend-ui-ux`: This is TUI, not web frontend

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential (must complete before Wave 3)
  - **Blocks**: Tasks 9-13 (all tabs render inside this shell)
  - **Blocked By**: Tasks 4, 7

  **References**:

  **Pattern References**:
  - `src/signaldb-cli/src/tui/app.rs` — App struct from Task 4

  **External References**:
  - ratatui Tabs widget: `https://docs.rs/ratatui/latest/ratatui/widgets/struct.Tabs.html`
  - ratatui Layout: `https://docs.rs/ratatui/latest/ratatui/layout/struct.Layout.html`
  - LucasPickering/slumber tabs implementation: `https://github.com/LucasPickering/slumber/blob/master/crates/tui/src/view/common/tabs.rs`

  **Acceptance Criteria**:

  - [ ] TDD: Snapshot tests for tab bar rendering (all tabs, admin-hidden variant)
  - [ ] TDD: Snapshot tests for status bar (connected, disconnected, error states)
  - [ ] TDD: Key event tests (1-5 switch tabs, Tab cycles, q quits)
  - [ ] `cargo test -p signaldb-cli` passes all shell tests

  **Agent-Executed QA Scenarios:**

  ```
  Scenario: Tab navigation with keyboard
    Tool: interactive_bash (tmux)
    Preconditions: signaldb-cli built, TUI running
    Steps:
      1. tmux new-session: cargo run -p signaldb-cli -- tui --url http://localhost:3000
      2. Wait for: tab bar visible (timeout: 10s)
      3. Assert: "Dashboard" tab is highlighted (active)
      4. Send keys: "2"
      5. Assert: "Traces" tab is now highlighted
      6. Send keys: "5"
      7. Assert: "Admin" tab is highlighted (if admin key provided)
      8. Send keys: "q"
      9. Assert: process exits cleanly
    Expected Result: Tab switching works via number keys
    Evidence: Terminal output captured

  Scenario: Status bar shows connection info
    Tool: Bash (cargo test — snapshot)
    Preconditions: TestBackend terminal
    Steps:
      1. Create AppState with connection_status=Connected, url="http://localhost:3000"
      2. Render status bar
      3. Assert: snapshot contains "Connected" and "localhost:3000"
    Expected Result: Status bar displays connection info
    Evidence: Snapshot file
  ```

  **Commit**: YES
  - Message: `feat(cli): implement tab navigation shell and status bar`
  - Files: `src/signaldb-cli/src/tui/components/tabs.rs`, `status_bar.rs`, `mod.rs`, `app.rs`
  - Pre-commit: `cargo test -p signaldb-cli`

---

- [x] 9. Dashboard Tab

  **What to do**:
  - Implement `src/signaldb-cli/src/tui/components/dashboard/mod.rs`:
    - Dashboard component implementing `Component` trait
    - Layout: 2x2 grid of panels + 1 full-width panel at bottom
      ```
      ┌─────────────────┬─────────────────┐
      │ Service Health   │ WAL Status      │
      ├─────────────────┼─────────────────┤
      │ Connection Pool  │ Tenant Overview │
      ├─────────────────┴─────────────────┤
      │ System Resources                   │
      └───────────────────────────────────┘
      ```
  - Implement each panel as a sub-component:
    - `service_health.rs`: Table widget listing services — columns: Name, Status, Address, Uptime
      - Data from: `SELECT service_name, metric_value FROM _signaldb_metrics WHERE metric_name = 'service_status'`
    - `wal_status.rs`: Table widget — columns: Service, Unprocessed, Active Writers, Segments
      - Data from: `SELECT * FROM _signaldb_metrics WHERE metric_name LIKE 'wal_%'`
    - `pool_stats.rs`: Bar chart or table — columns: Service, Current, Max, Utilization%
      - Data from: `SELECT * FROM _signaldb_metrics WHERE metric_name LIKE 'flight_pool_%'`
    - `tenant_overview.rs`: Table — columns: Tenant, Datasets, Status
      - Data from admin API: `list_tenants()` (needs admin permission, hide panel for tenant users)
    - `system_resources.rs`: Sparklines for CPU + memory per service
      - Data from: `SELECT * FROM _signaldb_metrics WHERE metric_name IN ('system_cpu_usage_percent', 'system_memory_used_bytes')`
  - Background data fetching: on `Event::Tick`, spawn async tasks to query each panel's data via `FlightSqlClient`
  - Update `DashboardState` in `AppState` when data arrives via `DataEvent`
  - Handle loading states: show "Loading..." placeholder while first fetch in progress
  - Handle error states: show "Failed to load: {error}" with retry hint

  **Must NOT do**:
  - Do not fetch data synchronously — always async via background tasks
  - Do not show Tenant Overview panel for non-admin users (graceful degradation)
  - Do not panic on query errors — display error in panel

  **Recommended Agent Profile**:
  - **Category**: `visual-engineering`
    - Reason: Complex TUI layout with multiple panels, tables, sparklines
  - **Skills**: [`rust-patterns`, `flight-schemas`]
    - `rust-patterns`: Async state update patterns, error handling
    - `flight-schemas`: Understand RecordBatch → domain type conversion
  - **Skills Evaluated but Omitted**:
    - `storage-layout`: Dashboard reads data, doesn't interact with storage

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3 (with Tasks 10, 11, 12, 13)
  - **Blocks**: Task 14
  - **Blocked By**: Tasks 1, 5, 8

  **References**:

  **Pattern References**:
  - `src/signaldb-cli/src/tui/client/flight.rs` — Flight SQL client from Task 5
  - `src/signaldb-cli/src/tui/components/tabs.rs` — Component trait from Task 8

  **External References**:
  - ratatui Table widget: `https://docs.rs/ratatui/latest/ratatui/widgets/struct.Table.html`
  - ratatui Sparkline widget: `https://docs.rs/ratatui/latest/ratatui/widgets/struct.Sparkline.html`
  - ratatui Layout constraints: `https://ratatui.rs/how-to/layout/`

  **Acceptance Criteria**:

  - [ ] TDD: Snapshot tests for each panel (loaded state, loading state, error state)
  - [ ] TDD: Tests for data fetching → state update flow
  - [ ] Dashboard renders 5 panels in correct layout
  - [ ] Panels show "Loading..." on initial render
  - [ ] Non-admin users see 4 panels (Tenant Overview hidden)

  **Agent-Executed QA Scenarios:**

  ```
  Scenario: Dashboard renders with mock data
    Tool: Bash (cargo test — snapshot)
    Preconditions: TestBackend terminal, AppState with mock dashboard data
    Steps:
      1. Create AppState with sample service health, WAL stats, pool stats
      2. Render Dashboard component to TestBackend
      3. Assert: snapshot matches expected layout
      4. Assert: service names visible in health grid
      5. Assert: WAL unprocessed count visible
    Expected Result: All 5 panels render with data
    Evidence: insta snapshot file

  Scenario: Dashboard degrades for tenant user
    Tool: Bash (cargo test — snapshot)
    Preconditions: AppState with Permission::Tenant
    Steps:
      1. Create AppState with tenant permission
      2. Render Dashboard component
      3. Assert: Tenant Overview panel NOT visible
      4. Assert: remaining 4 panels render correctly
    Expected Result: 4 panels visible, no tenant overview
    Evidence: insta snapshot file
  ```

  **Commit**: YES
  - Message: `feat(cli): implement Dashboard tab with 5 monitoring panels`
  - Files: `src/signaldb-cli/src/tui/components/dashboard/`
  - Pre-commit: `cargo test -p signaldb-cli`

---

- [x] 10. Traces Tab

  **What to do**:
  - Implement `src/signaldb-cli/src/tui/components/traces/mod.rs`:
    - Traces component with two-pane layout:
      ```
      ┌──────────────────────────────────────┐
      │ Search: [tags, duration, limit]      │
      ├──────────────────┬───────────────────┤
      │ Trace List       │ Trace Detail      │
      │ (table/list)     │ (waterfall +      │
      │                  │  JSON detail)     │
      └──────────────────┴───────────────────┘
      ```
  - `traces/search_bar.rs`:
    - Input field for search parameters
    - Support Tempo search API params: tags, min_duration, max_duration, limit
    - Label as "Search" not "TraceQL" (API doesn't support TraceQL parsing)
    - Press Enter to execute search
    - Press `/` from trace list to focus search bar
  - `traces/trace_list.rs`:
    - Table widget listing trace results — columns: Trace ID, Root Service, Root Operation, Duration, Spans, Start Time
    - Data from Flight SQL: `SELECT * FROM traces WHERE ...` or Tempo search API
    - Arrow key navigation (up/down), Enter to select → show detail
    - Show trace count and result metadata
  - `traces/trace_detail.rs`:
    - Trace waterfall view using Canvas widget (from `widgets/waterfall.rs`):
      - Each span as a horizontal bar positioned by start time relative to trace start
      - Indentation by depth (parent-child hierarchy)
      - Color coding by service name
      - Duration labels on bars
    - Span detail view below waterfall:
      - When a span is selected in waterfall, show its attributes
      - Use JSON viewer widget (from `widgets/json_viewer.rs`) via `tui-tree-widget`
      - Show: service name, operation, duration, status, all attributes/tags
    - Navigation: arrow keys to select spans in waterfall, Tab to switch between waterfall and JSON view
  - Implement `src/signaldb-cli/src/tui/widgets/waterfall.rs`:
    - Custom widget rendering span bars using `ratatui::widgets::canvas::Canvas`
    - Or simpler approach: `Paragraph` with Unicode block characters (█) for bars
    - Time axis at top, span names on left
  - Implement `src/signaldb-cli/src/tui/widgets/json_viewer.rs`:
    - Wrapper around `tui-tree-widget::Tree`
    - Convert `serde_json::Value` to `TreeItem` hierarchy
    - Collapsible nodes for nested objects/arrays

  **Must NOT do**:
  - Do not implement TraceQL parser — use simple key=value tag filters
  - Do not implement trace comparison or diff views (V2)

  **Recommended Agent Profile**:
  - **Category**: `visual-engineering`
    - Reason: Complex multi-pane UI, custom waterfall widget, tree viewer — heavy visual work
  - **Skills**: [`rust-patterns`, `flight-schemas`, `tempo-api`]
    - `rust-patterns`: Widget trait implementation, Canvas drawing
    - `flight-schemas`: RecordBatch → trace/span conversion
    - `tempo-api`: Understand Tempo search API parameters and response format
  - **Skills Evaluated but Omitted**:
    - `architecture`: UI component work

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3 (with Tasks 9, 11, 12, 13)
  - **Blocks**: Task 14
  - **Blocked By**: Tasks 5, 8

  **References**:

  **Pattern References**:
  - `src/signaldb-cli/src/tui/client/flight.rs` — Flight SQL client (Task 5)
  - `src/router/src/endpoints/tempo.rs` — Tempo search API params and response format

  **External References**:
  - ratatui Canvas widget: `https://docs.rs/ratatui/latest/ratatui/widgets/canvas/struct.Canvas.html`
  - tui-tree-widget: `https://docs.rs/tui-tree-widget/latest/tui_tree_widget/`
  - Hyperqueue timeline chart: `https://github.com/It4innovations/hyperqueue/blob/main/crates/hyperqueue/src/dashboard/ui/screens/autoalloc/alloc_timeline_chart.rs`

  **Acceptance Criteria**:

  - [ ] TDD: Snapshot tests for trace list (empty, with results, selected row)
  - [ ] TDD: Snapshot tests for waterfall view with mock span data
  - [ ] TDD: Snapshot tests for JSON viewer with nested span attributes
  - [ ] TDD: Key event tests (arrow nav, Enter selects, / focuses search)
  - [ ] Waterfall correctly shows span hierarchy with indentation
  - [ ] JSON viewer expands/collapses nested objects

  **Agent-Executed QA Scenarios:**

  ```
  Scenario: Trace list renders search results
    Tool: Bash (cargo test — snapshot)
    Preconditions: Mock trace data with 3 traces
    Steps:
      1. Set TracesState with 3 mock trace results
      2. Render trace list component
      3. Assert: 3 rows visible with trace IDs
      4. Assert: first row is highlighted (selected)
    Expected Result: Trace list displays results with selection
    Evidence: insta snapshot file

  Scenario: Waterfall renders span hierarchy
    Tool: Bash (cargo test — snapshot)
    Preconditions: Mock trace with parent span + 3 child spans
    Steps:
      1. Create TraceDetail with 4 spans (1 parent, 3 children)
      2. Render waterfall widget
      3. Assert: parent span bar is widest (full duration)
      4. Assert: child spans are indented
      5. Assert: spans ordered by start time
    Expected Result: Visual hierarchy of spans
    Evidence: insta snapshot file

  Scenario: JSON viewer shows span attributes
    Tool: Bash (cargo test — snapshot)
    Preconditions: Mock span with nested attributes
    Steps:
      1. Create span with attributes: {"http.method": "GET", "http.url": "...", "nested": {"key": "val"}}
      2. Render JSON viewer
      3. Assert: tree nodes visible
      4. Assert: nested object is collapsible
    Expected Result: Attribute tree renders correctly
    Evidence: insta snapshot file
  ```

  **Commit**: YES
  - Message: `feat(cli): implement Traces tab with search, waterfall, and JSON detail`
  - Files: `src/signaldb-cli/src/tui/components/traces/`, `widgets/waterfall.rs`, `widgets/json_viewer.rs`
  - Pre-commit: `cargo test -p signaldb-cli`

---

- [x] 11. Logs Tab

  **What to do**:
  - Implement `src/signaldb-cli/src/tui/components/logs/mod.rs`:
    - Layout:
      ```
      ┌──────────────────────────────────────┐
      │ SQL Query: [editable text field]     │
      ├──────────────────────────────────────┤
      │ Log Results (table)                  │
      │ Timestamp | Severity | Body | Attrs  │
      │ ...                                  │
      ├──────────────────────────────────────┤
      │ Log Detail (JSON viewer)             │
      └──────────────────────────────────────┘
      ```
  - `logs/query_bar.rs`:
    - Editable SQL text input for querying the logs table
    - Default query: `SELECT timestamp, severity_text, body FROM logs ORDER BY timestamp DESC LIMIT 100`
    - Press Enter to execute
    - Press `/` from results to focus query bar
    - Show query execution time in status
  - `logs/log_table.rs`:
    - Table widget for log results — columns from query result RecordBatch
    - Dynamic columns based on SQL query (not hardcoded)
    - Arrow key navigation, Enter to show detail
    - Row coloring by severity (ERROR=red, WARN=yellow, INFO=default, DEBUG=dim)
  - `logs/log_detail.rs`:
    - JSON viewer (reuse `widgets/json_viewer.rs` from Task 10) showing full log record
    - All attributes, resource attributes, body
  - Data access: Flight SQL via `FlightSqlClient.query_sql()`
  - Auto-refresh on Tick event with the configured refresh rate (re-run last query)

  **Must NOT do**:
  - Do not implement live log tailing/streaming — V1 is poll-based via Flight SQL
  - Do not hardcode column names — support whatever the SQL query returns

  **Recommended Agent Profile**:
  - **Category**: `visual-engineering`
    - Reason: Table rendering, text input, dynamic columns
  - **Skills**: [`rust-patterns`, `flight-schemas`]
    - `rust-patterns`: Dynamic table rendering from RecordBatch
    - `flight-schemas`: Understand log table schema
  - **Skills Evaluated but Omitted**:
    - `tempo-api`: Logs don't use Tempo API

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3 (with Tasks 9, 10, 12, 13)
  - **Blocks**: Task 14
  - **Blocked By**: Tasks 1, 5, 8

  **References**:

  **Pattern References**:
  - `src/signaldb-cli/src/commands/query.rs` — Existing SQL query + Arrow → table display code
  - `src/signaldb-cli/src/tui/widgets/json_viewer.rs` — Reuse from Task 10

  **API/Type References**:
  - `src/common/src/flight/schema.rs` — Log table schema definition

  **Acceptance Criteria**:

  - [ ] TDD: Snapshot tests for log table (empty, with results, severity coloring)
  - [ ] TDD: Tests for SQL query execution → table update
  - [ ] TDD: Tests for dynamic column rendering from RecordBatch
  - [ ] Log detail JSON viewer shows full record on Enter

  **Agent-Executed QA Scenarios:**

  ```
  Scenario: Log table renders query results
    Tool: Bash (cargo test — snapshot)
    Preconditions: Mock RecordBatch with 5 log rows
    Steps:
      1. Create LogsState with mock RecordBatch
      2. Render log table
      3. Assert: 5 rows visible
      4. Assert: columns match RecordBatch schema
    Expected Result: Dynamic table renders correctly
    Evidence: insta snapshot file

  Scenario: Severity coloring applied
    Tool: Bash (cargo test — snapshot)
    Preconditions: Mock logs with ERROR, WARN, INFO severity
    Steps:
      1. Create RecordBatch with mixed severities
      2. Render log table
      3. Assert: ERROR row has red style
      4. Assert: WARN row has yellow style
    Expected Result: Severity colors applied
    Evidence: insta snapshot file
  ```

  **Commit**: YES
  - Message: `feat(cli): implement Logs tab with Flight SQL query interface`
  - Files: `src/signaldb-cli/src/tui/components/logs/`
  - Pre-commit: `cargo test -p signaldb-cli`

---

- [x] 12. Metrics Tab

  **What to do**:
  - Implement `src/signaldb-cli/src/tui/components/metrics/mod.rs`:
    - Layout:
      ```
      ┌──────────────────────────────────────┐
      │ SQL Query: [editable text field]     │
      ├──────────────────────────────────────┤
      │ Metric Sparklines                    │
      │ cpu_usage:    ▁▃▅▇█▆▄▂             │
      │ memory_used:  ▂▂▃▃▅▆▇█             │
      │ wal_backlog:  █▇▅▃▂▁▁▁             │
      ├──────────────────────────────────────┤
      │ Raw Data Table                       │
      └──────────────────────────────────────┘
      ```
  - `metrics/query_bar.rs`:
    - SQL text input for metrics queries
    - Default query: `SELECT timestamp, metric_name, metric_value FROM _signaldb_metrics ORDER BY timestamp DESC LIMIT 500`
    - Pre-built query shortcuts (keybinds):
      - `F1`: System CPU/memory overview
      - `F2`: WAL stats over time
      - `F3`: Connection pool utilization
  - `metrics/sparkline_panel.rs`:
    - Group time-series data by metric_name
    - Render one `ratatui::widgets::Sparkline` per metric
    - Automatically scale to available height
    - Show metric name + current value + min/max labels
    - Use `widgets/sparkline.rs` wrapper for consistent styling
  - `metrics/data_table.rs`:
    - Raw data table below sparklines (similar to Logs tab table)
    - Dynamic columns from query result
  - Implement `src/signaldb-cli/src/tui/widgets/sparkline.rs`:
    - Wrapper around `ratatui::widgets::Sparkline`
    - Helper to convert `Vec<f64>` → `Vec<u64>` (Sparkline only accepts u64)
    - Auto-scaling based on min/max values
    - Configurable direction (right-to-left for time series)

  **Must NOT do**:
  - Do not implement complex charting (no line charts, area charts) — sparklines only for V1
  - Do not try to render the non-functional Tempo metrics API — use Flight SQL directly

  **Recommended Agent Profile**:
  - **Category**: `visual-engineering`
    - Reason: Sparkline rendering, dynamic layout based on metric count
  - **Skills**: [`rust-patterns`, `flight-schemas`]
    - `rust-patterns`: Data transformation (RecordBatch → sparkline data points)
    - `flight-schemas`: Metrics table schema
  - **Skills Evaluated but Omitted**:
    - `tempo-api`: Not using Tempo metrics API (it's stubbed)

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3 (with Tasks 9, 10, 11, 13)
  - **Blocks**: Task 14
  - **Blocked By**: Tasks 1, 5, 8

  **References**:

  **Pattern References**:
  - `src/signaldb-cli/src/tui/components/logs/query_bar.rs` — Reuse SQL input pattern from Task 11
  - Self-monitoring schema from Task 1 — defines metric names and structure

  **External References**:
  - ratatui Sparkline widget: `https://docs.rs/ratatui/latest/ratatui/widgets/struct.Sparkline.html`
  - graelo/pumas sparkline usage: `https://github.com/graelo/pumas/blob/main/src/ui/tab_overview.rs`

  **Acceptance Criteria**:

  - [ ] TDD: Snapshot tests for sparkline panel (single metric, multiple metrics)
  - [ ] TDD: Tests for f64 → u64 scaling in sparkline wrapper
  - [ ] TDD: Tests for metric grouping (group by metric_name)
  - [ ] F1/F2/F3 shortcut queries execute correctly

  **Agent-Executed QA Scenarios:**

  ```
  Scenario: Sparklines render time-series data
    Tool: Bash (cargo test — snapshot)
    Preconditions: Mock data with 3 metrics, 50 data points each
    Steps:
      1. Create MetricsState with grouped metric data
      2. Render sparkline panel
      3. Assert: 3 sparklines visible
      4. Assert: metric names displayed as labels
    Expected Result: Sparklines render with labels
    Evidence: insta snapshot file

  Scenario: Sparkline scaling handles large values
    Tool: Bash (cargo test)
    Preconditions: Values ranging from 0 to 1_000_000
    Steps:
      1. Pass large values to sparkline wrapper
      2. Assert: converted u64 values are proportionally scaled
      3. Assert: no overflow or panic
    Expected Result: Scaling works for large ranges
    Evidence: Test output captured
  ```

  **Commit**: YES
  - Message: `feat(cli): implement Metrics tab with sparklines and Flight SQL`
  - Files: `src/signaldb-cli/src/tui/components/metrics/`, `widgets/sparkline.rs`
  - Pre-commit: `cargo test -p signaldb-cli`

---

- [x] 13. Admin Tab

  **What to do**:
  - Implement `src/signaldb-cli/src/tui/components/admin/mod.rs`:
    - Admin component only rendered when `Permission::Admin`
    - Sub-tab navigation within Admin: `[T]enants  [K]eys  [D]atasets`
    - Layout:
      ```
      ┌──────────────────────────────────────┐
      │ Admin: [T]enants  [K]eys  [D]atasets │
      ├──────────────────┬───────────────────┤
      │ List (table)     │ Detail/Form       │
      │                  │                   │
      └──────────────────┴───────────────────┘
      ```
  - `admin/tenants.rs`:
    - Left pane: tenant list table — columns: ID, Name, Default Dataset, Created
    - Right pane: selected tenant detail or create/edit form
    - Actions: `c` to create new, `Enter` to view detail, `e` to edit, `d` to delete
    - Create form: text inputs for id, name, default_dataset
    - Data from `AdminClient.list_tenants()`
  - `admin/api_keys.rs`:
    - Left pane: API key list for selected tenant — columns: Name, Key (masked), Created
    - Actions: `c` to create new, `d` to revoke
    - Create form: text input for key name
    - Data from `AdminClient.list_api_keys(tenant_id)`
  - `admin/datasets.rs`:
    - Left pane: dataset list for selected tenant — columns: ID, Is Default, Created
    - Actions: `c` to create new, `d` to delete
    - Data from `AdminClient.list_datasets(tenant_id)`
  - **Confirmation dialogs** (CRITICAL for destructive operations):
    - Implement `src/signaldb-cli/src/tui/components/admin/confirm_dialog.rs`
    - Modal overlay that asks "Type '{resource_name}' to confirm deletion"
    - Text input must match exactly before delete executes
    - Cancel with `Esc`
  - Text input widget: reusable text input component for forms and confirmations
    - Cursor movement, backspace, character insertion
    - Single-line input with visible cursor

  **Must NOT do**:
  - Do not show Admin tab at all for non-admin users (tab should not appear)
  - Do not allow delete without confirmation dialog
  - Do not show full API key values — mask with `sk-****-last4`

  **Recommended Agent Profile**:
  - **Category**: `visual-engineering`
    - Reason: Forms, modals, table selection, sub-tab navigation — complex interactive UI
  - **Skills**: [`rust-patterns`, `multi-tenancy`]
    - `rust-patterns`: Form state management, modal overlay rendering
    - `multi-tenancy`: Understand tenant/dataset/API key relationships and constraints
  - **Skills Evaluated but Omitted**:
    - `architecture`: Admin is a CRUD UI, not system architecture

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3 (with Tasks 9, 10, 11, 12)
  - **Blocks**: Task 14
  - **Blocked By**: Tasks 6, 8

  **References**:

  **Pattern References**:
  - `src/signaldb-cli/src/tui/client/admin.rs` — Admin API client from Task 6
  - `src/signaldb-cli/src/commands/tenant.rs` — Existing tenant operations (field names, validation)
  - `src/signaldb-cli/src/commands/api_key.rs` — API key operations
  - `src/signaldb-cli/src/commands/dataset.rs` — Dataset operations

  **API/Type References**:
  - `src/router/src/endpoints/admin.rs:18-155` — Admin API request/response shapes

  **Acceptance Criteria**:

  - [ ] TDD: Snapshot tests for tenant list, API key list, dataset list
  - [ ] TDD: Snapshot tests for create form, edit form, confirmation dialog
  - [ ] TDD: Tests for confirmation dialog (type-to-confirm logic)
  - [ ] TDD: Tests for API key masking
  - [ ] Admin tab hidden for non-admin users (no rendering, no key handler)
  - [ ] Delete confirmation requires exact name match

  **Agent-Executed QA Scenarios:**

  ```
  Scenario: Tenant list renders with CRUD actions
    Tool: Bash (cargo test — snapshot)
    Preconditions: Mock admin client returns 3 tenants
    Steps:
      1. Create AdminState with 3 tenants
      2. Render admin tenants component
      3. Assert: 3 rows visible
      4. Assert: action hints visible (c: Create, d: Delete, Enter: Detail)
    Expected Result: Tenant list with action hints
    Evidence: insta snapshot file

  Scenario: Delete confirmation dialog blocks without match
    Tool: Bash (cargo test)
    Preconditions: Confirmation dialog for tenant "acme"
    Steps:
      1. Open confirmation dialog for "acme"
      2. Type "wrong"
      3. Press Enter
      4. Assert: dialog stays open (delete NOT executed)
      5. Type "acme"
      6. Press Enter
      7. Assert: dialog closes, delete callback invoked
    Expected Result: Type-to-confirm prevents accidental deletion
    Evidence: Test output captured

  Scenario: API keys are masked
    Tool: Bash (cargo test — snapshot)
    Preconditions: Mock API key "sk-acme-prod-key-123"
    Steps:
      1. Render API key list with mock key
      2. Assert: displayed as "sk-****-y-123" or similar mask
      3. Assert: full key never visible
    Expected Result: Key values masked in display
    Evidence: insta snapshot file
  ```

  **Commit**: YES
  - Message: `feat(cli): implement Admin tab with tenant/key/dataset CRUD and confirmations`
  - Files: `src/signaldb-cli/src/tui/components/admin/`
  - Pre-commit: `cargo test -p signaldb-cli`

---

- [x] 14. Integration, Polish, and Final Verification

  **What to do**:
  - Wire all tabs together in `app.rs`:
    - Ensure tab switching preserves per-tab state
    - Ensure background data tasks are cancelled when switching tabs (avoid stale updates)
    - Ensure `Event::Tick` triggers refresh for active tab only (not all tabs)
  - Error handling polish:
    - Status bar shows last error with timestamp
    - Connection lost → all panels show "Disconnected" + auto-retry on next tick
    - Auth errors → show "Authentication failed" + suggest checking API key
  - Keyboard shortcut documentation:
    - `?` key opens help overlay listing all shortcuts
    - Help overlay: modal with keybind reference
  - Run full verification suite:
    - `cargo build --workspace` — no breakage
    - `cargo test -p signaldb-cli` — all TUI tests pass
    - `cargo clippy --workspace --all-targets -- -D warnings` — clean
    - `cargo machete --with-metadata` — no unused deps
    - `cargo fmt --check` — formatted
  - Review all snapshot files (insta) for visual correctness
  - Ensure `--help` text is accurate and complete

  **Must NOT do**:
  - Do not add new features — this is integration and polish only
  - Do not refactor working code unless it causes integration issues

  **Recommended Agent Profile**:
  - **Category**: `deep`
    - Reason: Integration testing, edge case hunting, cross-component interaction verification
  - **Skills**: [`rust-patterns`, `dev-workflow`]
    - `rust-patterns`: Error handling patterns, clean shutdown
    - `dev-workflow`: Pre-commit verification (clippy, fmt, machete, tests)
  - **Skills Evaluated but Omitted**:
    - `architecture`: Integration is within the TUI, not system-wide

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Sequential (Wave 4 — final task)
  - **Blocks**: None (final)
  - **Blocked By**: Tasks 9, 10, 11, 12, 13

  **References**:

  **Pattern References**:
  - All previous task outputs
  - `src/signaldb-cli/src/tui/app.rs` — Main app orchestration

  **Acceptance Criteria**:

  - [ ] `cargo build --workspace` succeeds
  - [ ] `cargo test -p signaldb-cli` passes ALL tests (0 failures)
  - [ ] `cargo clippy --workspace --all-targets -- -D warnings` is clean
  - [ ] `cargo machete --with-metadata` shows no unused deps
  - [ ] `cargo fmt --check` passes
  - [ ] `?` key shows help overlay with all keybinds documented
  - [ ] Tab switching preserves state (switch away and back, data still there)
  - [ ] Connection failure shows graceful error in all tabs

  **Agent-Executed QA Scenarios:**

  ```
  Scenario: Full build and lint verification
    Tool: Bash
    Preconditions: All previous tasks completed
    Steps:
      1. cargo build --workspace
      2. Assert: exit code 0
      3. cargo test -p signaldb-cli
      4. Assert: all tests pass, exit code 0
      5. cargo clippy --workspace --all-targets -- -D warnings
      6. Assert: exit code 0
      7. cargo machete --with-metadata
      8. Assert: no unused dependencies reported
      9. cargo fmt --check
      10. Assert: exit code 0
    Expected Result: All quality gates pass
    Evidence: All command outputs captured

  Scenario: TUI launches and shows all tabs
    Tool: interactive_bash (tmux)
    Preconditions: SignalDB running locally (or mock), admin key configured
    Steps:
      1. tmux new-session: cargo run -p signaldb-cli -- tui --admin-key test-key
      2. Wait for: tab bar visible (timeout: 15s)
      3. Assert: 5 tabs visible (Dashboard, Traces, Logs, Metrics, Admin)
      4. Send keys: "1" through "5" sequentially
      5. Assert: each tab renders without panic
      6. Send keys: "?"
      7. Assert: help overlay appears with keybind list
      8. Send keys: Esc
      9. Assert: help overlay closes
      10. Send keys: "q"
      11. Assert: clean exit
    Expected Result: All tabs functional, help overlay works
    Evidence: Terminal output captured

  Scenario: Graceful handling when services are down
    Tool: interactive_bash (tmux)
    Preconditions: No SignalDB services running
    Steps:
      1. tmux new-session: cargo run -p signaldb-cli -- tui --url http://localhost:99999
      2. Wait for: TUI renders (timeout: 10s)
      3. Assert: status bar shows "Disconnected" or "Connection Failed"
      4. Assert: no panic, TUI remains responsive
      5. Send keys: "q"
      6. Assert: clean exit
    Expected Result: Graceful degradation, no crash
    Evidence: Terminal output captured
  ```

  **Commit**: YES
  - Message: `feat(cli): integrate all TUI tabs with polish and help overlay`
  - Files: `src/signaldb-cli/src/tui/app.rs`, help overlay component
  - Pre-commit: `cargo build --workspace && cargo test -p signaldb-cli && cargo clippy --workspace --all-targets -- -D warnings`

---

## Commit Strategy

| After Task | Message | Key Files | Verification |
|------------|---------|-----------|--------------|
| 1 | `feat(common): add self-monitoring infrastructure` | `src/common/src/self_monitoring/` | `cargo test -p common` |
| 2 | `feat(cli): scaffold TUI module structure and deps` | `src/signaldb-cli/src/tui/` | `cargo build -p signaldb-cli` |
| 3 | `test(cli): add TUI test infrastructure` | `src/signaldb-cli/Cargo.toml` (dev-deps) | `cargo test -p signaldb-cli` |
| 4 | `feat(cli): implement TUI event loop` | `tui/terminal.rs`, `event.rs`, `action.rs` | `cargo test -p signaldb-cli` |
| 5 | `feat(cli): add Flight SQL client for TUI` | `tui/client/flight.rs` | `cargo test -p signaldb-cli` |
| 6 | `feat(cli): add HTTP admin API client for TUI` | `tui/client/admin.rs` | `cargo test -p signaldb-cli` |
| 7 | `feat(cli): implement permission detection` | `tui/state.rs` | `cargo test -p signaldb-cli` |
| 8 | `feat(cli): implement tab navigation and status bar` | `tui/components/tabs.rs`, `status_bar.rs` | `cargo test -p signaldb-cli` |
| 9 | `feat(cli): implement Dashboard tab` | `tui/components/dashboard/` | `cargo test -p signaldb-cli` |
| 10 | `feat(cli): implement Traces tab` | `tui/components/traces/`, `widgets/` | `cargo test -p signaldb-cli` |
| 11 | `feat(cli): implement Logs tab` | `tui/components/logs/` | `cargo test -p signaldb-cli` |
| 12 | `feat(cli): implement Metrics tab` | `tui/components/metrics/` | `cargo test -p signaldb-cli` |
| 13 | `feat(cli): implement Admin tab` | `tui/components/admin/` | `cargo test -p signaldb-cli` |
| 14 | `feat(cli): TUI integration and polish` | `tui/app.rs`, help overlay | full suite |

---

## Success Criteria

### Verification Commands
```bash
# Build succeeds
cargo build --workspace

# All tests pass
cargo test -p signaldb-cli

# Clippy clean
cargo clippy --workspace --all-targets -- -D warnings

# No unused deps
cargo machete --with-metadata

# TUI help shows
cargo run -p signaldb-cli -- tui --help

# Format check
cargo fmt --check
```

### Final Checklist
- [ ] All 14 tasks completed with passing tests
- [ ] All "Must Have" items present
- [ ] All "Must NOT Have" items absent
- [ ] All 5 tabs render correctly
- [ ] Permission detection works (admin vs tenant)
- [ ] Self-monitoring writes metrics to internal tables
- [ ] TUI gracefully handles disconnected services
- [ ] Destructive admin operations require confirmation
- [ ] Terminal properly restored on exit (including panics)
- [ ] All insta snapshots reviewed and accepted
