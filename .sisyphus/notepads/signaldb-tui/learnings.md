
## Task 8: Tab Navigation Shell & Status Bar

### Patterns
- `INSTA_UPDATE=always cargo test` accepts all new snapshots without cargo-insta installed
- Status bar 3-column layout uses `Constraint::Percentage` — ensure test terminal is wide enough (120+ cols) to avoid truncation
- `ratatui::widgets::Tabs` aliased as `RatatuiTabs` to avoid conflict with `state::Tab`
- Component trait has 3 methods: handle_key_event/update/render — only render is called currently
- `#[allow(dead_code)]` needed on Component trait since handle_key_event/update aren't called until tab content components exist

### Key Decisions
- Tab bar uses `Block::default().borders(Borders::ALL).title("SignalDB")` for visual structure
- Status bar splits into 40/20/40 percent columns (left: connection, center: permission, right: keybinds)
- App owns `AppState`, `TabBar`, and `StatusBar` directly (not boxed trait objects)
- Added `NextTab`/`PrevTab` actions mapped to Tab/BackTab keys for cycling
- Module-level `#[allow(dead_code)]` on `state` module since many items only used from tests currently

### Files Modified
- `components/mod.rs` — Component trait definition
- `components/tabs.rs` — TabBar with ratatui::widgets::Tabs
- `components/status_bar.rs` — 3-column status bar
- `app.rs` — 3-row Layout, AppState ownership, SwitchTab/NextTab/PrevTab wiring
- `action.rs` — NextTab/PrevTab variants + Tab/BackTab key mappings
- `mod.rs` — Removed dead_code allow on components/state, re-added on state

## Task 9: Dashboard Tab Implementation

### Patterns Used
- Each dashboard panel follows the same tri-state pattern: `Loading | Loaded(data) | Error(msg)`
- All panels implement `Component` trait with `handle_key_event`, `update`, `render`
- `set_data()` and `set_error()` methods provide the data-loading interface
- Static `query()` methods return the SQL queries for each panel
- Admin-only visibility via `TenantOverviewPanel::is_visible(state)` + `TenantPlaceholderPanel` fallback

### Layout Structure
- 2x2+1 grid: `Constraint::Percentage(40/40/20)` vertical, `Percentage(50/50)` horizontal
- Top row: Service Health (cyan border) + WAL Status (yellow border)
- Middle row: Connection Pool (magenta border) + Tenant Overview (green/gray border)
- Bottom row: System Resources (blue border) - full width with CPU/Memory sparklines

### Key Decisions
- Used `ratatui::widgets::Sparkline` directly (not the custom sparkline widget stub) for CPU/memory
- Each panel extracts f64 values polymorphically from Float64/Int64/UInt64 Arrow arrays
- WAL values use human-readable formatting (K/M suffixes)
- System resources reverses query results (DESC → oldest-first for sparkline rendering)

### Testing
- 94 tests total, 12 insta snapshots for dashboard components
- `cargo insta` CLI not installed; used `find ... mv` to rename `.snap.new` → `.snap`
- Clippy caught `3.14` as `approx_constant` in test - changed to `3.17`
- `#[allow(dead_code)]` on dashboard module matches project pattern for scaffolded components

## Task 11: Logs Tab Implementation

### Patterns
- `LogTable::render` requires `&mut self` for `TableState` — Component trait's `render` takes `&self`, solved by cloning `LogTable` fields for rendering
- `TableState` implements `Copy` so don't use `.clone()` on it (clippy catches it)
- Default query text (83 chars) doesn't fit in 80-col terminal with borders — use 120+ cols for tests involving full query display
- `severity_col_idx` field needed `pub(crate)` visibility for `mod.rs` to construct `LogTable` clone
- `arrow::util::display::ArrayFormatter` provides generic fallback for unsupported column types
- Snapshot tests created via `INSTA_UPDATE=always` without needing `cargo-insta` CLI

### Key Decisions
- 3-row layout: QueryBar (Length(3)), LogTable (Min(0)), LogDetail (Length(10) when visible, Length(0) when hidden)
- Focus state enum: `QueryBar` vs `Table` — `/` key focuses query bar, Esc returns to table
- `pending_query: Option<String>` pattern for poll-based query dispatch (taken by app on tick)
- Detail panel toggles on Enter rather than always showing — saves screen space
- Severity coloring: ERROR/FATAL/CRITICAL=red, WARN/WARNING=yellow, DEBUG/TRACE=dark gray, INFO=default
- Dynamic columns: uses `format_cell()` that handles Utf8, LargeUtf8, Int64, UInt64, Float64, Boolean, Int32, UInt32, Timestamp, Date32 types with fallback to Arrow's display formatter

### Files Created
- `components/logs/mod.rs` — LogsPanel composing sub-components, implements Component trait
- `components/logs/query_bar.rs` — QueryBar with cursor tracking, Emacs-style keybindings (Ctrl-A/E/U/K)
- `components/logs/log_table.rs` — LogTable with dynamic columns, severity coloring, TableState scrolling
- `components/logs/log_detail.rs` — LogDetail rendering key-value pairs for selected row

### Test Count
- 142 total tests (up from 94 in task 9)
- 7 new snapshot tests for logs components

## Task 12: Metrics Tab with Sparklines

### Patterns
- Reused `QueryBar` and `LogTable` from logs tab directly (no code duplication)
- `LogTable` clone-for-render pattern same as logs/mod.rs — clone fields to work around `&mut self` vs `&self`
- `BTreeMap` used for sparkline data to maintain sorted metric names (deterministic render order)
- `widgets/sparkline.rs` existed as empty stub — filled with `scale_to_u64` function
- F-key shortcuts handled via `KeyCode::F(1)`, `KeyCode::F(2)`, `KeyCode::F(3)` in match arms

### Key Decisions
- 3-row layout: QueryBar(Length(3)), Sparklines(Min(10) or Length(3) when empty), Table(Min(0))
- `scale_to_u64` maps f64 → 0..100 u64 range; all-same values → flat line at 50; handles NaN/Inf
- Sparkline colors cycle through Cyan/Magenta/Green/Yellow/Blue/Red per metric
- `extract_sparkline_data` reverses values (DESC query → oldest-first for sparkline display)
- `extract_f64` duplicated from system_resources.rs — same polymorphic Float64/Int64/UInt64 extraction

### Files Modified/Created
- `widgets/sparkline.rs` — `scale_to_u64` with 9 unit tests
- `components/metrics/mod.rs` — MetricsPanel with 3-row layout, F1/F2/F3 shortcuts, 22 tests + 3 snapshots
- `components/mod.rs` — Added `pub mod metrics` with `#[allow(dead_code)]`

### Test Count
- 172 total tests (up from 142 in task 11)
- 3 new snapshot tests for metrics components
- 9 new unit tests for sparkline scaling

## Task 13: Admin Tab with Tenant/Key/Dataset CRUD

### Patterns
- Borrow checker: when `selected_tenant()` borrows `self`, must `.to_string()` values before calling `self.id_input.set_text()` — immutable borrow from lookup conflicts with mutable set_text
- Collapsible-if: Clippy catches nested `if let Some(...) { if !x.is_empty() { } }` — use `if let Some(...) && !x.is_empty() {}`
- API key masking takes last 4 chars literally — `sk-acme-staging-456` → last 4 = `-456` → `sk-****--456` (double hyphen is correct)
- `Clear` widget from ratatui used to blank area under modal overlay before rendering dialog
- Sub-tab navigation uses uppercase letters (T/K/D) to avoid conflicts with CRUD keys (c/d/e)
- `DeleteContext` enum tracks which sub-tab initiated deletion so confirm dialog can dispatch correctly
- `pending_action: Option<XxxAction>` pattern used by all sub-tabs — parent polls and takes action

### Key Decisions
- Sub-tab bar uses `ratatui::widgets::Tabs` with underlined first letter (T, K, D)
- Two-pane layout: 50/50 horizontal split (list left, detail/form right)
- Confirm dialog: centered modal overlay with red border, type-to-confirm pattern
- TextInput widget is standalone in widgets/ for reuse across multiple forms
- Admin panel only renders when `Permission::Admin` — returns immediately otherwise
- API keys and datasets auto-sync to selected tenant when switching sub-tabs

### Files Created
- `widgets/text_input.rs` — Reusable text input with cursor, Emacs keybindings
- `components/admin/confirm_dialog.rs` — Type-to-confirm deletion modal
- `components/admin/tenants.rs` — Tenant CRUD with list/detail/create/edit views
- `components/admin/api_keys.rs` — API key list with create/revoke, masked display
- `components/admin/datasets.rs` — Dataset list with create/delete
- `components/admin/mod.rs` — Admin panel composing sub-tabs with Component trait

### Test Count
- 261 total tests (up from 172 in task 12)
- 89 new tests for admin components
- 17 new snapshot tests across admin sub-tabs and text input widget

## Task 14: Integration, Polish, and Final Verification

### Patterns
- Keep each tab component as an owned field in `App` so tab switching naturally preserves per-tab state.
- Route `Event::Tick` through an active-tab match to avoid unnecessary refresh work in inactive tabs.
- For modal overlays, render after main content and status bar, and use `ratatui::widgets::Clear` before drawing the modal block.
- Centralize error-to-UI mapping in app-level helpers (`flight_error_message`, `admin_error_message`, `set_active_tab_error`) to keep tab refresh logic simple.
- Add `required-features` to bench targets in crate manifests when clippy runs with `--all-targets`, so optional benchmark deps don't break CI.

### Key Decisions
- Added a dedicated `HelpOverlay` component with `Action::ToggleHelp` triggered by `?`; when help is open, key handling is short-circuited to the overlay.
- Implemented active-tab-only refresh paths: dashboard panel queries, logs query execution, metrics query execution, and admin CRUD/refresh loop.
- Extended `AppState` with `last_error_at` plus `set_error`/`clear_error`, and surfaced timestamped errors in the status bar.
- Used a unified auth error message (`Authentication failed - check API key`) across Flight and Admin failures.
- Wired admin tab background behavior via `AdminPanel::refresh()` to process pending CRUD actions and reload tenants/keys/datasets.

### Files Modified
- `src/signaldb-cli/src/tui/app.rs`
- `src/signaldb-cli/src/tui/action.rs`
- `src/signaldb-cli/src/tui/state.rs`
- `src/signaldb-cli/src/tui/components/mod.rs`
- `src/signaldb-cli/src/tui/components/help.rs` (new)
- `src/signaldb-cli/src/tui/components/status_bar.rs`
- `src/signaldb-cli/src/tui/components/admin/mod.rs`
- `src/signaldb-cli/src/tui/components/admin/tenants.rs`
- `src/signaldb-cli/src/tui/components/admin/api_keys.rs`
- `src/signaldb-cli/src/tui/components/admin/datasets.rs`
- `src/signaldb-cli/src/tui/components/dashboard/tenant_overview.rs`
- `src/signaldb-cli/src/tui/components/logs/mod.rs`
- `src/signaldb-cli/src/tui/components/logs/log_table.rs`
- `src/signaldb-cli/src/tui/client/mod.rs`
- `src/signaldb-cli/src/tui/client/admin.rs`
- `src/writer/Cargo.toml`
- Snapshot updates:
  - `src/signaldb-cli/src/tui/snapshots/signaldb_cli__tui__app__tests__app_full_layout.snap`
  - `src/signaldb-cli/src/tui/components/snapshots/signaldb_cli__tui__components__status_bar__tests__status_bar_connected.snap`
  - `src/signaldb-cli/src/tui/components/snapshots/signaldb_cli__tui__components__status_bar__tests__status_bar_disconnected.snap`
  - `src/signaldb-cli/src/tui/components/snapshots/signaldb_cli__tui__components__status_bar__tests__status_bar_error.snap`

### Test Count
- `cargo test -p signaldb-cli`: 266 passed, 0 failed.

## Task 10: Traces Tab with Waterfall Visualization

### Patterns
- Clone-for-render pattern: `TreeState` doesn't impl Clone, use `TreeState::default()` for render clones
- Waterfall bar width must account for label + bar + duration label to avoid ratatui clipping
- `tui-tree-widget` 0.24: `Tree::new(&items)` returns Result, identifiers must be unique among siblings
- `json_to_tree_items` recursive conversion with unique `"{key}_{idx}"` identifiers to avoid collisions

### Key Decisions
- Unicode block chars (█) instead of Canvas widget for waterfall — simpler, no additional dependencies
- Simple DSL parser for search params (`tags=x minDuration=y limit=z`) rather than full TraceQL
- Three-focus model: SearchBar / TraceList / TraceDetail with `/` always accessible
- Detail view uses clone approach like other tabs to work around `&self` render + `&mut self` state

### Files Created
- `src/signaldb-cli/src/tui/widgets/waterfall.rs` — WaterfallSpan, build_waterfall_spans, render_waterfall
- `src/signaldb-cli/src/tui/widgets/json_viewer.rs` — json_to_tree_items, render_json_tree
- `src/signaldb-cli/src/tui/components/traces/mod.rs` — TracesPanel with Component trait
- `src/signaldb-cli/src/tui/components/traces/search_bar.rs` — TraceSearchBar with DSL parsing
- `src/signaldb-cli/src/tui/components/traces/trace_list.rs` — TraceList with tri-state data model
- `src/signaldb-cli/src/tui/components/traces/trace_detail.rs` — TraceDetailView with waterfall + JSON

### Files Modified
- `src/signaldb-cli/src/tui/components/mod.rs` — Added `pub mod traces`
- `src/signaldb-cli/src/tui/app.rs` — Wired TracesPanel into App struct, handle_key, update, render, refresh

### Test Count
- 344 tests total (was 266 before traces, +78 new trace-related tests)
- 15 snapshot tests created for traces components
- Clippy clean with -D warnings
