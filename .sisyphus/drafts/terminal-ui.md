# Draft: Terminal UI for SignalDB

## Research Findings

### Existing CLI Infrastructure
- **CLI Framework**: Clap 4.5 with derive + env features
- **Dedicated CLI crate**: `src/signaldb-cli/` — management CLI for tenant/api_key/dataset/query operations
- **Shared CLI utilities**: `src/common/src/cli.rs` — CommonArgs, CommonCommands
- **No TUI dependencies**: Zero ratatui/crossterm/tui code exists
- **All services are headless**: No interactive dashboards currently

### Available Data Sources for TUI
- **Health Checks**: `/health` on Router (3000) and Acceptor (4318)
- **Service Discovery**: ServiceRegistry with capability-based routing
- **Flight Pool Stats**: Connection counts, pool sizes
- **WAL Status**: Unprocessed entries, segment counts, processing rate
- **WAL Processor Stats**: Active writer counts
- **Admin API**: Tenant, dataset, API key CRUD operations
- **Tempo API**: Trace/log/metric queries
- **Multi-tenant views**: Per-tenant WAL isolation, dataset counts

## Requirements (confirmed)
- **Purpose**: All-in-one TUI — dashboard + query explorer + admin console
- **Permission-aware**: Feature visibility depends on user permissions (admin vs regular user)
- **Target users**: 
  - SREs/operators monitoring SignalDB clusters
  - Developers working with SignalDB locally
  - End users querying observability data (traces, logs, metrics)
- **Crate location**: Extend existing `signaldb-cli` with a `tui` subcommand

## Technical Decisions
- Extend `src/signaldb-cli/` rather than create new crate
- Permission-based view system (admin features gated behind auth)
- **Framework**: ratatui + crossterm (de-facto standard)
- **Navigation**: Tab-based with keyboard shortcuts (1-5 or similar)
- **Auth**: API key via signaldb.toml or SIGNALDB_API_KEY env var, auto-detect permission level
- **Scope**: Full vision from start — all tabs in single plan
- **Tabs**: Dashboard, Traces, Logs, Metrics, Admin

## KEY ARCHITECTURAL DECISION: Flight SQL as universal data interface + OTLP self-monitoring
- ALL data access via Flight SQL — traces, logs, metrics, AND system status
- No bespoke HTTP status endpoints needed
- **Self-monitoring via OTLP (dogfooding)**:
  - Services use OpenTelemetry SDK to instrument themselves
  - Services export their telemetry via OTLP to the Acceptor (same protocol as external clients)
  - Normal write path processes it: Acceptor → WAL → Writer → Iceberg tables
  - Dedicated internal tenant (`_system`) isolates self-monitoring data
  - Zero new write-path code — just OTLP client configuration in each service
  - `signal-producer` crate already demonstrates the OpenTelemetry SDK pattern
- TUI queries everything through one unified Flight SQL interface
- This is a "meta monitoring stack" — SignalDB monitors itself via its own pipeline

## Dashboard Panels (confirmed — ALL via Flight SQL over self-monitoring data)
1. Service Health Grid — from self-monitoring metrics
2. WAL Status — from self-monitoring metrics
3. Connection Pool Stats — from self-monitoring metrics
4. Tenant Overview — from self-monitoring metrics
5. System Resources — from self-monitoring metrics (sysinfo crate or similar)

## Admin Tab
- Full CRUD: create, read, update, delete tenants/datasets/API keys (still via HTTP admin API)

## Refresh Rate
- Configurable, default 5s

## Data Viewer Features (confirmed — ALL of these)
1. TraceQL search bar — query input for trace filtering
2. Trace waterfall view — span hierarchy as timeline diagram
3. Log browsing via Flight SQL — query logs table (no live tail V1)
4. Metric sparklines — inline charts for time series via Flight SQL
5. JSON detail pane — expand any item to see raw attributes

## Color Scheme
- Terminal-native: Use 16 ANSI colors, respect user's terminal theme

## Test Strategy
- **TDD**: Red-green-refactor for all components
- Test layers: API client, state management, rendering logic
- Need to check if test infra exists in signaldb-cli

## Open Questions (minor — can be decided during implementation)
- Keyboard shortcut scheme details (Tab, 1-5, /, :, etc.)
- Error handling UX: status bar messages vs popup dialogs

## Scope Boundaries
- INCLUDE: 
  - Self-monitoring infrastructure (services emit own telemetry)
  - All 5 tabs (Dashboard, Traces, Logs, Metrics, Admin)
  - All dashboard panels via self-monitoring + Flight SQL
  - Full CRUD admin via HTTP admin API
  - All viewer features (waterfall, sparklines, JSON detail)
  - Permission gating (admin vs tenant key detection)
  - Configurable refresh
  - Config-based auth (signaldb.toml / env var)
  - TUI deps always compiled (no feature gate)
- EXCLUDE: 
  - Mouse support (keyboard-only navigation)
  - Export/save query results to files from TUI
  - Plugin/extension system (V2)
  - Multi-cluster view (V2)
  - Remote TUI support (V2)
  - Live log tailing (V1 uses Flight SQL browse, not streaming)
