## Why

SignalDB now has three client-facing surfaces — the HTTP/Flight API, the MCP
server (`signaldb-mcp`), and the CLI (`signaldb-cli`) — but they are not
feature-equal and do not share a single access path. The CLI reaches queries by
hand-writing its own Arrow Flight client, bypassing `signaldb-sdk` entirely,
while the MCP server is already a pure SDK consumer. The SDK itself is
OpenAPI-generated and therefore HTTP-only: it cannot express the Flight query
transport or operational control at all. The result is drift by construction —
each surface can gain or lose capabilities independently, and there is no
structural guarantee that what an operator can do through one is possible
through the others.

This change establishes a single rule — **CLI and MCP may only consume the
SDK** — and makes the SDK cover the full API surface, so that feature parity
across API, SDK, CLI, and MCP becomes structural rather than aspirational.

## What Changes

- Give `signaldb-sdk` full query coverage matching how the server is actually
  built: **SQL** over the router's Arrow Flight transport (returning Arrow rows)
  via a hand-written query module, and **PromQL/LogQL/TraceQL** over the existing
  HTTP compatibility endpoints (returning their native Tempo/Loki/Prometheus JSON
  shapes). The three native-language endpoints are annotated into the code-first
  OpenAPI so they generate into the SDK; only SQL (gRPC/Flight, which OpenAPI
  cannot describe) is hand-written. Output is **native per language** — no
  uniform-row normalization and no `--compat` flag.
- Make the **CLI a pure SDK consumer**: remove the hand-written
  `FlightServiceClient` usage in `commands/query.rs` and `tui/client/flight.rs`
  and route all queries through the SDK. Reorganize the command tree so queries
  are one `query` command with a mutually-exclusive language flag —
  `signaldb query --sql|--promql|--logql|--traceql '<q>'` — where the language
  determines signal, transport, and output shape (the three native flags imply
  their signal; `--sql` is the cross-signal case). Management moves under
  `admin <noun>`; operational control under `ops <verb>`.
- Add operational control to the **router as an OpenAPI-annotated ops proxy**
  (`/api/v1/ops/*`) that forwards to the compactor's existing Flight
  `do_action` surface. The initial scope is **compaction control** (run,
  status, dry-run), which the compactor already exposes as actions; retention
  enforcement, snapshot expiration, and orphan cleanup run as compactor
  background loops with no control surface and are deferred (they need matching
  compactor actions). Because the proxy is annotated, it regenerates into the
  SDK, and CLI `ops` verbs plus MCP ops tools land from the same source.
- Bring the **MCP tool surface to feature parity** with the CLI: every SDK
  capability is exposed as an MCP tool, including query and ops.
- Add a **parity guarantee and enforcement test**: a check that enumerates the
  SDK's public surface and asserts every capability is reachable through both a
  CLI verb and an MCP tool, failing CI on under-exposure.
- **Out of scope (explicit):** ingest/write from the CLI or MCP. Data ingestion
  stays with the OTLP acceptor and `signal-producer`.

## Capabilities

### New Capabilities

- `client-surface-parity`: the invariant that every operator/user capability is
  reachable identically through the API, the SDK, the CLI, and the MCP server;
  the SDK-only consumption rule for CLI and MCP; and the enforcement mechanism
  that prevents drift.
- `cli-command-surface`: the `signaldb` CLI's observable behavior — command
  taxonomy (`query --<lang>` / `admin` / `ops`), native per-language query
  output, auth/endpoint/config resolution, exit codes, and its status as a pure
  SDK consumer.
- `mcp-tool-surface`: the `signaldb-mcp` server's tool set — the tools exposed,
  their parity with the CLI, and its status as a pure SDK consumer.
- `operational-control-api`: operator-facing operational control (compaction,
  retention, snapshot expiration, orphan cleanup, status/health) exposed through
  the router and reachable via the SDK, so it is available to CLI and MCP alike.

### Modified Capabilities

<!-- No existing capability's REQUIREMENTS change. admin-management-api-contract
     behavior is unchanged; the CLI already consumes it via the SDK and this
     change only reorganizes CLI presentation of it. Ops is added as a new
     capability rather than a modification of the admin contract. -->

## Impact

- **Crates:** `signaldb-sdk` (hand-written Flight SQL query module + generated
  clients for the annotated PromQL/LogQL/TraceQL endpoints; unified `Client`),
  `signaldb-cli` (remove direct Flight usage in `commands/query.rs` and
  `tui/client/flight.rs`; re-taxonomize commands), `mcp-server` (add query + ops
  tools), `router` (annotate the existing PromQL/LogQL/TraceQL compat endpoints
  into OpenAPI; new OpenAPI-annotated `/api/v1/ops/*` proxy; implement the
  currently-unimplemented Flight `do_action` path or an HTTP equivalent),
  `compactor` (control surface reached via the proxy — existing Flight
  `do_action` reused), `tests-integration` (three-way parity test), `common`
  (shared config resolution if surfaced).
- **API surface:** the PromQL/LogQL/TraceQL compat endpoints gain OpenAPI
  annotations (no behavior change); new `/api/v1/ops/*` router endpoints.
  **BREAKING (lands in Phase 1)** for the CLI command tree — existing top-level
  `signaldb-cli tenant|api-key|dataset` move under `admin`, and query invocation
  changes to `query --<lang>`. Phase 0 is non-breaking (annotation-only on
  existing endpoints; response shapes unchanged). No change to OTLP ingest, the Tempo/LogQL/PromQL
  query _semantics_ or their JSON response shapes, Flight wire schemas, or
  on-disk Iceberg/WAL layout.
- **Generation regimes:** the SDK stays mostly generated (HTTP, code-first
  OpenAPI — now including the query-compat endpoints) with one hand-written
  Flight module for SQL. The dividing line and the parity test are captured in
  `design.md`.
- **Docs:** `docs/architecture/overview.md` (CLI section), `docs/users/
authentication.md` (CLI invocation examples), and the `multi-tenancy` skill's
  CLI references need updating for the new command taxonomy.
