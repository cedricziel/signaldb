## Context

See proposal.md. `src/router/src/endpoints/pyroscope.rs`: six handlers, `RenderParams`/`DiscoveryParams` (`Deserialize` only), responses built as `serde_json::Value`/ad-hoc structs; mounted at `/pyroscope` and `/api/profiles` in `lib.rs`; listed in `openapi.rs::ALLOWLISTED_ROUTES`. UI `src/ui/src/api/pyroscope.ts` uses raw fetch (`retryingFetch` since #1260). MCP has `get_profile` (query IR) and `discover_attributes(traces|logs|metrics)`. CLI has no profile commands. Parity check maps every `signaldb_sdk::OPERATIONS` entry.

## Goals / Non-Goals

**Goals:** contract completeness (OpenAPI → SDK/TS), CLI+MCP surfaces so parity is unconditional, UI on the generated client for these ops.
**Non-Goals:** changing Pyroscope wire semantics; adding new profile query features.

## Decisions

**D1 — Typed schemas.** Add `ToSchema` to `RenderParams`/`DiscoveryParams` (as `IntoParams` query params) and introduce typed response structs where the handler currently builds JSON (`FlamebearerResponse` mirroring Pyroscope's flamebearer v1: `names`, `levels`, `numTicks`, `maxSelf`, `format`, `units`, `spyName`, …; `ProfileTypesResponse`, `LabelNamesResponse`, `LabelValuesResponse`, `ProfilesByTraceResponse`). Keep wire JSON byte-identical (add tests that the old handler output equals the typed struct's serialization for the fixtures).

**D2 — Operation ids** `pyroscope_render`, `pyroscope_render_diff`, `pyroscope_label_names`, `pyroscope_label_values`, `pyroscope_profile_types`, `profiles_by_trace`; tag `profiles`; 401/403/429 declared like the other compat surfaces (429 via the shared `RateLimited` response). Remove the six from `ALLOWLISTED_ROUTES`, add to `KNOWN_ROUTES`. Progenitor: responses have one error type each → no `homogenize` complication expected.

**D3 — CLI `profiles` group** (`commands/profiles.rs`): thin SDK calls, native JSON output via the existing `print_json_response`; `--from/--until` accept the same relative/absolute forms as Pyroscope (`now-1h`, unix seconds).

**D4 — MCP tools.** `discover_profile_types`, `search_profiles`, `compare_profiles`, `profiles_for_trace`, plus `signal: "profiles"` on `discover_attributes` (label names / values). Read-only annotations; payload cap + `truncated` like the other query tools; results are SDK shapes.

**D5 — UI.** Replace `src/ui/src/api/pyroscope.ts` internals with the generated operations (keep the module's exported functions to limit churn, or delete it and update callers — whichever is smaller); the raw `fetch` disappears; `retryingFetch` still applies via the client config.

**D6 — Parity.** Map the six operations to CLI paths and MCP tools; no exclusions.

## Risks / Trade-offs

- [Typed response structs drift from Pyroscope's wire format] → equality tests against captured fixtures for each handler.
- [Both this change and `management-api-key-scope` touch `openapi.rs`, generated files, MCP server.rs, CLI, and the parity manifest] → stacked on top of it; rebase + regenerate.

## Migration Plan

Additive; regenerate clients; no rollback concerns.
