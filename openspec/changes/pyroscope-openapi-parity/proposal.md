## Why

The Pyroscope-compatible profile endpoints (`/pyroscope/render`, `/render-diff`, `/label-names`, `/label-values`, `/profile-types`, and `/api/profiles/trace/{trace_id}`) are the only tenant-facing HTTP surface outside the OpenAPI document. Consequently they are absent from the Rust SDK and the TypeScript client, invisible to the CLI and MCP, and the OpenAPI route drift guard carries them on an allowlist. That breaks the `client-surface-parity` rule ("an endpoint outside the document is invisible to every client and is therefore a parity defect") and leaves profiling — a first-class signal since `otlp-profiles-ingestion` — without a scriptable or agent-facing discovery/query path other than raw HTTP or the generic Query IR.

## What Changes

- The six Pyroscope endpoints get OpenAPI operations (`pyroscope_render`, `pyroscope_render_diff`, `pyroscope_label_names`, `pyroscope_label_values`, `pyroscope_profile_types`, `profiles_by_trace`) with typed params and response schemas (Pyroscope's flamebearer JSON for render/diff, string lists for discovery, the trace-profile correlation shape for `profiles_by_trace`), and are removed from the route allowlist. Rust SDK and TS client regenerated.
- CLI: `profiles {types, labels, label-values, render, diff, by-trace}` group (native Pyroscope JSON output, consistent with the compat query surfaces).
- MCP: `discover_profile_types`, `search_profiles` (render → flamebearer, bounded), `compare_profiles` (render-diff), `profiles_for_trace`; `discover_attributes` gains `signal: "profiles"` (label names/values). Existing `get_profile` unchanged.
- UI: already consumes these endpoints through hand-written `pyroscope.ts` raw fetch — it moves onto the generated client for these operations (retiring that raw-fetch file, one of the four the `ui-migrate-to-generated-sdk` change tracks).
- Parity: the new operations are mapped in the whole-SDK check (CLI + MCP), no exclusions.
- Docs: `docs/users/profiles.md` (API table now generated-contract-backed; CLI/MCP usage), MCP tool catalogue, `tempo-api` skill (implemented endpoints table).

No **BREAKING** changes: wire behavior of the endpoints is unchanged; only the contract and clients grow.

## Capabilities

### New Capabilities

- (none)

### Modified Capabilities

- `client-surface-parity`: "SDK covers the full API surface" — the Pyroscope compat endpoints are part of the documented HTTP surface; the route allowlist no longer names them.
- `mcp-tool-surface`: "MCP tools cover the full client capability set" — profile discovery/query tools listed alongside the other compat surfaces; `discover_attributes` covers `profiles`.
- `cli-command-surface`: "Command taxonomy" — `profiles <verb>` group for the Pyroscope compat surface (kept off `query` because Pyroscope has no single query-language flag; its selector + range are per-verb parameters).
- `explore-ui-profiles`: "Flame graphs render via Query IR" is unchanged; ADDED requirement that the UI's Pyroscope-compat calls go through the generated client.

## Impact

- **router**: `endpoints/pyroscope.rs` utoipa annotations + `ToSchema` on `RenderParams`/`DiscoveryParams`/response types (`FlamebearerResponse` etc.), `openapi.rs` path list, `KNOWN_ROUTES`/`ALLOWLISTED_ROUTES` update; regenerate.
- **signaldb-sdk / src/ui/src/api/gen**: regenerated; `src/ui/src/api/pyroscope.ts` rewritten onto the generated client (or deleted with call sites updated).
- **signaldb-cli**: new `commands/profiles.rs`.
- **mcp-server**: four tools + `discover_attributes(profiles)`; payload caps apply.
- **tests-integration**: parity mapping; e2e ingest a profile → `profile-types` lists its type via CLI and MCP.
- **docs/skills**: as above.
