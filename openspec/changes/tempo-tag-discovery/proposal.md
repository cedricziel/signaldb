## Why

The Tempo tag-discovery endpoints cannot see the tenant's data: `/api/search/tags` (v1 and v2) returns a hardcoded three-name list and `/api/search/tag/{tag}/values` answers `501` for every attribute that is not a dedicated column (#1073). Everything downstream inherits the blindness — Grafana's TraceQL autocomplete, the MCP `discover_attributes` tool (its default `traces` signal!), the CLI's `discover` command, and any UI key suggestion — even though the same attributes are already enumerable through Query IR aggregates and the querier already does exactly this discovery for Loki labels and Prometheus label names. This is the largest functional hole an agent or a Grafana user hits first.

## What Changes

- The querier gains trace attribute discovery mirroring the existing log/metric label discovery: distinct attribute keys (resource and span scopes, plus intrinsics) and distinct values for a key, over a bounded time window, from the tenant's traces table.
- The router's Tempo `search/tags` (v1 and v2, with `scope`), `search/tag/{tag}/values` (v1 and v2, scoped tag names) return real, tenant- and window-scoped names and values; the `501` for non-column tags is gone. Unknown tags return an empty list, never an error; the `status`/`kind` intrinsics keep their enum values. Optional `start`/`end` bound the window; without them the default lookback matches the Loki metadata endpoints.
- MCP `discover_attributes` (traces), CLI `discover`, and Grafana get real data with no client change. The UI's traces tab gains filter-key suggestions backed by the tag names endpoint (parity with the logs tab's key autocomplete).
- Tag scans are bounded (sample limit + window) and their cost is observable (query-stage span like the label endpoints).

No **BREAKING** changes: response shapes are the Tempo ones already documented; only the content becomes real. Clients that relied on the `501` to detect "not queryable" (none known; the UI already stopped) would now see values.

## Capabilities

### New Capabilities

- `trace-attribute-discovery`: enumeration of trace attribute keys and values for a tenant over a time window, exposed through the Tempo tag endpoints and thereby through the MCP/CLI/UI discovery surfaces.

### Modified Capabilities

- (none — the archived `mcp-server` requirement already routes `discover_attributes(traces)` through the Tempo tag endpoints; its behavior improves without a contract change)

## Impact

- **querier**: `query/trace.rs` (or a `trace_tags` module) — `get_tags`, `get_tag_values` over `resource_attributes` + `span_attributes` (v2 schema) with the same sampled-scan pattern as `logs.rs::get_labels`; Flight tickets `trace_tags:` / `trace_tag_values:` in `flight.rs`.
- **router**: `endpoints/tempo.rs` — replace `RESOURCE_TAGS`/`INTRINSIC_TAGS` constants and `tag_value_column`-or-501 with querier calls; v2 scopes; utoipa responses updated (drop the `501`).
- **signaldb-sdk / TS client**: regenerated (response schemas unchanged; the `501` documentation goes away).
- **src/ui**: traces tab filter-key suggestions via `searchTags` (reuse `mergeLabelSuggestions`).
- **tests-integration**: e2e — ingest spans with custom attributes, assert names and values via the Tempo endpoints, MCP `discover_attributes`, and CLI `discover`.
- **docs**: `docs/users/tempo-api-reference.md` (tags endpoints now real; window semantics), MCP/CLI discovery docs; `tempo-api` skill (remove the "hardcoded/501" caveat).
- Closes #1073.
