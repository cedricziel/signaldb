## Context

See proposal.md — Why. Current state:

- `src/router/src/endpoints/tempo.rs`: `search_tags`/`search_tags_v2` return `RESOURCE_TAGS ∪ INTRINSIC_TAGS` (`["service.name"]`, `["name","status"]`) and take no state/tenant; `tag_values_for` maps `service.name`→`service_name`, `name`→`span_name` (via a raw distinct SQL Flight ticket in `distinct_column_values`), `status`→static, else `501`.
- `src/querier/src/query/logs.rs::get_labels/get_label_values` and `metrics.rs::get_labels/get_label_values` already implement bounded discovery: time window, `select_columns` of the attribute maps, `LABEL_SCAN_LIMIT = 1000` sample, `attr_documents()` decoding of map/JSON attribute columns, `BTreeSet` union with known columns. Flight tickets `label_names:tenant:dataset[:params]` etc. in `querier/src/flight.rs`; the router's Loki/Prom handlers call them.
- Traces v2 schema columns: `resource_attributes`, `span_attributes` (map-typed on new tables, JSON on old), dedicated `service_name`, `span_name`, `span_kind`, `status_code`, `duration_nanos`.
- Consumers: MCP `discover_attributes(traces)` and CLI `discover attributes --signal traces` call `search_tags`/`search_tag_values` through the SDK; UI logs tab has `mergeLabelSuggestions` for key autocomplete; traces tab has none.

## Goals / Non-Goals

**Goals:** real names/values with the same cost profile as label discovery; v2 scopes; zero client changes for MCP/CLI/Grafana; UI key suggestions on traces.

**Non-Goals:** an attribute index for exact/unsampled discovery (#411, and the otel-native-schema warm index) — the sampled scan is the honest interim; TraceQL metrics; counts per value (the UI facets already do that via Query IR).

## Decisions

**D1 — Discovery lives in the querier, next to its siblings.** New `querier/src/query/trace_tags.rs` (or fns on the trace query struct): `get_tags(start,end,tenant,dataset, scope: Option<Scope>) -> TagNames{resource,span,intrinsic}` and `get_tag_values(tag, scope, start, end, tenant, dataset) -> Vec<String>`. Reuse `time_window`, `attr_context_of`, `attr_documents`, `LABEL_SCAN_LIMIT` (lift the shared helpers to a `query/attrs.rs` module if they are private to logs). Dedicated-column tags (`service.name`, `name`, `kind`, `status`) resolve via `distinct()` on the column; intrinsics `status`/`kind` return enums. Values bounded to the sample; sorted.

**D2 — Flight tickets `trace_tags:` / `trace_tag_values:`**, same grammar as `label_names:` (`tenant:dataset[:k=v&…]` with `start`, `end`, `scope`, `tag`), added to `TicketRequest` and the do_get dispatch with the existing per-ticket instrumentation (query-stage span names follow the label-discovery ones). _Alternative:_ keep building raw SQL in the router (`distinct_column_values`) — rejected: map columns and JSON-vs-map dual formats belong behind the querier's helpers, not in router SQL strings.

**D3 — Router handlers become thin.** `search_tags` gains `State`, tenant, `start/end`; v2 adds `scope`. `tag_values_for` calls the ticket for every tag; the `501` arm and the `RESOURCE_TAGS`/`INTRINSIC_TAGS` constants go (intrinsics list moves into the querier's response). Default window: end = now, start = end − 1 h (as Loki metadata endpoints; note Tempo's own default is broader — the docs state ours). Scoped names normalised (`resource.`/`span.`/leading `.` stripped) before lookup; v2 responses echo the scoped name.

**D4 — Response contract unchanged.** `tempo_api::TagSearchResponse` / `v2::TagSearchResponse` / `TagValuesResponse` are already the Tempo shapes; utoipa annotations drop the `501` and document window/scope params. Regenerate SDK/TS (schemas unchanged; docs change).

**D5 — UI.** Traces tab filter-key input gets suggestions from `searchTags` (v1, current window) merged with registry hits via the existing `mergeLabelSuggestions`; values on demand from `searchTagValues` when a key is chosen. Small, uses generated client.

**D6 — Truncation.** Tempo's v1 shape has no truncation flag; v2 has `metrics.inspectedBytes`-style fields we do not fill. We cap values at the sample bound and document it; the MCP tool's existing `truncated` flag applies to its own payload cap only.

## Risks / Trade-offs

- [Sampled scan misses rare keys/values] → same trade-off as Loki labels today; documented; the warm index (otel-native-schema) is the real fix.
- [Old JSON-attribute tables vs map tables] → `attr_documents` already handles both; test both fixtures.
- [1 h default window surprises Grafana users expecting more] → `start`/`end` honoured; docs state the default; can widen later without contract change.

## Migration Plan

Deploy querier + router together (new tickets). Rollback = revert; clients unaffected.

## Open Questions

- Whether to widen the default window to 24 h for traces once cost is measured on hive — deferrable, no spec change (spec says "matches the Loki metadata endpoints"; if both move, both docs move).
