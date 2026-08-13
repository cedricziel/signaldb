## 1. Fix matching-span row navigation

- [x] 1.1 Write a failing test that clicking a "recent matching spans" row
      switches to the Traces signal and opens the trace waterfall (not just
      setting a bare `trace` param, which TracesView is the only consumer of).
- [x] 1.2 Fix `EntityDetail.tsx`'s `onOpenTrace` to pass
      `{ signal: "traces", trace: traceId }`.

## 2. Backend: capture db.query.text on catalog spans

- [x] 2.1 Write a failing test asserting a catalog DB client span carries
      `db.query.text` with literals sanitized to placeholders
      (`db_catalog_span_semconv.rs`).
- [x] 2.2 Add the `db.query.text` `Empty` field to `db_client_span()` in
      `self_monitoring/spans.rs`; add a pin test for the semconv attribute
      name.
- [x] 2.3 Implement `Catalog::record_query_text` and wire it into
      register/heartbeat/list/deregister ingester operations (SQLite and
      PostgreSQL arms) in `catalog.rs`.
- [x] 2.4 Document `db.query.text` in
      `docs/operations/self-monitoring-traces.md`.

## 3. Top statements table for database entities

- [x] 3.1 Write a failing test that a database entity detail page renders a
      read-only "Top statements" table (rows do not drill in or navigate).
- [x] 3.2 Add a `topValues` field to `EntityTypeDef` (distinct from the
      existing drillable `breakdown`); implement the database entity's
      `topValues: { field: "db.query.text", label: "Top statements" }` and
      render it in `EntityDetail.tsx` via a non-drillable `EntityTable`.

## 4. Span-kind coloring in the trace waterfall

- [x] 4.1 Write a failing test for `fetchSpanKinds` (Query IR `rows` query
      over `span_id`/`span_kind`, scoped to a trace) in `api/spanKinds.ts`.
- [x] 4.2 Implement it, confirming it does not touch the Tempo trace-fetch
      wire shape.
- [x] 4.3 Write failing tests for waterfall bar color-coding by kind, the
      legend, and the span detail panel's kind chip.
- [x] 4.4 Implement `KIND_CLASS`/`kindClass` and wire into
      `TracesView.tsx`/`SpanDetail`; add kind color rules to `traces.css`.

## 5. Service time-by-dependency-category breakdown

- [x] 5.1 Write failing tests for `fetchDependencyBreakdown`: five parallel
      Query IR sum(duration)/count aggregate queries (baseline + one per
      category), "Other" derived as the baseline remainder, and each
      category query filtered by service + CLIENT kind + attribute
      existence.
- [x] 5.2 Implement `api/dependencyBreakdown.ts`.
- [x] 5.3 Write failing tests for `DependencyBreakdown.tsx`: proportional bar + legend when data exists, an empty-state note when there's no
      dependency traffic, hidden for non-service entity types and at the
      breakdown drill-in depth.
- [x] 5.4 Implement `DependencyBreakdown.tsx`; wire it into
      `EntityDetail.tsx` on a service's own page; add `.dep-*` styles to
      `catalog.css`.

## 6. Verification

- [x] 6.1 `cargo test -p common --test db_catalog_span_semconv`.
- [x] 6.2 `pnpm run typecheck && pnpm run lint && pnpm vitest run` (src/ui).
- [x] 6.3 Live-verify against a real deployment: matching-span-row
      navigation, the top-statements table, span-kind waterfall coloring,
      and the dependency-time breakdown rendering correctly on a real
      service's Catalog page.

Surface parity: this is a UI change plus the one backend span-attribute
addition it depends on; `span.kind`, `db.query.text`, and the
dependency-category attributes are already queryable via the existing
Query IR/HTTP/MCP surfaces, so no additional OpenAPI, SDK, or MCP task is
needed.
