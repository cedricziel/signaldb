## 1. Backend: exception attributes on the traces source

- [x] 1.1 Write failing `ir_planner` tests: resolving `exception.type`/
      `.message`/`.stacktrace` from a span's `exception` event; a span with
      an error status but no event resolves them absent; grouping by
      `exception.type` with counts.
- [x] 1.2 Add `Resolved::EventAttribute` to `common::query_ir::resolver`.
- [x] 1.3 Implement the `ir_event_attr` DataFusion UDF and
      `extract_event_attr` (reusing `common::model::span::parse_span_events`)
      in `ir_planner.rs`.
- [x] 1.4 Special-case the four exception attribute names in the traces
      `SchemaResolver`'s `resolve()`; wire `Resolved::EventAttribute` through
      `value_expr`, predicate-leaf building, and row projection.
- [x] 1.5 `cargo test -p querier --lib exception`; `cargo test -p common
    --lib`.
- [x] 1.6 Document exception-attribute addressing in
      `docs/users/querying-ir.md` and the `architecture` skill.

## 2. UI: exception grouping and example lookup

- [x] 2.1 Write failing tests for `buildErrorGroupDoc`/`fetchErrorGroups`
      (traces + logs aggregates merged and ranked by count) and
      `buildErrorExampleDoc`/`fetchErrorExample` (pinned lookup, trace id +
      stacktrace decoding, null when no example exists).
- [x] 2.2 Implement `api/errors.ts` over the generated Query IR client — no
      hand-written `fetch`.

## 3. UI: the Errors tab

- [x] 3.1 Write failing tests for `ErrorsView`: empty state, ranked group
      list across both sources, stacktrace fetch on selection, and a "View
      trace" action that appears only when the example carries a trace id.
- [x] 3.2 Implement `features/errors/ErrorsView.tsx` and `errors.css`.
- [x] 3.3 Register the `errors` signal in `lib/urlState.ts` and wire the tab
      into `features/explore/ExploreView.tsx`.

## 4. Verification and docs

- [x] 4.1 `pnpm run typecheck && pnpm run lint && pnpm vitest run` (src/ui).
- [x] 4.2 Live-verify against a real deployment: the logs-sourced path
      end-to-end (grouping, counts, first/last-seen, stacktrace decoding, and
      correctly omitting the trace link for an untraced example) against
      real ingested data. The traces-sourced path (span-event exceptions) is
      covered by the querier unit tests; no live example was available in
      the deployment's current window to click through.
- [x] 4.3 Document the Errors tab in `docs/users/explore-ui.md`.

Surface parity: this is a UI change consuming a Query-IR-only backend
extension — no new HTTP endpoint, OpenAPI operation, Rust SDK surface, or
MCP tool is needed; grouping/filtering by `exception.type` is already
reachable by any Query IR client.
