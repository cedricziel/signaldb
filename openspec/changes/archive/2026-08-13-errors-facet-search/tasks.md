## 1. Facet search

- [x] 1.1 Write failing tests for `errorFacetValues`/`applyErrorFilters`:
      summed counts per value ranked by count, a facet's own filter not
      narrowing itself, null-valued groups excluded.
- [x] 1.2 Implement `lib/errorFacets.ts`.
- [x] 1.3 Write failing tests for `ErrorFacets`: collapsed by default,
      expands to values with counts, adds/removes a filter on click.
- [x] 1.4 Implement `features/errors/ErrorFacets.tsx`, reusing the traces
      facet sidebar's styling; extend the shared sidebar layout rules in
      `explore.css`.
- [x] 1.5 Wire filter state and the sidebar into `ErrorsView.tsx`; add an
      integration test that a facet selection narrows the visible table.

## 2. Stacktrace readability

- [x] 2.1 Write failing tests for `parseStacktraceLines`: header vs. frame
      classification, leading-whitespace stripping, vendor-directory
      detection, trailing-blank-line trimming.
- [x] 2.2 Implement `lib/stacktrace.ts`.
- [x] 2.3 Render per-line boxes with their own hanging indent in
      `ErrorsView.tsx`'s `Stacktrace` component; style header/frame/vendor
      lines in `errors.css`.

## 3. Verification

- [x] 3.1 `pnpm run typecheck && pnpm run lint && pnpm vitest run`.
- [x] 3.2 Live-verify against a real deployment: expanding a facet,
      filtering by a selected value with a removable chip, and a real
      captured stacktrace rendering with the app's own frame legible against
      dimmed react-dom/vite-internal noise.
