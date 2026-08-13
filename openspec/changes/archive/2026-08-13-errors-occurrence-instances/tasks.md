## 1. Backend query

- [x] 1.1 Write failing tests for `buildErrorOccurrencesDoc`/
      `fetchErrorOccurrences`: pinned type/message/service, per-source
      timestamp field, newest-first order, `limit: 25`, decoding multiple
      rows including one with no trace id.
- [x] 1.2 Implement in `api/errors.ts`, replacing
      `buildErrorExampleDoc`/`fetchErrorExample`.

## 2. UI

- [x] 2.1 Write failing tests for `ErrorsView`: multiple occurrences each
      with their own trace link, an occurrence with no trace id offering no
      link, expanding an occurrence's own stacktrace, and a trace-link click
      not also expanding the row.
- [x] 2.2 Implement the occurrence list and per-row expand/trace-link
      wiring; style in `errors.css`.

## 3. Verification

- [x] 3.1 `pnpm run typecheck && pnpm run lint && pnpm vitest run`.
- [x] 3.2 Live-verify against a real deployment: a count-2 group listing two
      distinct occurrences, each independently expandable to its own
      stacktrace.
