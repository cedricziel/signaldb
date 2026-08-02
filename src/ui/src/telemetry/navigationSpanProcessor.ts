// Normalizes the browser auto-instrumentation's route/navigation span.
//
// `getWebAutoInstrumentations` (see `index.ts`) emits a root navigation span
// whose name is the current URL, e.g.
//   `Navigation: /ui?signal=metrics&promql=http.server.active_requests&tenant=…`
// The unbounded query string makes the span *name* a high-cardinality key,
// which breaks grouping and aggregation in the trace UI (every distinct query
// state becomes its own "operation").
//
// This processor rewrites such spans at start: the URL moves into
// `url.full` / `url.path` / `url.query` (OTel semantic conventions) and the name
// collapses to the static, low-cardinality `Navigation`. It keys off the
// observed `Navigation: ` name prefix so it stays correct regardless of which
// bundled web instrumentation produced the span. Runs at `onStart` so the rename
// is in effect before the span is exported at end.

import type { Context } from "@opentelemetry/api";
import type { Span, SpanProcessor } from "@opentelemetry/sdk-trace-base";

const NAVIGATION_PREFIX = "Navigation: ";
const NORMALIZED_NAME = "Navigation";

export class NavigationSpanProcessor implements SpanProcessor {
  onStart(span: Span, _parentContext?: Context): void {
    if (!span.name.startsWith(NAVIGATION_PREFIX)) return;

    const url = span.name.slice(NAVIGATION_PREFIX.length);
    if (url) {
      span.setAttribute("url.full", url);
      const queryStart = url.indexOf("?");
      if (queryStart === -1) {
        span.setAttribute("url.path", url);
      } else {
        span.setAttribute("url.path", url.slice(0, queryStart));
        span.setAttribute("url.query", url.slice(queryStart + 1));
      }
    }
    span.updateName(NORMALIZED_NAME);
  }

  onEnd(): void {}

  shutdown(): Promise<void> {
    return Promise.resolve();
  }

  forceFlush(): Promise<void> {
    return Promise.resolve();
  }
}
