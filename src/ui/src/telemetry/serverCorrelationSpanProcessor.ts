// Correlates the browser's document-load trace with the server span that
// served the document, for deployments where the document has no
// `<meta name="traceparent">` to establish a real parent (see
// `telemetry/documentTraceContext.ts` and `initTelemetry()` in `index.ts`,
// which is the primary mechanism — this processor is the fallback).
//
// A dev proxy or a router build that predates the meta tag still returns the
// server's span context on every response via
// `Server-Timing: traceparent;desc="..."` (see `serverTiming.ts`). This
// processor reads it off the navigation performance entry and attaches it to
// the `documentLoad` root span as a span *link*.
//
// Link, not parent, deliberately: unlike the meta tag (read before the
// `documentLoad` span exists, so it can be wired in as a real parent at
// creation time), the performance entry is only reliably populated *after*
// the navigation completes — by then the span already exists and a
// `SpanProcessor` cannot change its parent. A link is also the safer choice
// here regardless: if the server sampled its span out (flags `00`) a parent
// would point at a span that is never exported and the client trace would
// dangle; a link to an unexported span is harmless.

import { TraceFlags, type Context } from "@opentelemetry/api";
import type { Span, SpanProcessor } from "@opentelemetry/sdk-trace-base";
import {
  serverTraceContextFromEntry,
  type EntryWithServerTiming,
} from "./serverTiming";

// Root span name emitted by `@opentelemetry/instrumentation-document-load`.
const DOCUMENT_LOAD_SPAN = "documentLoad";

/** The browser's navigation performance entry, if available. */
function navigationEntry(): EntryWithServerTiming | undefined {
  return performance.getEntriesByType("navigation")[0] as
    EntryWithServerTiming | undefined;
}

export class ServerCorrelationSpanProcessor implements SpanProcessor {
  constructor(
    private readonly entryProvider: () =>
      EntryWithServerTiming | undefined = navigationEntry,
  ) {}

  onStart(span: Span, _parentContext?: Context): void {
    if (span.name !== DOCUMENT_LOAD_SPAN) return;
    // Telemetry must never break the app: any Performance API oddity (old
    // browser, stripped headers, malformed value) degrades to "no link".
    try {
      const ctx = serverTraceContextFromEntry(this.entryProvider());
      if (!ctx) return;
      span.addLink({
        context: {
          traceId: ctx.traceId,
          spanId: ctx.spanId,
          traceFlags: ctx.sampled ? TraceFlags.SAMPLED : TraceFlags.NONE,
          isRemote: true,
        },
      });
    } catch {
      // Swallow: correlation is best-effort by design.
    }
  }

  onEnd(): void {}

  shutdown(): Promise<void> {
    return Promise.resolve();
  }

  forceFlush(): Promise<void> {
    return Promise.resolve();
  }
}
