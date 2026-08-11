// Builds the real OTel parent context for the `documentLoad` span from
// `<meta name="traceparent" content="...">`, which `serve_index_html` in
// `src/router/src/ui.rs` injects into every `index.html` response.
//
// This must be read and turned into a Context *before* `documentLoad` is
// created (see `initTelemetry()` in `index.ts`, which wraps
// `DocumentLoadInstrumentation`'s registration in
// `context.with(documentTraceParentContext(), ...)`): a `SpanProcessor`
// cannot change a span's parent after the fact, only attach links (see
// `serverCorrelationSpanProcessor.ts`, the fallback for when this tag is
// absent).
//
// Parenting is unconditional, including when the server sampled its span out
// (`traceparent` flags `00`). OpenTelemetry JS's default sampler
// (`ParentBasedSampler`) then drops `documentLoad` and everything chained
// under it — a deliberate trade-off (see docs/users/response-trace-context.md):
// real parent-child edges for the common case, at the cost of losing some
// page loads' RUM data whenever the server's root span sampling drops it.

import {
  context as apiContext,
  trace,
  TraceFlags,
  type Context,
} from "@opentelemetry/api";
import { parseTraceparent } from "./serverTiming";

/** The `content` of `<meta name="traceparent">`, if the server injected one. */
function metaTagTraceparent(): string | null {
  return (
    document
      .querySelector('meta[name="traceparent"]')
      ?.getAttribute("content") ?? null
  );
}

/**
 * Build a Context carrying the server's span as a remote parent, layered
 * onto `base`. Returns `null` (never throws) when no meta tag is present or
 * its value does not parse as a `traceparent` (see `parseTraceparent`).
 */
export function documentTraceParentContext(
  base: Context = apiContext.active(),
  metaTagProvider: () => string | null = metaTagTraceparent,
): Context | null {
  let raw: string | null;
  try {
    raw = metaTagProvider();
  } catch {
    return null;
  }
  const ctx = parseTraceparent(raw);
  if (!ctx) return null;
  return trace.setSpanContext(base, {
    traceId: ctx.traceId,
    spanId: ctx.spanId,
    traceFlags: ctx.sampled ? TraceFlags.SAMPLED : TraceFlags.NONE,
    isRemote: true,
  });
}
