// Parsing of the trace context SignalDB services return on HTTP responses
// via `Server-Timing: traceparent;desc="00-<trace-id>-<span-id>-<flags>"`.
//
// The browser exposes Server-Timing entries through the Performance API
// (`PerformanceResourceTiming.serverTiming` / `PerformanceNavigationTiming.
// serverTiming`) even for requests no JS initiated — above all the document
// itself — which is what makes this the only way to correlate the initial
// page load with the server's trace.

/** The server-side trace context recovered from a Server-Timing entry. */
export interface ServerTraceContext {
  traceId: string;
  spanId: string;
  traceFlags: number;
  /** Whether the server's sampler kept the span (`flags & 0x01`). */
  sampled: boolean;
}

/** Minimal shape of a Performance API entry carrying serverTiming data. */
export interface EntryWithServerTiming {
  serverTiming?: ReadonlyArray<{
    name: string;
    description: string;
    duration?: number;
  }>;
}

// Strict W3C traceparent value: version 00 only, lowercase hex, exact widths.
const TRACEPARENT_RE = /^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/;

/**
 * Parse a raw W3C `traceparent` value (from a `Server-Timing` entry, a
 * `<meta name="traceparent">` tag, or any other carrier). Returns `null`
 * (never throws) for a missing value, a version other than `00`, non-hex or
 * wrong-width ids, or an all-zero trace/span id.
 */
export function parseTraceparent(
  value: string | null | undefined,
): ServerTraceContext | null {
  const match = value?.match(TRACEPARENT_RE);
  if (!match) return null;
  const [, traceId, spanId, flagsHex] = match;
  if (!traceId || !spanId || !flagsHex) return null;
  if (traceId === "0".repeat(32) || spanId === "0".repeat(16)) return null;
  const traceFlags = Number.parseInt(flagsHex, 16);
  return { traceId, spanId, traceFlags, sampled: (traceFlags & 0x01) === 0x01 };
}

/**
 * Extract the server's trace context from a performance entry's
 * `serverTiming` list. Returns `null` (never throws) when the entry carries
 * no `traceparent` metric or its description does not parse.
 */
export function serverTraceContextFromEntry(
  entry: EntryWithServerTiming | null | undefined,
): ServerTraceContext | null {
  const metric = entry?.serverTiming?.find((m) => m.name === "traceparent");
  return parseTraceparent(metric?.description);
}
