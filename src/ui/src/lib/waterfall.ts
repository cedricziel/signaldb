// Pure waterfall geometry: order spans depth-first by start time and compute
// per-span offsets as fractions of the trace duration.

import type { TempoSpan } from "../api/tempo";

export interface WaterfallRow {
  span: TempoSpan;
  depth: number;
  /** Bar offset/size as percentages of the trace duration. */
  leftPct: number;
  widthPct: number;
  startOffsetMs: number;
  durationMs: number;
  /** The span recorded no duration but has children, so its bar was drawn
   * over its subtree's extent rather than as a sliver. */
  extentInferred: boolean;
}

export interface Waterfall {
  rows: WaterfallRow[];
  traceStartNs: bigint;
  traceDurationNs: bigint;
  errorCount: number;
  services: string[];
}

export function buildWaterfall(spans: TempoSpan[]): Waterfall {
  if (spans.length === 0) {
    return {
      rows: [],
      traceStartNs: 0n,
      traceDurationNs: 0n,
      errorCount: 0,
      services: [],
    };
  }

  const byId = new Map(spans.map((s) => [s.spanId, s]));
  const children = new Map<string, TempoSpan[]>();
  const roots: TempoSpan[] = [];
  for (const span of spans) {
    // Spans whose parent is missing from the payload render as roots so a
    // partial trace still produces a usable waterfall.
    const parent = span.parentSpanId;
    if (parent !== null && byId.has(parent)) {
      const list = children.get(parent) ?? [];
      list.push(span);
      children.set(parent, list);
    } else {
      roots.push(span);
    }
  }

  const byStart = (a: TempoSpan, b: TempoSpan) =>
    BigInt(a.startNs) < BigInt(b.startNs) ? -1 : 1;

  let start = BigInt(spans[0]!.startNs);
  let end = start;
  for (const s of spans) {
    const sStart = BigInt(s.startNs);
    const sEnd = sStart + BigInt(s.durNs);
    if (sStart < start) start = sStart;
    if (sEnd > end) end = sEnd;
  }
  const total = end - start > 0n ? end - start : 1n;

  // Latest end among a span's descendants — the extent a parent with no
  // recorded duration is drawn over.
  const subtreeEnd = (span: TempoSpan): bigint => {
    let latest = BigInt(span.startNs) + BigInt(span.durNs);
    for (const child of children.get(span.spanId) ?? []) {
      const childEnd = subtreeEnd(child);
      if (childEnd > latest) latest = childEnd;
    }
    return latest;
  };

  const rows: WaterfallRow[] = [];
  const visit = (span: TempoSpan, depth: number) => {
    const spanStart = BigInt(span.startNs);
    const offset = spanStart - start;
    const dur = BigInt(span.durNs);
    // A parent with no recorded end (an un-ended root, a duration the
    // producer never set) covers its children instead of drawing as a
    // sliver; for the root that is the whole trace.
    const extentInferred =
      dur === 0n && (children.get(span.spanId)?.length ?? 0) > 0;
    const drawn = extentInferred ? subtreeEnd(span) - spanStart : dur;
    // Per-mille precision is plenty for bar geometry.
    const leftPct = Number((offset * 1000n) / total) / 10;
    const widthPct = Math.max(0.3, Number((drawn * 1000n) / total) / 10);
    rows.push({
      span,
      depth,
      leftPct,
      widthPct,
      startOffsetMs: Number(offset / 1000n) / 1000,
      durationMs: Number(dur / 1000n) / 1000,
      extentInferred,
    });
    for (const child of (children.get(span.spanId) ?? []).sort(byStart)) {
      visit(child, depth + 1);
    }
  };
  for (const root of roots.sort(byStart)) visit(root, 0);

  return {
    rows,
    traceStartNs: start,
    traceDurationNs: total,
    errorCount: spans.filter((s) => s.status === "error").length,
    services: [...new Set(spans.map((s) => s.serviceName))],
  };
}

export function formatDurationMs(ms: number): string {
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)} s`;
  if (ms >= 1) return `${ms.toFixed(ms < 10 ? 1 : 0)} ms`;
  return `${(ms * 1000).toFixed(0)} µs`;
}

/** Like {@link formatDurationMs}, but for a duration that may not exist at
 * all — e.g. a catalog entity discovered only via a non-trace signal, which
 * carries no span duration to measure (`TraceGroup.traceCount === 0`).
 * `0ms` would misreport that as a real (if implausibly fast) measurement. */
export function formatDurationMsOrDash(
  traceCount: number | undefined,
  ms: number,
): string {
  return traceCount === 0 ? "–" : formatDurationMs(ms);
}
