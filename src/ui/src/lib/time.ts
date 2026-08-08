// Time-range model shared by every explore view. Relative ranges re-resolve
// "now" on each query so live refreshes slide the window forward.

export type TimeRange =
  | { type: "relative"; seconds: number }
  | { type: "absolute"; fromMs: number; toMs: number };

export interface ResolvedRange {
  fromMs: number;
  toMs: number;
}

export const DEFAULT_RANGE: TimeRange = { type: "relative", seconds: 3600 };

export const RANGE_PRESETS: { label: string; seconds: number }[] = [
  { label: "Last 5 min", seconds: 300 },
  { label: "Last 15 min", seconds: 900 },
  { label: "Last 1 h", seconds: 3600 },
  { label: "Last 6 h", seconds: 21600 },
  { label: "Last 24 h", seconds: 86400 },
  { label: "Last 7 d", seconds: 604800 },
];

export function resolveRange(range: TimeRange, nowMs: number): ResolvedRange {
  if (range.type === "absolute") {
    return { fromMs: range.fromMs, toMs: range.toMs };
  }
  return { fromMs: nowMs - range.seconds * 1000, toMs: nowMs };
}

/** Loki expects nanosecond timestamps as strings. */
export function msToNanos(ms: number): string {
  return (BigInt(Math.round(ms)) * 1_000_000n).toString();
}

export function nanosToMs(ns: string): number {
  return Number(BigInt(ns) / 1_000_000n);
}

/**
 * URL encoding for ranges: relative as a duration ("15m", "1h"), absolute as
 * "fromMs-toMs". Round-trips through parseRangeParam.
 */
export function rangeToParam(range: TimeRange): string {
  if (range.type === "absolute") {
    return `${range.fromMs}-${range.toMs}`;
  }
  return secondsToDuration(range.seconds);
}

export function parseRangeParam(param: string | null): TimeRange {
  if (!param) return DEFAULT_RANGE;
  const abs = /^(\d+)-(\d+)$/.exec(param);
  if (abs) {
    const fromMs = Number(abs[1]);
    const toMs = Number(abs[2]);
    if (fromMs < toMs) return { type: "absolute", fromMs, toMs };
    return DEFAULT_RANGE;
  }
  const seconds = durationToSeconds(param);
  if (seconds !== null && seconds > 0) return { type: "relative", seconds };
  return DEFAULT_RANGE;
}

export function secondsToDuration(seconds: number): string {
  if (seconds % 86400 === 0) return `${seconds / 86400}d`;
  if (seconds % 3600 === 0) return `${seconds / 3600}h`;
  if (seconds % 60 === 0) return `${seconds / 60}m`;
  return `${seconds}s`;
}

export function durationToSeconds(value: string): number | null {
  const m = /^(\d+)(s|m|h|d)$/.exec(value.trim());
  if (!m) return null;
  const n = Number(m[1]);
  switch (m[2]) {
    case "s":
      return n;
    case "m":
      return n * 60;
    case "h":
      return n * 3600;
    case "d":
      return n * 86400;
    default:
      return null;
  }
}

/**
 * Pick a histogram step that yields roughly `buckets` buckets, snapped to a
 * human-friendly duration.
 */
export function stepForRange(range: ResolvedRange, buckets = 45): string {
  const spanSeconds = Math.max(1, (range.toMs - range.fromMs) / 1000);
  const raw = spanSeconds / buckets;
  const steps = [
    1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 21600,
    43200, 86400,
  ];
  const step = steps.find((s) => s >= raw) ?? 86400;
  return secondsToDuration(step);
}

export function formatTimestamp(ms: number): string {
  const d = new Date(ms);
  const pad = (n: number, w = 2) => String(n).padStart(w, "0");
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
}

const DAY_MS = 86_400_000;

/**
 * Build a chart-axis label formatter sized to the window it labels.
 *
 * `formatTimestamp` is always time-of-day, which makes both ends of a 24h
 * window render as the same string. The granularity here follows the window:
 * time-only inside one calendar day, date + time once it crosses midnight,
 * and date-only past a couple of days where the time of day is noise.
 */
export function axisLabelFormatter(
  fromMs: number,
  toMs: number,
): (ms: number) => string {
  const pad = (n: number) => String(n).padStart(2, "0");
  const date = (d: Date) => `${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
  const time = (d: Date) => `${pad(d.getHours())}:${pad(d.getMinutes())}`;

  const from = new Date(fromMs);
  const to = new Date(toMs);
  const sameDay =
    from.getFullYear() === to.getFullYear() &&
    from.getMonth() === to.getMonth() &&
    from.getDate() === to.getDate();

  if (sameDay) {
    return (ms) => {
      const d = new Date(ms);
      return `${time(d)}:${pad(d.getSeconds())}`;
    };
  }
  if (toMs - fromMs > 2 * DAY_MS) {
    return (ms) => date(new Date(ms));
  }
  return (ms) => {
    const d = new Date(ms);
    return `${date(d)} ${time(d)}`;
  };
}

export function formatRangeLabel(range: TimeRange): string {
  if (range.type === "relative") {
    const preset = RANGE_PRESETS.find((p) => p.seconds === range.seconds);
    return preset ? preset.label : `Last ${secondsToDuration(range.seconds)}`;
  }
  const fmt = (ms: number) => {
    const d = new Date(ms);
    const pad = (n: number) => String(n).padStart(2, "0");
    return `${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
  };
  return `${fmt(range.fromMs)} → ${fmt(range.toMs)}`;
}
