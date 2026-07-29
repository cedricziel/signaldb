// Client for the router's Loki-compatible API (/loki/api/v1). SignalDB
// materializes `severity_text` as Loki's `level` label; other materialized
// attributes appear as stream labels.

import { msToNanos, nanosToMs, type ResolvedRange } from "../lib/time";

export interface LogRow {
  tsNs: string;
  tsMs: number;
  line: string;
  labels: Record<string, string>;
}

export interface HistogramSeries {
  level: string;
  /** [timestampMs, count] pairs, ascending. */
  points: [number, number][];
}

interface LokiStreamResult {
  stream: Record<string, string>;
  values: [string, string][];
}

interface LokiMatrixResult {
  metric: Record<string, string>;
  values: [number, string][];
}

interface LokiResponse<T> {
  status: string;
  data: { resultType: string; result: T[] };
}

async function lokiFetch<T>(path: string, params: URLSearchParams): Promise<T> {
  const res = await fetch(`/loki/api/v1/${path}?${params}`, {
    headers: { Accept: "application/json" },
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(
      `Loki API ${path} failed (${res.status}): ${body.slice(0, 300)}`,
    );
  }
  return (await res.json()) as T;
}

export async function lokiQueryLogs(
  query: string,
  range: ResolvedRange,
  limit: number,
): Promise<LogRow[]> {
  const params = new URLSearchParams({
    query,
    start: msToNanos(range.fromMs),
    end: msToNanos(range.toMs),
    limit: String(limit),
    direction: "backward",
  });
  const res = await lokiFetch<LokiResponse<LokiStreamResult>>(
    "query_range",
    params,
  );
  if (res.data.resultType !== "streams") {
    throw new Error(
      `Expected a log query but got resultType=${res.data.resultType}`,
    );
  }
  const rows: LogRow[] = [];
  for (const stream of res.data.result) {
    for (const [tsNs, line] of stream.values) {
      rows.push({ tsNs, tsMs: nanosToMs(tsNs), line, labels: stream.stream });
    }
  }
  // Streams arrive independently ordered; merge to newest-first.
  rows.sort((a, b) => (BigInt(a.tsNs) < BigInt(b.tsNs) ? 1 : -1));
  return rows;
}

export async function lokiQueryHistogram(
  query: string,
  range: ResolvedRange,
  step: string,
): Promise<HistogramSeries[]> {
  const params = new URLSearchParams({
    query,
    start: msToNanos(range.fromMs),
    end: msToNanos(range.toMs),
    step,
  });
  const res = await lokiFetch<LokiResponse<LokiMatrixResult>>(
    "query_range",
    params,
  );
  if (res.data.resultType !== "matrix") {
    throw new Error(
      `Expected a metric query but got resultType=${res.data.resultType}`,
    );
  }
  return res.data.result.map((r) => ({
    level: r.metric["level"] ?? "unknown",
    points: r.values.map(([t, v]): [number, number] => [t * 1000, Number(v)]),
  }));
}

export async function lokiLabels(range: ResolvedRange): Promise<string[]> {
  const params = new URLSearchParams({
    start: msToNanos(range.fromMs),
    end: msToNanos(range.toMs),
  });
  const res = await lokiFetch<{ status: string; data: string[] }>(
    "labels",
    params,
  );
  return res.data ?? [];
}

export async function lokiLabelValues(
  label: string,
  range: ResolvedRange,
): Promise<string[]> {
  const params = new URLSearchParams({
    start: msToNanos(range.fromMs),
    end: msToNanos(range.toMs),
  });
  const res = await lokiFetch<{ status: string; data: string[] }>(
    `label/${encodeURIComponent(label)}/values`,
    params,
  );
  return res.data ?? [];
}
