// Client for the router's Loki-compatible API (/loki/api/v1), layered over
// the generated OpenAPI SDK — the UI never hand-writes the HTTP call for
// endpoints the SDK covers (see docs/architecture/openapi-codegen.md,
// "Adding or changing an endpoint"), mirroring api/queryIr.ts. SignalDB
// materializes `severity_text` as Loki's `level` label; other materialized
// attributes appear as stream labels.
import "./client";

import { logqlLabels, logqlLabelValues, logqlQueryRange } from "./gen";
import { msToNanos, nanosToMs, type ResolvedRange } from "../lib/time";
import { ApiError, tenantHeaders } from "./http";

export interface LogRow {
  tsNs: string;
  tsMs: number;
  line: string;
  labels: Record<string, string>;
  /**
   * Loki "structured metadata": per-line fields such as `trace_id`/`span_id`
   * that vary line to line, unlike `labels`, which is constant for every
   * row in the same stream.
   */
  metadata: Record<string, string>;
}

export interface HistogramSeries {
  level: string;
  /** [timestampMs, count] pairs, ascending. */
  points: [number, number][];
}

interface LokiStreamResult {
  stream: Record<string, string>;
  // Loki 3.0 entries carry an optional 3rd element for structured metadata.
  values: [string, string, Record<string, string>?][];
}

interface LokiMatrixResult {
  metric: Record<string, string>;
  values: [number, string][];
}

/**
 * Unwrap a generated-client result carrying `{resultType, result}` data.
 * Only checks the HTTP-level outcome (matching the original hand-written
 * client, which never inspected a body-level `status` field for these
 * endpoints) — an HTTP failure throws `ApiError` with the status embedded.
 */
function unwrapLoki<T>(
  res: { error?: unknown; data?: unknown; response?: Response },
  what: string,
): T {
  if (res.error || !res.data) {
    const status = res.response?.status ?? 500;
    const detail = typeof res.error === "string" ? `: ${res.error}` : "";
    throw new ApiError(`${what} failed (${status})${detail}`, status);
  }
  return (res.data as { data: T }).data;
}

export async function lokiQueryLogs(
  query: string,
  range: ResolvedRange,
  limit: number,
): Promise<LogRow[]> {
  const res = await logqlQueryRange({
    query: {
      query,
      start: msToNanos(range.fromMs),
      end: msToNanos(range.toMs),
      limit,
      direction: "backward",
    },
    headers: tenantHeaders(),
  });
  const data = unwrapLoki<{ resultType: string; result: LokiStreamResult[] }>(
    res,
    "Loki query_range",
  );
  if (data.resultType !== "streams") {
    throw new Error(
      `Expected a log query but got resultType=${data.resultType}`,
    );
  }
  const rows: LogRow[] = [];
  for (const stream of data.result) {
    for (const [tsNs, line, metadata] of stream.values) {
      rows.push({
        tsNs,
        tsMs: nanosToMs(tsNs),
        line,
        labels: stream.stream,
        metadata: metadata ?? {},
      });
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
  const res = await logqlQueryRange({
    query: {
      query,
      start: msToNanos(range.fromMs),
      end: msToNanos(range.toMs),
      step,
    },
    headers: tenantHeaders(),
  });
  const data = unwrapLoki<{ resultType: string; result: LokiMatrixResult[] }>(
    res,
    "Loki query_range",
  );
  if (data.resultType !== "matrix") {
    throw new Error(
      `Expected a metric query but got resultType=${data.resultType}`,
    );
  }
  return data.result.map((r) => ({
    level: r.metric["level"] ?? "unknown",
    points: r.values.map(([t, v]): [number, number] => [t * 1000, Number(v)]),
  }));
}

export async function lokiLabels(range: ResolvedRange): Promise<string[]> {
  const res = await logqlLabels({
    query: { start: msToNanos(range.fromMs), end: msToNanos(range.toMs) },
    headers: tenantHeaders(),
  });
  return unwrapLoki<string[] | undefined>(res, "Loki labels") ?? [];
}

export async function lokiLabelValues(
  label: string,
  range: ResolvedRange,
): Promise<string[]> {
  const res = await logqlLabelValues({
    path: { name: label },
    query: { start: msToNanos(range.fromMs), end: msToNanos(range.toMs) },
    headers: tenantHeaders(),
  });
  return unwrapLoki<string[] | undefined>(res, "Loki label values") ?? [];
}
