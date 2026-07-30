// Client for the router's Prometheus-compatible API (/prometheus/api/v1).

import type { ResolvedRange } from "../lib/time";
import { ApiError, tenantHeaders } from "./http";

export interface PromSeries {
  labels: Record<string, string>;
  /** [timestampMs, value] pairs, ascending. */
  points: [number, number][];
}

interface PromMatrixResult {
  metric: Record<string, string>;
  values: [number, string][];
}

interface PromResponse {
  status: string;
  data: { resultType: string; result: PromMatrixResult[] };
  error?: string;
}

export async function promQueryRange(
  promql: string,
  range: ResolvedRange,
  stepSeconds: number,
): Promise<PromSeries[]> {
  const params = new URLSearchParams({
    query: promql,
    start: String(range.fromMs / 1000),
    end: String(range.toMs / 1000),
    step: String(stepSeconds),
  });
  const res = await fetch(`/prometheus/api/v1/query_range?${params}`, {
    headers: tenantHeaders(),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new ApiError(
      `Prometheus query_range failed (${res.status}): ${body.slice(0, 300)}`,
      res.status,
    );
  }
  const json = (await res.json()) as PromResponse;
  if (json.status !== "success") {
    throw new Error(`Prometheus query failed: ${json.error ?? json.status}`);
  }
  if (json.data.resultType !== "matrix") {
    throw new Error(`Expected a matrix result but got ${json.data.resultType}`);
  }
  return json.data.result.map((r) => ({
    labels: r.metric,
    points: r.values.map(([t, v]): [number, number] => [t * 1000, Number(v)]),
  }));
}

/** Prometheus-style series label: `name{k="v", …}`. */
export function seriesName(labels: Record<string, string>): string {
  const { __name__: name, ...rest } = labels;
  const pairs = Object.entries(rest)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `${k}="${v}"`);
  if (pairs.length === 0) return name ?? "value";
  return `${name ?? ""}{${pairs.join(", ")}}`;
}

// ---- metadata (feeds the visual builder's metric/label/value pickers) ----

interface PromMetadataResponse {
  status: string;
  data?: string[];
  error?: string;
}

async function promMetadata(
  path: string,
  range: ResolvedRange,
  extra?: Record<string, string>,
): Promise<string[]> {
  const params = new URLSearchParams({
    start: String(range.fromMs / 1000),
    end: String(range.toMs / 1000),
    ...extra,
  });
  const res = await fetch(`/prometheus/api/v1/${path}?${params}`, {
    headers: tenantHeaders(),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new ApiError(
      `Prometheus ${path} failed (${res.status}): ${body.slice(0, 300)}`,
      res.status,
    );
  }
  const json = (await res.json()) as PromMetadataResponse;
  if (json.status !== "success") {
    throw new Error(`Prometheus ${path} failed: ${json.error ?? json.status}`);
  }
  return json.data ?? [];
}

/** Label names available for filtering/grouping in the current window. */
export function promLabelNames(range: ResolvedRange): Promise<string[]> {
  return promMetadata("labels", range);
}

/** Distinct values of a single label. */
export function promLabelValues(
  label: string,
  range: ResolvedRange,
): Promise<string[]> {
  return promMetadata(`label/${encodeURIComponent(label)}/values`, range);
}

/** Metric names — the distinct values of the reserved `__name__` label. */
export function promMetricNames(range: ResolvedRange): Promise<string[]> {
  return promLabelValues("__name__", range);
}

/** Per-label cardinality, from `/api/v1/label_stats` (a SignalDB extension). */
export interface LabelStat {
  name: string;
  /** Approximate distinct value count (a floor when `capped`). */
  distinct_estimate: number;
  /** Fraction of scanned rows carrying the label, in `[0, 1]`. */
  presence: number;
  /** True when `distinct_estimate` hit the analyzer's cardinality cap. */
  capped: boolean;
}

interface LabelStatsResponse {
  status: string;
  data?: LabelStat[];
  error?: string;
}

/**
 * Cardinality statistics for each label in the window. Only labels whose data
 * has been compacted at least once appear; the builder treats missing labels
 * as "unknown cardinality".
 */
export async function promLabelStats(
  range: ResolvedRange,
): Promise<LabelStat[]> {
  const params = new URLSearchParams({
    start: String(range.fromMs / 1000),
    end: String(range.toMs / 1000),
  });
  const res = await fetch(`/prometheus/api/v1/label_stats?${params}`, {
    headers: tenantHeaders(),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new ApiError(
      `Prometheus label_stats failed (${res.status}): ${body.slice(0, 300)}`,
      res.status,
    );
  }
  const json = (await res.json()) as LabelStatsResponse;
  if (json.status !== "success") {
    throw new Error(
      `Prometheus label_stats failed: ${json.error ?? json.status}`,
    );
  }
  return json.data ?? [];
}
