// Client for the router's Prometheus-compatible API (/prometheus/api/v1).

import type { ResolvedRange } from "../lib/time";

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
    headers: { Accept: "application/json" },
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(
      `Prometheus query_range failed (${res.status}): ${body.slice(0, 300)}`,
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
