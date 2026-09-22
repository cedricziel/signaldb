// Client for the router's Prometheus-compatible query_range endpoint
// (/prometheus/api/v1/query_range), layered over the generated OpenAPI SDK —
// the UI never hand-writes the HTTP call for endpoints the SDK covers (see
// docs/architecture/openapi-codegen.md, "Adding or changing an endpoint"),
// mirroring api/queryIr.ts. Discovery (metric names, label keys/values,
// cardinality) is api/ir/discovery.ts; this module stays only for range
// queries a PromQL formula still needs (see the metrics builder's rate/
// formula tasks in openspec/changes/explore-ui-query-ir).
import "./client";

import { promqlQueryRange } from "./gen";
import type { ResolvedRange } from "../lib/time";
import { ApiError, retryAfterMsFrom, tenantHeaders } from "./http";

export interface PromSeries {
  labels: Record<string, string>;
  /** [timestampMs, value] pairs, ascending. */
  points: [number, number][];
}

interface PromMatrixResult {
  metric: Record<string, string>;
  values: [number, string][];
}

/** The Prometheus `{status, data, error}` envelope this API returns. */
interface PromEnvelope<T> {
  status: string;
  data?: T;
  error?: string;
}

/**
 * Unwrap a generated-client result carrying a `PromEnvelope`: an HTTP-level
 * failure (non-2xx, or no body) throws `ApiError`; a body-level
 * `status: "error"` response throws a plain `Error` — the same two-tier
 * distinction the hand-written client used, which `isAuthError` (401 →
 * `ApiError`) depends on.
 */
function unwrapProm<T>(
  res: { error?: unknown; data?: unknown; response?: Response },
  what: string,
): T {
  if (res.error || !res.data) {
    const status = res.response?.status ?? 500;
    const detail = typeof res.error === "string" ? `: ${res.error}` : "";
    throw new ApiError(
      `${what} failed (${status})${detail}`,
      status,
      retryAfterMsFrom(res.response),
    );
  }
  const body = res.data as PromEnvelope<T>;
  if (body.status !== "success") {
    throw new Error(`${what} failed: ${body.error ?? body.status}`);
  }
  return body.data as T;
}

export async function promQueryRange(
  promql: string,
  range: ResolvedRange,
  stepSeconds: number,
): Promise<PromSeries[]> {
  const res = await promqlQueryRange({
    query: {
      query: promql,
      start: String(range.fromMs / 1000),
      end: String(range.toMs / 1000),
      step: String(stepSeconds),
    },
    headers: tenantHeaders(),
  });
  const data = unwrapProm<{ resultType: string; result: PromMatrixResult[] }>(
    res,
    "Prometheus query_range",
  );
  if (data.resultType !== "matrix") {
    throw new Error(`Expected a matrix result but got ${data.resultType}`);
  }
  return data.result.map((r) => ({
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
