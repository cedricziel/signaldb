// Client for the router's Prometheus-compatible API (/prometheus/api/v1),
// layered over the generated OpenAPI SDK — the UI never hand-writes the HTTP
// call for endpoints the SDK covers (see docs/architecture/openapi-codegen.md,
// "Adding or changing an endpoint"), mirroring api/queryIr.ts.
import "./client";

import { promqlLabels, promqlLabelValues, promqlQueryRange } from "./gen";
import type { ResolvedRange } from "../lib/time";
import {
  ApiError,
  retryAfterMsFrom,
  retryingFetch,
  tenantHeaders,
} from "./http";

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

// ---- metadata (feeds the visual builder's metric/label/value pickers) ----

/** Label names available for filtering/grouping in the current window. */
export async function promLabelNames(range: ResolvedRange): Promise<string[]> {
  const res = await promqlLabels({
    query: {
      start: String(range.fromMs / 1000),
      end: String(range.toMs / 1000),
    },
    headers: tenantHeaders(),
  });
  return unwrapProm<string[] | undefined>(res, "Prometheus labels") ?? [];
}

/** Distinct values of a single label. */
export async function promLabelValues(
  label: string,
  range: ResolvedRange,
): Promise<string[]> {
  const res = await promqlLabelValues({
    path: { name: label },
    query: {
      start: String(range.fromMs / 1000),
      end: String(range.toMs / 1000),
    },
    headers: tenantHeaders(),
  });
  return unwrapProm<string[] | undefined>(res, "Prometheus label values") ?? [];
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

/**
 * Cardinality statistics for each label in the window. Only labels whose data
 * has been compacted at least once appear; the builder treats missing labels
 * as "unknown cardinality".
 *
 * `label_stats` is a SignalDB extension not yet in the OpenAPI document
 * (deferred — see openspec change `mcp-server` design D9), so this one call
 * stays on raw `fetch` until it is annotated.
 */
export async function promLabelStats(
  range: ResolvedRange,
): Promise<LabelStat[]> {
  const params = new URLSearchParams({
    start: String(range.fromMs / 1000),
    end: String(range.toMs / 1000),
  });
  const res = await retryingFetch(`/prometheus/api/v1/label_stats?${params}`, {
    headers: tenantHeaders(),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new ApiError(
      `Prometheus label_stats failed (${res.status}): ${body.slice(0, 300)}`,
      res.status,
      retryAfterMsFrom(res),
    );
  }
  const json = (await res.json()) as PromEnvelope<LabelStat[]>;
  if (json.status !== "success") {
    throw new Error(
      `Prometheus label_stats failed: ${json.error ?? json.status}`,
    );
  }
  return json.data ?? [];
}
