/**
 * Trace-volume series for the traces tab's chart.
 *
 * Deliberately a Query IR aggregate rather than a client-side bucketing of the
 * Tempo search response: search is bounded by the view's row limit, so
 * bucketing it would show volume only for the newest N traces — the exact
 * truncation artefact the volume chart exists to avoid. The IR aggregate is
 * evaluated server-side over the whole window with no limit on the path.
 */
import type { HeatmapResult, QueryIrRequest, QueryIrResponse } from "./gen";
import { runIrQuery } from "./queryIr";
import { msToNanos, type ResolvedRange } from "../lib/time";
import { facetField, type TraceFilter } from "../lib/traceFilters";
import type { VolumeSeries } from "../features/explore/SignalHistogram";
export type TraceLatencyHeatmap = HeatmapResult & {
  window: { start_ns: number; end_ns: number };
};

// Geometric millisecond bounds provide useful resolution from sub-ms spans to
// slow requests without letting callers create an unbounded result matrix.
export const LATENCY_BUCKET_BOUNDS_NS = [
  1, 2, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 30_000,
].map((ms) => ms * 1_000_000);

/** Stacking order, bottom to top. */
export const STATUS_ORDER = ["ok", "unset", "error"];

export const STATUS_COLORS: Record<string, string> = {
  ok: "var(--ok)",
  unset: "var(--info-bar)",
  error: "var(--err-bar)",
};

/**
 * Fold OTLP status spellings onto the display keys. The server may return the
 * short form (`Error`) or the enum name (`STATUS_CODE_ERROR`), and a
 * null-valued group arrives as the literal string `"null"`.
 */
export function normalizeStatus(value: string): string {
  const v = value.toLowerCase();
  if (v.includes("error")) return "error";
  if (v === "ok" || v === "status_code_ok") return "ok";
  return "unset";
}

/**
 * Build the IR document for a status-stacked volume series. `range.from`/`to`
 * are nanosecond integers rendered as strings, per the endpoint's contract.
 */
export function buildTraceVolumeDoc(
  range: ResolvedRange,
  step: string,
  filters: TraceFilter[] = [],
): QueryIrRequest {
  // The chart must describe the same traces the list shows, so the active
  // filters narrow it too.
  const where = traceFilterStages(filters);
  return {
    irVersion: 1,
    from: "traces",
    range: {
      from: String(msToNanos(range.fromMs)),
      to: String(msToNanos(range.toMs)),
    },
    result: "series",
    pipeline: [
      ...where,
      {
        aggregate: {
          by: ["status.code"],
          aggs: [{ fn: "count", as: "n" }],
          step,
        },
      },
    ],
  };
}

function traceFilterStages(filters: TraceFilter[]): Record<string, unknown>[] {
  return filters.flatMap((f) => {
    const facet = facetField(f.field);
    if (!facet) return [];
    return [
      { where: { field: facet.irField, op: "eq", value: f.value } },
    ] as Record<string, unknown>[];
  });
}

/** Build the v2 terminal heatmap relation over the full selected window. */
export function buildTraceLatencyHeatmapDoc(
  range: ResolvedRange,
  step: string,
  filters: TraceFilter[] = [],
): QueryIrRequest {
  return {
    irVersion: 2,
    from: "traces",
    range: {
      from: String(msToNanos(range.fromMs)),
      to: String(msToNanos(range.toMs)),
    },
    result: "heatmap",
    pipeline: [
      ...traceFilterStages(filters),
      {
        heatmap: {
          x: { step, align: "epoch" },
          y: {
            of: "duration",
            bounds: LATENCY_BUCKET_BOUNDS_NS.map((bound) => `${bound}ns`),
            overflow: true,
          },
          value: { fn: "count", as: "count" },
        },
      },
    ],
  };
}

/** The label the server used for the grouping — it sanitizes dots to underscores. */
function statusLabel(labels: Record<string, string>): string {
  return labels["status_code"] ?? labels["status.code"] ?? "";
}

/**
 * Convert an IR `series` envelope into chart series: keyed by display status,
 * points converted from nanoseconds to milliseconds. Malformed points are
 * dropped rather than rendered as NaN-height bars.
 */
export function seriesFromIrResponse(res: QueryIrResponse): VolumeSeries[] {
  return (res.series ?? []).map((s) => ({
    key: normalizeStatus(statusLabel(s.labels)),
    points: s.points.flatMap((p): [number, number][] => {
      const [tNs, value] = p as [unknown, unknown];
      if (typeof tNs !== "number" || typeof value !== "number") return [];
      return [[Math.round(tNs / 1e6), value]];
    }),
  }));
}

export async function fetchTraceVolume(
  range: ResolvedRange,
  step: string,
  filters: TraceFilter[] = [],
): Promise<VolumeSeries[]> {
  return seriesFromIrResponse(
    await runIrQuery(buildTraceVolumeDoc(range, step, filters)),
  );
}

export async function fetchTraceLatencyHeatmap(
  range: ResolvedRange,
  step: string,
  filters: TraceFilter[] = [],
): Promise<TraceLatencyHeatmap> {
  const response = await runIrQuery(
    buildTraceLatencyHeatmapDoc(range, step, filters),
  );
  const heatmap = response.heatmap as HeatmapResult | undefined;
  if (!heatmap) throw new Error("IR heatmap response omitted heatmap data");
  return { ...heatmap, window: response.window };
}
