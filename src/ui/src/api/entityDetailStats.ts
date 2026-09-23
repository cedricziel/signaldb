/**
 * KPI stats and sparkline series for one pinned catalog entity — the
 * "Service overview" page's card row (PR 2 of the entity-detail rework;
 * `EntityDetail.tsx` does not consume this yet).
 *
 * Traces-only: error rate and duration percentiles exist only on spans, the
 * same restriction `EntityRed` in `./catalog.ts` already carries. The
 * previous-period figures are the same query run again against the window
 * of equal length immediately before `range` — the Query IR has no time-shift
 * stage, so the shift is composed client-side from two ordinary queries
 * rather than reaching for a compat endpoint. Likewise "peak rate" is the
 * max of a per-bucket count series divided by the step, not an IR aggregate —
 * there is no max-over-buckets stage either.
 */
import type { QueryIrRequest, QueryIrResponse } from "./gen";
import { pinsWhere, type EntityPin } from "./catalog";
import { runIrQuery } from "./queryIr";
import { msToNanos, type ResolvedRange } from "../lib/time";
import { ERROR_PATTERN } from "./traceGroups";
import type { EntityTypeDef } from "../features/catalog/entityTypes";

const NANOS_PER_MS = 1_000_000;

/** This period's own length immediately before it — same duration, no gap. */
export function previousPeriod(range: ResolvedRange): ResolvedRange {
  const span = range.toMs - range.fromMs;
  return { fromMs: range.fromMs - span, toMs: range.fromMs };
}

/** The `where` stages every query in this module shares: the entity type's
 * span-kind scope (if any), then one equality/absence check per pin. Mirrors
 * `buildEntitySourceDoc`'s scope in `./catalog.ts`. */
function scopeWhere(
  entity: EntityTypeDef,
  pinned: EntityPin[],
): Record<string, unknown>[] {
  return [
    ...(entity.spanKindScope
      ? [
          {
            where: {
              field: "span_kind",
              op: "eq",
              value: entity.spanKindScope,
            },
          },
        ]
      : []),
    ...pinsWhere(pinned),
  ];
}

function rangeDoc(range: ResolvedRange) {
  return {
    from: msToNanos(range.fromMs),
    to: msToNanos(range.toMs),
  };
}

/** One row of count/errors/percentiles/last-seen, pinned to an exact
 * entity — `entity.identity` is grouped on only so the row carries a known
 * column layout to decode; the pins already narrow it to one entity. */
export function buildEntityStatsDoc(
  entity: EntityTypeDef,
  range: ResolvedRange,
  pinned: EntityPin[],
): QueryIrRequest {
  return {
    irVersion: 1,
    from: "traces",
    range: rangeDoc(range),
    result: "table",
    pipeline: [
      ...scopeWhere(entity, pinned),
      {
        aggregate: {
          by: entity.identity,
          aggs: [
            { fn: "count", as: "n" },
            {
              fn: "count",
              as: "errors",
              where: {
                field: "status.code",
                op: "regex",
                value: ERROR_PATTERN,
              },
            },
            { fn: "quantile", of: "duration", arg: 0.5, as: "p50" },
            { fn: "quantile", of: "duration", arg: 0.95, as: "p95" },
            { fn: "quantile", of: "duration", arg: 0.99, as: "p99" },
            { fn: "max", of: "start_time_unix_nano", as: "last" },
          ],
        },
      },
      { limit: 1 },
    ],
  };
}

/** A stepped count series, pinned the same way — the IR allows exactly one
 * aggregate output per step bucket, so the rate/error/p95 sparklines below
 * are three of these (plus one quantile variant) rather than one document. */
export function buildEntityCountSeriesDoc(
  entity: EntityTypeDef,
  range: ResolvedRange,
  pinned: EntityPin[],
  stepSeconds: number,
  errorsOnly: boolean,
): QueryIrRequest {
  return {
    irVersion: 1,
    from: "traces",
    range: rangeDoc(range),
    result: "series",
    pipeline: [
      ...scopeWhere(entity, pinned),
      {
        aggregate: {
          by: entity.identity,
          aggs: [
            {
              fn: "count",
              as: "n",
              ...(errorsOnly
                ? {
                    where: {
                      field: "status.code",
                      op: "regex",
                      value: ERROR_PATTERN,
                    },
                  }
                : {}),
            },
          ],
          step: `${stepSeconds}s`,
        },
      },
    ],
  };
}

/** A stepped p95 duration series, pinned the same way. */
export function buildEntityP95SeriesDoc(
  entity: EntityTypeDef,
  range: ResolvedRange,
  pinned: EntityPin[],
  stepSeconds: number,
): QueryIrRequest {
  return {
    irVersion: 1,
    from: "traces",
    range: rangeDoc(range),
    result: "series",
    pipeline: [
      ...scopeWhere(entity, pinned),
      {
        aggregate: {
          by: entity.identity,
          aggs: [{ fn: "quantile", of: "duration", arg: 0.95, as: "p95" }],
          step: `${stepSeconds}s`,
        },
      },
    ],
  };
}

export interface SeriesPoint {
  tMs: number;
  value: number;
}

/** One entity's KPI figures over one window — either the current one, or
 * the previous one for the "+6% vs prev" comparison. */
export interface EntityStats {
  count: number;
  ratePerSec: number;
  errorRate: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  /** Max per-bucket rate across the window, not the window-average rate. */
  peakRatePerSec: number;
  lastNs: string;
}

export interface EntityKpis {
  /** Absent when the window holds no matching spans at all. */
  current?: EntityStats;
  /** Absent both when the current window is empty and when the shifted
   * previous window holds no matching spans — a comparison needs both
   * halves, or it isn't one. */
  previous?: EntityStats;
  /** ~`stepSeconds`-bucketed series for the KPI cards' sparklines, over
   * `range` (not the previous period). */
  series: {
    rate: SeriesPoint[];
    errorRate: SeriesPoint[];
    p95: SeriesPoint[];
  };
}

/** One decoded `buildEntityStatsDoc` row, before the rate/peak figures that
 * need the window length and the count series to compute. */
interface RawStatsRow {
  count: number;
  errors: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  lastNs: string;
}

function decodeStatsRow(
  res: QueryIrResponse,
  entity: EntityTypeDef,
): RawStatsRow | undefined {
  const row = res.rows?.[0] as unknown[] | undefined;
  if (!row) return undefined;
  const d = entity.identity.length;
  const num = (i: number) => {
    const v = row[d + i];
    return typeof v === "number" ? v : 0;
  };
  const last = row[d + 5];
  return {
    count: num(0),
    errors: num(1),
    p50Ms: num(2) / NANOS_PER_MS,
    p95Ms: num(3) / NANOS_PER_MS,
    p99Ms: num(4) / NANOS_PER_MS,
    lastNs: last == null ? "0" : String(last),
  };
}

function decodeCountSeries(res: QueryIrResponse): SeriesPoint[] {
  const s = res.series?.[0];
  if (!s) return [];
  return s.points.flatMap((p): SeriesPoint[] => {
    const [tNs, v] = p as [unknown, unknown];
    if (typeof tNs !== "number" || typeof v !== "number") return [];
    return [{ tMs: Math.round(tNs / NANOS_PER_MS), value: v }];
  });
}

function peakRatePerSec(counts: SeriesPoint[], stepSeconds: number): number {
  if (counts.length === 0) return 0;
  return Math.max(...counts.map((p) => p.value)) / stepSeconds;
}

function toEntityStats(
  raw: RawStatsRow,
  rangeSeconds: number,
  peak: number,
): EntityStats {
  return {
    count: raw.count,
    ratePerSec: rangeSeconds > 0 ? raw.count / rangeSeconds : 0,
    errorRate: raw.count > 0 ? raw.errors / raw.count : 0,
    p50Ms: raw.p50Ms,
    p95Ms: raw.p95Ms,
    p99Ms: raw.p99Ms,
    peakRatePerSec: peak,
    lastNs: raw.lastNs,
  };
}

/** Zips a total-count series against an error-count series sharing the same
 * step buckets into a per-bucket error fraction. A bucket the error series
 * has no point for (no errors that step) reads as 0, not "missing". */
function errorRateSeries(
  counts: SeriesPoint[],
  errors: SeriesPoint[],
): SeriesPoint[] {
  const errorByT = new Map(errors.map((p) => [p.tMs, p.value]));
  return counts.map((p) => ({
    tMs: p.tMs,
    value: p.value > 0 ? (errorByT.get(p.tMs) ?? 0) / p.value : 0,
  }));
}

/**
 * This entity's KPI figures for `range`, the same figures for the equal-
 * length period immediately before it, and the sparkline series for the
 * current period's cards.
 */
export async function fetchEntityKpis(
  entity: EntityTypeDef,
  range: ResolvedRange,
  pinned: EntityPin[],
  stepSeconds: number,
): Promise<EntityKpis> {
  const prevRange = previousPeriod(range);
  const rangeSeconds = (range.toMs - range.fromMs) / 1000;

  const [statsRes, countRes, errorRes, p95Res, prevStatsRes, prevCountRes] =
    await Promise.all([
      runIrQuery(buildEntityStatsDoc(entity, range, pinned)),
      runIrQuery(
        buildEntityCountSeriesDoc(entity, range, pinned, stepSeconds, false),
      ),
      runIrQuery(
        buildEntityCountSeriesDoc(entity, range, pinned, stepSeconds, true),
      ),
      runIrQuery(buildEntityP95SeriesDoc(entity, range, pinned, stepSeconds)),
      runIrQuery(buildEntityStatsDoc(entity, prevRange, pinned)),
      runIrQuery(
        buildEntityCountSeriesDoc(
          entity,
          prevRange,
          pinned,
          stepSeconds,
          false,
        ),
      ),
    ]);

  const countSeries = decodeCountSeries(countRes);
  const errorSeries = decodeCountSeries(errorRes);
  const p95Series = decodeCountSeries(p95Res).map((p) => ({
    tMs: p.tMs,
    value: p.value / NANOS_PER_MS,
  }));
  const prevCountSeries = decodeCountSeries(prevCountRes);

  const rawCurrent = decodeStatsRow(statsRes, entity);
  const rawPrevious = decodeStatsRow(prevStatsRes, entity);

  return {
    current: rawCurrent
      ? toEntityStats(
          rawCurrent,
          rangeSeconds,
          peakRatePerSec(countSeries, stepSeconds),
        )
      : undefined,
    // Comparing against an empty previous window is comparing against
    // nothing, not against zero — reported as no comparison rather than a
    // misleading "+∞%".
    previous:
      rawCurrent && rawPrevious
        ? toEntityStats(
            rawPrevious,
            rangeSeconds,
            peakRatePerSec(prevCountSeries, stepSeconds),
          )
        : undefined,
    series: {
      rate: countSeries.map((p) => ({
        tMs: p.tMs,
        value: p.value / stepSeconds,
      })),
      errorRate: errorRateSeries(countSeries, errorSeries),
      p95: p95Series,
    },
  };
}
