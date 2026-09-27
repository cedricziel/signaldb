/**
 * Real user monitoring: apps, KPIs, Web Vitals, sessions-over-time and the
 * browser/device breakdown behind the Real users page (Overview tab) — see
 * `openspec/changes/real-user-monitoring/specs/explore-ui-rum/spec.md`.
 *
 * RUM data is plain OTel logs, no dedicated table (design.md — Context): a
 * frontend app is a `service.name` that sent at least one RUM event, vitals
 * are `browser.web_vital` records, sessions are grouped by `session.id`.
 * Every read here is one bounded IR query; nothing reaches a compat API.
 *
 * `count_distinct` (irVersion 9) is used for the true distinct-session and
 * distinct-user figures; on a server that doesn't have it yet the query
 * fails outright (no client-side fallback — see design.md's rejection of
 * client-side distinct counting as unbounded). The hive deployment used to
 * validate these shapes caps at irVersion 8 (a v9 document is rejected
 * outright: "unsupported irVersion 9"), so every v9 document here was
 * checked with its v8 equivalent (`count` in place of `count_distinct`)
 * instead, confirming the field names and `where` scopes resolve.
 *
 * A `step` aggregate allows exactly **one** aggregate output — a hard,
 * version-independent IR rule ("a `step` aggregate requires exactly one
 * aggregate output", confirmed against hive), not something a later server
 * version relaxes. So the KPI strip's three bucketed metrics and the
 * sessions-over-time chart's two can't share one `aggregate` stage. Instead
 * each metric is its own single-output sub-query, bundled into **one HTTP
 * request** via the multi-query/formula document (`{queries, formulas,
 * result: "series"}`, D5) that `api/ir/metrics.ts` already sends for the
 * Metrics tab's formulas — an identity formula (`{name: k, expr: k}`) per
 * metric surfaces its own series under that name in the combined response,
 * with no arithmetic between them. This combined shape couldn't be
 * validated against hive: the MCP `query_ir` tool used for the shapes above
 * only accepts the single-document schema (confirmed by testing — it
 * rejects a `queries`-keyed body as a malformed single document, at every
 * irVersion), which is that tool's own limitation, not a signal the router
 * rejects it — the shape is the one `api/ir/metrics.ts` already exercises
 * in production.
 */
import type {
  MultiQueryIrRequest,
  QueryFormula,
  QueryIrRequest,
  QueryIrResponse,
} from "./gen";
import { decodePoints, rangeDoc, runIrQuery, type IrPoint } from "./queryIr";
import type { ResolvedRange } from "../lib/time";
import type { VitalName, VitalRating } from "../features/rum/rumModel";

/** A log record carrying any of these `event_name`s, or any record with a
 * `session.id` at all, counts as a RUM event — the spec's frontend-app
 * predicate. */
const RUM_EVENT_NAMES = [
  "browser.web_vital",
  "browser.navigation",
  "browser.user_action.click",
  "browser.resource_timing",
];

function rumEventWhere(): Record<string, unknown> {
  return {
    where: {
      or: [
        { field: "session.id", op: "exists" },
        ...RUM_EVENT_NAMES.map((v) => ({
          field: "event_name",
          op: "eq" as const,
          value: v,
        })),
      ],
    },
  };
}

function serviceWhere(app: string): Record<string, unknown> {
  return { where: { field: "service.name", op: "eq", value: app } };
}

export interface RumApp {
  serviceName: string;
  /** `resource.telemetry.sdk.language` of the app's most recent record
   * (`webjs`, `swift`, …) — drives the platform-aware labels; absent when
   * no record carries it. */
  sdkLanguage: string | null;
  /** `resource.deployment.environment.name` of the app's most recent
   * record — shown next to the app switcher; absent when no record
   * carries it. */
  env: string | null;
  /** `resource.service.version` of the app's most recent record — shown
   * next to the app switcher; absent when no record carries it. */
  version: string | null;
  /** RUM-event count in the window, busiest first. */
  count: number;
}

/** Every frontend app (a `service.name` with RUM events in the window),
 * busiest first — the app switcher's source and the default selection. */
export function buildRumAppsDoc(range: ResolvedRange): QueryIrRequest {
  return {
    irVersion: 8,
    from: "logs",
    range: rangeDoc(range),
    result: "table",
    pipeline: [
      rumEventWhere(),
      {
        aggregate: {
          by: ["service.name"],
          aggs: [
            { fn: "count", as: "n" },
            {
              fn: "last",
              of: "resource.telemetry.sdk.language",
              as: "sdk_lang",
            },
            {
              fn: "last",
              of: "resource.deployment.environment.name",
              as: "env_name",
            },
            { fn: "last", of: "resource.service.version", as: "svc_version" },
          ],
        },
      },
      { order: [{ of: "n", dir: "desc" }] },
      { limit: 50 },
    ],
  };
}

export function rumAppsFromResponse(res: QueryIrResponse): RumApp[] {
  return (res.rows ?? []).map((row) => {
    const [serviceName, n, sdkLang, env, version] = row as [
      string,
      number,
      string | null,
      string | null,
      string | null,
    ];
    return {
      serviceName,
      sdkLanguage: sdkLang ?? null,
      env: env ?? null,
      version: version ?? null,
      count: typeof n === "number" ? n : 0,
    };
  });
}

export async function fetchRumApps(range: ResolvedRange): Promise<RumApp[]> {
  return rumAppsFromResponse(await runIrQuery(buildRumAppsDoc(range)));
}

// ---- KPIs: one bucketed read per metric, bundled into one request --------

export type RumKpiMetric =
  "sessions" | "users" | "sessions_with_errors" | "page_views";

export type RumKpiSeriesPoint = IrPoint;

/** The aggregate a KPI metric reduces to — always exactly one output (see
 * the module doc), scoped by its own `where` when the metric counts a
 * subset of records rather than every RUM record. */
function kpiAgg(metric: RumKpiMetric) {
  switch (metric) {
    case "sessions_with_errors":
      return {
        fn: "count_distinct" as const,
        of: "session.id",
        as: "n",
        where: {
          field: "event_name",
          op: "eq" as const,
          value: "exception",
        },
      };
    case "page_views":
      return {
        fn: "count" as const,
        as: "n",
        where: {
          field: "event_name",
          op: "eq" as const,
          value: "browser.navigation",
        },
      };
    case "users":
      return { fn: "count_distinct" as const, of: "user.id", as: "n" };
    case "sessions":
      return { fn: "count_distinct" as const, of: "session.id", as: "n" };
  }
}

/** Every named KPI metric's bucketed series over `[range.fromMs - span,
 * range.toMs)`, in one HTTP request (see the module doc). The earlier half
 * of each metric's series is the previous window, the later half is
 * `range` itself (design.md decision 3a); the caller splits each with
 * `splitKpiSeries`, same as a single-metric read would. */
export function buildKpisDoc(
  app: string,
  range: ResolvedRange,
  bucketCount: number,
  metrics: readonly RumKpiMetric[],
): MultiQueryIrRequest {
  const span = range.toMs - range.fromMs;
  const stepMs = Math.max(1000, Math.round(span / bucketCount));
  const doubled = { fromMs: range.fromMs - span, toMs: range.toMs };
  const step = `${Math.round(stepMs / 1000)}s`;
  const queries: Record<string, QueryIrRequest> = {};
  const formulas: QueryFormula[] = [];
  for (const metric of metrics) {
    queries[metric] = {
      irVersion: 9,
      from: "logs",
      range: rangeDoc(doubled),
      result: "series",
      pipeline: [
        serviceWhere(app),
        rumEventWhere(),
        { aggregate: { aggs: [kpiAgg(metric)], step } },
      ],
    };
    formulas.push({ name: metric, expr: metric });
  }
  return { queries, formulas, result: "series" };
}

/** Decodes the combined KPIs response, keyed by metric — each named
 * query's identity formula tags its series with `labels.formula`. A metric
 * with no matching series (nothing recorded, or the server dropped an
 * empty result) decodes to an empty array, not a missing key. */
export function kpisFromResponse(
  res: QueryIrResponse,
  metrics: readonly RumKpiMetric[],
): Record<string, RumKpiSeriesPoint[]> {
  const out: Record<string, RumKpiSeriesPoint[]> = {};
  for (const metric of metrics) out[metric] = [];
  for (const s of res.series ?? []) {
    const metric = s.labels?.formula;
    if (typeof metric === "string" && metric in out) {
      out[metric] = decodePoints(s.points);
    }
  }
  return out;
}

export async function fetchKpis(
  app: string,
  range: ResolvedRange,
  metrics: readonly RumKpiMetric[],
  bucketCount = 30,
): Promise<Record<string, RumKpiSeriesPoint[]>> {
  const res = await runIrQuery(buildKpisDoc(app, range, bucketCount, metrics));
  return kpisFromResponse(res, metrics);
}

// ---- Web Vitals -----------------------------------------------------------

export interface RumVitalRow {
  name: string;
  rating: string | null;
  count: number;
  p75: number | null;
}

export function buildVitalsDoc(
  app: string,
  range: ResolvedRange,
): QueryIrRequest {
  return {
    irVersion: 8,
    from: "logs",
    range: rangeDoc(range),
    result: "table",
    pipeline: [
      serviceWhere(app),
      { where: { field: "event_name", op: "eq", value: "browser.web_vital" } },
      {
        aggregate: {
          by: ["browser.web_vital.name", "browser.web_vital.rating"],
          aggs: [
            { fn: "count", as: "n" },
            {
              fn: "quantile",
              of: "browser.web_vital.value",
              arg: 0.75,
              as: "p75",
            },
          ],
        },
      },
    ],
  };
}

export function vitalRowsFromResponse(res: QueryIrResponse): RumVitalRow[] {
  return (res.rows ?? []).map((row) => {
    const [name, rating, n, p75] = row as [
      string | null,
      string | null,
      number,
      number | null,
    ];
    return {
      name: name ?? "",
      rating,
      count: typeof n === "number" ? n : 0,
      p75: typeof p75 === "number" ? p75 : null,
    };
  });
}

export interface RumVitalData {
  /** p75 across every rating for this vital — undefined with no records. */
  p75?: number;
  counts: Partial<Record<VitalRating, number>>;
}

/** Groups the raw per-(name, rating) rows into one entry per vital name.
 * The overall p75 is the highest-count rating group's own p75 as a stand-in
 * for the ungrouped p75 when only per-rating quantiles are available; where
 * the source instead answers with a single ungrouped row per vital, that
 * row's `rating` is null and its `p75` is used directly. */
export function vitalsByName(
  rows: RumVitalRow[],
): Map<VitalName, RumVitalData> {
  const out = new Map<VitalName, RumVitalData>();
  for (const row of rows) {
    const name = row.name.toLowerCase() as VitalName;
    if (!out.has(name)) out.set(name, { counts: {} });
    const entry = out.get(name)!;
    if (
      row.rating === "good" ||
      row.rating === "needs-improvement" ||
      row.rating === "poor"
    ) {
      entry.counts[row.rating] = (entry.counts[row.rating] ?? 0) + row.count;
    }
  }
  return out;
}

export async function fetchVitals(
  app: string,
  range: ResolvedRange,
): Promise<Map<VitalName, RumVitalData>> {
  const rows = vitalRowsFromResponse(
    await runIrQuery(buildVitalsDoc(app, range)),
  );
  const byName = vitalsByName(rows);
  // The p75 that spans every rating is a separate read: a per-rating
  // quantile is not the vital's own p75 (ratings are exclusive buckets of
  // the same value distribution, not a resampling of it).
  const p75s = vitalP75FromResponse(
    await runIrQuery(buildVitalP75Doc(app, range)),
  );
  for (const [name, p75] of p75s) {
    const entry = byName.get(name) ?? { counts: {} };
    entry.p75 = p75;
    byName.set(name, entry);
  }
  return byName;
}

function buildVitalP75Doc(app: string, range: ResolvedRange): QueryIrRequest {
  return {
    irVersion: 8,
    from: "logs",
    range: rangeDoc(range),
    result: "table",
    pipeline: [
      serviceWhere(app),
      { where: { field: "event_name", op: "eq", value: "browser.web_vital" } },
      {
        aggregate: {
          by: ["browser.web_vital.name"],
          aggs: [
            {
              fn: "quantile",
              of: "browser.web_vital.value",
              arg: 0.75,
              as: "p75",
            },
          ],
        },
      },
    ],
  };
}

function vitalP75FromResponse(res: QueryIrResponse): Map<VitalName, number> {
  const out = new Map<VitalName, number>();
  for (const row of res.rows ?? []) {
    const [name, p75] = row as [string | null, number | null];
    if (name && typeof p75 === "number") {
      out.set(name.toLowerCase() as VitalName, p75);
    }
  }
  return out;
}

// ---- Sessions over time (with / without errors) --------------------------

export interface RumSessionSeries {
  total: RumKpiSeriesPoint[];
  withErrors: RumKpiSeriesPoint[];
}

/** Distinct sessions per bucket, and the same scoped to sessions carrying
 * an `exception` — bundled into one request the same way `buildKpisDoc`
 * bundles the KPI metrics (a `step` aggregate allows only one output, so
 * the two counts can't share an `aggregate` stage). */
export function buildSessionsOverTimeDoc(
  app: string,
  range: ResolvedRange,
  stepSeconds: number,
): MultiQueryIrRequest {
  function sub(errorsOnly: boolean): QueryIrRequest {
    return {
      irVersion: 9,
      from: "logs",
      range: rangeDoc(range),
      result: "series",
      pipeline: [
        serviceWhere(app),
        rumEventWhere(),
        {
          aggregate: {
            aggs: [
              {
                fn: "count_distinct",
                of: "session.id",
                as: "n",
                ...(errorsOnly
                  ? {
                      where: {
                        field: "event_name",
                        op: "eq",
                        value: "exception",
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
  return {
    queries: { total: sub(false), with_errors: sub(true) },
    formulas: [
      { name: "total", expr: "total" },
      { name: "with_errors", expr: "with_errors" },
    ],
    result: "series",
  };
}

export function sessionsOverTimeFromResponse(
  res: QueryIrResponse,
): RumSessionSeries {
  const byFormula = new Map<string, RumKpiSeriesPoint[]>();
  for (const s of res.series ?? []) {
    const f = s.labels?.formula;
    if (typeof f === "string") byFormula.set(f, decodePoints(s.points));
  }
  return {
    total: byFormula.get("total") ?? [],
    withErrors: byFormula.get("with_errors") ?? [],
  };
}

export async function fetchSessionsOverTime(
  app: string,
  range: ResolvedRange,
  stepSeconds: number,
): Promise<RumSessionSeries> {
  return sessionsOverTimeFromResponse(
    await runIrQuery(buildSessionsOverTimeDoc(app, range, stepSeconds)),
  );
}

// ---- Browser / device breakdown ------------------------------------------

export interface RumBreakdownRow {
  value: string | null;
  count: number;
}

export interface RumBreakdownOptions {
  /** Excludes records missing `field` entirely, so they don't show up as a
   * spurious "null" row — used for `browser.brands` (design.md — Context:
   * missing before this change's own instrumentation update; the hive
   * deployment confirms it) but not for a field every record carries
   * (`browser.mobile`). */
  requireField?: boolean;
  limit?: number;
}

/** One field's value counts for the app, busiest first — shared by the
 * Overview tab's browser and device breakdowns (and any future one). */
export function buildBreakdownDoc(
  app: string,
  range: ResolvedRange,
  field: string,
  opts: RumBreakdownOptions = {},
): QueryIrRequest {
  return {
    irVersion: 8,
    from: "logs",
    range: rangeDoc(range),
    result: "table",
    pipeline: [
      serviceWhere(app),
      rumEventWhere(),
      ...(opts.requireField
        ? [{ where: { field, op: "exists" as const } }]
        : []),
      { aggregate: { by: [field], aggs: [{ fn: "count", as: "n" }] } },
      { order: [{ of: "n", dir: "desc" as const }] },
      ...(opts.limit !== undefined ? [{ limit: opts.limit }] : []),
    ],
  };
}

export function breakdownFromResponse(res: QueryIrResponse): RumBreakdownRow[] {
  return (res.rows ?? []).map((row) => {
    const [value, n] = row as [unknown, number];
    return {
      value: value == null ? null : String(value),
      count: typeof n === "number" ? n : 0,
    };
  });
}

export async function fetchBreakdown(
  app: string,
  range: ResolvedRange,
  field: string,
  opts: RumBreakdownOptions = {},
): Promise<RumBreakdownRow[]> {
  return breakdownFromResponse(
    await runIrQuery(buildBreakdownDoc(app, range, field, opts)),
  );
}

export type { VitalName, VitalRating };
