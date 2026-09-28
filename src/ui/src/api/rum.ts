/**
 * Real user monitoring: apps, KPIs, Web Vitals, sessions-over-time and the
 * browser/device breakdown behind the Real users page (Overview tab) — see
 * `openspec/specs/explore-ui-rum/spec.md`.
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
import {
  isSdkExportPath,
  urlTemplate,
  type VitalName,
  type VitalRating,
} from "../features/rum/rumModel";

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

// ---- Network: requests grouped by URL template -----------------------
//
// Two reads, merged client-side (design.md decision 3): `buildNetworkRequestsDoc`
// groups every client HTTP span by its raw (method, url.full, server.address)
// for the total calls, p75 and error share; `buildNetworkCorrelateDoc` joins
// each to its server-kind child via `correlate` for the traced count and the
// backend's own p75 (`docs/users/querying-ir.md`'s "Joining spans to their
// parents"). `networkRowsFromResponses` merges the two by that raw key, then
// buckets by (method, origin, URL template) since the raw grouping is too
// fine for display (`/orders/48213` and `/orders/91820` are one endpoint).

function clientSpanWhere(app: string): Record<string, unknown> {
  return {
    where: {
      and: [
        { field: "service.name", op: "eq", value: app },
        { field: "span_kind", op: "eq", value: "Client" },
      ],
    },
  };
}

/** ≥400, or the span itself recorded an error — the spec's error-share
 * predicate for a client HTTP span. */
const HTTP_ERROR_WHERE = {
  or: [
    { field: "status.code", op: "eq", value: "Error" },
    { field: "http.response.status_code", op: "gte", value: 400 },
  ],
};

const NETWORK_GROUP_LIMIT = 500;

/** The `correlate`-joined pair of stages shared by `buildNetworkCorrelateDoc`
 * and `buildTracedShareDoc`'s traced query: join to the parent span, then
 * keep only rows where that parent is one of the app's own client spans and
 * this (child) row is its server-kind callee — the "traced" join, once. */
function correlatedServerChildOfAppClient(
  app: string,
): Record<string, unknown>[] {
  return [
    { correlate: { to: "parent", kind: "inner" } },
    {
      where: {
        and: [
          { field: "parent.service.name", op: "eq", value: app },
          { field: "parent.span_kind", op: "eq", value: "Client" },
          { field: "span_kind", op: "eq", value: "Server" },
        ],
      },
    },
  ];
}

export function buildNetworkRequestsDoc(
  app: string,
  range: ResolvedRange,
): QueryIrRequest {
  return {
    irVersion: 8,
    from: "traces",
    range: rangeDoc(range),
    result: "table",
    pipeline: [
      clientSpanWhere(app),
      {
        aggregate: {
          by: ["http.request.method", "url.full", "server.address"],
          aggs: [
            { fn: "count", as: "n" },
            { fn: "quantile", of: "duration", arg: 0.75, as: "p75" },
            { fn: "count", as: "errors", where: HTTP_ERROR_WHERE },
          ],
        },
      },
      { order: [{ of: "n", dir: "desc" }] },
      { limit: NETWORK_GROUP_LIMIT + 1 },
    ],
  };
}

export function buildNetworkCorrelateDoc(
  app: string,
  range: ResolvedRange,
): QueryIrRequest {
  return {
    irVersion: 9,
    from: "traces",
    range: rangeDoc(range),
    result: "table",
    pipeline: [
      ...correlatedServerChildOfAppClient(app),
      {
        aggregate: {
          by: [
            "parent.http.request.method",
            "parent.url.full",
            "parent.server.address",
            "service.name",
          ],
          aggs: [
            // A client span can have more than one server-kind child (a
            // retry, a fan-out) — `count` would count children, not calls,
            // and could push a group's traced share past 100%.
            { fn: "count_distinct", of: "parent.span_id", as: "n" },
            { fn: "quantile", of: "duration", arg: 0.75, as: "server_p75" },
          ],
        },
      },
      { order: [{ of: "n", dir: "desc" }] },
      { limit: NETWORK_GROUP_LIMIT + 1 },
    ],
  };
}

export interface RumRequestRow {
  method: string;
  origin: string;
  template: string;
  calls: number;
  tracedCalls: number;
  errorCalls: number;
  /** p75 across every call in the group (traced or not), ms — null with no
   * duration recorded. */
  totalP75Ms: number | null;
  /** p75 of the server-kind child's own duration, over the traced subset,
   * ms — undefined for a group with no traced call. */
  backendP75Ms?: number;
  /** The backend `service.name` seen for this group's traced calls —
   * undefined for a group with no traced call. */
  backendService?: string;
  /** A request to SignalDB's own telemetry export endpoint — the spec's
   * "marked as SDK export, not as untraced". */
  isSdkExport: boolean;
  /** False when the correlate read hit its group cap before reaching this
   * request, so `tracedCalls` may undercount rather than mean "untraced". */
  tracedKnown: boolean;
}

const NS_PER_MS = 1_000_000;

interface RawGroupKey {
  method: string;
  urlFull: string;
  serverAddress: string | null;
}

function rawGroupKey(k: RawGroupKey): string {
  return `${k.method}\u0000${k.urlFull}\u0000${k.serverAddress ?? ""}`;
}

interface RawTotal extends RawGroupKey {
  calls: number;
  p75Ms: number | null;
  errors: number;
}

interface RawTraced extends RawGroupKey {
  backendService: string | null;
  tracedCalls: number;
  serverP75Ms: number | null;
}

function totalsFromResponse(res: QueryIrResponse): RawTotal[] {
  return (res.rows ?? []).map((row) => {
    const [method, urlFull, serverAddress, n, p75, errors] = row as [
      string | null,
      string | null,
      string | null,
      number,
      number | null,
      number,
    ];
    return {
      method: method ?? "",
      urlFull: urlFull ?? "",
      serverAddress: serverAddress ?? null,
      calls: typeof n === "number" ? n : 0,
      p75Ms: typeof p75 === "number" ? p75 / NS_PER_MS : null,
      errors: typeof errors === "number" ? errors : 0,
    };
  });
}

function tracedFromResponse(res: QueryIrResponse): RawTraced[] {
  return (res.rows ?? []).map((row) => {
    const [method, urlFull, serverAddress, backendService, n, serverP75] =
      row as [
        string | null,
        string | null,
        string | null,
        string | null,
        number,
        number | null,
      ];
    return {
      method: method ?? "",
      urlFull: urlFull ?? "",
      serverAddress: serverAddress ?? null,
      backendService: backendService ?? null,
      tracedCalls: typeof n === "number" ? n : 0,
      serverP75Ms: typeof serverP75 === "number" ? serverP75 / NS_PER_MS : null,
    };
  });
}

/** Weighted mean of a p75 per merged group — an approximation (a mean of
 * p75s is not the merged population's own p75), documented as such in
 * `RumRequestRow`'s display, not hidden as an exact figure. */
function weightedMean(
  pairs: { value: number; weight: number }[],
): number | undefined {
  const totalWeight = pairs.reduce((s, p) => s + p.weight, 0);
  if (totalWeight <= 0) return undefined;
  return pairs.reduce((s, p) => s + p.value * p.weight, 0) / totalWeight;
}

/** Merges the two network reads into display rows, bucketed by (method,
 * origin, URL template) — see the module doc above. */
export function networkRowsFromResponses(
  totalsRes: QueryIrResponse,
  tracedRes: QueryIrResponse,
): RumRequestRow[] {
  // Both reads ask for one row over the cap, so an overflow is detectable.
  const tracedRows = tracedFromResponse(tracedRes);
  const tracedTruncated = tracedRows.length > NETWORK_GROUP_LIMIT;
  // One raw key can have several traced rows, one per backend service.
  const tracedByKey = new Map<string, RawTraced[]>();
  for (const t of tracedRows.slice(0, NETWORK_GROUP_LIMIT)) {
    const key = rawGroupKey(t);
    tracedByKey.set(key, [...(tracedByKey.get(key) ?? []), t]);
  }

  interface Bucket {
    method: string;
    origin: string;
    template: string;
    calls: number;
    tracedCalls: number;
    errorCalls: number;
    p75Pairs: { value: number; weight: number }[];
    backendP75Pairs: { value: number; weight: number }[];
    callsByService: Map<string, number>;
    tracedKnown: boolean;
  }
  const buckets = new Map<string, Bucket>();

  for (const total of totalsFromResponse(totalsRes).slice(
    0,
    NETWORK_GROUP_LIMIT,
  )) {
    const parsed = urlTemplate(total.urlFull);
    const origin = parsed?.origin ?? total.serverAddress ?? "unknown";
    const template = parsed?.template ?? total.urlFull;
    const bucketKey = `${total.method}\u0000${origin}\u0000${template}`;

    const bucket: Bucket = buckets.get(bucketKey) ?? {
      method: total.method,
      origin,
      template,
      calls: 0,
      tracedCalls: 0,
      errorCalls: 0,
      p75Pairs: [],
      backendP75Pairs: [],
      callsByService: new Map(),
      tracedKnown: true,
    };
    bucket.calls += total.calls;
    bucket.errorCalls += total.errors;
    if (total.p75Ms !== null) {
      bucket.p75Pairs.push({ value: total.p75Ms, weight: total.calls });
    }
    const matches = tracedByKey.get(rawGroupKey(total)) ?? [];
    if (tracedTruncated && matches.length === 0) bucket.tracedKnown = false;
    for (const traced of matches) {
      bucket.tracedCalls += traced.tracedCalls;
      if (traced.backendService) {
        bucket.callsByService.set(
          traced.backendService,
          (bucket.callsByService.get(traced.backendService) ?? 0) +
            traced.tracedCalls,
        );
      }
      if (traced.serverP75Ms !== null) {
        bucket.backendP75Pairs.push({
          value: traced.serverP75Ms,
          weight: traced.tracedCalls,
        });
      }
    }
    buckets.set(bucketKey, bucket);
  }

  return Array.from(buckets.values())
    .map((b): RumRequestRow => ({
      method: b.method,
      origin: b.origin,
      template: b.template,
      calls: b.calls,
      tracedCalls: b.tracedCalls,
      errorCalls: b.errorCalls,
      totalP75Ms: weightedMean(b.p75Pairs) ?? null,
      backendP75Ms: weightedMean(b.backendP75Pairs),
      backendService: busiestService(b.callsByService),
      isSdkExport: isSdkExportPath(b.template),
      tracedKnown: b.tracedKnown,
    }))
    .sort((a, b) => b.calls - a.calls);
}

function busiestService(callsByService: Map<string, number>) {
  let best: string | undefined;
  let bestCalls = -1;
  for (const [service, calls] of callsByService) {
    if (calls > bestCalls) [best, bestCalls] = [service, calls];
  }
  return best;
}

export async function fetchNetworkRequests(
  app: string,
  range: ResolvedRange,
): Promise<RumRequestRow[]> {
  const [totalsRes, tracedRes] = await Promise.all([
    runIrQuery(buildNetworkRequestsDoc(app, range)),
    runIrQuery(buildNetworkCorrelateDoc(app, range)),
  ]);
  return networkRowsFromResponses(totalsRes, tracedRes);
}

// ---- Resources by initiator type -------------------------------------

export interface RumResourceRow {
  initiatorType: string;
  count: number;
  transferBytes: number;
  p75Ms: number;
  maxTransferBytes: number;
}

export function buildResourcesDoc(
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
      {
        where: {
          field: "event_name",
          op: "eq",
          value: "browser.resource_timing",
        },
      },
      {
        aggregate: {
          by: ["browser.resource_timing.initiator_type"],
          aggs: [
            { fn: "count", as: "n" },
            {
              fn: "sum",
              of: "browser.resource_timing.transfer_size",
              as: "bytes",
            },
            {
              fn: "quantile",
              of: "browser.resource_timing.duration",
              arg: 0.75,
              as: "p75",
            },
            {
              fn: "max",
              of: "browser.resource_timing.transfer_size",
              as: "max_bytes",
            },
          ],
        },
      },
      { order: [{ of: "n", dir: "desc" }] },
    ],
  };
}

export function resourcesFromResponse(res: QueryIrResponse): RumResourceRow[] {
  return (res.rows ?? []).map((row) => {
    const [type, n, bytes, p75, maxBytes] = row as [
      string | null,
      number,
      number | null,
      number | null,
      number | null,
    ];
    return {
      initiatorType: type ?? "unknown",
      count: typeof n === "number" ? n : 0,
      transferBytes: typeof bytes === "number" ? bytes : 0,
      p75Ms: typeof p75 === "number" ? p75 : 0,
      maxTransferBytes: typeof maxBytes === "number" ? maxBytes : 0,
    };
  });
}

export async function fetchResources(
  app: string,
  range: ResolvedRange,
): Promise<RumResourceRow[]> {
  return resourcesFromResponse(await runIrQuery(buildResourcesDoc(app, range)));
}

// ---- Traced-share KPI (Overview + Setup) ------------------------------

/** Same "one bucketed read over twice the window" shape as `buildKpisDoc`
 * (design.md decision 3a), but the metric itself is a ratio: the share of
 * the app's client spans with a server child, via `correlate` — the
 * verified-working shape from `docs/users/querying-ir.md`'s "Joining spans
 * to their parents", `count_distinct` of `parent.span_id` over `count` of
 * every client span. */
const SDK_EXPORT_URL_REGEX = "/v1/(traces|logs|metrics)([?#]|$)";

function notSdkExport(field: string) {
  return { not: { field, op: "regex", value: SDK_EXPORT_URL_REGEX } };
}

export function buildTracedShareDoc(
  app: string,
  range: ResolvedRange,
  bucketCount: number,
): MultiQueryIrRequest {
  const span = range.toMs - range.fromMs;
  const stepMs = Math.max(1000, Math.round(span / bucketCount));
  const doubled = { fromMs: range.fromMs - span, toMs: range.toMs };
  const step = `${Math.round(stepMs / 1000)}s`;
  const totalQuery: QueryIrRequest = {
    irVersion: 9,
    from: "traces",
    range: rangeDoc(doubled),
    result: "series",
    pipeline: [
      clientSpanWhere(app),
      { where: notSdkExport("url.full") },
      { aggregate: { aggs: [{ fn: "count", as: "n" }], step } },
    ],
  };
  const tracedQuery: QueryIrRequest = {
    irVersion: 9,
    from: "traces",
    range: rangeDoc(doubled),
    result: "series",
    pipeline: [
      ...correlatedServerChildOfAppClient(app),
      { where: notSdkExport("parent.url.full") },
      {
        aggregate: {
          aggs: [{ fn: "count_distinct", of: "parent.span_id", as: "n" }],
          step,
        },
      },
    ],
  };
  return {
    queries: { traced: tracedQuery, total: totalQuery },
    // Identity formulas expose each operand's own series (same trick as
    // `buildKpisDoc`) — `splitTracedShare` divides their summed halves
    // itself rather than trusting a per-bucket `traced / total` average,
    // which a bucket with no client spans would turn into a division by
    // zero.
    formulas: [
      { name: "traced", expr: "traced" },
      { name: "total", expr: "total" },
    ],
    result: "series",
  };
}

export interface RumTracedShareSeries {
  traced: RumKpiSeriesPoint[];
  total: RumKpiSeriesPoint[];
}

export function tracedShareFromResponse(
  res: QueryIrResponse,
): RumTracedShareSeries {
  const byFormula = new Map<string, RumKpiSeriesPoint[]>();
  for (const s of res.series ?? []) {
    const f = s.labels?.formula;
    if (typeof f === "string") byFormula.set(f, decodePoints(s.points));
  }
  return {
    traced: byFormula.get("traced") ?? [],
    total: byFormula.get("total") ?? [],
  };
}

export async function fetchTracedShare(
  app: string,
  range: ResolvedRange,
  bucketCount = 30,
): Promise<RumTracedShareSeries> {
  return tracedShareFromResponse(
    await runIrQuery(buildTracedShareDoc(app, range, bucketCount)),
  );
}

export type { VitalName, VitalRating };
