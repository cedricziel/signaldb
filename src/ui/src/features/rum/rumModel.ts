/**
 * Pure model helpers behind the Real users page: Web Vitals thresholds,
 * formatting and rating (`explore-ui-rum`'s "Web Vitals from
 * browser.web_vital events" requirement), distribution shares for the
 * good/needs-improvement/poor bar, the KPI bucketed-series split, and the
 * page's tab list (shared by the tab strip and the command palette). No
 * fetching, no React — see `api/rum.ts` and `useRumData.ts` for those. The
 * KPI change indicator itself is `lib/relChange.ts`, shared with
 * `features/overview`.
 */

/** `browser.web_vital.name` values, lowercase as the SDK emits them. */
export type VitalName = "lcp" | "inp" | "cls" | "fcp" | "ttfb";

export const VITAL_NAMES: readonly VitalName[] = [
  "lcp",
  "inp",
  "cls",
  "fcp",
  "ttfb",
];

export type VitalRating = "good" | "needs-improvement" | "poor";

/** Web Vitals thresholds (good ≤ first, poor > second), in the value's own
 * unit — ms for every vital but CLS, which is unitless. */
const THRESHOLDS: Record<VitalName, [number, number]> = {
  lcp: [2500, 4000],
  inp: [200, 500],
  cls: [0.1, 0.25],
  fcp: [1800, 3000],
  ttfb: [800, 1800],
};

export const VITAL_LABELS: Record<VitalName, string> = {
  lcp: "LCP",
  inp: "INP",
  cls: "CLS",
  fcp: "FCP",
  ttfb: "TTFB",
};

export const VITAL_TITLES: Record<VitalName, string> = {
  lcp: "Largest Contentful Paint",
  inp: "Interaction to Next Paint",
  cls: "Cumulative Layout Shift",
  fcp: "First Contentful Paint",
  ttfb: "Time to First Byte",
};

const RATING_LABELS: Record<VitalRating, string> = {
  good: "Good",
  "needs-improvement": "Needs improvement",
  poor: "Poor",
};

export function ratingLabel(rating: VitalRating): string {
  return RATING_LABELS[rating];
}

/** Rates a p75 value against the vital's thresholds — used when a rating
 * needs deriving from a raw number (e.g. the aggregated p75), independent
 * of the per-record `browser.web_vital.rating` the distribution bar reads. */
export function rateVital(name: VitalName, p75: number): VitalRating {
  const [good, poor] = THRESHOLDS[name];
  if (p75 <= good) return "good";
  if (p75 <= poor) return "needs-improvement";
  return "poor";
}

/** LCP/FCP/TTFB in seconds, INP in milliseconds, CLS unitless — per the
 * spec's display units. `value` is in the record's own unit (ms, or
 * unitless for CLS). */
export function formatVitalValue(name: VitalName, value: number): string {
  switch (name) {
    case "lcp":
    case "fcp":
    case "ttfb":
      return `${(value / 1000).toFixed(value >= 10000 ? 0 : 1)} s`;
    case "inp":
      return `${Math.round(value)} ms`;
    case "cls":
      return value.toFixed(2);
  }
}

/** `—` for a vital with no records in the window, never `0`. */
export const NO_VITAL_DATA = "—";

export interface VitalShare {
  rating: VitalRating;
  count: number;
  share: number;
  threshold: string;
}

const THRESHOLD_TEXT: Record<VitalName, string> = {
  lcp: "≤2.5s good · >4s poor",
  inp: "≤200ms good · >500ms poor",
  cls: "≤0.1 good · >0.25 poor",
  fcp: "≤1.8s good · >3s poor",
  ttfb: "≤0.8s good · >1.8s poor",
};

export function vitalThresholdText(name: VitalName): string {
  return THRESHOLD_TEXT[name];
}

/** The good/poor threshold bound, formatted in the vital's own display
 * unit — for the distribution tooltip's "Good ≤ 2.5 s" / "Poor > 4 s" rows
 * (the prototype's `VitalDist`). */
export function vitalThresholdBound(
  name: VitalName,
  which: "good" | "poor",
): string {
  const [good, poor] = THRESHOLDS[name];
  return formatVitalValue(name, which === "good" ? good : poor);
}

/** CSS colour token for a rating's *text* (the word "Good"/"Poor"), distinct
 * from `RATING_COLOR`-style swatch colours used for the distribution bar —
 * matches the prototype's `ok-text`/`warn-banner-text`/`err` choice, which
 * reads better as inline text than the bar's saturated `--ok`/`--warn`. */
export function ratingTextColorVar(rating: VitalRating): string {
  return rating === "good"
    ? "var(--ok-text)"
    : rating === "poor"
      ? "var(--err)"
      : "var(--warn-banner-text)";
}

/** CSS colour token for a rating's *swatch* (the distribution bar segment
 * and its tooltip row) — matches the prototype's `RATE_COLOR`. */
export function ratingSwatchColorVar(rating: VitalRating): string {
  return rating === "good"
    ? "var(--ok)"
    : rating === "poor"
      ? "var(--err)"
      : "var(--warn)";
}

/** The good/needs-improvement/poor distribution bar's shares, always in
 * that order, summing to 1 (or all zero when there are no records). */
export function vitalShares(
  counts: Partial<Record<VitalRating, number>>,
): VitalShare[] {
  const order: VitalRating[] = ["good", "needs-improvement", "poor"];
  const total = order.reduce((s, r) => s + (counts[r] ?? 0), 0);
  return order.map((rating) => {
    const count = counts[rating] ?? 0;
    return {
      rating,
      count,
      share: total > 0 ? count / total : 0,
      threshold: "",
    };
  });
}

export interface VitalFigure {
  name: VitalName;
  /** Undefined when the window holds no record for this vital — render
   * `NO_VITAL_DATA`, not `0`. */
  p75?: number;
  rating?: VitalRating;
  formatted: string;
  shares: VitalShare[];
}

/** One vital card's figures from its p75 and per-rating counts. */
export function vitalFigure(
  name: VitalName,
  p75: number | undefined,
  counts: Partial<Record<VitalRating, number>>,
): VitalFigure {
  const hasData = p75 !== undefined && !Number.isNaN(p75);
  return {
    name,
    p75: hasData ? p75 : undefined,
    rating: hasData ? rateVital(name, p75) : undefined,
    formatted: hasData ? formatVitalValue(name, p75) : NO_VITAL_DATA,
    shares: vitalShares(counts),
  };
}

// ---- KPI delta / sparkline -----------------------------------------------

export interface KpiSeriesPoint {
  tMs: number;
  value: number;
}

export interface KpiFigure {
  /** Sum (or, for a share KPI, the ratio) over the current half of the
   * bucketed window. */
  value: number;
  /** Same figure for the equal-length window immediately before; undefined
   * when that half has no data to compare against. */
  previous?: number;
  /** The current half's points only — what the card's sparkline draws. */
  series: KpiSeriesPoint[];
}

/** Splits one bucketed series spanning `[from, from + 2*span)` into the
 * earlier and later halves at their midpoint, and sums each half — the "one
 * bucketed read over twice the window" pattern (design.md decision 3a):
 * the later half is this window's value and sparkline, the earlier half is
 * the previous-window comparison. */
export function splitKpiSeries(
  points: KpiSeriesPoint[],
  midMs: number,
): KpiFigure {
  const previous = points.filter((p) => p.tMs < midMs);
  const current = points.filter((p) => p.tMs >= midMs);
  return {
    value: current.reduce((s, p) => s + p.value, 0),
    previous:
      previous.length > 0
        ? previous.reduce((s, p) => s + p.value, 0)
        : undefined,
    series: current,
  };
}

// ---- KPI share (traced requests) ------------------------------------------

export interface KpiShareFigure {
  /** sum(traced) / sum(total) over the current half — exact, not an average
   * of per-bucket ratios (a bucket with zero client spans has an undefined
   * ratio, which would skew a mean). `0` when the half has no client spans
   * at all — check `hasData` to tell "0% traced" from "nothing to measure". */
  value: number;
  /** Same ratio for the equal-length window before; undefined when that
   * half has no client spans to divide by. */
  previous?: number;
  /** Per-bucket traced/total for the current half, buckets with no client
   * spans omitted (an undefined ratio, not a `0`, would misdraw the
   * sparkline) — what the card's sparkline draws. */
  series: KpiSeriesPoint[];
  /** Whether the current half saw any client span at all — false makes
   * `value`'s `0` a "no data" reading rather than a genuine 0% traced. */
  hasData: boolean;
}

/** Splits a traced-count and a total-count series (design.md decision 3a's
 * "one bucketed read over twice the window", applied to a ratio metric) via
 * `splitKpiSeries` on each operand, then divides sums rather than averaging
 * per-bucket ratios — see `KpiShareFigure`. */
export function splitTracedShare(
  traced: KpiSeriesPoint[],
  total: KpiSeriesPoint[],
  midMs: number,
): KpiShareFigure {
  const tracedFigure = splitKpiSeries(traced, midMs);
  const totalFigure = splitKpiSeries(total, midMs);
  // Traced rows are bucketed by the server child's start and total rows by
  // the client span's, so a request can land in different buckets; clamp so
  // that skew never reads as more than 100%.
  const share = (traced: number, total: number) => Math.min(1, traced / total);
  const tracedByT = new Map(tracedFigure.series.map((p) => [p.tMs, p.value]));
  const series = totalFigure.series.flatMap((p): KpiSeriesPoint[] =>
    p.value > 0
      ? [{ tMs: p.tMs, value: share(tracedByT.get(p.tMs) ?? 0, p.value) }]
      : [],
  );
  return {
    value:
      totalFigure.value > 0 ? share(tracedFigure.value, totalFigure.value) : 0,
    previous:
      totalFigure.previous !== undefined && totalFigure.previous > 0
        ? share(tracedFigure.previous ?? 0, totalFigure.previous)
        : undefined,
    series,
    hasData: totalFigure.value > 0,
  };
}

// ---- Network: URL templates and SDK export detection ----------------------

export interface UrlTemplate {
  origin: string;
  template: string;
}

const NUMERIC_SEGMENT = /^[0-9]+$/;
const UUID_SEGMENT =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const LONG_HEX_SEGMENT = /^[0-9a-f]{16,}$/i;

/** Buckets a request URL into a stable route template for grouping, when
 * the record itself carries no `url.template` (browser fetch spans don't —
 * see the module's callers): the path of `url.full` with its query
 * stripped and id-like segments (numeric, UUID, or a long hex string)
 * replaced by `:id`. `origin` is the host only (`reviews.partner-cdn.com`,
 * not `https://reviews.partner-cdn.com`) — the spec's own display form for
 * an origin. `null` for a URL that doesn't parse. */
export function urlTemplate(urlFull: string): UrlTemplate | null {
  let url: URL;
  try {
    url = new URL(urlFull);
  } catch {
    return null;
  }
  const segments = url.pathname.split("/").filter((s) => s !== "");
  const templated = segments.map((s) =>
    NUMERIC_SEGMENT.test(s) || UUID_SEGMENT.test(s) || LONG_HEX_SEGMENT.test(s)
      ? ":id"
      : s,
  );
  return { origin: url.host, template: `/${templated.join("/")}` };
}

/** A regex matching any `url.full` whose path fits `route`, with `:param`
 * segments matching one path segment — the server-side counterpart of
 * `urlTemplate` for records that carry no `url.template`. */
export function routeUrlRegex(route: string): string {
  const path = route
    .split("/")
    .map((seg) =>
      seg.startsWith(":")
        ? "[^/?#]+"
        : seg.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"),
    )
    .join("/");
  return `^[a-z]+://[^/]+${path}/?([?#]|$)`;
}

const SDK_EXPORT_SUFFIXES = ["/v1/traces", "/v1/logs", "/v1/metrics"];

/** A request to the telemetry export endpoint itself — marked "SDK export"
 * rather than counted toward the Network tab's untraced-origin callout
 * (`explore-ui-rum`'s "Network tab" requirement). */
export function isSdkExportPath(path: string): boolean {
  return SDK_EXPORT_SUFFIXES.some((suffix) => path.endsWith(suffix));
}

// ---- Pages: route resolution -----------------------------------------

/** No `url.template` and no parseable `url.full` — the Pages tab's
 * missing-route bucket (design decision 3/3a). */
export const MISSING_ROUTE = null;

/** The route a page-scoped record belongs to: `url.template` when the app
 * set it, else the path template derived from `url.full`, else
 * `MISSING_ROUTE`. Shared by every Pages-tab query decoder so they all
 * bucket the same way. */
export function resolveRoute(
  template: string | null | undefined,
  full: string | null | undefined,
): string | null {
  if (template) return template;
  if (full) return urlTemplate(full)?.template ?? null;
  return MISSING_ROUTE;
}

/** The last 2-3 segments of a `browser.css_selector` path for compact
 * display — a full DOM path selector can be extremely long, and the last
 * few segments are what identifies the clicked element. The full value
 * still belongs in a `title` attribute at the call site. */
export function cssSelectorLabel(selector: string): string {
  const parts = selector.split(/\s*>\s*/).filter((p) => p !== "");
  return parts.slice(-3).join(" > ");
}

// ---- Pages: load breakdown --------------------------------------------

export interface LoadPhase {
  label: string;
  ms: number;
}

/** Phase boundaries (ms, from `fetchStart`) needed for
 * `loadBreakdownPhases` — p75s of `browser.resource_timing`'s basic phases
 * plus `browser.navigation_timing`'s DOM/load milestones, all relative to
 * the navigation's own `fetchStart` per the Navigation Timing spec. */
export interface NavTimingP75 {
  domainLookupStart?: number;
  domainLookupEnd?: number;
  connectStart?: number;
  connectEnd?: number;
  requestStart?: number;
  responseStart?: number;
  responseEnd?: number;
  domInteractive?: number;
  domContentLoadedEventEnd?: number;
  loadEventEnd?: number;
}

/** The load waterfall's named, non-overlapping phases (DNS through load),
 * each clamped to zero — a p75 blend across boundary fields isn't
 * guaranteed monotonic, and a negative bar would misdraw. A phase with an
 * unrecorded boundary is left out rather than measured from zero. */
export function loadBreakdownPhases(p75: NavTimingP75): LoadPhase[] {
  const spans: [string, number | undefined, number | undefined][] = [
    ["DNS", p75.domainLookupStart, p75.domainLookupEnd],
    ["Connect + TLS", p75.connectStart, p75.connectEnd],
    ["Request → first byte", p75.requestStart, p75.responseStart],
    ["Response", p75.responseStart, p75.responseEnd],
    ["DOM processing", p75.responseEnd, p75.domInteractive],
    ["DOMContentLoaded", p75.domInteractive, p75.domContentLoadedEventEnd],
    ["Load", p75.domContentLoadedEventEnd, p75.loadEventEnd],
  ];
  return spans.flatMap(([label, start, end]) =>
    start === undefined || end === undefined
      ? []
      : [{ label, ms: Math.max(0, end - start) }],
  );
}

// ---- Tabs ------------------------------------------------------------

export type RumTab =
  "overview" | "pages" | "network" | "interactions" | "setup";

/** The tabs this build ships, in display order — the page's tab strip and
 * the command palette both map over this (`explore-ui-rum`'s "Real users
 * command palette entries" requirement), so a later group's new tab needs
 * adding only here. Final order per `rum-explore-tabs`: Overview, Pages,
 * Sessions, Errors, Network, Interactions, Setup — Sessions and Errors ship
 * in later groups. */
export const RUM_TABS: { id: RumTab; label: string }[] = [
  { id: "overview", label: "Overview" },
  { id: "pages", label: "Pages" },
  { id: "network", label: "Network" },
  { id: "interactions", label: "Interactions" },
  { id: "setup", label: "Setup" },
];

/** An unknown tab settles on Overview, matching the route's own fallback. */
export function rumTabFromParam(value: string | undefined): RumTab {
  return RUM_TABS.some((t) => t.id === value) ? (value as RumTab) : "overview";
}
