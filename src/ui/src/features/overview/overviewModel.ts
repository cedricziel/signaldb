// The Overview page's derived figures — health, KPI card contents, the
// setup checklist — as pure functions of the fetched data, so the rules the
// design fixes (health thresholds, change colouring, step order) are tested
// without rendering the page.

import type { CatalogEntity } from "../../api/catalog";
import type { EntityKpis, SeriesPoint } from "../../api/entityDetailStats";
import type { Deploy, VersionSighting } from "../../api/overview";
import type { VolumeSeries } from "../../components/SignalHistogram";
import { compactCount } from "../../lib/vizFormat";
import { compositeKey } from "../../lib/traceGroups";

export type Health = "critical" | "degraded" | "healthy";

/** Critical at ≥ 2% errors; degraded at ≥ 0.5% errors or p95 above 500 ms.
 * The error thresholds match `ServiceGraph`'s own severity colouring. */
export function healthOf(errorRate: number, p95Ms: number): Health {
  if (errorRate >= 0.02) return "critical";
  if (errorRate >= 0.005 || p95Ms > 500) return "degraded";
  return "healthy";
}

const HEALTH_RANK: Record<Health, number> = {
  critical: 0,
  degraded: 1,
  healthy: 2,
};

export interface ServiceRow {
  /** Catalog composite identity key — links and series lookups use it. */
  key: string;
  values: (string | null)[];
  name: string;
  ratePerSec: number;
  errorRate: number;
  p95Ms: number;
  health: Health;
  /** Which sources saw this service in the window. */
  sources: Set<string>;
}

/** The catalog's service entities as table rows, worst health first, then
 * busiest. Services with no spans (seen only in logs or metrics) have no
 * RED figures and sort last among the healthy. */
export function serviceRows(
  entities: CatalogEntity[],
  rangeSeconds: number,
): ServiceRow[] {
  return entities
    .map((e): ServiceRow => {
      const red = e.red;
      const traces = red?.traces ?? 0;
      const errorRate = traces > 0 ? (red?.errors ?? 0) / traces : 0;
      const p95Ms = red?.p95Ms ?? 0;
      return {
        key: compositeKey(e.values),
        values: e.values,
        name: e.values[0] ?? "(not set)",
        ratePerSec: rangeSeconds > 0 ? traces / rangeSeconds : 0,
        errorRate,
        p95Ms,
        health: healthOf(errorRate, p95Ms),
        sources: new Set(
          e.observations.filter((o) => o.count > 0).map((o) => o.source),
        ),
      };
    })
    .sort(
      (a, b) =>
        HEALTH_RANK[a.health] - HEALTH_RANK[b.health] ||
        b.ratePerSec - a.ratePerSec,
    );
}

export function healthCounts(rows: ServiceRow[]): Record<Health, number> {
  const out: Record<Health, number> = { critical: 0, degraded: 0, healthy: 0 };
  for (const r of rows) out[r.health] += 1;
  return out;
}

// ---- KPI cards ---------------------------------------------------------

export type ChangeTone = "good" | "bad" | "neutral";

export interface KpiFigure {
  label: string;
  value: string;
  unit: string;
  /** Absent when there's no previous period to compare against. */
  change?: { text: string; tone: ChangeTone };
  valueTone: "neutral" | "error";
  detail: string;
  series: SeriesPoint[];
  seriesTone: "neutral" | "error";
  formatPoint: (v: number) => string;
}

/** A per-second rate as a card value + unit, switching to per-minute when
 * under one request a second so the number never reads "0.0". */
export function rateFigure(perSec: number): { value: string; unit: string } {
  if (perSec >= 1 || perSec === 0) {
    return { value: compactCount(perSec, "", 0.01), unit: "req/s" };
  }
  return { value: compactCount(perSec * 60, "", 0.1), unit: "req/min" };
}

export function durationFigure(ms: number): { value: string; unit: string } {
  if (ms >= 1000) return { value: (ms / 1000).toFixed(2), unit: "s" };
  return { value: String(Math.round(ms)), unit: "ms" };
}

/** Relative change; `upIsBad` picks which direction reads red. */
function relChange(
  current: number,
  previous: number,
  upIsBad: boolean,
): KpiFigure["change"] {
  if (previous === 0) return undefined;
  const pct = Math.round(((current - previous) / previous) * 100);
  if (pct === 0) return { text: "±0%", tone: "neutral" };
  const up = pct > 0;
  return {
    text: `${up ? "+" : "−"}${Math.abs(pct)}%`,
    tone: up === upIsBad ? "bad" : "good",
  };
}

/** Error-rate change in percentage points, to two decimals — error rates
 * on a healthy system live well below one point. */
function ppChangeFine(current: number, previous: number): KpiFigure["change"] {
  const pp = (current - previous) * 100;
  if (Math.abs(pp) < 0.005) return { text: "±0 pp", tone: "neutral" };
  return {
    text: `${pp > 0 ? "+" : "−"}${Math.abs(pp).toFixed(2)} pp`,
    tone: pp > 0 ? "bad" : "good",
  };
}

function msChange(current: number, previous: number): KpiFigure["change"] {
  const d = Math.round(current - previous);
  if (d === 0) return { text: "±0 ms", tone: "neutral" };
  return {
    text: `${d > 0 ? "+" : "−"}${Math.abs(d)} ms`,
    tone: d > 0 ? "bad" : "good",
  };
}

export function kpiFigures(
  kpis: EntityKpis | undefined,
  rows: ServiceRow[],
  ingest: VolumeSeries[] | undefined,
): KpiFigure[] {
  const cur = kpis?.current;
  const prev = kpis?.previous;
  const busiest = [...rows].sort((a, b) => b.ratePerSec - a.ratePerSec)[0];
  const rate = rateFigure(cur?.ratePerSec ?? 0);
  const errorRate = cur?.errorRate ?? 0;
  const p95 = durationFigure(cur?.p95Ms ?? 0);
  const over2 = rows.filter((r) => r.errorRate >= 0.02).length;

  const ingestTotals = (ingest ?? []).map((s) => ({
    key: s.key,
    total: s.points.reduce((a, [, v]) => a + v, 0),
  }));
  const ingestTotal = ingestTotals.reduce((a, s) => a + s.total, 0);
  const ingestSeries = mergeVolume(ingest ?? []);
  const shares = [...ingestTotals]
    .sort((a, b) => b.total - a.total)
    .slice(0, 2)
    .filter((s) => s.total > 0)
    .map((s) => `${s.key} ${Math.round((s.total / ingestTotal) * 100)}%`);

  return [
    {
      label: "Requests",
      ...rate,
      change:
        cur && prev
          ? relChange(cur.ratePerSec, prev.ratePerSec, false)
          : undefined,
      valueTone: "neutral",
      detail: busiest
        ? `busiest: ${busiest.name}`
        : cur
          ? `peak ${rateFigure(cur.peakRatePerSec).value} ${rateFigure(cur.peakRatePerSec).unit}`
          : "no root spans",
      series: kpis?.series.rate ?? [],
      seriesTone: "neutral",
      formatPoint: (v) => {
        const f = rateFigure(v);
        return `${f.value} ${f.unit}`;
      },
    },
    {
      label: "Error rate",
      value: (errorRate * 100).toFixed(2),
      unit: "%",
      change:
        cur && prev ? ppChangeFine(cur.errorRate, prev.errorRate) : undefined,
      valueTone: errorRate >= 0.005 ? "error" : "neutral",
      detail: `${over2} service${over2 === 1 ? "" : "s"} above 2%`,
      series: kpis?.series.errorRate ?? [],
      seriesTone: "error",
      formatPoint: (v) => `${(v * 100).toFixed(2)}% errors`,
    },
    {
      label: "P95 latency",
      ...p95,
      change: cur && prev ? msChange(cur.p95Ms, prev.p95Ms) : undefined,
      valueTone: "neutral",
      detail: "all root spans",
      series: kpis?.series.p95 ?? [],
      seriesTone: "neutral",
      formatPoint: (v) => {
        const f = durationFigure(v);
        return `${f.value} ${f.unit} p95`;
      },
    },
    {
      label: "Ingest",
      value: compactCount(ingestTotal),
      unit: "records",
      valueTone: "neutral",
      detail: shares.length ? shares.join(" · ") : "nothing ingested",
      series: ingestSeries,
      seriesTone: "neutral",
      formatPoint: (v) => `${compactCount(v)} records`,
    },
  ];
}

/** Stacked signals summed per bucket — the Ingest card's single line. */
export function mergeVolume(series: VolumeSeries[]): SeriesPoint[] {
  const byT = new Map<number, number>();
  for (const s of series) {
    for (const [t, v] of s.points) byT.set(t, (byT.get(t) ?? 0) + v);
  }
  return [...byT.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([tMs, value]) => ({ tMs, value }));
}

// ---- deploys ----------------------------------------------------------

/** "v2.41.0 · 16m ago" for a service deployed in the window, the running
 * version alone otherwise, "—" without a `service.version`. */
export function lastDeployLabel(
  service: string,
  deploys: Deploy[],
  latest: Map<string, VersionSighting>,
  nowMs: number,
): string {
  const deploy = [...deploys].reverse().find((d) => d.service === service);
  if (deploy) return `${deploy.version} · ${ago(nowMs - deploy.atMs)}`;
  return latest.get(service)?.version ?? "—";
}

export function ago(ms: number): string {
  const m = Math.max(0, Math.round(ms / 60_000));
  if (m < 60) return `${m}m ago`;
  const h = Math.round(m / 60);
  if (h < 48) return `${h}h ago`;
  return `${Math.round(h / 24)}d ago`;
}

// ---- setup checklist ---------------------------------------------------

export interface SetupStep {
  id: string;
  title: string;
  detail: string;
  done: boolean;
  cta?: { label: string; href: string };
}

export interface SetupInputs {
  rows: ServiceRow[];
  /** `undefined` while the probe is pending. */
  githubLinked: boolean | undefined;
  /** Distinct members, or `undefined` when unknown (not an admin). */
  memberCount: number | undefined;
  canManage: boolean;
}

function names(rows: ServiceRow[]): string {
  const shown = rows.slice(0, 2).map((r) => r.name);
  const more = rows.length - shown.length;
  return more > 0 ? `${shown.join(", ")} +${more}` : shown.join(", ");
}

/** The checklist, in the design's order. Coverage is read off the window
 * the page shows: a service that reported nothing in it counts as missing. */
export function setupSteps({
  rows,
  githubLinked,
  memberCount,
  canManage,
}: SetupInputs): SetupStep[] {
  const traced = rows.filter((r) => r.sources.has("traces"));
  const untraced = rows.filter((r) => !r.sources.has("traces"));
  const withLogs = traced.filter((r) => r.sources.has("logs"));
  const withProfiles = traced.filter((r) => r.sources.has("profiles"));
  const instrument = {
    label: "Open Instrumentation",
    href: "/instrumentation",
  };

  const steps: SetupStep[] = [
    {
      id: "traces",
      title: "Send your first traces",
      detail: traced.length
        ? `${traced.length} service${traced.length === 1 ? "" : "s"} sending spans`
        : "no spans in this window",
      done: traced.length > 0,
      cta: instrument,
    },
    {
      id: "services",
      title: "Instrument every service",
      detail:
        `${traced.length} of ${rows.length} services send traces` +
        (untraced.length ? ` · ${names(untraced)} send none` : ""),
      done: rows.length > 0 && untraced.length === 0,
      cta: instrument,
    },
    {
      id: "logs",
      title: "Send logs from every service",
      detail: `${withLogs.length} of ${traced.length} instrumented services`,
      done: traced.length > 0 && withLogs.length === traced.length,
      cta: { label: "Send logs", href: "/instrumentation" },
    },
    {
      id: "profiles",
      title: "Enable continuous profiling",
      detail: `${withProfiles.length} of ${traced.length} services send profiles`,
      done: traced.length > 0 && withProfiles.length === traced.length,
      cta: { label: "Set up profiling", href: "/instrumentation" },
    },
    {
      id: "github",
      title: "Connect GitHub for source links",
      detail:
        githubLinked === undefined
          ? "checking…"
          : githubLinked
            ? "source links enabled"
            : "not connected",
      done: githubLinked === true,
      cta: canManage
        ? { label: "Connect GitHub", href: "/integrations/github" }
        : undefined,
    },
  ];
  if (memberCount !== undefined) {
    steps.push({
      id: "team",
      title: "Invite your team",
      detail:
        memberCount > 1 ? `${memberCount} members` : "you are the only member",
      done: memberCount > 1,
      cta: { label: "Invite members", href: "/manage" },
    });
  }
  return steps;
}
