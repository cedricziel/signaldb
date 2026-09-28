// React Query wiring for the Real users page: one query per card, keyed by
// the window/tenant scope (`rangeKey`) *and* the selected app — every RUM
// query is scoped to one app, so switching apps must issue new requests
// rather than serving another app's cached data under the same key.

import { useQuery } from "@tanstack/react-query";
import {
  fetchBreakdown,
  fetchKpis,
  fetchNetworkRequests,
  fetchResources,
  fetchRumApps,
  fetchSessionsOverTime,
  fetchVitals,
  type RumBreakdownOptions,
  type RumKpiMetric,
} from "../../api/rum";
import { fetchErrorGroups } from "../../api/errors";
import {
  durationToSeconds,
  stepForRange,
  type ResolvedRange,
} from "../../lib/time";
import { splitKpiSeries, type KpiFigure } from "./rumModel";

const STALE = 30_000;

export interface RumScope {
  range: ResolvedRange;
  /** `rangeScopeKey(state)` — the window and tenant/dataset scope. Every
   * query key below also carries `app` alongside it: the two together
   * identify a query, and `app` alone changing (the range and tenant
   * staying put) must still be a different cache entry and a new request. */
  rangeKey: string;
  app: string;
}

/** Buckets covering `range` for a KPI's sparkline; the underlying read
 * covers twice that (the previous window too — see `buildKpisDoc`). */
export const RUM_KPI_BUCKETS = 30;

export function useRumApps(range: ResolvedRange, rangeKey: string) {
  return useQuery({
    queryKey: ["rum-apps", rangeKey],
    queryFn: () => fetchRumApps(range),
    staleTime: STALE,
  });
}

export interface RumKpis {
  sessions: KpiFigure;
  sessionsWithErrors: KpiFigure;
  pageViews: KpiFigure;
}

const OVERVIEW_KPI_METRICS: readonly RumKpiMetric[] = [
  "sessions",
  "sessions_with_errors",
  "page_views",
];

/** The Overview KPI strip's three figures (each its current value,
 * previous-window delta and sparkline), from the one combined request
 * `api/rum.ts`'s `fetchKpis` bundles them into (see its module doc for why
 * that's a multi-query document rather than three separate reads). */
export function useRumKpis(scope: RumScope) {
  const { range, rangeKey, app } = scope;
  return useQuery({
    queryKey: ["rum-kpis", rangeKey, app],
    queryFn: async (): Promise<RumKpis> => {
      const byMetric = await fetchKpis(
        app,
        range,
        OVERVIEW_KPI_METRICS,
        RUM_KPI_BUCKETS,
      );
      return {
        sessions: splitKpiSeries(byMetric.sessions ?? [], range.fromMs),
        sessionsWithErrors: splitKpiSeries(
          byMetric.sessions_with_errors ?? [],
          range.fromMs,
        ),
        pageViews: splitKpiSeries(byMetric.page_views ?? [], range.fromMs),
      };
    },
    enabled: app !== "",
    staleTime: STALE,
  });
}

export function useRumVitals(scope: RumScope) {
  const { range, rangeKey, app } = scope;
  return useQuery({
    queryKey: ["rum-vitals", rangeKey, app],
    queryFn: () => fetchVitals(app, range),
    enabled: app !== "",
    staleTime: STALE,
  });
}

function rumStep(range: ResolvedRange): number {
  return durationToSeconds(stepForRange(range, 30)) ?? 60;
}

export function useRumSessionsOverTime(scope: RumScope) {
  const { range, rangeKey, app } = scope;
  const stepSeconds = rumStep(range);
  return useQuery({
    queryKey: ["rum-sessions-over-time", rangeKey, app, stepSeconds],
    queryFn: () => fetchSessionsOverTime(app, range, stepSeconds),
    enabled: app !== "",
    staleTime: STALE,
  });
}

export function useRumTopErrors(scope: RumScope) {
  const { range, rangeKey, app } = scope;
  return useQuery({
    queryKey: ["rum-top-errors", rangeKey, app],
    queryFn: () => fetchErrorGroups(range, app),
    enabled: app !== "",
    staleTime: STALE,
  });
}

/** The Network tab's requests table — client HTTP spans grouped by method
 * and URL template, split into client+network and backend time. */
export function useRumNetworkRequests(scope: RumScope) {
  const { range, rangeKey, app } = scope;
  return useQuery({
    queryKey: ["rum-network-requests", rangeKey, app],
    queryFn: () => fetchNetworkRequests(app, range),
    enabled: app !== "",
    staleTime: STALE,
  });
}

/** The Network tab's resources-by-initiator-type table. */
export function useRumResources(scope: RumScope) {
  const { range, rangeKey, app } = scope;
  return useQuery({
    queryKey: ["rum-resources", rangeKey, app],
    queryFn: () => fetchResources(app, range),
    enabled: app !== "",
    staleTime: STALE,
  });
}

/** One field's value breakdown for the app — shared by the browser
 * (`resource.browser.brands`) and device (`resource.browser.mobile`)
 * panels; a later breakdown just calls this with its own field. */
export function useRumBreakdown(
  scope: RumScope,
  field: string,
  opts?: RumBreakdownOptions,
) {
  const { range, rangeKey, app } = scope;
  return useQuery({
    queryKey: ["rum-breakdown", field, rangeKey, app],
    queryFn: () => fetchBreakdown(app, range, field, opts),
    enabled: app !== "",
    staleTime: STALE,
  });
}
