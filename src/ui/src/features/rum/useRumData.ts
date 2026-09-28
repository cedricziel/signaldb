// React Query wiring for the Real users page: one query per card, keyed by
// the window/tenant scope (`rangeKey`) *and* the selected app — every RUM
// query is scoped to one app, so switching apps must issue new requests
// rather than serving another app's cached data under the same key.

import { useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import {
  fetchBackendCalls,
  fetchBreakdown,
  fetchInteractions,
  fetchKpis,
  fetchLoadBreakdown,
  fetchNetworkRequests,
  fetchPages,
  fetchResources,
  fetchRumApps,
  fetchSessionsOverTime,
  fetchTracedShare,
  fetchVitals,
  type RumBreakdownOptions,
  type RumKpiMetric,
} from "../../api/rum";
import { fetchRumErrorGroupsWithBackendCause } from "../../api/rumErrorGroups";
import { fetchSessions } from "../../api/rumSessions";
import {
  fetchSessionDetail,
  type SessionEvent,
} from "../../api/rumSessionDetail";
import {
  durationToSeconds,
  stepForRange,
  type ResolvedRange,
} from "../../lib/time";
import { splitKpiSeries, splitTracedShare, type KpiFigure } from "./rumModel";

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
  users: KpiFigure;
  sessionsWithErrors: KpiFigure;
  pageViews: KpiFigure;
}

const OVERVIEW_KPI_METRICS: readonly RumKpiMetric[] = [
  "sessions",
  "users",
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
        users: splitKpiSeries(byMetric.users ?? [], range.fromMs),
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

/** The Errors tab's group list, and the Overview "Top errors" panel's same
 * data (same query key — one shared cache entry, not two requests, when
 * both are on screen) — `fetchRumErrorGroupsWithBackendCause` batches the
 * list read and the backend-cause read into one call (see its module doc). */
export function useRumErrorGroups(
  scope: RumScope,
  currentVersion: string | null,
) {
  const { range, rangeKey, app } = scope;
  return useQuery({
    queryKey: ["rum-error-groups", rangeKey, app, currentVersion ?? ""],
    queryFn: () =>
      fetchRumErrorGroupsWithBackendCause(app, range, currentVersion),
    enabled: app !== "",
    staleTime: STALE,
  });
}

/** The Overview KPI strip's and Setup checklist's traced-request share
 * (value, previous-window delta, sparkline) from the one `correlate`-backed
 * formula document `api/rum.ts`'s `fetchTracedShare` bundles (see its
 * module doc). */
export function useRumTracedShare(scope: RumScope) {
  const { range, rangeKey, app } = scope;
  return useQuery({
    queryKey: ["rum-traced-share", rangeKey, app],
    queryFn: async () => {
      const { traced, total } = await fetchTracedShare(
        app,
        range,
        RUM_KPI_BUCKETS,
      );
      return splitTracedShare(traced, total, range.fromMs);
    },
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

/** The Pages tab's route list — views, per-route vitals and error share. */
export function useRumPages(scope: RumScope) {
  const { range, rangeKey, app } = scope;
  return useQuery({
    queryKey: ["rum-pages", rangeKey, app],
    queryFn: () => fetchPages(app, range),
    enabled: app !== "",
    staleTime: STALE,
  });
}

/** The Pages tab detail panel's load waterfall for one route — only issued
 * once a route is selected. */
export function useRumLoadBreakdown(scope: RumScope, route: string) {
  const { range, rangeKey, app } = scope;
  return useQuery({
    queryKey: ["rum-load-breakdown", rangeKey, app, route],
    queryFn: async () => (await fetchLoadBreakdown(app, range, route)) ?? null,
    enabled: app !== "" && route !== "",
    staleTime: STALE,
  });
}

/** The Pages tab detail panel's backend calls for one route — joined to the
 * Network tab's own service names by the caller
 * (`joinBackendCallsToNetworkService`), not fetched again here. */
export function useRumBackendCalls(scope: RumScope, route: string) {
  const { range, rangeKey, app } = scope;
  return useQuery({
    queryKey: ["rum-backend-calls", rangeKey, app, route],
    queryFn: () => fetchBackendCalls(app, range, route),
    enabled: app !== "" && route !== "",
    staleTime: STALE,
  });
}

/** The Interactions tab's clicks by (target, page). */
export function useRumInteractions(scope: RumScope) {
  const { range, rangeKey, app } = scope;
  return useQuery({
    queryKey: ["rum-interactions", rangeKey, app],
    queryFn: () => fetchInteractions(app, range),
    enabled: app !== "",
    staleTime: STALE,
  });
}

/** The Sessions tab's list — one row per `session.id`. `filterText` is part
 * of the query key (and the request itself — see `api/rumSessions.ts`'s
 * module doc): it narrows which sessions are aggregated, unlike the quick
 * filters the tab applies client-side to this same result. */
export function useRumSessions(scope: RumScope, filterText: string) {
  const { range, rangeKey, app } = scope;
  return useQuery({
    queryKey: ["rum-sessions", rangeKey, app, filterText],
    queryFn: () => fetchSessions(app, range, filterText),
    enabled: app !== "",
    staleTime: STALE,
  });
}

export interface RumSessionDetail {
  events: SessionEvent[];
  isPending: boolean;
  isError: boolean;
  error: unknown;
  hasMore: boolean;
  moreCount?: number;
  isLoadingMore: boolean;
  loadMore: () => void;
}

/** One session's detail timeline (`?session=`) — accumulates pages loaded
 * via "Load more" (`api/rumSessionDetail.ts`'s cursor pagination) into one
 * ordered list, resetting whenever the session or scope changes. Plain
 * component state rather than `useInfiniteQuery`: each page depends on the
 * *previous* page's last timestamp, not a page index, and there's no
 * existing infinite-query usage in this codebase to match. */
export function useRumSessionDetail(
  scope: RumScope,
  sessionId: string,
): RumSessionDetail {
  const { range, rangeKey, app } = scope;
  const scopeKey = `${rangeKey}\u0000${app}\u0000${sessionId}`;
  const [cursor, setCursor] = useState<string | undefined>(undefined);
  const [pages, setPages] = useState<SessionEvent[][]>([]);
  // Resetting during render (React's own escape hatch for "derived state
  // that depends on a prop") rather than in a `useEffect`: an effect-based
  // reset lands one commit *after* the scope change, which can race a
  // same-commit data-arrival effect and drop the very page that change was
  // meant to fetch.
  const [seenScopeKey, setSeenScopeKey] = useState(scopeKey);
  if (seenScopeKey !== scopeKey) {
    setSeenScopeKey(scopeKey);
    setCursor(undefined);
    setPages([]);
  }
  const resetting = seenScopeKey !== scopeKey;

  const page = useQuery({
    queryKey: ["rum-session-detail", scopeKey, cursor],
    queryFn: () => fetchSessionDetail(sessionId, range, cursor),
    enabled: sessionId !== "" && !resetting,
    staleTime: STALE,
  });

  useEffect(() => {
    if (page.data && !resetting) {
      setPages((prev) =>
        cursor === undefined ? [page.data.events] : [...prev, page.data.events],
      );
    }
    // `cursor`/`resetting` intentionally excluded: this only decides how a
    // *newly arrived* page slots in, not something to re-run for.
  }, [page.data]);

  const events = pages.flat();

  return {
    events,
    isPending: resetting || page.isPending,
    isError: page.isError,
    error: page.error,
    hasMore: page.data?.hasMore ?? false,
    moreCount: page.data?.moreCount,
    isLoadingMore: cursor !== undefined && page.isFetching,
    loadMore: () => {
      const last = events[events.length - 1];
      if (last) setCursor(last.tsNs);
    },
  };
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
