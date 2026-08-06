// Every explore view is a URL: the signal lives in the path (/logs, /traces,
// ...) so views are separately navigable and bookmarkable; time range,
// filters, and other options live in search params alongside it.

import { useCallback } from "react";
import { useLocation, useNavigate, useParams } from "react-router";
import { filterFromParam, filterToParam, type LabelFilter } from "./filters";
import { DEFAULT_GROUP_BY } from "./traceGroups";
import {
  DEFAULT_RANGE,
  parseRangeParam,
  rangeToParam,
  type TimeRange,
} from "./time";

export type Signal = "logs" | "traces" | "metrics" | "profiles" | "query";

export interface ExploreState {
  signal: Signal;
  range: TimeRange;
  filters: LabelFilter[];
  search: string;
  raw: string;
  limit: number;
  live: boolean;
  /** Selected trace id — opens the trace view. */
  trace: string;
  /** Selected trace group value — dives into that group's trace list. */
  group: string;
  /** Trace grouping dimension: "span.name", "service.name", or an attribute key. */
  groupBy: string;
  /** PromQL expression for the metrics view. */
  promql: string;
  /** Profile type id (e.g. `cpu:nanoseconds`) — "" auto-picks the first. */
  profileType: string;
  /** Service filter for the profiles view — "" means all services. */
  profileService: string;
  /**
   * Explicit tenant/dataset context. Empty means "ambient default": the dev
   * proxy (or a future session) supplies it and no header is sent.
   */
  tenant: string;
  dataset: string;
}

export const DEFAULT_STATE: ExploreState = {
  signal: "logs",
  range: DEFAULT_RANGE,
  filters: [],
  search: "",
  raw: "",
  limit: 500,
  live: false,
  trace: "",
  group: "",
  groupBy: DEFAULT_GROUP_BY,
  promql: "",
  profileType: "",
  profileService: "",
  tenant: "",
  dataset: "",
};

export const SIGNALS: Signal[] = [
  "logs",
  "traces",
  "metrics",
  "profiles",
  "query",
];

/** Maps a `:signal` route param to a known signal, defaulting invalid/missing values to "logs". */
export function signalFromParam(value: string | undefined): Signal {
  return value && SIGNALS.includes(value as Signal)
    ? (value as Signal)
    : "logs";
}

/**
 * Parses the search-param half of explore state (everything but the signal,
 * which comes from the route path — see {@link signalFromParam}).
 */
export function parseExploreState(search: string): ExploreState {
  const p = new URLSearchParams(search);
  const limit = Number(p.get("limit"));
  return {
    signal: "logs",
    range: parseRangeParam(p.get("range")),
    filters: p
      .getAll("f")
      .map(filterFromParam)
      .filter((f): f is LabelFilter => f !== null),
    search: p.get("q") ?? "",
    raw: p.get("raw") ?? "",
    limit: Number.isFinite(limit) && limit > 0 ? Math.min(limit, 5000) : 500,
    live: p.get("live") === "1",
    trace: p.get("trace") ?? "",
    group: p.get("group") ?? "",
    groupBy: p.get("groupBy") || DEFAULT_GROUP_BY,
    promql: p.get("promql") ?? "",
    profileType: p.get("ptype") ?? "",
    profileService: p.get("psvc") ?? "",
    tenant: p.get("tenant") ?? "",
    dataset: p.get("dataset") ?? "",
  };
}

export function buildSearch(state: ExploreState): string {
  const p = new URLSearchParams();
  const rangeParam = rangeToParam(state.range);
  if (rangeParam !== rangeToParam(DEFAULT_RANGE)) p.set("range", rangeParam);
  for (const f of state.filters) p.append("f", filterToParam(f));
  if (state.search) p.set("q", state.search);
  if (state.raw) p.set("raw", state.raw);
  if (state.limit !== 500) p.set("limit", String(state.limit));
  if (state.live) p.set("live", "1");
  if (state.trace) p.set("trace", state.trace);
  if (state.group) p.set("group", state.group);
  if (state.groupBy !== DEFAULT_GROUP_BY) p.set("groupBy", state.groupBy);
  if (state.promql) p.set("promql", state.promql);
  if (state.profileType) p.set("ptype", state.profileType);
  if (state.profileService) p.set("psvc", state.profileService);
  if (state.tenant) p.set("tenant", state.tenant);
  if (state.dataset) p.set("dataset", state.dataset);
  const s = p.toString();
  return s === "" ? "" : `?${s}`;
}

/**
 * URL-backed state: the signal comes from the route's `:signal` path segment
 * (must be rendered under a matching route — see `routes.tsx`), everything
 * else from search params. Updates replace the current history entry
 * (queries as you refine are one entry) and switch path when the signal
 * changes; browser back/forward still works across externally navigated
 * URLs since it all flows through the router.
 */
export function useExploreState(): [
  ExploreState,
  (patch: Partial<ExploreState>) => void,
] {
  const location = useLocation();
  const navigate = useNavigate();
  const { signal: signalParam } = useParams<{ signal?: string }>();
  const state: ExploreState = {
    ...parseExploreState(location.search),
    signal: signalFromParam(signalParam),
  };

  const update = useCallback(
    (patch: Partial<ExploreState>) => {
      const next = { ...state, ...patch };
      navigate(`/${next.signal}${buildSearch(next)}`, { replace: true });
    },
    // `state` is recomputed each render from location.search/signalParam, so
    // depending on those (not `state` itself) still recreates `update`
    // exactly when its captured `state` would otherwise go stale.
    [navigate, location.search, signalParam],
  );

  return [state, update];
}
