// Every explore view is a URL: signal, time range, filters, and options all
// live in search params so views are deep-linkable and shareable.

import { useCallback, useEffect, useState } from "react";
import { filterFromParam, filterToParam, type LabelFilter } from "./filters";
import {
  DEFAULT_RANGE,
  parseRangeParam,
  rangeToParam,
  type TimeRange,
} from "./time";

export type Signal = "logs" | "traces" | "metrics";

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
  /** PromQL expression for the metrics view. */
  promql: string;
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
  promql: "",
};

const SIGNALS: Signal[] = ["logs", "traces", "metrics"];

export function parseExploreState(search: string): ExploreState {
  const p = new URLSearchParams(search);
  const signalParam = p.get("signal");
  const signal = SIGNALS.includes(signalParam as Signal)
    ? (signalParam as Signal)
    : "logs";
  const limit = Number(p.get("limit"));
  return {
    signal,
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
    promql: p.get("promql") ?? "",
  };
}

export function buildSearch(state: ExploreState): string {
  const p = new URLSearchParams();
  if (state.signal !== "logs") p.set("signal", state.signal);
  const rangeParam = rangeToParam(state.range);
  if (rangeParam !== rangeToParam(DEFAULT_RANGE)) p.set("range", rangeParam);
  for (const f of state.filters) p.append("f", filterToParam(f));
  if (state.search) p.set("q", state.search);
  if (state.raw) p.set("raw", state.raw);
  if (state.limit !== 500) p.set("limit", String(state.limit));
  if (state.live) p.set("live", "1");
  if (state.trace) p.set("trace", state.trace);
  if (state.promql) p.set("promql", state.promql);
  const s = p.toString();
  return s === "" ? "" : `?${s}`;
}

/**
 * URL-backed state. Updates use replaceState (queries as you refine are one
 * history entry); browser back/forward still works across externally
 * navigated URLs via popstate.
 */
export function useExploreState(): [
  ExploreState,
  (patch: Partial<ExploreState>) => void,
] {
  const [state, setState] = useState<ExploreState>(() =>
    parseExploreState(window.location.search),
  );

  useEffect(() => {
    const onPop = () => setState(parseExploreState(window.location.search));
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);

  const update = useCallback((patch: Partial<ExploreState>) => {
    setState((prev) => {
      const next = { ...prev, ...patch };
      const search = buildSearch(next);
      const url = `${window.location.pathname}${search}`;
      window.history.replaceState(null, "", url);
      return next;
    });
  }, []);

  return [state, update];
}
