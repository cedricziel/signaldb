// React Query wiring for the Evaluate pages: one query per figure, keyed by
// the window + tenant scope (`rangeKey`) and the page's own selection.

import { useQuery } from "@tanstack/react-query";
import {
  F,
  fetchAgents,
  fetchAgentSpans,
  fetchCaseResults,
  fetchCoverage,
  fetchEvaluators,
  fetchMeanSeries,
  fetchRunCases,
  fetchRuns,
  fetchStats,
  fetchTraces,
  fetchTraceVersions,
  fetchVersions,
  type ResultScope,
} from "../../api/evals";
import { getEvalSet, listEvalSets } from "../../api/evalSets";
import { previousPeriod } from "../../api/entityDetailStats";
import { WIDE_LOOKBACK_MS } from "../../api/traceDetail";
import {
  DEFAULT_RANGE,
  rangeScopeKey,
  resolveRange,
  type ResolvedRange,
  type TimeRange,
} from "../../lib/time";
import type { ShellContext } from "../../lib/outletState";
import type { EvalParams, ExploreState } from "../../lib/urlState";
import { pickRuns } from "./evalModel";

const STALE = 30_000;
const DAY_MS = 86_400_000;

/** Eval results arrive per release, not per minute: the Evaluate pages open
 * on the last 7 days rather than the app-wide one-hour default. */
const EVAL_DEFAULT_RANGE: TimeRange = {
  type: "relative",
  seconds: 7 * 86_400,
};

/** The window the Evaluate pages read: the URL's, or the last 7 days when
 * the URL names none (the app-wide default). */
export function evalRange(state: ExploreState): TimeRange {
  const r = state.range;
  return r.type === "relative" &&
    DEFAULT_RANGE.type === "relative" &&
    r.seconds === DEFAULT_RANGE.seconds
    ? EVAL_DEFAULT_RANGE
    : r;
}

/** Merges `patch` into the URL's eval params. */
export function evalsUpdater({ state, update }: ShellContext) {
  return (patch: Partial<EvalParams>) =>
    update({ evals: { ...state.evals, ...patch } });
}

export interface EvalScope {
  range: ResolvedRange;
  /** `rangeScopeKey` of the window plus tenant/dataset. */
  rangeKey: string;
}

function scopeOf(
  state: ExploreState,
  range: TimeRange,
  nowMs: number,
): EvalScope {
  return {
    range: resolveRange(range, nowMs),
    rangeKey: rangeScopeKey({ ...state, range }),
  };
}

export function evalScope(state: ExploreState, nowMs = Date.now()): EvalScope {
  return scopeOf(state, evalRange(state), nowMs);
}

/** Runs are addressed by id, so Compare and the case drilldown look them up
 * over a fixed lookback rather than the selected window. */
export const LOOKBACK_RANGE: TimeRange = {
  type: "relative",
  seconds: WIDE_LOOKBACK_MS / 1000,
};

function lookbackScope(state: ExploreState, nowMs = Date.now()): EvalScope {
  return scopeOf(state, LOOKBACK_RANGE, nowMs);
}

/** Daily buckets for windows of two days or more, hourly below. */
export function trendStep(range: ResolvedRange): { step: string; ms: number } {
  return range.toMs - range.fromMs >= 2 * DAY_MS
    ? { step: "1d", ms: DAY_MS }
    : { step: "1h", ms: 3_600_000 };
}

function useEvalQuery<T>(
  name: string,
  scope: EvalScope,
  key: unknown[],
  fn: (range: ResolvedRange) => Promise<T>,
  enabled = true,
) {
  return useQuery({
    queryKey: [name, scope.rangeKey, ...key],
    queryFn: () => fn(scope.range),
    staleTime: STALE,
    enabled,
  });
}

export function useAgents(scope: EvalScope) {
  return useEvalQuery("eval-agents", scope, [], fetchAgents);
}

export function useEvaluatorStats(
  scope: EvalScope,
  results: ResultScope,
  enabled = true,
) {
  return useEvalQuery(
    "eval-evaluator-stats",
    scope,
    [results],
    async (range) => {
      const [current, previous] = await Promise.all([
        fetchStats(range, results, [F.name]),
        fetchStats(previousPeriod(range), results, [F.name]),
      ]);
      return { current, previous };
    },
    enabled,
  );
}

export function useMeanSeries(
  scope: EvalScope,
  results: ResultScope,
  enabled = true,
) {
  const { step } = trendStep(scope.range);
  return useEvalQuery(
    "eval-mean-series",
    scope,
    [results, step],
    (range) => fetchMeanSeries(range, results, step),
    enabled,
  );
}

export function useVersions(
  scope: EvalScope,
  results: ResultScope,
  enabled = true,
) {
  return useEvalQuery(
    "eval-versions",
    scope,
    [results],
    (range) => fetchVersions(range, results),
    enabled,
  );
}

export function useCoverage(
  scope: EvalScope,
  results: ResultScope,
  enabled = true,
) {
  return useEvalQuery(
    "eval-coverage",
    scope,
    [results],
    (range) => fetchCoverage(range, results),
    enabled,
  );
}

export function useRuns(scope: EvalScope, results: ResultScope) {
  return useEvalQuery("eval-runs", scope, [results], (range) =>
    fetchRuns(range, results),
  );
}

export function useEvaluators(scope: EvalScope) {
  return useEvalQuery("eval-evaluators", scope, [], fetchEvaluators);
}

/** Per-case results of one run (an empty id disables it). */
export function useRunCases(scope: EvalScope, runId: string) {
  return useEvalQuery(
    "eval-run-cases",
    scope,
    [runId],
    (range) => fetchRunCases(range, runId),
    runId !== "",
  );
}

/** The two runs Compare and the case drilldown read: when the URL names
 * both, only those are looked up; otherwise the lookback's runs, with the
 * newest run that has a baseline as the default pair. */
export function useComparedRuns(state: ExploreState) {
  const scope = lookbackScope(state);
  const { baseline: b, candidate: c } = state.evals;
  const runs = useRuns(scope, b && c ? { runIds: [b, c] } : {});
  const { baseline, candidate } = pickRuns(runs.data ?? [], b, c);
  const base = useRunCases(scope, baseline?.id ?? "");
  const cand = useRunCases(scope, candidate?.id ?? "");
  return { scope, runs, baseline, candidate, base, cand };
}

/** Compare's agent spans for every case trace of the two runs; keyed by the
 * run pair, so it waits for both runs' cases. */
export function useAgentTraces(
  scope: EvalScope,
  baselineId: string,
  candidateId: string,
  traceIds: string[],
  enabled: boolean,
) {
  return useEvalQuery(
    "eval-agent-traces",
    scope,
    [baselineId, candidateId, traceIds],
    (range) => fetchAgentSpans(range, traceIds),
    enabled && traceIds.length > 0,
  );
}

export function useCaseResults(
  scope: EvalScope,
  runIds: string[],
  caseId: string,
) {
  return useEvalQuery(
    "eval-case-results",
    scope,
    [runIds, caseId],
    (range) => fetchCaseResults(range, runIds, caseId),
    caseId !== "" && runIds.length > 0,
  );
}

/** The full traces of one case in both runs. */
export function useCaseTraces(scope: EvalScope, traceIds: string[]) {
  return useEvalQuery(
    "eval-case-traces",
    scope,
    [traceIds],
    (range) => fetchTraces(range, traceIds),
    traceIds.length > 0,
  );
}

// ---- eval sets ---------------------------------------------------------------

/** Query keys of the eval-set endpoints, scoped to the tenant/dataset. */
export function evalSetKeys(state: ExploreState) {
  const scope = [state.tenant, state.dataset];
  return {
    all: ["eval-sets", ...scope],
    list: ["eval-sets", ...scope, "list"],
    one: (name: string) => ["eval-sets", ...scope, "set", name],
  };
}

export function useEvalSets(state: ExploreState) {
  return useQuery({
    queryKey: evalSetKeys(state).list,
    queryFn: listEvalSets,
    staleTime: STALE,
    retry: false,
  });
}

export function useEvalSet(state: ExploreState, name: string) {
  return useQuery({
    queryKey: evalSetKeys(state).one(name),
    queryFn: () => getEvalSet(name),
    staleTime: STALE,
    enabled: name !== "",
    retry: false,
  });
}

/** Runs over the fixed lookback: the eval-set pages show each set's newest
 * runs whatever window the shell holds. */
export function useLookbackRuns(state: ExploreState) {
  const scope = lookbackScope(state);
  return { scope, runs: useRuns(scope, {}) };
}

/** The `service.version`s of the traces an upload links to. */
export function useTraceVersions(state: ExploreState, traceIds: string[]) {
  return useEvalQuery(
    "eval-trace-versions",
    lookbackScope(state),
    [traceIds],
    (range) => fetchTraceVersions(range, traceIds),
    traceIds.length > 0,
  );
}
