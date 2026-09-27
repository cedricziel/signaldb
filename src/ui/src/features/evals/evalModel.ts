// The evaluation semantics every Evaluate page shares (see the
// `agent-evaluation-results` spec): the pass rule, run status, how two runs'
// cases compare, and how two tool trajectories differ. Pure functions over
// already-aggregated counts, so the pages and their tests agree.

import type { EvalRun } from "../../api/evals";
import { diffSequence } from "../processors/lineDiff";

export type Verdict = "pass" | "fail";

const PASS_LABELS = new Set([
  "pass",
  "passed",
  "true",
  "yes",
  "correct",
  "safe",
]);
const FAIL_LABELS = new Set([
  "fail",
  "failed",
  "false",
  "no",
  "incorrect",
  "unsafe",
]);

/** The verdict a label carries on its own, or null when it's not one of the
 * recognised pass/fail words (the score then decides). */
export function verdictOfLabel(label: string | null): Verdict | null {
  const l = label?.toLowerCase();
  if (!l) return null;
  if (PASS_LABELS.has(l)) return "pass";
  if (FAIL_LABELS.has(l)) return "fail";
  return null;
}

/** A numeric score at or above this passes when no label decides. */
export const PASS_THRESHOLD = 0.5;

/** The pass rule for one result: an evaluator error has no verdict, else a
 * recognised label decides, else the score against `PASS_THRESHOLD`. */
export function verdictOfResult(r: {
  error: string | null;
  label: string | null;
  score: number | null;
}): Verdict | null {
  if (r.error) return null;
  return (
    verdictOfLabel(r.label) ??
    (r.score === null ? null : r.score >= PASS_THRESHOLD ? "pass" : "fail")
  );
}

/** Results folded into counts. `pass + fail` can be less than
 * `results - errors`: results with neither a recognised label nor a score
 * are scored without a verdict. */
export interface EvalStats {
  results: number;
  /** Results with `error.type` set — excluded from pass rates and means. */
  errors: number;
  pass: number;
  fail: number;
  scoreSum: number;
  /** Non-error results carrying a numeric score. */
  scored: number;
  /** Non-error results per `score.label`. */
  labels: Record<string, number>;
}

export function emptyStats(): EvalStats {
  return {
    results: 0,
    errors: 0,
    pass: 0,
    fail: 0,
    scoreSum: 0,
    scored: 0,
    labels: {},
  };
}

/** One aggregated group of results sharing a label and error type:
 * `high`/`low` count scores at/above and below `PASS_THRESHOLD`. */
export interface ResultGroup {
  label: string | null;
  error: string | null;
  n: number;
  high: number;
  low: number;
  scoreSum: number;
  scored: number;
}

export function foldStats(acc: EvalStats, g: ResultGroup): EvalStats {
  const out = { ...acc, results: acc.results + g.n };
  if (g.error) {
    out.errors += g.n;
    return out;
  }
  out.scoreSum += g.scoreSum;
  out.scored += g.scored;
  if (g.label !== null)
    out.labels = { ...out.labels, [g.label]: (out.labels[g.label] ?? 0) + g.n };
  const verdict = verdictOfLabel(g.label);
  if (verdict === "pass") out.pass += g.n;
  else if (verdict === "fail") out.fail += g.n;
  else {
    out.pass += g.high;
    out.fail += g.low;
  }
  return out;
}

export function mergeStats(a: EvalStats, b: EvalStats): EvalStats {
  return {
    results: a.results + b.results,
    errors: a.errors + b.errors,
    pass: a.pass + b.pass,
    fail: a.fail + b.fail,
    scoreSum: a.scoreSum + b.scoreSum,
    scored: a.scored + b.scored,
    labels: mergeCounts(a.labels, b.labels),
  };
}

function mergeCounts(
  a: Record<string, number>,
  b: Record<string, number>,
): Record<string, number> {
  const out = { ...a };
  for (const [k, n] of Object.entries(b)) out[k] = (out[k] ?? 0) + n;
  return out;
}

/** The most common label and its share of the labelled results. */
export function topLabelOf(
  s: EvalStats,
): { label: string; share: number } | null {
  let total = 0;
  let best: [string, number] | null = null;
  for (const [label, n] of Object.entries(s.labels)) {
    total += n;
    if (!best || n > best[1]) best = [label, n];
  }
  return best && total ? { label: best[0], share: best[1] / total } : null;
}

export function passRateOf(s: EvalStats): number | null {
  const judged = s.pass + s.fail;
  return judged ? s.pass / judged : null;
}

export function meanOf(s: EvalStats): number | null {
  return s.scored ? s.scoreSum / s.scored : null;
}

export function mean(nums: number[]): number | null {
  return nums.length ? nums.reduce((a, b) => a + b, 0) / nums.length : null;
}

export interface StatsDelta {
  d: number;
  /** "score" when both sides have a mean, else "pp" (pass-rate change). */
  unit: "score" | "pp";
}

/** The change from `b` to `c`: the mean score's when both have one, else
 * the pass rate's when both have one. */
export function statsDelta(b: EvalStats, c: EvalStats): StatsDelta | null {
  const mb = meanOf(b);
  const mc = meanOf(c);
  if (mb !== null && mc !== null) return { d: mc - mb, unit: "score" };
  const rb = passRateOf(b);
  const rc = passRateOf(c);
  return rb !== null && rc !== null ? { d: rc - rb, unit: "pp" } : null;
}

/** The verdict of a single case/evaluator cell: its pass rate decides when
 * it has one (several trials average), else no verdict. */
export function verdictOf(s: EvalStats): Verdict | null {
  const rate = passRateOf(s);
  if (rate === null) return null;
  return rate >= PASS_THRESHOLD ? "pass" : "fail";
}

// ---- runs ----------------------------------------------------------------

/** A run still receiving results is in progress until it's been quiet this
 * long. */
const RUN_QUIET_MS = 10 * 60_000;

/** Results still arriving: the last one landed within `RUN_QUIET_MS`. */
export function isReceiving(lastMs: number, nowMs: number): boolean {
  return nowMs - lastMs < RUN_QUIET_MS;
}

export interface RunStatus {
  kind: "running" | "complete" | "partial";
  reasons: string[];
}

export function runStatus(
  run: { lastMs: number; errors: number; unlinked: number },
  nowMs: number,
): RunStatus {
  if (isReceiving(run.lastMs, nowMs)) return { kind: "running", reasons: [] };
  const reasons: string[] = [];
  if (run.errors) reasons.push(`${run.errors} not scored`);
  if (run.unlinked) reasons.push(`${run.unlinked} unmatched`);
  return { kind: reasons.length ? "partial" : "complete", reasons };
}

/** The run each run compares against: the newest earlier run of the same
 * eval set, keyed by run id. */
export function baselinesOf(runs: EvalRun[]): Map<string, EvalRun> {
  const bySet = new Map<string | null, EvalRun[]>();
  for (const r of runs) {
    let group = bySet.get(r.set);
    if (!group) bySet.set(r.set, (group = []));
    group.push(r);
  }
  const out = new Map<string, EvalRun>();
  for (const group of bySet.values()) {
    group.sort((a, b) => b.firstMs - a.firstMs);
    // Runs sharing a start time aren't each other's baseline.
    let j = 0;
    for (const run of group) {
      while (j < group.length && group[j]!.firstMs >= run.firstMs) j++;
      const earlier = group[j];
      if (earlier) out.set(run.id, earlier);
    }
  }
  return out;
}

/** The runs to compare: the URL's, else the newest run with a baseline and
 * that baseline. `runs` is newest first. */
export function pickRuns(
  runs: EvalRun[],
  baseline: string,
  candidate: string,
  baselines = baselinesOf(runs),
): { baseline: EvalRun | null; candidate: EvalRun | null } {
  const byId = (id: string) => runs.find((r) => r.id === id) ?? null;
  const c = candidate
    ? byId(candidate)
    : (runs.find((r) => baselines.has(r.id)) ?? null);
  const b = baseline
    ? byId(baseline)
    : c
      ? (baselines.get(c.id) ?? null)
      : null;
  return { baseline: b, candidate: c };
}

// ---- comparing two runs --------------------------------------------------

/** A mean score has to move at least this much to count as a change. */
export const SCORE_EPSILON = 0.05;

export type Direction = "worse" | "better" | "same";
export type CaseKind = "regression" | "improvement" | "unchanged";

/** Per evaluator stats for one case in one run. */
export type CaseScores = Map<string, EvalStats>;

function direction(b: EvalStats, c: EvalStats): Direction {
  const vb = verdictOf(b);
  const vc = verdictOf(c);
  if (vb === "pass" && vc === "fail") return "worse";
  if (vb === "fail" && vc === "pass") return "better";
  const mb = meanOf(b);
  const mc = meanOf(c);
  if (mb === null || mc === null) return "same";
  // Rounded so float noise (0.95 → 0.9 is 0.04999…) doesn't decide.
  const d = Math.round((mc - mb) * 1e6) / 1e6;
  if (d <= -SCORE_EPSILON) return "worse";
  if (d >= SCORE_EPSILON) return "better";
  return "same";
}

export interface CaseComparison {
  kind: CaseKind;
  noBaseline: boolean;
  evaluators: Map<string, Direction>;
  /** Sum of mean-score changes across evaluators; sorts drops and gains. */
  delta: number;
}

export function classifyCase(
  baseline: CaseScores | undefined,
  candidate: CaseScores,
): CaseComparison {
  const evaluators = new Map<string, Direction>();
  let delta = 0;
  if (!baseline) {
    const failing = [...candidate.values()].some(
      (s) => verdictOf(s) === "fail",
    );
    return {
      kind: failing ? "regression" : "unchanged",
      noBaseline: true,
      evaluators,
      delta: 0,
    };
  }
  for (const [name, c] of candidate) {
    const b = baseline.get(name);
    if (!b) continue;
    evaluators.set(name, direction(b, c));
    const mb = meanOf(b);
    const mc = meanOf(c);
    if (mb !== null && mc !== null) delta += mc - mb;
    else {
      const vb = verdictOf(b);
      const vc = verdictOf(c);
      if (vb && vc && vb !== vc) delta += vc === "pass" ? 1 : -1;
    }
  }
  const dirs = [...evaluators.values()];
  const kind: CaseKind = dirs.includes("worse")
    ? "regression"
    : dirs.includes("better")
      ? "improvement"
      : "unchanged";
  return { kind, noBaseline: false, evaluators, delta };
}

export interface CaseRow extends CaseComparison {
  caseId: string;
}

/** Every candidate case classified against the baseline: regressions by
 * largest drop, improvements by largest gain, unchanged by id. */
export function compareCases(
  baseline: Map<string, CaseScores>,
  candidate: Map<string, CaseScores>,
): CaseRow[] {
  const rows = [...candidate].map(([caseId, scores]) => ({
    caseId,
    ...classifyCase(baseline.get(caseId), scores),
  }));
  const order: Record<CaseKind, number> = {
    regression: 0,
    improvement: 1,
    unchanged: 2,
  };
  return rows.sort(
    (a, b) =>
      order[a.kind] - order[b.kind] ||
      (a.kind === "regression" ? a.delta - b.delta : b.delta - a.delta) ||
      a.caseId.localeCompare(b.caseId),
  );
}

export interface EvaluatorSummary {
  name: string;
  baseline: EvalStats;
  candidate: EvalStats;
  worse: number;
  better: number;
}

/** Per evaluator totals over both runs plus how many cases moved. */
export function summarizeEvaluators(
  baseline: Map<string, CaseScores>,
  candidate: Map<string, CaseScores>,
  rows: CaseRow[],
): EvaluatorSummary[] {
  const byName = new Map<string, EvaluatorSummary>();
  const entry = (name: string) => {
    let e = byName.get(name);
    if (!e) {
      e = {
        name,
        baseline: emptyStats(),
        candidate: emptyStats(),
        worse: 0,
        better: 0,
      };
      byName.set(name, e);
    }
    return e;
  };
  for (const scores of baseline.values())
    for (const [name, s] of scores)
      entry(name).baseline = mergeStats(entry(name).baseline, s);
  for (const scores of candidate.values())
    for (const [name, s] of scores)
      entry(name).candidate = mergeStats(entry(name).candidate, s);
  for (const row of rows)
    for (const [name, dir] of row.evaluators) {
      if (dir === "worse") entry(name).worse++;
      if (dir === "better") entry(name).better++;
    }
  return [...byName.values()].sort((a, b) => a.name.localeCompare(b.name));
}

// ---- tool trajectories ---------------------------------------------------

export type ToolMark = "same" | "skipped" | "reordered" | "repeated" | "new";

export interface ToolStep {
  name: string;
  kind: ToolMark;
}

/**
 * The candidate's tool calls aligned against the baseline's (longest common
 * subsequence), in call order, with baseline calls the candidate never made
 * inserted where they would have been. An extra call is "reordered" when
 * the baseline made it elsewhere, "repeated" when the baseline made it
 * fewer times, else "new".
 */
export function toolDiff(baseline: string[], candidate: string[]): ToolStep[] {
  const ops = diffSequence(baseline, candidate);
  const count = (kind: "removed" | "added") => {
    const out = new Map<string, number>();
    for (const o of ops)
      if (o.kind === kind) out.set(o.item, (out.get(o.item) ?? 0) + 1);
    return out;
  };
  const removed = count("removed");
  const added = count("added");
  // A name both dropped and added somewhere is a move: its first `moves`
  // additions are reorders and as many removals aren't skips.
  const moves = (name: string) =>
    Math.min(removed.get(name) ?? 0, added.get(name) ?? 0);
  const movedIn = new Map<string, number>();
  const movedOut = new Map<string, number>();
  const take = (seen: Map<string, number>, name: string) => {
    const used = seen.get(name) ?? 0;
    if (used >= moves(name)) return false;
    seen.set(name, used + 1);
    return true;
  };
  const inBaseline = new Set(baseline);
  const steps: ToolStep[] = [];
  for (const { kind, item: name } of ops) {
    if (kind === "same") steps.push({ name, kind: "same" });
    else if (kind === "added")
      steps.push({
        name,
        kind: take(movedIn, name)
          ? "reordered"
          : inBaseline.has(name)
            ? "repeated"
            : "new",
      });
    else if (!take(movedOut, name)) steps.push({ name, kind: "skipped" });
  }
  return steps;
}
