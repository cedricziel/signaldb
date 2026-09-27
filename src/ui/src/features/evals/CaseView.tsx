// Case drilldown (`/evals/compare/case`): one case of a comparison — the
// agent's trajectory with each result on the span it scored, the answer,
// and one card per evaluator with its explanation.

import { useMemo, useState } from "react";
import { Link } from "react-router";
import { EmptyState } from "../../components/EmptyState";
import { QueryError } from "../../components/QueryError";
import {
  agentTraceOf,
  F,
  type AgentTrace,
  type EvalResult,
  type EvalRun,
} from "../../api/evals";
import type { TempoSpan, TempoTrace } from "../../api/traceTypes";
import { toggleInSet } from "../../lib/collections";
import type { ShellContext } from "../../lib/outletState";
import {
  buildPath,
  DEFAULT_EVAL_PARAMS,
  viewHref,
  type EvalCaseMode,
} from "../../lib/urlState";
import { formatDurationMs } from "../../lib/waterfall";
import { mean, toolDiff, verdictOfResult, type Verdict } from "./evalModel";
import { fmtCount, fmtDateTime, fmtScore } from "./evalFormat";
import { ScoreBadge, Segmented } from "./EvalBits";
import {
  evalsUpdater,
  LOOKBACK_RANGE,
  useCaseResults,
  useCaseTraces,
  useComparedRuns,
} from "./useEvalData";
import "./evals.css";

const MODES: [EvalCaseMode, string][] = [
  ["candidate", "Candidate"],
  ["baseline", "Baseline"],
  ["side", "Side by side"],
];

/** The trajectory's row kind per GenAI operation; other GenAI spans show
 * their operation. */
const OP_KIND = new Map<string, "agent" | "tool" | undefined>([
  ["invoke_agent", "agent"],
  ["execute_tool", "tool"],
  ["chat", undefined],
]);

function resultText(r: EvalResult): string {
  if (r.error) return `error: ${r.error}`;
  if (r.score !== null && r.label && r.label !== String(r.score))
    return `${fmtScore(r.score)} ${r.label}`;
  return r.label ?? fmtScore(r.score);
}

type TrajectoryRow =
  | {
      kind: "span";
      span: TempoSpan;
      op: string;
      depth: number;
      leftPct: number;
      widthPct: number;
      durMs: number;
      results: EvalResult[];
    }
  | { kind: "ghost"; tool: string };

type SpanRow = Extract<TrajectoryRow, { kind: "span" }>;

function opOf(s: TempoSpan): string {
  const op = s.attributes[F.operation];
  return typeof op === "string" ? op : "";
}

/** The trace's GenAI spans in start order with timing and results, plus —
 * when a baseline is given — "expected, not called" rows for tool calls
 * the baseline made and this trace skipped. */
function trajectoryRows(
  trace: TempoTrace,
  results: EvalResult[],
  baselineTools?: string[],
): TrajectoryRow[] {
  const all = trace.spans
    .map((span) => ({ span, start: BigInt(span.startNs), op: opOf(span) }))
    .sort((a, b) => (a.start < b.start ? -1 : a.start > b.start ? 1 : 0));
  const genai = all.filter((s) => OP_KIND.has(s.op));
  const spans = genai.length ? genai : all;
  const t0 = spans[0]?.start ?? 0n;
  const totalNs = Math.max(1, trace.durationMs * 1_000_000);
  const bySpan = new Map<string, EvalResult[]>();
  for (const r of results) {
    if (!r.spanId) continue;
    let list = bySpan.get(r.spanId);
    if (!list) bySpan.set(r.spanId, (list = []));
    list.push(r);
  }
  const rows: SpanRow[] = spans.map(({ span, start, op }) => {
    const durNs = Number(span.durNs);
    return {
      kind: "span",
      span,
      op,
      depth: op === "invoke_agent" ? 0 : 1,
      leftPct: (Number(start - t0) / totalNs) * 100,
      widthPct: Math.max(0.6, (durNs / totalNs) * 100),
      durMs: durNs / 1_000_000,
      results: bySpan.get(span.spanId) ?? [],
    };
  });
  if (!baselineTools) return rows;

  const toolRows = rows.filter((r) => r.op === "execute_tool");
  const steps = toolDiff(
    baselineTools,
    toolRows.map((r) => String(r.span.attributes[F.tool] ?? r.span.name)),
  );
  // Each skipped call goes right after the tool call (or agent span) that
  // preceded it; with neither, at the end.
  const ghostsAfter = new Map<SpanRow | undefined, TrajectoryRow[]>();
  let anchor = rows.find((r) => r.op === "invoke_agent");
  let toolIdx = 0;
  for (const step of steps) {
    if (step.kind === "skipped") {
      let list = ghostsAfter.get(anchor);
      if (!list) ghostsAfter.set(anchor, (list = []));
      list.push({ kind: "ghost", tool: step.name });
    } else {
      anchor = toolRows[toolIdx++] ?? anchor;
    }
  }
  return [
    ...rows.flatMap((r) => [r, ...(ghostsAfter.get(r) ?? [])]),
    ...(ghostsAfter.get(undefined) ?? []),
  ];
}

function summarize(list: EvalResult[]): {
  text: string;
  verdict: Verdict | null;
} {
  if (list.length === 0) return { text: "—", verdict: null };
  if (list.length === 1)
    return { text: resultText(list[0]!), verdict: verdictOfResult(list[0]!) };
  const verdicts = list.map(verdictOfResult);
  const verdict = verdicts.includes("fail")
    ? "fail"
    : verdicts.every((v) => v === "pass")
      ? "pass"
      : null;
  const m = mean(list.flatMap((r) => (r.score === null ? [] : [r.score])));
  const text = m !== null ? fmtScore(m) : (verdict ?? "—");
  return { text: m !== null && verdict ? `${text} ${verdict}` : text, verdict };
}

type Role = "baseline" | "candidate";

interface Side {
  run: EvalRun | null;
  trace: TempoTrace | undefined;
  agent: AgentTrace | undefined;
  results: EvalResult[];
}

export function CaseView(shell: ShellContext) {
  const { state } = shell;
  const { scope, runs, baseline, candidate, base, cand } =
    useComparedRuns(state);
  const caseId = state.evals.case;
  const mode = state.evals.mode;
  const baseTraceId = base.data?.traces.get(caseId);
  const candTraceId = cand.data?.traces.get(caseId);
  const traceIds = useMemo(
    () => [baseTraceId, candTraceId].filter((x): x is string => !!x),
    [baseTraceId, candTraceId],
  );
  const tracesQ = useCaseTraces(scope, traceIds);
  const runIds = useMemo(
    () => [baseline?.id, candidate?.id].filter((x): x is string => !!x),
    [baseline?.id, candidate?.id],
  );
  const resultsQ = useCaseResults(scope, runIds, caseId);
  const [closed, setClosed] = useState<ReadonlySet<string>>(new Set());
  const setEvals = evalsUpdater(shell);

  const sides = useMemo((): Record<Role, Side> => {
    const results = resultsQ.data ?? [];
    const side = (run: EvalRun | null, traceId: string | undefined): Side => {
      const trace = traceId ? tracesQ.data?.get(traceId) : undefined;
      return {
        run,
        trace,
        agent: trace ? agentTraceOf(trace) : undefined,
        results: run ? results.filter((r) => r.runId === run.id) : [],
      };
    };
    return {
      baseline: side(baseline, baseTraceId),
      candidate: side(candidate, candTraceId),
    };
  }, [
    resultsQ.data,
    tracesQ.data,
    baseline,
    candidate,
    baseTraceId,
    candTraceId,
  ]);

  const showBase = mode === "baseline";
  const roles: Role[] = mode === "side" ? ["baseline", "candidate"] : [mode];
  const shown = sides[showBase ? "baseline" : "candidate"];
  const failing = new Set(
    sides.candidate.results
      .filter((r) => verdictOfResult(r) === "fail")
      .map((r) => r.name),
  ).size;

  const trajectories = useMemo(
    () => ({
      baseline: sides.baseline.trace
        ? trajectoryRows(sides.baseline.trace, sides.baseline.results)
        : [],
      candidate: sides.candidate.trace
        ? trajectoryRows(
            sides.candidate.trace,
            sides.candidate.results,
            sides.baseline.agent?.tools,
          )
        : [],
    }),
    [sides],
  );

  const cards = useMemo(() => {
    const spanById = new Map(
      (shown.trace?.spans ?? []).map((sp) => [sp.spanId, sp]),
    );
    const names = new Set(
      [...sides.candidate.results, ...sides.baseline.results].map(
        (r) => r.name,
      ),
    );
    return [...names].sort().map((name) => {
      const current = shown.results.filter((r) => r.name === name);
      const ops = current.map((r) => {
        const sp = r.spanId ? spanById.get(r.spanId) : undefined;
        return sp ? opOf(sp) || sp.name : "run";
      });
      const distinct = [...new Set(ops)];
      return {
        name,
        evaluator: current.find((r) => r.evaluator)?.evaluator ?? null,
        on:
          distinct.length === 1 && ops.length > 1
            ? `${ops.length} ${distinct[0]!.replace("execute_", "")} spans`
            : distinct.join(", ") || "—",
        current,
        previous: showBase
          ? []
          : sides.baseline.results.filter((r) => r.name === name),
      };
    });
  }, [sides, shown, showBase]);

  const compareHref = viewHref("/evals/compare", state, {
    evals: { ...state.evals, case: "", mode: DEFAULT_EVAL_PARAMS.mode },
  });
  const traceHref = (id: string) =>
    viewHref(buildPath("traces", id), state, { range: LOOKBACK_RANGE });

  const error = runs.error ?? resultsQ.error ?? tracesQ.error;

  return (
    <div className="evals" style={{ gap: 14 }}>
      <Link className="evals-back" to={compareHref}>
        ‹ Compare {baseline?.version ?? baseline?.id ?? "?"} →{" "}
        {candidate?.version ?? candidate?.id ?? "?"}
        {candidate?.set ? ` · ${candidate.set}` : ""}
      </Link>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 10,
          flexWrap: "wrap",
        }}
      >
        <h1 style={{ margin: 0, font: "600 var(--text-title) var(--mono)" }}>
          {caseId || "No case selected"}
        </h1>
        {!showBase && failing > 0 && (
          <ScoreBadge tone="fail">{failing} failing</ScoreBadge>
        )}
        <span className="evals-tag">
          {mode === "side"
            ? `${baseline?.version ?? "?"} → ${candidate?.version ?? "?"}`
            : (shown.run?.version ?? "")}
        </span>
        <span className="evals-bar-fill" />
        <Segmented
          label="View"
          value={mode}
          options={MODES}
          onChange={(m) => setEvals({ mode: m })}
        />
      </div>

      {error && <QueryError what="this case" error={error} />}

      {shown.trace && shown.agent && (
        <div className="evals-meta">
          <span>
            <span className="dim">trace</span>{" "}
            <Link to={traceHref(shown.trace.traceId)}>
              {shown.trace.traceId}
            </Link>
          </span>
          <span>
            <span className="dim">duration</span>{" "}
            {formatDurationMs(shown.agent.durationMs ?? shown.trace.durationMs)}
          </span>
          <span>
            <span className="dim">LLM calls</span> {shown.agent.llmCalls}
          </span>
          {shown.agent.tokens !== null && (
            <span>
              <span className="dim">tokens</span> {fmtCount(shown.agent.tokens)}
            </span>
          )}
          <span>
            <span className="dim">at</span>{" "}
            {fmtDateTime(Number(BigInt(shown.trace.startNs) / 1_000_000n))}
          </span>
        </div>
      )}

      <div className="evals-case-body">
        <div className="evals-case-main">
          <div className="evals-panes">
            {roles.map((role) => {
              const { trace, run } = sides[role];
              return trace ? (
                <TrajectoryPane
                  key={role}
                  title={`${run?.version ?? ""} ${role}`.trim()}
                  trace={trace}
                  rows={trajectories[role]}
                />
              ) : (
                <div key={role} className="evals-card">
                  <EmptyState title={`No ${role} trace for this case`} />
                </div>
              );
            })}
          </div>
          {(sides.candidate.agent?.input || sides.baseline.agent?.input) && (
            <div className="evals-convo">
              <h2>Conversation</h2>
              <div className="evals-msg">
                <div className="evals-msg-role">User</div>
                <div className="evals-msg-body user">
                  {sides.candidate.agent?.input ?? sides.baseline.agent?.input}
                </div>
              </div>
              <div className="evals-convo-grid">
                {roles.map((role) => (
                  <div key={role} className="evals-msg">
                    <div className="evals-msg-role">
                      Agent · {sides[role].run?.version ?? "?"}
                    </div>
                    <div className="evals-msg-body">
                      {sides[role].agent?.output ?? (
                        <span className="faint">no answer recorded</span>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
        <div className="evals-cards">
          {cards.map((c) => {
            const cur = summarize(c.current);
            const prev = summarize(c.previous);
            const open = !closed.has(c.name);
            const first = c.current[0];
            return (
              <div
                key={c.name}
                className={`evals-score-card${cur.verdict === "fail" ? " failing" : ""}`}
              >
                <button
                  type="button"
                  aria-expanded={open}
                  onClick={() => setClosed((s) => toggleInSet(s, c.name))}
                >
                  <span className="mono strong">{c.name}</span>
                  <span className="faint" style={{ fontSize: 12 }}>
                    {c.on}
                  </span>
                  <span className="evals-bar-fill" />
                  <ScoreBadge tone={cur.verdict}>{cur.text}</ScoreBadge>
                  {!showBase && (
                    <span className="mono faint" style={{ fontSize: 12 }}>
                      was {prev.text}
                    </span>
                  )}
                </button>
                {open && (
                  <>
                    {c.current
                      .filter((r) => r.explanation)
                      .map((r, i) => (
                        <p key={i}>{r.explanation}</p>
                      ))}
                    {first && (
                      <dl className="evals-fields">
                        <dt>gen_ai.evaluation.name</dt>
                        <dd>{c.name}</dd>
                        <dt>score.value</dt>
                        <dd>{first.score ?? "—"}</dd>
                        <dt>score.label</dt>
                        <dd>{first.label ?? "—"}</dd>
                        {first.error && (
                          <>
                            <dt>error.type</dt>
                            <dd>{first.error}</dd>
                          </>
                        )}
                        <dt>evaluator</dt>
                        <dd>{c.evaluator ?? "—"}</dd>
                        <dt>scored span</dt>
                        <dd>
                          {c.on}
                          {first.spanId ? ` · ${first.spanId.slice(0, 8)}` : ""}
                        </dd>
                        <dt>eval run</dt>
                        <dd>{first.runId ?? "—"}</dd>
                      </dl>
                    )}
                  </>
                )}
              </div>
            );
          })}
          {resultsQ.isSuccess && cards.length === 0 && (
            <EmptyState title="No results for this case" />
          )}
        </div>
      </div>
    </div>
  );
}

function TrajectoryPane({
  title,
  trace,
  rows,
}: {
  title: string;
  trace: TempoTrace;
  rows: TrajectoryRow[];
}) {
  return (
    <div className="evals-card" style={{ minWidth: 0 }}>
      <div
        className="evals-card-head"
        style={{ padding: "10px 12px", gap: 10 }}
      >
        <h2>Agent trajectory</h2>
        <span className="evals-tag">{title}</span>
        <span className="evals-bar-fill" />
        <span className="mono faint" style={{ fontSize: 11 }}>
          0 — {formatDurationMs(trace.durationMs)}
        </span>
      </div>
      <div className="evals-traj-head">
        <span>Span</span>
        <span>Timing</span>
        <span style={{ textAlign: "right" }}>Scores on this span</span>
      </div>
      {rows.map((r, i) =>
        r.kind === "ghost" ? (
          <div key={`g${i}`} className="evals-traj-row ghost">
            <div className="evals-traj-name">
              <span className="indent" />
              <span className="evals-kind">tool</span>
              <span>execute_tool {r.tool}</span>
            </div>
            <div style={{ fontSize: 12 }}>expected, not called</div>
            <div className="evals-traj-scores">
              <ScoreBadge tone="missing">missing vs baseline</ScoreBadge>
            </div>
          </div>
        ) : (
          <div key={r.span.spanId} className="evals-traj-row">
            <div className="evals-traj-name">
              {r.depth > 0 && <span className="indent" />}
              <span className="evals-kind">
                {OP_KIND.get(r.op) ?? (r.op || "span")}
              </span>
              <span title={r.span.name}>{r.span.name}</span>
            </div>
            <div className="evals-timing">
              <i
                className={OP_KIND.get(r.op)}
                style={{ left: `${r.leftPct}%`, width: `${r.widthPct}%` }}
              />
              <span>{formatDurationMs(r.durMs)}</span>
            </div>
            <div className="evals-traj-scores">
              {r.results.map((res, j) => (
                <ScoreBadge
                  key={j}
                  tone={verdictOfResult(res)}
                  title={res.explanation ?? undefined}
                >
                  {res.name} {resultText(res)}
                </ScoreBadge>
              ))}
            </div>
          </div>
        ),
      )}
    </div>
  );
}
