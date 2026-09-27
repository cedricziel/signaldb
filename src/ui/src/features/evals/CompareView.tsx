// Compare (`/evals/compare`): one eval set replayed on two agent versions,
// matched case by case. Regressions first — the question is "can I ship
// the candidate".

import { useMemo, useState } from "react";
import { Link } from "react-router";
import { CopyValueButton } from "../../components/CopyValueButton";
import { EmptyState } from "../../components/EmptyState";
import { QueryError } from "../../components/QueryError";
import type { AgentTrace, EvalRun } from "../../api/evals";
import type { ShellContext } from "../../lib/outletState";
import { viewHref } from "../../lib/urlState";
import { formatDurationMs } from "../../lib/waterfall";
import {
  compareCases,
  mean,
  meanOf,
  passRateOf,
  statsDelta,
  summarizeEvaluators,
  toolDiff,
  verdictOf,
  type CaseKind,
  type CaseRow,
  type CaseScores,
  type Direction,
  type ToolMark,
} from "./evalModel";
import {
  cellLabel,
  fmtCount,
  fmtDay,
  fmtDelta,
  fmtPct,
  fmtScore,
} from "./evalFormat";
import { EvalsHead } from "./EvalBits";
import {
  evalsUpdater,
  useAgentTraces,
  useComparedRuns,
  useRuns,
} from "./useEvalData";
import "./evals.css";

/** Each filter: kind, button label, swatch, caption after the case count,
 * and the empty state's title. */
const FILTERS: [CaseKind, string, string, string, string][] = [
  [
    "regression",
    "Regressions",
    "var(--err)",
    ", largest drop first",
    "No regressions",
  ],
  [
    "improvement",
    "Improvements",
    "var(--ok)",
    ", largest gain first",
    "No improvements",
  ],
  ["unchanged", "Unchanged", "var(--dim)", "", "No unchanged cases"],
];

const TOOL_TITLES: Record<ToolMark, string | undefined> = {
  same: undefined,
  skipped: "skipped: called in baseline, not in candidate",
  reordered: "reordered",
  repeated: "repeated",
  new: "new call, not in baseline",
};

const DIRECTION_TONE: Record<Direction, string> = {
  worse: "bad",
  better: "good",
  same: "",
};

function quantile(values: number[], q: number): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))]!;
}

/** Relative change from `b` to `c`, as whole percent. */
function relChange(b: number | null, c: number | null): number | null {
  return b === null || c === null || b === 0
    ? null
    : Math.round(((c - b) / b) * 100);
}

function runLabel(r: EvalRun): string {
  return `${r.version ?? r.id} · ${fmtDay(r.firstMs)} · ${r.id}`;
}

interface Perf {
  p95: number | null;
  tokens: number | null;
}

function perfOf(
  runTraces: Map<string, string> | undefined,
  traces: Map<string, AgentTrace> | undefined,
): Perf {
  const list = [...(runTraces?.values() ?? [])].flatMap((id) => {
    const t = traces?.get(id);
    return t ? [t] : [];
  });
  return {
    p95: quantile(
      list.flatMap((t) => (t.durationMs === null ? [] : [t.durationMs])),
      0.95,
    ),
    tokens: mean(list.flatMap((t) => (t.tokens === null ? [] : [t.tokens]))),
  };
}

export function CompareView(shell: ShellContext) {
  const { state } = shell;
  const { scope, runs, baseline, candidate, base, cand } =
    useComparedRuns(state);
  // The pickers list every run in the lookback, even when the URL pins two.
  const allRuns = useRuns(scope, {});
  const pickable = allRuns.data ?? runs.data ?? [];
  const setEvals = evalsUpdater(shell);

  const traceIds = useMemo(
    () => [
      ...(base.data?.traces.values() ?? []),
      ...(cand.data?.traces.values() ?? []),
    ],
    [base.data, cand.data],
  );
  const traces = useAgentTraces(
    scope,
    baseline?.id ?? "",
    candidate?.id ?? "",
    traceIds,
    (!baseline || !!base.data) && !!cand.data,
  );

  const [filter, setFilter] = useState<CaseKind>("regression");

  const rows = useMemo(
    () =>
      base.data && cand.data
        ? compareCases(base.data.cases, cand.data.cases)
        : [],
    [base.data, cand.data],
  );
  const summary = useMemo(
    () =>
      base.data && cand.data
        ? summarizeEvaluators(base.data.cases, cand.data.cases, rows)
        : [],
    [base.data, cand.data, rows],
  );
  const byKind = useMemo(() => {
    const out: Record<CaseKind, CaseRow[]> = {
      regression: [],
      improvement: [],
      unchanged: [],
    };
    for (const r of rows) out[r.kind].push(r);
    return out;
  }, [rows]);
  const shown = byKind[filter];
  const [, , , caption, emptyTitle] = FILTERS.find(([k]) => k === filter)!;
  const withoutBaseline =
    filter === "regression" ? shown.filter((r) => r.noBaseline).length : 0;
  const names = summary.map((s) => s.name);

  const [pb, pc] = useMemo(
    () => [
      perfOf(base.data?.traces, traces.data),
      perfOf(cand.data?.traces, traces.data),
    ],
    [base.data, cand.data, traces.data],
  );

  const agentOf = (
    runTraces: Map<string, string> | undefined,
    caseId: string,
  ) => {
    const id = runTraces?.get(caseId);
    return id ? traces.data?.get(id) : undefined;
  };

  const caseHref = (caseId: string) =>
    viewHref("/evals/compare/case", state, {
      evals: {
        ...state.evals,
        baseline: baseline?.id ?? "",
        candidate: candidate?.id ?? "",
        case: caseId,
      },
    });

  const error = runs.error ?? base.error ?? cand.error;
  const agent = candidate?.agent ?? baseline?.agent;

  return (
    <div className="evals">
      <EvalsHead
        title="Compare"
        sub={
          <>
            The same eval set replayed on two versions
            {agent && (
              <>
                {" "}
                of <span className="mono">{agent}</span>
              </>
            )}
            .
          </>
        }
        actions={
          <CopyValueButton
            value={window.location.href}
            label="to this comparison"
            text={["Copy link", "Link copied"]}
            className="btn"
          />
        }
      />
      <div className="evals-bar" style={{ padding: "10px 12px", gap: 10 }}>
        <span className="evals-picker-label">Baseline</span>
        <RunPicker
          label="Baseline run"
          runs={pickable}
          value={baseline?.id ?? ""}
          onChange={(id) => setEvals({ baseline: id })}
        />
        <span className="faint">vs</span>
        <span className="evals-picker-label">Candidate</span>
        <RunPicker
          label="Candidate run"
          runs={pickable}
          value={candidate?.id ?? ""}
          onChange={(id) => setEvals({ candidate: id })}
        />
        {candidate?.set && (
          <>
            <span className="faint">on</span>
            <span className="mono">{candidate.set}</span>
          </>
        )}
        <span className="evals-bar-fill" />
        <div style={{ display: "flex", gap: 6 }}>
          {FILTERS.map(([kind, label, color]) => (
            <button
              key={kind}
              type="button"
              className="evals-filter"
              aria-pressed={filter === kind}
              onClick={() => setFilter(kind)}
            >
              <i style={{ background: color }} />
              {label} <b>{fmtCount(byKind[kind].length)}</b>
            </button>
          ))}
        </div>
      </div>

      {error && <QueryError what="the comparison" error={error} />}
      {allRuns.isSuccess && pickable.length < 2 && (
        <EmptyState title="Two runs are needed to compare">
          Send a second run of the same eval set — see{" "}
          <Link to={viewHref("/evals/runs", state, {})}>Runs</Link>.
        </EmptyState>
      )}
      {baseline && candidate && baseline.set !== candidate.set && (
        <div role="status" className="evals-banner warn-callout">
          <span className="mono strong">note</span>
          <span>
            These runs replayed different eval sets ({baseline.set ?? "none"}{" "}
            and {candidate.set ?? "none"}). Only cases with the same id are
            compared.
          </span>
        </div>
      )}

      {summary.length > 0 && (
        <div className="evals-card">
          <table className="evals-table" style={{ minWidth: 820 }}>
            <thead>
              <tr>
                <th>Metric</th>
                <th className="num">Baseline</th>
                <th className="num">Candidate</th>
                <th className="num">Delta</th>
                <th style={{ paddingLeft: 24 }}>Pass rate</th>
                <th className="num">Cases worse</th>
                <th className="num">Cases better</th>
              </tr>
            </thead>
            <tbody>
              {summary.map((s) => {
                const rb = passRateOf(s.baseline);
                const rc = passRateOf(s.candidate);
                const delta = statsDelta(s.baseline, s.candidate);
                const labelOnly = delta
                  ? delta.unit === "pp"
                  : meanOf(s.baseline) === null && meanOf(s.candidate) === null;
                const d = fmtDelta(delta?.d ?? null, delta?.unit, true);
                return (
                  <tr key={s.name}>
                    <td className="mono strong">
                      {s.name}
                      {labelOnly && <span className="faint"> label</span>}
                    </td>
                    <td className="num dim">
                      {labelOnly ? fmtPct(rb, 1) : fmtScore(meanOf(s.baseline))}
                    </td>
                    <td className="num">
                      {labelOnly
                        ? fmtPct(rc, 1)
                        : fmtScore(meanOf(s.candidate))}
                    </td>
                    <td className={`num ${d.tone}`}>{d.text}</td>
                    <td className="mono dim" style={{ paddingLeft: 24 }}>
                      {fmtPct(rb)} → {fmtPct(rc)}
                    </td>
                    <td className={`num${s.worse > 5 ? " bad" : ""}`}>
                      {s.worse}
                    </td>
                    <td className="num">{s.better}</td>
                  </tr>
                );
              })}
              <PerfRow
                name="latency p95"
                b={pb.p95}
                c={pc.p95}
                fmt={formatDurationMs}
              />
              <PerfRow
                name="tokens / run"
                b={pb.tokens}
                c={pc.tokens}
                fmt={(v) => fmtCount(Math.round(v))}
              />
            </tbody>
          </table>
        </div>
      )}

      {rows.length > 0 && (
        <div className="evals-card">
          <div className="evals-card-head">
            <h2>Cases</h2>
            <span className="evals-caption">
              {shown.length} cases{caption}
              {withoutBaseline > 0 && ` · ${withoutBaseline} without baseline`}
            </span>
            <span className="evals-bar-fill" />
            <span className="evals-tools-legend">
              <span>
                <span className="evals-tool skipped">tool</span> skipped
              </span>
              <span>
                <span className="evals-tool reordered">tool</span> reordered or
                repeated
              </span>
              <span>
                <span className="evals-tool new">tool</span> new call
              </span>
            </span>
          </div>
          <table className="evals-table" style={{ minWidth: 1000 }}>
            <thead>
              <tr>
                <th>Case</th>
                <th>User input</th>
                {names.map((n) => (
                  <th key={n}>{n}</th>
                ))}
                <th>Tools called, candidate vs baseline</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {shown.map((row) => (
                <CaseLine
                  key={row.caseId}
                  row={row}
                  names={names}
                  baseline={base.data?.cases.get(row.caseId)}
                  candidate={cand.data?.cases.get(row.caseId)}
                  baseTrace={agentOf(base.data?.traces, row.caseId)}
                  candTrace={agentOf(cand.data?.traces, row.caseId)}
                  href={caseHref(row.caseId)}
                />
              ))}
            </tbody>
          </table>
          {shown.length === 0 && <EmptyState title={emptyTitle} />}
        </div>
      )}
    </div>
  );
}

function RunPicker({
  label,
  runs,
  value,
  onChange,
}: {
  label: string;
  runs: EvalRun[];
  value: string;
  onChange: (id: string) => void;
}) {
  return (
    <select
      className="evals-picker"
      aria-label={label}
      value={value}
      onChange={(e) => onChange(e.target.value)}
    >
      {!value && <option value="">pick a run</option>}
      {runs.map((r) => (
        <option key={r.id} value={r.id}>
          {runLabel(r)}
        </option>
      ))}
    </select>
  );
}

function PerfRow({
  name,
  b,
  c,
  fmt,
}: {
  name: string;
  b: number | null;
  c: number | null;
  fmt: (v: number) => string;
}) {
  if (b === null && c === null) return null;
  const pct = relChange(b, c);
  // Lower latency and fewer tokens are better.
  const tone = !pct ? "" : pct < 0 ? "good" : "bad";
  return (
    <tr>
      <td className="mono strong">{name}</td>
      <td className="num dim">{b === null ? "—" : fmt(b)}</td>
      <td className="num">{c === null ? "—" : fmt(c)}</td>
      <td className={`num ${tone}`}>
        {pct === null
          ? "—"
          : `${pct > 0 ? "+" : pct < 0 ? "−" : ""}${Math.abs(pct)}%`}
      </td>
      <td className="mono dim" style={{ paddingLeft: 24 }}>
        —
      </td>
      <td className="num">—</td>
      <td className="num">—</td>
    </tr>
  );
}

function CaseLine({
  row,
  names,
  baseline,
  candidate,
  baseTrace,
  candTrace,
  href,
}: {
  row: CaseRow;
  names: string[];
  baseline: CaseScores | undefined;
  candidate: CaseScores | undefined;
  baseTrace: AgentTrace | undefined;
  candTrace: AgentTrace | undefined;
  href: string;
}) {
  const steps = useMemo(
    () =>
      candTrace
        ? toolDiff(baseTrace?.tools ?? candTrace.tools, candTrace.tools)
        : [],
    [baseTrace, candTrace],
  );
  return (
    <tr>
      <td className="mono strong nowrap">
        {row.caseId}
        {row.noBaseline && (
          <div style={{ marginTop: 4 }}>
            <span className="evals-tag dashed" style={{ fontSize: 10.5 }}>
              no baseline
            </span>
          </div>
        )}
      </td>
      <td style={{ maxWidth: 260, textWrap: "pretty" }}>
        {candTrace?.input ?? baseTrace?.input ?? (
          <span className="faint">—</span>
        )}
      </td>
      {names.map((n) => {
        const b = baseline?.get(n);
        const c = candidate?.get(n);
        const dir = row.evaluators.get(n);
        const vc = c ? verdictOf(c) : null;
        const tone = dir ? DIRECTION_TONE[dir] : "";
        return (
          <td key={n} className="mono nowrap">
            <span className="dim">{cellLabel(b)}</span>{" "}
            <span className="faint">→</span>{" "}
            <b className={tone} title={vc ?? undefined}>
              {cellLabel(c)}
            </b>
          </td>
        );
      })}
      <td>
        <div className="evals-tools">
          {steps.map((s, i) => (
            <span key={i} style={{ display: "inline-flex", gap: 4 }}>
              {i > 0 && <span className="faint">→</span>}
              <span
                className={`evals-tool ${s.kind}`}
                title={TOOL_TITLES[s.kind]}
              >
                {s.name}
              </span>
            </span>
          ))}
          {!candTrace && <span className="faint">no trace</span>}
        </div>
      </td>
      <td className="nowrap" style={{ textAlign: "right" }}>
        <Link to={href}>Open ›</Link>
      </td>
    </tr>
  );
}
