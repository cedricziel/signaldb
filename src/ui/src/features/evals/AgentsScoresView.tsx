// Agents & scores (`/evals`): how an agent's evaluator scores move over the
// window, which evaluator dropped most, and how much of the agent's traffic
// is scored at all.

import { useMemo, useRef, useState } from "react";
import { Link, useNavigate } from "react-router";
import { EmptyState } from "../../components/EmptyState";
import { KpiCard, KpiStrip, type KpiChange } from "../../components/KpiCard";
import { QueryError } from "../../components/QueryError";
import { ShareBar } from "../../components/ShareBar";
import { Sparkline } from "../../components/Sparkline";
import { TimeRangePicker } from "../../components/TimeRangePicker";
import { useVizPointer, VizTooltip } from "../../components/VizTooltip";
import {
  EVAL_EVENT,
  F,
  type DailyMean,
  type VersionSpan,
} from "../../api/evals";
import type { ShellContext } from "../../lib/outletState";
import { seriesColorVar } from "../../lib/promSeries";
import { thinnedTimeAxisLabels, type ResolvedRange } from "../../lib/time";
import { viewHref, type EvalSource } from "../../lib/urlState";
import { formatTimeBucket, pluralCount } from "../../lib/vizFormat";
import {
  emptyStats,
  meanOf,
  mergeStats,
  passRateOf,
  SCORE_EPSILON,
  statsDelta,
  type EvalStats,
  type StatsDelta,
} from "./evalModel";
import {
  fmtCount,
  fmtDelta,
  fmtMeanOrLabel,
  fmtPct,
  fmtScore,
} from "./evalFormat";
import {
  EvalsHead,
  PassBar,
  PillSelect,
  RESULT_EXAMPLE,
  Segmented,
} from "./EvalBits";
import {
  evalRange,
  evalScope,
  evalsUpdater,
  trendStep,
  useAgents,
  useCoverage,
  useEvaluators,
  useEvaluatorStats,
  useMeanSeries,
  useVersions,
} from "./useEvalData";
import "./evals.css";

const SOURCES: [EvalSource, string][] = [
  ["offline", "Offline evals"],
  ["production", "Production"],
  ["both", "Both"],
];

/** Coverage under this share gets a warning: trends may not be
 * representative. */
const LOW_COVERAGE = 0.5;

/** An evaluator whose errors exceed this share of its results gets a
 * banner. */
const ERROR_BANNER_SHARE = 0.05;

interface EvaluatorRow {
  name: string;
  current: EvalStats;
  mean: number | null;
  /** The previous window's mean. */
  prevMean: number | null;
  delta: StatsDelta | null;
  regressed: boolean;
}

/** Evaluators with their change against the previous window, biggest drop
 * first. */
function evaluatorRows(
  current: Map<string, EvalStats>,
  previous: Map<string, EvalStats>,
): EvaluatorRow[] {
  return [...current]
    .map(([name, s]) => {
      const prev = previous.get(name);
      const delta = prev ? statsDelta(prev, s) : null;
      return {
        name,
        current: s,
        mean: meanOf(s),
        prevMean: prev ? meanOf(prev) : null,
        delta,
        regressed: delta !== null && delta.d <= -SCORE_EPSILON,
      };
    })
    .sort(
      (a, b) =>
        (a.delta?.d ?? 0) - (b.delta?.d ?? 0) || a.name.localeCompare(b.name),
    );
}

/** A KPI tile's change line from a signed delta. */
function kpiChange(
  d: number | null,
  unit: StatsDelta["unit"],
  suffix = "",
): KpiChange | undefined {
  if (d === null) return undefined;
  const { text, tone } = fmtDelta(d, unit);
  return {
    text: `${text}${suffix}`,
    direction: tone === "good" ? "up" : tone === "bad" ? "down" : "flat",
    tone: tone || "neutral",
  };
}

export function AgentsScoresView(shell: ShellContext) {
  const { state, update } = shell;
  const navigate = useNavigate();
  const scope = evalScope(state);
  const agents = useAgents(scope);
  const agent = state.evals.agent || agents.data?.[0] || "";
  // Wait for the agent list rather than querying unscoped first.
  const ready = state.evals.agent !== "" || !agents.isPending;
  const results = { agent, source: state.evals.source };
  const setEvals = evalsUpdater(shell);

  const stats = useEvaluatorStats(scope, results, ready);
  const series = useMeanSeries(scope, results, ready);
  const versions = useVersions(scope, results, ready);
  const coverage = useCoverage(scope, results, ready);
  const evaluators = useEvaluators(scope);

  const rows = useMemo(
    () =>
      stats.data ? evaluatorRows(stats.data.current, stats.data.previous) : [],
    [stats.data],
  );
  const total = rows.reduce(
    (acc, r) => mergeStats(acc, r.current),
    emptyStats(),
  );
  const prevTotal = [...(stats.data?.previous.values() ?? [])].reduce(
    mergeStats,
    emptyStats(),
  );
  const rate = passRateOf(total);
  const prevRate = passRateOf(prevTotal);
  const worst = rows.find((r) => r.delta !== null && r.delta.d < 0);
  const errorRows = rows.filter(
    (r) => r.current.errors / (r.current.results || 1) >= ERROR_BANNER_SHARE,
  );
  const colors = new Map(
    rows
      .map((r) => r.name)
      .sort()
      .map((n, i) => [n, seriesColorVar(i)]),
  );
  const infoByName = new Map((evaluators.data ?? []).map((e) => [e.name, e]));
  const trendByName = new Map((series.data ?? []).map((s) => [s.evaluator, s]));
  const { ms: stepMs } = trendStep(scope.range);

  const queryHref = viewHref("/query", state, {
    range: evalRange(state),
    querySource: "logs",
    queryResult: "rows",
    queryRun: true,
    queryFilters: [
      { label: "event_name", op: "=", value: EVAL_EVENT },
      ...(agent ? [{ label: F.agent, op: "=" as const, value: agent }] : []),
    ],
  });

  const loaded = stats.isSuccess;
  const empty = loaded && total.results === 0;
  const error = stats.error ?? agents.error;

  return (
    <div className="evals">
      <EvalsHead
        title="Agents & scores"
        sub={
          <>
            Mean evaluator scores from <code>{EVAL_EVENT}</code> events, by
            agent and version.
          </>
        }
        actions={
          <Link className="btn" to={queryHref}>
            Open in Query
          </Link>
        }
      />
      <div className="evals-bar">
        <PillSelect
          label="agent"
          value={agent}
          options={agents.data ?? []}
          allowAll={false}
          onChange={(v) => setEvals({ agent: v })}
        />
        <span className="evals-divider" />
        <Segmented
          label="Source"
          value={state.evals.source}
          options={SOURCES}
          onChange={(source) => setEvals({ source })}
        />
        <span className="evals-bar-fill" />
        <TimeRangePicker
          range={evalRange(state)}
          onChange={(r) => update({ range: r })}
        />
      </div>

      {error && <QueryError what="evaluation results" error={error} />}

      {empty && (
        <NoEvaluations
          agent={agent}
          agentRuns={coverage.data?.agentRuns}
          runsHref={viewHref("/evals/runs", state, {})}
        />
      )}

      {!empty && (
        <>
          {errorRows.map((r) => (
            <div key={r.name} role="status" className="evals-banner">
              <span className="evals-tag dashed">error</span>
              <span>
                The {r.name} evaluator errored on {fmtCount(r.current.errors)}{" "}
                results. They are left out of pass rates and are not counted as
                failures.
              </span>
            </div>
          ))}
          <KpiStrip>
            <CoverageTile
              agentRuns={coverage.data?.agentRuns}
              scoredRuns={coverage.data?.scoredRuns}
            />
            <KpiCard
              label="Pass rate, all evaluators"
              value={fmtPct(rate, 1)}
              change={kpiChange(
                rate !== null && prevRate !== null ? rate - prevRate : null,
                "pp",
                " vs previous window",
              )}
            />
            <KpiCard
              label="Worst-moving evaluator"
              value={worst?.name ?? "—"}
              detail={
                worst
                  ? `${fmtScore(worst.prevMean)} → ${fmtScore(worst.mean)}`
                  : undefined
              }
              change={
                worst?.delta
                  ? kpiChange(worst.delta.d, worst.delta.unit)
                  : undefined
              }
            />
            <KpiCard
              label="Evaluator errors"
              value={fmtCount(total.errors)}
              detail="Not scored, not failed."
              className={errorRows.length ? "evals-kpi-dashed" : undefined}
            />
          </KpiStrip>

          <MeanChart
            series={series.data ?? []}
            versions={versions.data ?? []}
            colors={colors}
            range={scope.range}
            onPickBucket={(fromMs, toMs) =>
              navigate(
                viewHref("/evals/runs", state, {
                  range: { type: "absolute", fromMs, toMs },
                  evals: { ...state.evals, agent },
                }),
              )
            }
          />

          <div className="evals-card">
            <div className="evals-card-head">
              <h2>Evaluators</h2>
              <span className="evals-caption">
                sorted by biggest drop against the previous window
              </span>
            </div>
            <table className="evals-table" style={{ minWidth: 900 }}>
              <thead>
                <tr>
                  <th>Evaluator</th>
                  <th>Scores</th>
                  <th>Judge</th>
                  <th className="num">Results</th>
                  <th style={{ width: 150 }}>Pass rate</th>
                  <th className="num">Mean</th>
                  <th className="num">Δ</th>
                  <th>Trend</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => {
                  const info = infoByName.get(r.name);
                  const d = fmtDelta(
                    r.delta?.d ?? null,
                    r.delta?.unit ?? (r.mean === null ? "pp" : "score"),
                  );
                  return (
                    <tr key={r.name}>
                      <td className="evals-row-name">
                        {r.regressed && (
                          <span
                            className="evals-row-flag"
                            aria-label="regressed"
                          />
                        )}
                        <div className="mono strong">{r.name}</div>
                      </td>
                      <td>
                        <span className="evals-tag plain">
                          {info?.operation ?? "—"}
                        </span>
                      </td>
                      <td className="dim mono nowrap">
                        {info?.versions[0] ?? "—"}
                      </td>
                      <td className="num">
                        {fmtCount(r.current.results - r.current.errors)}
                        {r.current.errors > 0 && (
                          <div style={{ fontSize: 11 }}>
                            <span className="evals-tag dashed">
                              {fmtCount(r.current.errors)} errored
                            </span>
                          </div>
                        )}
                      </td>
                      <td>
                        <PassBar stats={r.current} />
                      </td>
                      <td className="num">{fmtMeanOrLabel(r.current)}</td>
                      <td className={`num ${d.tone}`}>{d.text}</td>
                      <td>
                        <Sparkline
                          points={(trendByName.get(r.name)?.points ?? []).map(
                            (p) => ({ x: p.tMs, v: p.value }),
                          )}
                          tone={r.regressed ? "error" : "neutral"}
                          width={80}
                          height={20}
                          strokeWidth={1.5}
                          valueLabel="mean"
                          formatValue={(v) => v.toFixed(2)}
                          formatLabel={(x) => formatTimeBucket(x, stepMs)}
                          ariaLabel={`${r.name} mean over time`}
                          emptyText="—"
                        />
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {!loaded && <EmptyState title="Loading evaluators…" />}
          </div>
        </>
      )}
    </div>
  );
}

function CoverageTile({
  agentRuns,
  scoredRuns,
}: {
  agentRuns: number | undefined;
  scoredRuns: number | undefined;
}) {
  const share =
    agentRuns && scoredRuns !== undefined
      ? Math.min(1, scoredRuns / agentRuns)
      : null;
  const low = share !== null && share < LOW_COVERAGE;
  return (
    <KpiCard
      label="Runs scored"
      value={scoredRuns === undefined ? "–" : fmtCount(scoredRuns)}
      unit={agentRuns === undefined ? undefined : `of ${fmtCount(agentRuns)}`}
      detail={
        low
          ? `${fmtPct(share)}. Under 50% coverage: trends may not reflect every run.`
          : fmtPct(share)
      }
      className={low ? "evals-kpi-warn" : undefined}
    >
      <ShareBar
        fraction={share ?? 0}
        fillColor={low ? "var(--warn-bar)" : "var(--ok)"}
      />
    </KpiCard>
  );
}

function NoEvaluations({
  agent,
  agentRuns,
  runsHref,
}: {
  agent: string;
  agentRuns: number | undefined;
  runsHref: string;
}) {
  return (
    <div className="evals-empty">
      <div className="evals-empty-text">
        <div className="evals-eyebrow">No evaluations yet</div>
        <h2>
          {agent && agentRuns
            ? `${agent} sent ${pluralCount(agentRuns, "run")} in this window. None of them has a score.`
            : "No evaluator results in this window."}
        </h2>
        <p>
          Scores come from evaluators: code checks, LLM judges or classifiers
          that you run. Each result is a <code>{EVAL_EVENT}</code> log record
          sent over OTLP with the trace and span id of the span it scores.
          SignalDB picks them up with your traces. There is nothing to install.
        </p>
        <div className="evals-actions">
          <Link className="btn btn-primary" to={runsHref}>
            How offline runs work
          </Link>
        </div>
      </div>
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          gap: 8,
          minWidth: 0,
        }}
      >
        <div className="dim" style={{ fontSize: 12 }}>
          One result, from your eval harness (Python OTel SDK):
        </div>
        <pre className="evals-code">{RESULT_EXAMPLE}</pre>
        <div className="faint" style={{ fontSize: 12 }}>
          Score a tool span to see results per step in the case view.
        </div>
      </div>
    </div>
  );
}

const MAX_X_TICKS = 8;

/** The y axis: 0.05 steps around the data, never outside [0, 1]. */
function scoreAxis(values: number[]): { lo: number; hi: number } {
  if (values.length === 0) return { lo: 0, hi: 1 };
  const lo = Math.max(0, Math.floor((Math.min(...values) - 0.02) * 20) / 20);
  const hi = Math.min(1, Math.ceil((Math.max(...values) + 0.02) * 20) / 20);
  return hi - lo < 0.1 ? { lo: Math.max(0, hi - 0.1), hi } : { lo, hi };
}

function MeanChart({
  series,
  versions,
  colors,
  range,
  onPickBucket,
}: {
  series: DailyMean[];
  versions: VersionSpan[];
  colors: Map<string, string>;
  range: ResolvedRange;
  onPickBucket: (fromMs: number, toMs: number) => void;
}) {
  const hostRef = useRef<HTMLDivElement>(null);
  const pointer = useVizPointer(hostRef);
  const [hover, setHover] = useState<number | null>(null);
  const { step, ms: stepMs } = trendStep(range);
  const buckets = useMemo(() => {
    const ts = new Set<number>();
    for (const s of series) for (const p of s.points) ts.add(p.tMs);
    return [...ts].sort((a, b) => a - b);
  }, [series]);
  const axisLabels = useMemo(
    () => thinnedTimeAxisLabels(buckets, MAX_X_TICKS, stepMs),
    [buckets, stepMs],
  );
  const { lo, hi } = scoreAxis(
    series.flatMap((s) => s.points.map((p) => p.value)),
  );
  const x = (t: number) =>
    buckets.length < 2
      ? 500
      : ((t - buckets[0]!) / (buckets[buckets.length - 1]! - buckets[0]!)) *
        1000;
  const y = (v: number) => ((hi - v) / (hi - lo)) * 220;
  const pct = (t: number) => `${x(t) / 10}%`;
  const ticks = Array.from({ length: 6 }, (_, i) => hi - ((hi - lo) * i) / 5);
  const versionAt = (t: number) =>
    [...versions].reverse().find((v) => v.firstMs <= t + stepMs)?.version;

  if (series.length === 0) return null;
  return (
    <div className="evals-chart">
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 16,
          flexWrap: "wrap",
        }}
      >
        <h2>Mean score by evaluator</h2>
        <span className="evals-caption">
          {step === "1d" ? "daily" : "hourly"} · click a bucket to open its runs
        </span>
        <span style={{ flex: 1 }} />
        {series.map((s) => (
          <span key={s.evaluator} className="evals-legend">
            <i style={{ background: colors.get(s.evaluator) }} />
            {s.evaluator}
          </span>
        ))}
      </div>
      <div className="evals-plot">
        <div className="evals-plot-y" style={{ height: 220 }}>
          {ticks.map((t) => (
            <span key={t}>{t.toFixed(2)}</span>
          ))}
        </div>
        <div
          ref={hostRef}
          className="evals-plot-area"
          style={{ height: 220 }}
          onPointerMove={pointer.track}
          onPointerLeave={() => {
            pointer.clear();
            setHover(null);
          }}
        >
          <div className="evals-plot-grid">
            {ticks.map((t) => (
              <div key={t} />
            ))}
          </div>
          {versions.slice(1).map((v) => (
            <div
              key={v.version}
              className="evals-marker"
              style={{ left: pct(v.firstMs) }}
            >
              <span>{v.version}</span>
            </div>
          ))}
          <svg
            viewBox="0 0 1000 220"
            preserveAspectRatio="none"
            width="100%"
            height="220"
            aria-hidden="true"
          >
            {series.map((s) => (
              <polyline
                key={s.evaluator}
                points={s.points
                  .map((p) => `${x(p.tMs).toFixed(1)},${y(p.value).toFixed(1)}`)
                  .join(" ")}
                fill="none"
                stroke={colors.get(s.evaluator)}
                strokeWidth="2"
                vectorEffect="non-scaling-stroke"
                strokeLinejoin="round"
              />
            ))}
          </svg>
          <div className="evals-buckets">
            {buckets.map((t, i) => (
              <button
                key={t}
                type="button"
                aria-label={`Open runs for ${formatTimeBucket(t, stepMs)}`}
                onPointerEnter={() => setHover(i)}
                onFocus={(e) => {
                  setHover(i);
                  pointer.anchorTo(e.currentTarget);
                }}
                onBlur={() => {
                  setHover(null);
                  pointer.clear();
                }}
                onClick={() => onPickBucket(t, t + stepMs)}
              />
            ))}
          </div>
          {hover !== null && pointer.anchor && buckets[hover] !== undefined && (
            <VizTooltip
              anchor={pointer.anchor}
              host={pointer.host}
              title={`${formatTimeBucket(buckets[hover], stepMs)}${versionAt(buckets[hover]) ? ` · ${versionAt(buckets[hover])}` : ""}`}
              rows={series.map((s) => {
                const p = s.points.find((q) => q.tMs === buckets[hover]);
                return {
                  swatch: colors.get(s.evaluator),
                  label: s.evaluator,
                  value: p ? p.value.toFixed(2) : "–",
                  muted: !p,
                };
              })}
              valueWidthCh={4}
            />
          )}
        </div>
      </div>
      <div className="evals-plot-x">
        {buckets.map((t, i) => (
          <span key={t}>{axisLabels[i]}</span>
        ))}
      </div>
    </div>
  );
}
