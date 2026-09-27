// Small pieces shared by the Evaluate pages.
import type { ReactNode } from "react";
import { ShareBar } from "../../components/ShareBar";
import { passRateOf, type EvalStats } from "./evalModel";
import { fmtPct } from "./evalFormat";

export function EvalsHead({
  title,
  sub,
  actions,
}: {
  title: string;
  sub?: ReactNode;
  actions?: ReactNode;
}) {
  return (
    <div className="evals-head">
      <div className="evals-head-text">
        <h1>{title}</h1>
        {sub && <div className="evals-sub">{sub}</div>}
      </div>
      {actions && <div className="evals-actions">{actions}</div>}
    </div>
  );
}

/** `key [value ▾]` filter pill. The empty value means "all". */
export function PillSelect({
  label,
  value,
  options,
  allLabel = "all",
  allowAll = true,
  onChange,
}: {
  label: string;
  value: string;
  options: string[];
  allLabel?: string;
  /** Offer the empty "all" value; off where the page always picks one. */
  allowAll?: boolean;
  onChange: (v: string) => void;
}) {
  return (
    <label className={`evals-pill${value ? " set" : ""}`}>
      <span className="evals-pill-key">{label}</span>
      <select
        aria-label={label}
        value={value}
        onChange={(e) => onChange(e.target.value)}
      >
        {allowAll && <option value="">{allLabel}</option>}
        {options.map((o) => (
          <option key={o} value={o}>
            {o}
          </option>
        ))}
      </select>
    </label>
  );
}

export function Segmented<T extends string>({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: T;
  options: [T, string][];
  onChange: (v: T) => void;
}) {
  return (
    <div role="group" aria-label={label} className="evals-segmented">
      {options.map(([id, text]) => (
        <button
          key={id}
          type="button"
          aria-pressed={value === id}
          onClick={() => onChange(id)}
        >
          {text}
        </button>
      ))}
    </div>
  );
}

/** Pass-rate bar: the passing share over the failing one. */
export function PassBar({ stats }: { stats: EvalStats }) {
  return (
    <div className="evals-passbar">
      <ShareBar
        ariaLabel="pass rate"
        segments={[
          {
            key: "pass",
            value: stats.pass,
            color: "var(--ok)",
            label: "pass",
          },
          {
            key: "fail",
            value: stats.fail,
            color: "color-mix(in oklab, var(--err) 22%, var(--surface3))",
            label: "fail",
          },
        ].filter((seg) => seg.value > 0)}
      />
      <span>{fmtPct(passRateOf(stats))}</span>
    </div>
  );
}

/** A result's verdict as a badge; `tone` null renders neutral. */
export function ScoreBadge({
  tone,
  children,
  title,
}: {
  tone: "pass" | "fail" | "partial" | "missing" | null;
  children: ReactNode;
  title?: string;
}) {
  return (
    <span className={`evals-badge${tone ? ` ${tone}` : ""}`} title={title}>
      {children}
    </span>
  );
}

/** The log-record form of one result, as a harness would send it. */
export const RESULT_EXAMPLE = `logger.emit(LogRecord(
  event_name="gen_ai.evaluation.result",
  trace_id=scored_span.trace_id,
  span_id=scored_span.span_id,
  attributes={
    "gen_ai.evaluation.name": "Correctness",
    "gen_ai.evaluation.score.value": 0.9,
    "gen_ai.evaluation.score.label": "pass",
    "gen_ai.evaluation.explanation": "Matches the reference…",
    "gen_ai.agent.name": "support-triage",
    "gen_ai.agent.version": "v1.8.0",
    "signaldb.eval.run_id": "run-0927-1004",
    "signaldb.eval.set": "triage-golden-200",
    "signaldb.eval.case_id": "case-117",
    "signaldb.eval.evaluator": "correctness-judge@1.4",
  },
))`;
