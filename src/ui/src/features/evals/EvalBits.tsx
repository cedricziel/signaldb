// Small pieces shared by the Evaluate pages.
import type { ReactNode } from "react";
import { EvalApiError } from "../../api/evalSets";
import { toErrorMessage } from "../../api/http";
import { Dialog } from "../../components/Dialog";
import { ShareBar } from "../../components/ShareBar";
import { viewHref, type ExploreState } from "../../lib/urlState";
import { passRateOf, type EvalStats } from "./evalModel";
import { fmtPct } from "./evalFormat";

export function EvalsHead({
  title,
  sub,
  actions,
}: {
  title: ReactNode;
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

const DOCS = "https://github.com/cedricziel/signaldb/blob/main/docs/users";
export const CASE_FORMAT_DOC = `${DOCS}/eval-sets.md#the-set-and-its-cases`;
export const RESULTS_FORMAT_DOC = `${DOCS}/evaluations.md#upload-a-results-file`;

/** Compare for two runs; an empty baseline lets Compare pick the
 * candidate's previous run of the same set. */
export function compareHref(
  state: ExploreState,
  baseline: string,
  candidate: string,
): string {
  return viewHref("/evals/compare", state, {
    evals: { ...state.evals, baseline, candidate, case: "" },
  });
}

/** An eval set's page. */
export function setHref(state: ExploreState, name: string): string {
  return viewHref(`/evals/sets/${encodeURIComponent(name)}`, state);
}

/** The Evaluate dialogs' frame: title row with a close button, a body and
 * a footer. */
export function EvalDialog({
  label,
  wide,
  onClose,
  children,
  footer,
  tabs,
}: {
  label: string;
  wide?: boolean;
  onClose: () => void;
  children: ReactNode;
  footer: ReactNode;
  tabs?: ReactNode;
}) {
  return (
    <Dialog
      label={label}
      onClose={onClose}
      className={`evals-dialog${wide ? " wide" : ""}`}
    >
      <div className="evals evals-dlg">
        <div className="evals-dlg-head">
          <h2>{label}</h2>
          <button
            type="button"
            className="btn btn-ghost"
            aria-label="Close"
            onClick={onClose}
          >
            ✕
          </button>
        </div>
        {tabs}
        <div className="evals-dlg-body">{children}</div>
        <div className="evals-dlg-foot">{footer}</div>
      </div>
    </Dialog>
  );
}

/** The problems behind a failed write: the server's message plus, for an
 * upload, each invalid row. */
export function WriteError({ error }: { error: unknown }) {
  const details = error instanceof EvalApiError ? error.details : [];
  return (
    <div role="alert" className="evals-errors-wrap">
      <div className="error-text">{toErrorMessage(error)}</div>
      {details.length > 0 && (
        <ul className="evals-errors" aria-label="Problems">
          {details.map((d, i) => (
            <li key={i}>
              {d.row != null && `line ${d.row}`}
              {d.column && ` · ${d.column}`}
              {(d.row != null || d.column) && ": "}
              {d.reason}
            </li>
          ))}
        </ul>
      )}
    </div>
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
