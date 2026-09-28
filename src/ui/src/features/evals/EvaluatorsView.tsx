// Evaluators (`/evals/evaluators`): every evaluator that sent a result in
// the window. The name in `gen_ai.evaluation.name` is the evaluator; there
// is nothing to create.

import { EmptyState } from "../../components/EmptyState";
import { QueryError } from "../../components/QueryError";
import { TimeRangePicker } from "../../components/TimeRangePicker";
import type { EvaluatorInfo } from "../../api/evals";
import type { ShellContext } from "../../lib/outletState";
import { ago } from "../overview/overviewModel";
import { fmtCount, fmtDay } from "./evalFormat";
import { isReceiving } from "./evalModel";
import { EvalsHead } from "./EvalBits";
import { evalRange, evalScope, useEvaluators } from "./useEvalData";
import "./evals.css";

function outputKind(e: EvaluatorInfo): string {
  const score = e.scored > 0;
  const label = e.labelled > 0;
  if (score && label) return "score + label";
  if (score) return "score";
  if (label) return "label";
  return "—";
}

function seenIn(e: EvaluatorInfo): string {
  if (e.offline === 0) return "production";
  if (e.offline === e.results) return "offline runs";
  return "offline runs · production";
}

export function EvaluatorsView({ state, update }: ShellContext) {
  const now = Date.now();
  const evaluators = useEvaluators(evalScope(state));
  const rows = evaluators.data ?? [];
  return (
    <div className="evals">
      <EvalsHead
        title="Evaluators"
        sub={
          <>
            An evaluator shows up here the first time one of its results
            arrives. There's nothing to create: the name in{" "}
            <code>gen_ai.evaluation.name</code> is the evaluator.
          </>
        }
        actions={
          <TimeRangePicker
            range={evalRange(state)}
            onChange={(r) => update({ range: r })}
          />
        }
      />
      {evaluators.error && (
        <QueryError what="evaluators" error={evaluators.error} />
      )}
      <div className="evals-card">
        <table className="evals-table" style={{ minWidth: 900 }}>
          <thead>
            <tr>
              <th>Evaluator</th>
              <th>Versions seen</th>
              <th>Scores</th>
              <th>Output</th>
              <th>Seen in</th>
              <th className="num">Results</th>
              <th>Last result</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((e) => (
              <tr key={e.name}>
                <td>
                  <div className="mono strong">{e.name}</div>
                  <div className="faint" style={{ fontSize: 12 }}>
                    first seen {fmtDay(e.firstMs)}
                  </div>
                </td>
                <td className="mono dim">{e.versions.join(", ") || "—"}</td>
                <td>
                  <span className="evals-tag plain">{e.operation ?? "—"}</span>
                </td>
                <td className="mono dim">{outputKind(e)}</td>
                <td className="dim" style={{ fontSize: 12 }}>
                  {seenIn(e)}
                </td>
                <td className="num">{fmtCount(e.results)}</td>
                <td
                  className={`mono nowrap ${isReceiving(e.lastMs, now) ? "good" : "dim"}`}
                >
                  {ago(now - e.lastMs)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {evaluators.isPending && <EmptyState title="Loading evaluators…" />}
        {evaluators.isSuccess && rows.length === 0 && (
          <EmptyState title="No evaluator results in this range" />
        )}
      </div>
      <div className="faint" style={{ fontSize: 12 }}>
        Output kind and scored span type are read from the results. Send{" "}
        <code>signaldb.eval.evaluator</code> with a version (for example{" "}
        <code>trajectory-match@2.1.0</code>) to tell judge changes apart from
        agent changes.
      </div>
    </div>
  );
}
