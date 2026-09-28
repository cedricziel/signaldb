// The trace query behind "Add traces" and a New eval set started from real
// traces: which agent runs to take (`POST /api/v1/eval-sets/{name}/cases/
// from-traces`) and how to fill the new cases.

import { useId } from "react";
import type { AppendCasesFromTracesRequest } from "../../api/evalSets";
import type { ExploreState } from "../../lib/urlState";
import { evalScope, useAgents, useEvaluators } from "./useEvalData";

export type TraceWindow = "1d" | "7d" | "30d";

export interface TraceQuery {
  agent: string;
  window: TraceWindow;
  /** `gen_ai.evaluation.name` the traces must have failed; "" for any. */
  failing: string;
  sample: number;
  expectedTools: boolean;
  referenceFromAnswer: boolean;
}

export function defaultTraceQuery(agent: string): TraceQuery {
  return {
    agent,
    window: "7d",
    failing: "",
    sample: 50,
    expectedTools: true,
    referenceFromAnswer: false,
  };
}

const WINDOWS: [TraceWindow, string][] = [
  ["1d", "last 24 hours"],
  ["7d", "last 7 days"],
  ["30d", "last 30 days"],
];

/** The server takes 1-1000 cases per call. */
export const MAX_SAMPLE = 1000;

export function sampleOk(n: number): boolean {
  return Number.isInteger(n) && n >= 1 && n <= MAX_SAMPLE;
}

export function traceRequest(q: TraceQuery): AppendCasesFromTracesRequest {
  return {
    range: { from: `now-${q.window}`, to: "now" },
    ...(q.agent.trim() ? { agent: q.agent.trim() } : {}),
    ...(q.failing ? { failing_evaluator: q.failing } : {}),
    sample: q.sample,
    expected_tools: q.expectedTools,
    reference_from_answer: q.referenceFromAnswer,
  };
}

export function TraceQueryFields({
  state,
  query,
  onChange,
}: {
  state: ExploreState;
  query: TraceQuery;
  onChange: (q: TraceQuery) => void;
}) {
  const scope = evalScope(state);
  const agents = useAgents(scope);
  const evaluators = useEvaluators(scope);
  const listId = useId();
  const set = (patch: Partial<TraceQuery>) => onChange({ ...query, ...patch });
  const names = (evaluators.data ?? []).map((e) => e.name);

  return (
    <>
      <div className="evals-query">
        <label className="evals-field">
          Agent
          <input
            className="evals-input"
            value={query.agent}
            list={listId}
            onChange={(e) => set({ agent: e.target.value })}
          />
          <datalist id={listId}>
            {(agents.data ?? []).map((a) => (
              <option key={a} value={a} />
            ))}
          </datalist>
        </label>
        <div className="evals-grid3">
          <label className="evals-field">
            Window
            <select
              className="evals-input"
              value={query.window}
              onChange={(e) => set({ window: e.target.value as TraceWindow })}
            >
              {WINDOWS.map(([id, label]) => (
                <option key={id} value={id}>
                  {label}
                </option>
              ))}
            </select>
          </label>
          <label className="evals-field">
            Failing evaluator
            <select
              className="evals-input"
              value={query.failing}
              onChange={(e) => set({ failing: e.target.value })}
            >
              <option value="">any result</option>
              {names.map((n) => (
                <option key={n} value={n}>
                  {n} = fail
                </option>
              ))}
            </select>
          </label>
          <label className="evals-field">
            Sample
            <input
              className="evals-input"
              type="number"
              min={1}
              max={MAX_SAMPLE}
              value={Number.isNaN(query.sample) ? "" : query.sample}
              aria-invalid={!sampleOk(query.sample)}
              onChange={(e) => set({ sample: e.target.valueAsNumber })}
            />
          </label>
        </div>
        <div className="dim">
          operation = invoke_agent · newest runs first, traces already in the
          set skipped
        </div>
      </div>
      <label className="evals-check">
        <input
          type="checkbox"
          checked={query.expectedTools}
          onChange={(e) => set({ expectedTools: e.target.checked })}
        />
        <span>Use the tools actually called as the expected trajectory</span>
      </label>
      <label className="evals-check">
        <input
          type="checkbox"
          checked={query.referenceFromAnswer}
          onChange={(e) => set({ referenceFromAnswer: e.target.checked })}
        />
        <span>Use the agent's answer as the reference</span>
      </label>
      {query.failing && (
        <span className="evals-note" style={{ marginTop: -6, paddingLeft: 21 }}>
          These runs failed {query.failing}. Review the references before the
          next replay.
        </span>
      )}
    </>
  );
}
