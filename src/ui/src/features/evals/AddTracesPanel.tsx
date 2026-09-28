// "Add traces" on an eval set: append one case per matching agent trace.
// The endpoint has no dry run, so the counts it answers with are shown
// after the call.

import { useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import {
  addCasesFromTraces,
  type AppendCasesFromTracesOutcome,
} from "../../api/evalSets";
import type { ExploreState } from "../../lib/urlState";
import { fmtCount } from "./evalFormat";
import { WriteError } from "./EvalBits";
import {
  defaultTraceQuery,
  sampleOk,
  TraceQueryFields,
  traceRequest,
  type TraceQuery,
} from "./TraceQueryFields";
import { evalSetKeys } from "./useEvalData";

export function AddTracesPanel({
  state,
  name,
  agent,
  onClose,
}: {
  state: ExploreState;
  name: string;
  agent: string;
  onClose: () => void;
}) {
  const [query, setQuery] = useState<TraceQuery>(() =>
    defaultTraceQuery(agent),
  );
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [outcome, setOutcome] = useState<AppendCasesFromTracesOutcome>();
  const queryClient = useQueryClient();

  async function add() {
    setBusy(true);
    setError(null);
    try {
      setOutcome(await addCasesFromTraces(name, traceRequest(query)));
      await queryClient.invalidateQueries({ queryKey: evalSetKeys(state).all });
    } catch (e) {
      setError(e);
    } finally {
      setBusy(false);
    }
  }

  return (
    <aside className="evals-panel" aria-label="Add traces">
      <div className="evals-panel-head">
        <h2>Add traces</h2>
        <button
          type="button"
          className="btn btn-ghost"
          aria-label="Close Add traces"
          onClick={onClose}
        >
          ✕
        </button>
      </div>
      <TraceQueryFields state={state} query={query} onChange={setQuery} />
      {outcome && (
        <div className="evals-stats" role="status" aria-label="Last add">
          <div>
            Matches <b>{fmtCount(outcome.matches)}</b>
          </div>
          <div>
            Already in set{" "}
            <b className="dim">{fmtCount(outcome.already_present)}</b>
          </div>
          <div>
            Added <b>{fmtCount(outcome.added)}</b>
          </div>
        </div>
      )}
      {error !== null && <WriteError error={error} />}
      <div className="evals-panel-foot">
        <span className="evals-note" style={{ flex: 1 }}>
          Traces already in the set are skipped.
        </span>
        <button
          type="button"
          className="btn btn-primary"
          disabled={busy || !sampleOk(query.sample)}
          onClick={() => void add()}
        >
          {busy
            ? "Adding…"
            : `Add up to ${sampleOk(query.sample) ? fmtCount(query.sample) : "?"} cases`}
        </button>
      </div>
    </aside>
  );
}
