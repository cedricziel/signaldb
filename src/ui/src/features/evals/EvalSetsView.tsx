// Eval sets (`/evals/sets`): the dataset's eval sets with what they were
// built from and how their newest run scored. Sets come from the eval-sets
// API; runs are a Query IR read over the fixed lookback.

import { useMemo, useState } from "react";
import { Link } from "react-router";
import { EmptyState } from "../../components/EmptyState";
import { QueryError } from "../../components/QueryError";
import { isForbidden } from "../../api/evalSets";
import type { ShellContext } from "../../lib/outletState";
import { passRateOf } from "./evalModel";
import { builtFrom, runsBySet } from "./evalSetModel";
import { fmtCount, fmtDay, fmtPct } from "./evalFormat";
import { EvalsHead, setHref } from "./EvalBits";
import { NewSetDialog } from "./NewSetDialog";
import { useEvalSets, useLookbackRuns } from "./useEvalData";
import "./evals.css";

export function EvalSetsView({ state }: ShellContext) {
  const sets = useEvalSets(state);
  const { runs } = useLookbackRuns(state);
  const bySet = useMemo(() => runsBySet(runs.data ?? []), [runs.data]);
  const [creating, setCreating] = useState(false);
  const canCreate = !!sets.data?._links.create;

  return (
    <div className="evals">
      <EvalsHead
        title="Eval sets"
        sub="Fixed lists of test cases: an input, the tools you expect, and a reference answer. Pull one in your eval script, replay it, upload the scores."
        actions={
          canCreate && (
            <button
              type="button"
              className="btn btn-primary"
              onClick={() => setCreating(true)}
            >
              New eval set…
            </button>
          )
        }
      />
      {runs.error && <QueryError what="eval runs" error={runs.error} />}
      {sets.error ? (
        isForbidden(sets.error) ? (
          <EmptyState title="You can't read eval sets in this dataset">
            Reading eval sets needs the <code>evals:read</code> scope.
          </EmptyState>
        ) : (
          <QueryError what="eval sets" error={sets.error} />
        )
      ) : !sets.data ? (
        <div className="evals-card">
          <EmptyState title="Loading eval sets…" />
        </div>
      ) : sets.data.items.length === 0 ? (
        <NoSets onCreate={canCreate ? () => setCreating(true) : undefined} />
      ) : (
        <div className="evals-card">
          <table className="evals-table" style={{ minWidth: 900 }}>
            <thead>
              <tr>
                <th>Eval set</th>
                <th>Agent</th>
                <th className="num">Cases</th>
                <th>Built from</th>
                <th>Last run</th>
                <th className="num">Pass rate</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {sets.data.items.map((s) => {
                const last = bySet.get(s.name)?.[0];
                return (
                  <tr key={s.name}>
                    <td>
                      <Link to={setHref(state, s.name)} className="mono strong">
                        {s.name}
                      </Link>
                      {s.description && (
                        <div className="dim" style={{ fontSize: 12 }}>
                          {s.description}
                        </div>
                      )}
                    </td>
                    <td className="mono">{s.agent}</td>
                    <td className="num">{fmtCount(s.case_count)}</td>
                    <td className="dim" style={{ fontSize: 12 }}>
                      {builtFrom(s)}
                    </td>
                    <td className="mono nowrap">
                      {last ? (
                        <>
                          {last.version && (
                            <span className="evals-tag">{last.version}</span>
                          )}{" "}
                          <span className="dim">{fmtDay(last.firstMs)}</span>
                        </>
                      ) : (
                        <span className="faint">
                          {runs.isPending ? "…" : "never run"}
                        </span>
                      )}
                    </td>
                    <td className="num">
                      {last ? fmtPct(passRateOf(last.stats)) : "—"}
                    </td>
                    <td className="mono dim nowrap">
                      {fmtDay(Date.parse(s.updated_at))}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
      {creating && (
        <NewSetDialog state={state} onClose={() => setCreating(false)} />
      )}
    </div>
  );
}

function NoSets({ onCreate }: { onCreate?: () => void }) {
  return (
    <div className="evals-empty" style={{ gridTemplateColumns: "1fr" }}>
      <div className="evals-empty-text" style={{ maxWidth: 620 }}>
        <div className="evals-eyebrow">No eval sets yet</div>
        <h2>Keep the cases your agent is tested on in one place.</h2>
        <p>
          Start from a JSONL file of cases, from real agent traces, or empty.
          Your eval script pulls the set, replays it, and uploads the scores.
        </p>
        {onCreate && (
          <div>
            <button
              type="button"
              className="btn btn-primary"
              onClick={onCreate}
            >
              New eval set…
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
