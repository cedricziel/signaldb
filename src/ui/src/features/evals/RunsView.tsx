// Runs (`/evals/runs`): the offline eval runs in the window. A run is every
// result sharing one `signaldb.eval.run_id`; nothing registers it.

import { useMemo, useState } from "react";
import { Link } from "react-router";
import { EmptyState } from "../../components/EmptyState";
import { QueryError } from "../../components/QueryError";
import { TimeRangePicker } from "../../components/TimeRangePicker";
import type { ShellContext } from "../../lib/outletState";
import { viewHref, type ExploreState } from "../../lib/urlState";
import { baselinesOf, passRateOf, runStatus } from "./evalModel";
import { fmtCount, fmtDateTime, fmtPct } from "./evalFormat";
import { EvalsHead, PillSelect, RESULT_EXAMPLE, ScoreBadge } from "./EvalBits";
import {
  evalRange,
  evalScope,
  evalsUpdater,
  useAgents,
  useRuns,
} from "./useEvalData";
import "./evals.css";

export function compareHref(
  state: ExploreState,
  baseline: string,
  candidate: string,
): string {
  return viewHref("/evals/compare", state, {
    evals: { ...state.evals, baseline, candidate, case: "" },
  });
}

export function RunsView(shell: ShellContext) {
  const { state, update } = shell;
  const scope = evalScope(state);
  const agents = useAgents(scope);
  const agent = state.evals.agent;
  const runs = useRuns(scope, agent ? { agent } : {});
  const [set, setSet] = useState("");
  const all = useMemo(() => runs.data ?? [], [runs.data]);
  const sets = useMemo(
    () => [...new Set(all.flatMap((r) => (r.set ? [r.set] : [])))].sort(),
    [all],
  );
  const baselines = useMemo(() => baselinesOf(all), [all]);
  const shown = set ? all.filter((r) => r.set === set) : all;
  const now = Date.now();
  const setEvals = evalsUpdater(shell);

  return (
    <div className="evals">
      <EvalsHead
        title="Runs"
        sub="Offline eval runs: one agent version scored on one eval set. Your harness replays the set and sends the scores; SignalDB groups them by run id."
      />
      {runs.error && <QueryError what="eval runs" error={runs.error} />}
      {runs.isSuccess && all.length === 0 && !agent ? (
        <NoRuns />
      ) : (
        <>
          <div className="evals-bar">
            <PillSelect
              label="agent"
              value={agent}
              options={agents.data ?? []}
              onChange={(v) => setEvals({ agent: v })}
            />
            <PillSelect
              label="eval set"
              value={set}
              options={sets}
              onChange={setSet}
            />
            <span className="evals-bar-fill" />
            <span className="evals-bar-note">{shown.length} runs</span>
            <TimeRangePicker
              range={evalRange(state)}
              onChange={(r) => update({ range: r })}
            />
          </div>
          <div className="evals-card">
            <table className="evals-table" style={{ minWidth: 900 }}>
              <thead>
                <tr>
                  <th>Run</th>
                  <th>Eval set</th>
                  <th>Version</th>
                  <th>Started</th>
                  <th className="num">Cases</th>
                  <th className="num">Results</th>
                  <th className="num">Pass rate</th>
                  <th>Status</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {shown.map((run) => {
                  const status = runStatus(run, now);
                  const baseline = baselines.get(run.id);
                  return (
                    <tr key={run.id}>
                      <td className="mono strong nowrap">{run.id}</td>
                      <td className="mono">{run.set ?? "—"}</td>
                      <td>
                        {run.version ? (
                          <span className="evals-tag">{run.version}</span>
                        ) : (
                          "—"
                        )}
                      </td>
                      <td className="mono dim nowrap">
                        {fmtDateTime(run.firstMs)}
                        <div
                          className="faint"
                          style={{ fontFamily: "var(--ui)" }}
                        >
                          {run.agent}
                        </div>
                      </td>
                      <td className="num">{fmtCount(run.cases)}</td>
                      <td className="num">{fmtCount(run.results)}</td>
                      <td className="num">{fmtPct(passRateOf(run.stats))}</td>
                      <td>
                        <RunStatusCell
                          kind={status.kind}
                          reasons={status.reasons}
                        />
                      </td>
                      <td className="nowrap" style={{ textAlign: "right" }}>
                        {status.kind !== "running" && baseline && (
                          <Link to={compareHref(state, baseline.id, run.id)}>
                            Compare ›
                          </Link>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {runs.isPending && <EmptyState title="Loading runs…" />}
            {runs.isSuccess && shown.length === 0 && (
              <EmptyState title="No runs in this range" />
            )}
          </div>
        </>
      )}
    </div>
  );
}

function RunStatusCell({
  kind,
  reasons,
}: {
  kind: "running" | "complete" | "partial";
  reasons: string[];
}) {
  if (kind === "running")
    return (
      <span className="mono dim strong" style={{ fontSize: 11 }}>
        receiving results
      </span>
    );
  if (kind === "complete") return <ScoreBadge tone="pass">complete</ScoreBadge>;
  return (
    <>
      <ScoreBadge tone="partial">partial</ScoreBadge>
      <div className="dim" style={{ fontSize: 12, marginTop: 3 }}>
        {reasons.join(" · ")}
      </div>
    </>
  );
}

function NoRuns() {
  return (
    <div className="evals-empty" style={{ gridTemplateColumns: "1fr" }}>
      <div className="evals-empty-text" style={{ maxWidth: 620 }}>
        <div className="evals-eyebrow">No runs yet</div>
        <h2>Run your evals where they already run, then send the scores.</h2>
        <p>
          SignalDB doesn't replay your agent or run your judges. It stores the
          results, links them to the traces from the replay, and compares
          versions.
        </p>
      </div>
      <ol className="evals-steps">
        <li>
          <span className="mono faint strong">1 · Replay</span>
          <span>
            Run the eval set on the new agent version with tracing on, as usual.
          </span>
        </li>
        <li>
          <span className="mono faint strong">2 · Score</span>
          <span>
            Run your evaluators: code checks, LLM judges, classifiers.
          </span>
        </li>
        <li>
          <span className="mono faint strong">3 · Send</span>
          <span>
            Emit one <code>gen_ai.evaluation.result</code> log record per result
            over OTLP, with the run attributes below.
          </span>
        </li>
      </ol>
      <pre className="evals-code">{RESULT_EXAMPLE}</pre>
      <p className="faint" style={{ fontSize: 12 }}>
        Runs appear here within a minute and are marked complete after 10
        minutes without new results.
      </p>
    </div>
  );
}
