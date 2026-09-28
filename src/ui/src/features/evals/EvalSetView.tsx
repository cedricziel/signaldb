// One eval set (`/evals/sets/{name}`): its cases with each case's score in
// the set's newest run, Export JSONL, Add traces, and a Settings tab to
// delete it. The set comes from the eval-sets API; runs and per-case scores
// are Query IR reads over the fixed lookback.

import { useQueryClient } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { Link, useNavigate } from "react-router";
import { ConfirmButton } from "../../components/ConfirmButton";
import { EmptyState } from "../../components/EmptyState";
import { QueryError } from "../../components/QueryError";
import {
  deleteEvalSet,
  isForbidden,
  isNotFound,
  type EvalCase,
} from "../../api/evalSets";
import { buildPath, viewHref } from "../../lib/urlState";
import { downloadText } from "../../lib/download";
import type { ShellContext } from "../../lib/outletState";
import { casesToJsonl, caseScore, runsBySet } from "./evalSetModel";
import { fmtCount, fmtDateTime } from "./evalFormat";
import { compareHref, EvalsHead, ScoreBadge, WriteError } from "./EvalBits";
import { AddTracesPanel } from "./AddTracesPanel";

import {
  evalSetKeys,
  LOOKBACK_RANGE,
  useEvalSet,
  useLookbackRuns,
  useRunCases,
} from "./useEvalData";
import "./evals.css";

const PAGE = 50;

type Tab = "cases" | "settings";

export function EvalSetView({
  state,
  name,
}: ShellContext & {
  name: string;
}) {
  const set = useEvalSet(state, name);
  const { scope, runs } = useLookbackRuns(state);
  const setRuns = useMemo(
    () => runsBySet(runs.data ?? []).get(name) ?? [],
    [runs.data, name],
  );
  const [latest, previous] = setRuns;
  const scores = useRunCases(scope, latest?.id ?? "");
  const [tab, setTab] = useState<Tab>("cases");
  const [adding, setAdding] = useState(false);
  const [shown, setShown] = useState(PAGE);
  const [deleteError, setDeleteError] = useState<unknown>(null);
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const data = set.data;
  const cases = data?.cases ?? [];
  const canWrite = !!data?._links.append_cases_from_traces;
  const canDelete = !!data?._links.delete;
  const runsHref = viewHref("/evals/runs", state, {
    range: LOOKBACK_RANGE,
    evals: { ...state.evals, set: name },
  });

  if (set.error) {
    return (
      <div className="evals">
        <EvalsHead title={name} />
        {isNotFound(set.error) ? (
          <EmptyState title={`No eval set named ${name}`}>
            It isn't in this dataset.{" "}
            <Link to={viewHref("/evals/sets", state)}>All eval sets</Link>
          </EmptyState>
        ) : isForbidden(set.error) ? (
          <EmptyState title="You can't read eval sets in this dataset">
            Reading eval sets needs the <code>evals:read</code> scope.
          </EmptyState>
        ) : (
          <QueryError what={`eval set ${name}`} error={set.error} />
        )}
      </div>
    );
  }

  async function remove() {
    setDeleteError(null);
    try {
      await deleteEvalSet(name);
      await queryClient.invalidateQueries({ queryKey: evalSetKeys(state).all });
      navigate(viewHref("/evals/sets", state));
    } catch (e) {
      setDeleteError(e);
    }
  }

  const traceHref = (id: string) =>
    viewHref(buildPath("traces", id), state, { range: LOOKBACK_RANGE });

  return (
    <div className="evals">
      <EvalsHead
        title={
          <>
            <span className="mono">{name}</span>
            {data && (
              <span className="evals-count">
                {fmtCount(data.case_count)} cases
              </span>
            )}
          </>
        }
        sub={
          data && (
            <>
              {data.description && <>{data.description}. </>}
              Test cases for <code>{data.agent}</code>.{" "}
              {latest
                ? `Last replayed on ${latest.version ?? latest.id}, ${fmtDateTime(latest.firstMs)}.`
                : runs.isPending
                  ? ""
                  : "Not replayed in the last 30 days."}
            </>
          )
        }
        actions={
          data && (
            <>
              <button
                type="button"
                className="btn"
                disabled={cases.length === 0}
                onClick={() =>
                  downloadText(
                    `${name}.jsonl`,
                    casesToJsonl(cases),
                    "application/x-ndjson",
                  )
                }
              >
                Export JSONL
              </button>
              <button
                type="button"
                className="btn btn-primary"
                aria-expanded={adding}
                disabled={!canWrite}
                title={canWrite ? undefined : "You can't add cases to this set"}
                onClick={() => setAdding((v) => !v)}
              >
                Add traces…
              </button>
            </>
          )
        }
      />

      <div className="evals-tabs" role="tablist" aria-label="Eval set">
        <button
          type="button"
          role="tab"
          aria-selected={tab === "cases"}
          onClick={() => setTab("cases")}
        >
          Cases
        </button>
        <Link role="tab" aria-selected={false} to={runsHref}>
          Runs{runs.isSuccess && ` (${setRuns.length})`}
        </Link>
        <button
          type="button"
          role="tab"
          aria-selected={tab === "settings"}
          onClick={() => setTab("settings")}
        >
          Settings
        </button>
      </div>

      {runs.error && <QueryError what="eval runs" error={runs.error} />}
      {scores.error && <QueryError what="case scores" error={scores.error} />}

      {tab === "settings" ? (
        <div className="evals-card" style={{ padding: 14 }}>
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            <dl className="evals-fields" style={{ maxWidth: 520 }}>
              <dt>name</dt>
              <dd>{name}</dd>
              <dt>agent</dt>
              <dd>{data?.agent ?? "—"}</dd>
              <dt>description</dt>
              <dd>{data?.description || "—"}</dd>
              <dt>created</dt>
              <dd>{data ? fmtDateTime(Date.parse(data.created_at)) : "—"}</dd>
              <dt>updated</dt>
              <dd>{data ? fmtDateTime(Date.parse(data.updated_at)) : "—"}</dd>
            </dl>
            <div>
              <ConfirmButton
                label="Delete eval set"
                prompt={`Delete ${name} and its ${fmtCount(cases.length)} cases?`}
                disabled={!canDelete}
                onConfirm={() => void remove()}
              />
            </div>
            {!canDelete && data && (
              <span className="evals-note">
                Deleting needs the <code>evals:write</code> scope.
              </span>
            )}
            {deleteError !== null && <WriteError error={deleteError} />}
          </div>
        </div>
      ) : (
        <>
          <ol className="evals-steps" aria-label="How a set is used">
            <li>
              <span className="evals-step-title">
                <span>1</span>Pull the set
              </span>
              <span className="evals-step-line">
                <code>GET /api/v1/eval-sets/{name}</code>
              </span>
            </li>
            <li>
              <span className="evals-step-title">
                <span>2</span>Replay the agent
              </span>
              <span className="evals-step-line">
                Run each input on the new version, traced as usual
              </span>
            </li>
            <li>
              <span className="evals-step-title">
                <span>3</span>Score
              </span>
              <span className="evals-step-line">
                Upload the results, or send{" "}
                <code>gen_ai.evaluation.result</code> over OTLP
              </span>
            </li>
            <li>
              <span className="evals-step-title">
                <span>4</span>Compare
              </span>
              <span className="evals-step-line">
                {latest && previous ? (
                  <Link to={compareHref(state, previous.id, latest.id)}>
                    {previous.version ?? previous.id} →{" "}
                    {latest.version ?? latest.id} ›
                  </Link>
                ) : (
                  "Two runs of this set are needed"
                )}
              </span>
            </li>
          </ol>
          <div className="evals-set-body">
            <div className="evals-card">
              <table className="evals-table" style={{ minWidth: 760 }}>
                <thead>
                  <tr>
                    <th>Case</th>
                    <th>Input</th>
                    <th>Expected tools</th>
                    <th>Reference</th>
                    <th>Source</th>
                    <th className="num">Last score</th>
                  </tr>
                </thead>
                <tbody>
                  {cases.slice(0, shown).map((c) => (
                    <CaseRow
                      key={c.id}
                      c={c}
                      score={caseScore(scores.data?.cases.get(c.id))}
                      traceHref={traceHref}
                    />
                  ))}
                </tbody>
              </table>
              {set.isPending && <EmptyState title="Loading cases…" />}
              {set.isSuccess && cases.length === 0 && (
                <EmptyState title="No cases yet">
                  Add cases from traces, or append them with{" "}
                  <code>signaldb-cli admin eval-sets append</code>.
                </EmptyState>
              )}
              {cases.length > 0 && (
                <div className="evals-card-foot">
                  <span>
                    Showing {fmtCount(Math.min(shown, cases.length))} of{" "}
                    {fmtCount(cases.length)}
                  </span>
                  {shown < cases.length && (
                    <button
                      type="button"
                      className="btn btn-ghost"
                      onClick={() => setShown((n) => n + PAGE)}
                    >
                      Load more
                    </button>
                  )}
                </div>
              )}
            </div>
            {adding && data && (
              <AddTracesPanel
                state={state}
                name={name}
                agent={data.agent}
                onClose={() => setAdding(false)}
              />
            )}
          </div>
        </>
      )}
    </div>
  );
}

function CaseRow({
  c,
  score,
  traceHref,
}: {
  c: EvalCase;
  score: ReturnType<typeof caseScore>;
  traceHref: (id: string) => string;
}) {
  const source = c.source ?? { kind: "hand_written" as const };
  return (
    <tr>
      <td className="mono strong nowrap">{c.id}</td>
      <td style={{ maxWidth: 320, textWrap: "pretty" }}>{c.input}</td>
      <td className="mono dim" style={{ fontSize: 11.5 }}>
        {c.expected_tools?.length ? c.expected_tools.join(" → ") : "—"}
      </td>
      <td className="dim" style={{ fontSize: 12, maxWidth: 240 }}>
        {c.reference || <span className="faint">—</span>}
      </td>
      <td className="nowrap" style={{ fontSize: 12 }}>
        {source.kind === "trace" ? (
          <Link to={traceHref(source.trace_id)} className="mono">
            trace {source.trace_id.slice(0, 8)}…
          </Link>
        ) : (
          <span className="faint">
            {source.kind === "upload" ? "upload" : "hand-written"}
          </span>
        )}
      </td>
      <td className="num">
        {score ? (
          <ScoreBadge tone={score.tone} title={score.title}>
            {score.text}
          </ScoreBadge>
        ) : (
          <span className="faint">—</span>
        )}
      </td>
    </tr>
  );
}
