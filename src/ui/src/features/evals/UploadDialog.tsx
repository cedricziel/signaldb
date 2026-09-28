// Upload eval results (from Runs): a JSONL/CSV results file previewed
// client-side and sent to `POST /api/v1/evals/results`, plus the CLI and
// OTLP ways of sending the same results.

import { useQueryClient } from "@tanstack/react-query";
import { useId, useMemo, useRef, useState } from "react";
import { Link } from "react-router";
import { CopyValueButton } from "../../components/CopyValueButton";
import {
  MAX_BODY_BYTES,
  uploadResults,
  type EvalResultsUploadResponse,
} from "../../api/evalSets";
import { viewHref, type ExploreState } from "../../lib/urlState";
import {
  fmtBytes,
  formatOf,
  countMatchingCases,
  previewResults,
  RESULT_COLUMNS,
  validSetName,
  type ResultsPreview,
} from "./evalSetModel";
import { fmtCount, fmtPct, fmtScore } from "./evalFormat";
import {
  compareHref,
  EvalDialog,
  RESULT_EXAMPLE,
  RESULTS_FORMAT_DOC,
  WriteError,
} from "./EvalBits";

import {
  evalScope,
  useAgents,
  useEvalSet,
  useEvalSets,
  useTraceVersions,
} from "./useEvalData";

type Tab = "file" | "cli" | "otlp";

const TABS: [Tab, string][] = [
  ["file", "Upload file"],
  ["cli", "CLI / CI"],
  ["otlp", "Send over OTLP"],
];

const SAMPLE_ROW = `{"case_id":"case-117","trace_id":"4bf92f35…","span_id":"9a1c7f03…",
 "name":"ToolTrajectory","score":0.33,"label":"fail",
 "explanation":"check_policy was skipped…","evaluator":"trajectory-match@2.1.0"}`;

/** Trace ids sent to the version lookup: enough to find the replay's
 * version without a huge `in` list. */
const VERSION_PROBE = 200;

interface ChosenFile {
  name: string;
  size: number;
  text: string;
  format: "csv" | "jsonl";
  preview: ResultsPreview;
  /** Chosen once per file, so retrying the same upload can't create a
   * second run. */
  runId: string;
}

function newRunId(): string {
  return typeof crypto !== "undefined" && "randomUUID" in crypto
    ? crypto.randomUUID()
    : `run-${Date.now().toString(36)}`;
}

export function cliSnippet(agent: string, set: string): string {
  return `signaldb-cli evals upload results.jsonl \\
  --agent ${agent || "support-triage"} \\
  --version "$GIT_TAG" \\
  --set ${set || "triage-golden-200"} \\
  --compare-to latest:v1.7.3 \\
  --fail-if "ToolTrajectory.mean < 0.85"`;
}

export function UploadDialog({
  state,
  initialAgent = "",
  initialSet = "",
  onClose,
}: {
  state: ExploreState;
  initialAgent?: string;
  initialSet?: string;
  onClose: () => void;
}) {
  const [tab, setTab] = useState<Tab>("file");
  const [file, setFile] = useState<ChosenFile>();
  const [agent, setAgent] = useState(initialAgent);
  const [version, setVersion] = useState<string | null>(null);
  const [set, setSet] = useState(initialSet);
  const [over, setOver] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [result, setResult] = useState<EvalResultsUploadResponse>();
  const input = useRef<HTMLInputElement>(null);
  const ids = { agents: useId(), sets: useId() };
  const queryClient = useQueryClient();

  const agents = useAgents(evalScope(state));
  const sets = useEvalSets(state);
  const setNames = (sets.data?.items ?? []).map((s) => s.name);
  const known = setNames.includes(set);
  const chosenSet = useEvalSet(state, known ? set : "");
  const matched = useMemo(
    () =>
      file && chosenSet.data
        ? countMatchingCases(file.preview.caseIds, chosenSet.data.cases)
        : null,
    [file, chosenSet.data],
  );
  const probe = useMemo(
    () => file?.preview.traceIds.slice(0, VERSION_PROBE) ?? [],
    [file],
  );
  const versions = useTraceVersions(state, probe);
  const prefilled = versions.data?.[0] ?? "";
  const effectiveVersion = version ?? prefilled;

  async function choose(f: File | undefined) {
    if (!f) return;
    setError(null);
    const text = await f.text();
    const format = formatOf(f.name);
    setFile({
      name: f.name,
      size: f.size,
      text,
      format,
      preview: previewResults(text, format),
      runId: newRunId(),
    });
  }

  const tooBig = !!file && file.size > MAX_BODY_BYTES;
  const ready =
    !!file &&
    !tooBig &&
    file.preview.rows > 0 &&
    agent.trim() !== "" &&
    effectiveVersion.trim() !== "" &&
    validSetName(set) &&
    !busy;

  async function upload() {
    if (!file) return;
    setBusy(true);
    setError(null);
    try {
      setResult(
        await uploadResults(file.text, {
          agent: agent.trim(),
          version: effectiveVersion.trim(),
          set,
          runId: file.runId,
          format: file.format,
        }),
      );
      await queryClient.invalidateQueries({ queryKey: ["eval-runs"] });
    } catch (e) {
      setError(e);
    } finally {
      setBusy(false);
    }
  }

  const snippet = tab === "cli" ? cliSnippet(agent, set) : RESULT_EXAMPLE;

  if (result)
    return (
      <EvalDialog
        label="Upload eval results"
        wide
        onClose={onClose}
        footer={
          <>
            <span className="evals-bar-fill" />
            <button type="button" className="btn" onClick={onClose}>
              Close
            </button>
          </>
        }
      >
        <UploadResult state={state} result={result} onClose={onClose} />
      </EvalDialog>
    );

  return (
    <EvalDialog
      label="Upload eval results"
      wide
      onClose={onClose}
      tabs={
        <div className="evals-tabs" role="tablist" aria-label="How to send">
          {TABS.map(([id, label]) => (
            <button
              key={id}
              type="button"
              role="tab"
              aria-selected={tab === id}
              onClick={() => setTab(id)}
            >
              {label}
            </button>
          ))}
        </div>
      }
      footer={
        <>
          <a href={RESULTS_FORMAT_DOC} target="_blank" rel="noreferrer">
            Results format
          </a>
          <span className="evals-bar-fill" />
          <button type="button" className="btn" onClick={onClose}>
            Cancel
          </button>
          {tab === "file" ? (
            <button
              type="button"
              className="btn btn-primary"
              disabled={!ready}
              onClick={() => void upload()}
            >
              {busy
                ? "Uploading…"
                : file
                  ? `Upload ${fmtCount(file.preview.rows)} results`
                  : "Upload results"}
            </button>
          ) : (
            <CopyValueButton
              value={snippet}
              label={tab === "cli" ? "the CLI command" : "the OTLP example"}
              className="btn btn-primary"
            />
          )}
        </>
      }
    >
      <input
        ref={input}
        type="file"
        accept=".jsonl,.ndjson,.csv,text/csv,application/x-ndjson"
        aria-label="Results file"
        hidden
        onChange={(e) => void choose(e.target.files?.[0])}
      />
      {tab === "file" && (
        <>
          {file ? (
            <FilePreview file={file} onReplace={() => input.current?.click()} />
          ) : (
            <>
              <button
                type="button"
                className="evals-drop"
                data-over={over || undefined}
                onClick={() => input.current?.click()}
                onDragOver={(e) => {
                  e.preventDefault();
                  setOver(true);
                }}
                onDragLeave={() => setOver(false)}
                onDrop={(e) => {
                  e.preventDefault();
                  setOver(false);
                  void choose(e.dataTransfer.files[0]);
                }}
              >
                <b>Drop a results file, or click to choose</b>
                <span>
                  .jsonl or .csv · one row per evaluator result · up to 32 MiB
                </span>
              </button>
              <pre className="evals-code">{SAMPLE_ROW}</pre>
            </>
          )}
          {tooBig && (
            <div role="alert" className="error-text">
              {file.name} is {fmtBytes(file.size)}; uploads are limited to 32
              MiB. Split the file into several runs' worth, or use the CLI.
            </div>
          )}
          <div className="evals-section-label">Run</div>
          <div className="evals-grid3">
            <label className="evals-field">
              Agent
              <input
                className="evals-input"
                value={agent}
                list={ids.agents}
                placeholder="support-triage"
                onChange={(e) => setAgent(e.target.value)}
              />
              <datalist id={ids.agents}>
                {(agents.data ?? []).map((a) => (
                  <option key={a} value={a} />
                ))}
              </datalist>
            </label>
            <label className="evals-field">
              Agent version
              <input
                className="evals-input"
                value={effectiveVersion}
                placeholder="v1.8.0"
                onChange={(e) => setVersion(e.target.value)}
              />
            </label>
            <label className="evals-field">
              Eval set
              <input
                className="evals-input"
                value={set}
                list={ids.sets}
                placeholder="triage-golden-200"
                aria-invalid={set !== "" && !validSetName(set)}
                onChange={(e) => setSet(e.target.value)}
              />
              <datalist id={ids.sets}>
                {setNames.map((n) => (
                  <option key={n} value={n} />
                ))}
              </datalist>
            </label>
          </div>
          <RunNote
            file={file}
            set={set}
            known={known}
            matched={matched}
            prefilled={version === null && prefilled !== ""}
          />
          {file && file.preview.runLevel > 0 && (
            <div role="status" className="evals-warn">
              <b className="mono">{fmtCount(file.preview.runLevel)} rows</b>{" "}
              have no <code>trace_id</code>. They'll count toward the run's
              scores but won't appear on a span in the trace view.
            </div>
          )}
          {error !== null && <WriteError error={error} />}
        </>
      )}
      {tab === "cli" && (
        <>
          <p className="evals-note" style={{ margin: 0 }}>
            Add this step to CI after your eval script. It uploads the file and
            prints a link to the comparison.
          </p>
          <pre className="evals-code">{snippet}</pre>
          <p className="evals-note" style={{ margin: 0 }}>
            <code>--fail-if</code> exits non-zero so the pipeline can block a
            release. Uses the API key in <code>SIGNALDB_API_KEY</code>.
          </p>
        </>
      )}
      {tab === "otlp" && (
        <>
          <p className="evals-note" style={{ margin: 0 }}>
            If your eval harness already exports OpenTelemetry, emit one{" "}
            <code>gen_ai.evaluation.result</code> log record per result, linked
            to the span it scores (a span event of the same name works too).
            SignalDB groups them into a run by the attributes below.
          </p>
          <pre className="evals-code">{snippet}</pre>
          <p className="evals-note" style={{ margin: 0 }}>
            Runs sent this way appear in Runs within a minute and are marked
            complete after 10 minutes without new results.
          </p>
        </>
      )}
    </EvalDialog>
  );
}

function FilePreview({
  file,
  onReplace,
}: {
  file: ChosenFile;
  onReplace: () => void;
}) {
  const p = file.preview;
  const hasVerdict = ["score", "label", "error"].some((c) => p.columns.has(c));
  return (
    <>
      <div className="evals-file">
        <span className="mono strong">{file.name}</span>
        <span className="dim">
          {fmtBytes(file.size)} · {fmtCount(p.rows)} rows
        </span>
        <span className="evals-bar-fill" />
        <button type="button" className="btn btn-ghost" onClick={onReplace}>
          Replace
        </button>
      </div>
      <div className="evals-stats" aria-label="File contents">
        <div>
          Cases <b>{fmtCount(p.caseIds.size)}</b>
        </div>
        <div>
          Evaluators found <b>{fmtCount(p.evaluators.size)}</b>
        </div>
        <div>
          Linked to a span <b>{fmtCount(p.linked)}</b>
        </div>
        <div>
          Run-level only{" "}
          <b className={p.runLevel > 0 ? "warn" : undefined}>
            {fmtCount(p.runLevel)}
          </b>
        </div>
      </div>
      <div className="evals-section-label">Columns</div>
      <dl className="evals-columns">
        {RESULT_COLUMNS.map((c) => (
          <div key={c.column}>
            <dt>
              {c.target}
              {c.required && (
                <span className="req" title="required">
                  {" "}
                  *
                </span>
              )}
            </dt>
            <dd>
              ←{" "}
              {p.columns.has(c.column) ? (
                <span className="evals-colchip">{c.column}</span>
              ) : (
                <span
                  className={`evals-colchip missing${c.required ? " bad" : ""}`}
                >
                  not in file
                </span>
              )}
            </dd>
          </div>
        ))}
      </dl>
      {!hasVerdict && p.rows > 0 && (
        <div className="evals-note warn">
          Every row needs a <code>score</code>, a <code>label</code> or an{" "}
          <code>error</code>; this file has none of those columns.
        </div>
      )}
      {p.errors.length > 0 && (
        <ul className="evals-errors" aria-label="Rows that can't be read">
          {p.errors.slice(0, 100).map((e, i) => (
            <li key={i}>
              line {e.line}: {e.reason}
            </li>
          ))}
        </ul>
      )}
    </>
  );
}

function RunNote({
  file,
  set,
  known,
  matched,
  prefilled,
}: {
  file: ChosenFile | undefined;
  set: string;
  known: boolean;
  /** How many of the file's case ids the chosen stored set holds. */
  matched: number | null;
  prefilled: boolean;
}) {
  if (!file) return null;
  const ids = file.preview.caseIds;
  return (
    <div className="evals-note">
      {set && !known && validSetName(set) && (
        <>
          <span className="mono">{set}</span> isn't a stored eval set; the run
          is recorded under that name anyway.{" "}
        </>
      )}
      {matched !== null && (
        <>
          <span className={`mono ${matched === ids.size ? "ok" : "warn"}`}>
            {fmtCount(matched)} of {fmtCount(ids.size)}
          </span>{" "}
          case IDs match {set}.{" "}
        </>
      )}
      {prefilled && (
        <>
          The version was read from <code>service.version</code> on the linked
          traces.
        </>
      )}
    </div>
  );
}

function UploadResult({
  state,
  result,
  onClose,
}: {
  state: ExploreState;
  result: EvalResultsUploadResponse;
  onClose: () => void;
}) {
  return (
    <>
      <div role="status">
        Uploaded run <b className="mono">{result.run_id}</b>:{" "}
        {fmtCount(result.rows)} results for {fmtCount(result.cases)} cases of{" "}
        <span className="mono">{result.set}</span> ({result.agent}{" "}
        {result.version}).
      </div>
      <div className="evals-preview">
        <table className="evals-table">
          <thead>
            <tr>
              <th>Evaluator</th>
              <th className="num">Results</th>
              <th className="num">Errors</th>
              <th className="num">Mean</th>
              <th className="num">Pass rate</th>
            </tr>
          </thead>
          <tbody>
            {result.evaluators.map((e) => (
              <tr key={e.name}>
                <td className="mono strong">{e.name}</td>
                <td className="num">{fmtCount(e.results)}</td>
                <td className="num">{fmtCount(e.errors)}</td>
                <td className="num">{fmtScore(e.mean ?? null)}</td>
                <td className="num">{fmtPct(e.pass_rate ?? null)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div style={{ display: "flex", gap: 12 }}>
        <Link
          to={viewHref("/evals/runs", state, {
            evals: { ...state.evals, set: result.set },
          })}
          onClick={onClose}
        >
          Open in Runs ›
        </Link>
        <Link to={compareHref(state, "", result.run_id)} onClick={onClose}>
          Compare with the previous run ›
        </Link>
      </div>
    </>
  );
}
