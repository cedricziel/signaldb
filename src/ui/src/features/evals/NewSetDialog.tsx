// New eval set: a name and agent, started from a JSONL file of cases, from
// real agent traces (create, then append from a trace query), or empty.

import { useQueryClient } from "@tanstack/react-query";
import { useId, useRef, useState } from "react";
import { useNavigate } from "react-router";
import {
  addCasesFromTraces,
  createEvalSet,
  type AppendCasesFromTracesOutcome,
} from "../../api/evalSets";
import { toErrorMessage } from "../../api/http";
import type { ExploreState } from "../../lib/urlState";
import {
  parseCasesJsonl,
  validSetName,
  type ParsedCases,
} from "./evalSetModel";
import { fmtCount } from "./evalFormat";
import { CASE_FORMAT_DOC, EvalDialog, setHref, WriteError } from "./EvalBits";
import {
  defaultTraceQuery,
  sampleOk,
  TraceQueryFields,
  traceRequest,
  type TraceQuery,
} from "./TraceQueryFields";
import { evalScope, evalSetKeys, useAgents } from "./useEvalData";

type Source = "jsonl" | "traces" | "empty";

const SOURCES: [Source, string, string][] = [
  ["jsonl", "Upload JSONL", "Cases you already have"],
  ["traces", "Real traces", "Pick runs with a query"],
  ["empty", "Empty", "Add cases later"],
];

const PREVIEW_ROWS = 8;

export function NewSetDialog({
  state,
  onClose,
}: {
  state: ExploreState;
  onClose: () => void;
}) {
  const agents = useAgents(evalScope(state));
  const [name, setName] = useState("");
  const [agent, setAgent] = useState("");
  const [source, setSource] = useState<Source>("jsonl");
  const [file, setFile] = useState<{
    name: string;
    parsed: ParsedCases;
    noReference: number;
  }>();
  const [query, setQuery] = useState<TraceQuery>(() => defaultTraceQuery(""));
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [partial, setPartial] = useState<{ name: string; error: unknown }>();
  const fileInput = useRef<HTMLInputElement>(null);
  const listId = useId();
  const queryClient = useQueryClient();
  const navigate = useNavigate();

  const nameOk = validSetName(name);
  const cases = file?.parsed.cases ?? [];
  const traceQuery = { ...query, agent: query.agent || agent };
  const partialHref = partial && setHref(state, partial.name);
  const ready =
    nameOk &&
    agent.trim() !== "" &&
    !busy &&
    (source === "empty" ||
      (source === "jsonl" &&
        cases.length > 0 &&
        file?.parsed.errors.length === 0) ||
      (source === "traces" && sampleOk(query.sample)));

  const cta =
    source === "empty"
      ? "Create empty set"
      : source === "traces"
        ? `Create with up to ${Number.isNaN(query.sample) ? "?" : fmtCount(query.sample)} cases`
        : `Create with ${fmtCount(cases.length)} cases`;

  async function readFile(f: File) {
    const parsed = parseCasesJsonl(await f.text());
    setFile({
      name: f.name,
      parsed,
      noReference: parsed.cases.filter((c) => !c.reference).length,
    });
  }

  async function create() {
    setBusy(true);
    setError(null);
    try {
      await createEvalSet({
        name,
        agent: agent.trim(),
        cases: source === "jsonl" ? cases : [],
      });
    } catch (e) {
      setError(e);
      setBusy(false);
      return;
    }
    let added: AppendCasesFromTracesOutcome | null = null;
    if (source === "traces") {
      try {
        added = await addCasesFromTraces(name, traceRequest(traceQuery));
      } catch (e) {
        setPartial({ name, error: e });
      }
    }
    await queryClient.invalidateQueries({ queryKey: evalSetKeys(state).all });
    setBusy(false);
    if (source !== "traces" || added) {
      onClose();
      navigate(setHref(state, name));
    }
  }

  return (
    <EvalDialog
      label="New eval set"
      onClose={onClose}
      footer={
        <>
          <a href={CASE_FORMAT_DOC} target="_blank" rel="noreferrer">
            Case format
          </a>
          <span className="evals-bar-fill" />
          <button type="button" className="btn" onClick={onClose}>
            Cancel
          </button>
          <button
            type="button"
            className="btn btn-primary"
            disabled={!ready || !!partial}
            onClick={() => void create()}
          >
            {busy ? "Creating…" : cta}
          </button>
        </>
      }
    >
      <div className="evals-grid2">
        <div className="evals-field">
          <label className="evals-field">
            Name
            <input
              className="evals-input"
              value={name}
              placeholder="refund-edge-cases-40"
              aria-invalid={name !== "" && !nameOk}
              onChange={(e) => setName(e.target.value)}
            />
          </label>
          {name !== "" && !nameOk && (
            <span className="evals-note warn">
              Lowercase letters, digits, <code>-</code>, <code>_</code> and{" "}
              <code>.</code>, starting with a letter or digit.
            </span>
          )}
        </div>
        <label className="evals-field">
          Agent
          <input
            className="evals-input"
            value={agent}
            list={listId}
            placeholder="support-triage"
            onChange={(e) => setAgent(e.target.value)}
          />
          <datalist id={listId}>
            {(agents.data ?? []).map((a) => (
              <option key={a} value={a} />
            ))}
          </datalist>
        </label>
      </div>
      <fieldset
        className="evals-field"
        style={{ border: 0, padding: 0, margin: 0 }}
      >
        <legend style={{ padding: 0, marginBottom: 4 }}>Start from</legend>
        <div className="evals-radios">
          {SOURCES.map(([id, label, hint]) => (
            <label key={id} className="evals-radio">
              <input
                type="radio"
                name="source"
                value={id}
                checked={source === id}
                onChange={() => setSource(id)}
              />
              <b>{label}</b>
              <span>{hint}</span>
            </label>
          ))}
        </div>
      </fieldset>

      {source === "jsonl" && (
        <>
          <input
            ref={fileInput}
            type="file"
            accept=".jsonl,.ndjson,.json,application/x-ndjson"
            aria-label="Cases file"
            hidden
            onChange={(e) => {
              const f = e.target.files?.[0];
              if (f) void readFile(f);
            }}
          />
          {file ? (
            <JsonlPreview
              name={file.name}
              parsed={file.parsed}
              noReference={file.noReference}
              onReplace={() => fileInput.current?.click()}
            />
          ) : (
            <button
              type="button"
              className="evals-drop"
              onClick={() => fileInput.current?.click()}
            >
              <b>Choose a JSONL file of cases</b>
              <span>
                one case per line: id, input, expected_tools, reference
              </span>
            </button>
          )}
        </>
      )}
      {source === "traces" && (
        <TraceQueryFields
          state={state}
          query={traceQuery}
          onChange={setQuery}
        />
      )}
      {source === "empty" && (
        <div className="evals-dashed">
          The set starts with no cases. Add them later from traces, by uploading
          JSONL, or one at a time.
        </div>
      )}
      {error !== null && <WriteError error={error} />}
      {partial && partialHref && (
        <div role="alert" className="evals-warn">
          Created <b className="mono">{partial.name}</b>, but adding cases from
          traces failed: {toErrorMessage(partial.error)}{" "}
          <a
            href={partialHref}
            onClick={(e) => {
              e.preventDefault();
              onClose();
              navigate(partialHref);
            }}
          >
            Open the set ›
          </a>
        </div>
      )}
    </EvalDialog>
  );
}

function JsonlPreview({
  name,
  parsed,
  noReference,
  onReplace,
}: {
  name: string;
  parsed: ParsedCases;
  noReference: number;
  onReplace: () => void;
}) {
  const { cases, errors } = parsed;
  return (
    <>
      <div className="evals-file">
        <span className="mono strong">{name}</span>
        <span className="dim">
          {fmtCount(cases.length + errors.length)} rows
        </span>
        <span className="evals-bar-fill" />
        <button type="button" className="btn btn-ghost" onClick={onReplace}>
          Replace
        </button>
      </div>
      {cases.length > 0 && (
        <div className="evals-preview">
          <table className="evals-table">
            <thead>
              <tr>
                <th>id</th>
                <th>input</th>
                <th>expected_tools</th>
                <th>reference</th>
              </tr>
            </thead>
            <tbody>
              {cases.slice(0, PREVIEW_ROWS).map((c) => (
                <tr key={c.id}>
                  <td className="mono nowrap">{c.id}</td>
                  <td>
                    <div className="evals-ellipsis">{c.input}</div>
                  </td>
                  <td className="mono dim">
                    <div className="evals-ellipsis">
                      {c.expected_tools?.join(" → ") || "—"}
                    </div>
                  </td>
                  <td className="mono">
                    {c.reference ? (
                      <span className="good">yes</span>
                    ) : (
                      <span className="evals-note warn">missing</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <div className="evals-note">
        {fmtCount(cases.length)} cases read.
        {noReference > 0 && (
          <>
            {" "}
            <span className="warn">
              {fmtCount(noReference)} have no reference answer
            </span>
            ; judges that compare against the reference will skip them.
          </>
        )}
      </div>
      {errors.length > 0 && (
        <ul className="evals-errors" aria-label="Lines that can't be read">
          {errors.map((e) => (
            <li key={e.line}>
              line {e.line}: {e.reason}
            </li>
          ))}
        </ul>
      )}
    </>
  );
}
