import { useMutation } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import { testProcessor, type ProcessorSpec, type TestResponse } from "./api";
import { diffLines } from "./lineDiff";
import { SAMPLE_PAYLOADS } from "./samples";
import { toErrorMessage } from "../../api/http";

interface Props {
  signal: "traces" | "logs" | "metrics";
  dataset: string | null;
  /** The unsaved processor spec currently in the editor. */
  spec: ProcessorSpec;
}

/**
 * Dry-run "Test" panel (D6/D8): a sample OTLP JSON payload for the selected
 * signal, editable, submitted against the current (unsaved) processor via
 * `POST /api/v1/processors:test`. Renders a before/after line diff of the
 * payload plus per-statement match/error counts. Never writes to the WAL.
 */
export function TestPanel({ signal, dataset, spec }: Props) {
  const [payloadText, setPayloadText] = useState(() =>
    JSON.stringify(SAMPLE_PAYLOADS[signal], null, 2),
  );
  const [result, setResult] = useState<TestResponse | null>(null);
  const [before, setBefore] = useState("");
  const [error, setError] = useState<string | null>(null);

  // A run's callbacks only apply their result if they're still the latest
  // submission by the time the response arrives: the textarea stays
  // editable while a request is pending, and resetting the sample or
  // changing the signal must not let a slow, now-superseded response
  // overwrite the panel with a stale result/error/diff base.
  const revisionRef = useRef(0);
  const bumpRevision = () => {
    revisionRef.current += 1;
    return revisionRef.current;
  };

  // The signal is chosen in the parent editor; when it changes, the sample
  // payload (and any stale result from the previous signal) should reset
  // rather than silently mismatch the new signal on the next dry run.
  useEffect(() => {
    bumpRevision();
    setPayloadText(JSON.stringify(SAMPLE_PAYLOADS[signal], null, 2));
    setResult(null);
    setError(null);
    // `bumpRevision` is a stable ref-based helper; intentionally excluded to
    // keep this effect scoped to `signal` changes only.
  }, [signal]);

  type RunVariables = { revision: number; submittedPayload: string };

  const run = useMutation<TestResponse, Error, RunVariables>({
    mutationFn: async ({ submittedPayload }) => {
      const payload = JSON.parse(submittedPayload) as unknown;
      return testProcessor({
        signal,
        dataset,
        processors: [spec],
        payload,
      });
    },
    onSuccess: (response, { revision, submittedPayload }) => {
      if (revision !== revisionRef.current) return;
      setResult(response);
      setBefore(submittedPayload);
      setError(null);
    },
    onError: (e, { revision }) => {
      if (revision !== revisionRef.current) return;
      setResult(null);
      setError(toErrorMessage(e));
    },
  });

  const runTest = () => {
    const revision = bumpRevision();
    run.mutate({ revision, submittedPayload: payloadText });
  };

  const resetSample = () => {
    bumpRevision();
    setPayloadText(JSON.stringify(SAMPLE_PAYLOADS[signal], null, 2));
    setResult(null);
    setError(null);
  };

  const editPayload = (text: string) => {
    // Editing while a request is in flight must invalidate it too, or a
    // response for the pre-edit text can land after the edit and be
    // rendered as if it belonged to the text now in the textarea.
    bumpRevision();
    setPayloadText(text);
  };

  const afterText = result ? JSON.stringify(result.payload, null, 2) : null;
  const diff = afterText ? diffLines(before, afterText) : null;

  return (
    <div className="processors-test-panel">
      <div className="processors-test-head">
        <h2>Test</h2>
        <button type="button" onClick={resetSample}>
          Reset sample
        </button>
      </div>
      <label htmlFor="processors-test-payload">Sample OTLP/JSON payload</label>
      <textarea
        id="processors-test-payload"
        className="processors-test-textarea"
        value={payloadText}
        onChange={(e) => editPayload(e.target.value)}
        rows={12}
      />
      <button
        type="button"
        className="btn btn-primary"
        onClick={runTest}
        disabled={run.isPending}
      >
        Run test
      </button>

      {error && (
        <p className="error-text" role="alert">
          {error}
        </p>
      )}

      {result && (
        <>
          <h3>Per-statement results</h3>
          <ul className="processors-statement-results">
            {result.statements.map((s, i) => (
              <li key={`${s.processor}-${s.index}-${i}`}>
                {s.processor} statement {s.index}: {s.matched} match
                {s.matched === 1 ? "" : "es"}, {s.errors} error
                {s.errors === 1 ? "" : "s"}
              </li>
            ))}
          </ul>

          <h3>Diff</h3>
          <pre className="processors-diff" aria-label="payload diff">
            {diff!.map((line, index) => (
              <div
                key={index}
                className={`processors-diff-line processors-diff-${line.kind}`}
              >
                {line.kind === "removed" ? "- " : line.kind === "added" ? "+ " : "  "}
                {line.text}
              </div>
            ))}
          </pre>
        </>
      )}
    </div>
  );
}
