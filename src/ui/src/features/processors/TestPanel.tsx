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

  const run = useMutation({
    mutationFn: async () => {
      const revision = bumpRevision();
      const submittedPayload = payloadText;
      const payload = JSON.parse(payloadText) as unknown;
      const response = await testProcessor({
        signal,
        dataset,
        processors: [spec],
        payload,
      });
      return { response, revision, submittedPayload };
    },
    onSuccess: ({ response, revision, submittedPayload }) => {
      if (revision !== revisionRef.current) return;
      setResult(response);
      setBefore(submittedPayload);
      setError(null);
    },
    onError: (e) => {
      // The mutation's own error path (thrown before the revision/payload
      // pair is captured, e.g. a JSON.parse failure) has no revision to
      // check against, so it always applies — that mirrors a synchronous
      // validation failure on the current input, never a superseded one.
      setResult(null);
      setError(toErrorMessage(e));
    },
  });

  const resetSample = () => {
    bumpRevision();
    setPayloadText(JSON.stringify(SAMPLE_PAYLOADS[signal], null, 2));
    setResult(null);
    setError(null);
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
        onChange={(e) => setPayloadText(e.target.value)}
        rows={12}
      />
      <button
        type="button"
        className="btn btn-primary"
        onClick={() => run.mutate()}
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
