import { useMutation } from "@tanstack/react-query";
import { useEffect, useState } from "react";
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

  // The signal is chosen in the parent editor; when it changes, the sample
  // payload (and any stale result from the previous signal) should reset
  // rather than silently mismatch the new signal on the next dry run.
  useEffect(() => {
    setPayloadText(JSON.stringify(SAMPLE_PAYLOADS[signal], null, 2));
    setResult(null);
    setError(null);
  }, [signal]);

  const run = useMutation({
    mutationFn: async () => {
      const payload = JSON.parse(payloadText) as unknown;
      return testProcessor({
        signal,
        dataset,
        processors: [spec],
        payload,
      });
    },
    onSuccess: (response) => {
      setResult(response);
      setBefore(payloadText);
      setError(null);
    },
    onError: (e) => {
      setResult(null);
      setError(toErrorMessage(e));
    },
  });

  const resetSample = () => {
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
            {result.statements.map((s) => (
              <li key={s.index}>
                statement {s.index}: {s.matched} match
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
