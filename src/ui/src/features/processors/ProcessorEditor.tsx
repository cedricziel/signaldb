import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { Link, Navigate, useNavigate, useParams } from "react-router";
import {
  createProcessor,
  getProcessor,
  replaceProcessor,
  validateProcessor,
  type ProcessorSpec,
  type StatementError,
} from "./api";
import { PROCESSORS } from "./paths";
import { TestPanel } from "./TestPanel";
import { useProcessorsSession } from "./useProcessorsSession";
import { toErrorMessage } from "../../api/http";

const SIGNALS = ["traces", "logs", "metrics"] as const;
type Signal = (typeof SIGNALS)[number];
const ERROR_MODES = ["ignore", "silent", "propagate"] as const;
const ALL_DATASETS = "";

/**
 * `/processors/new` and `/processors/:name/edit` — create/edit a tenant
 * processor: name, description, signal, dataset (tenant datasets + "all
 * datasets"), enabled, priority, error mode, and one OTTL statement per
 * line. Validates on blur/explicit action, annotating failing lines; Save
 * is disabled while errors remain. Includes the dry-run test panel.
 */
export function ProcessorEditor() {
  const { name } = useParams<{ name?: string }>();
  const { isTenantAdmin, isLoading, tenant, datasets } =
    useProcessorsSession();
  const editing = name !== undefined;
  const stored = useQuery({
    queryKey: ["processors", tenant, name],
    queryFn: () => getProcessor(name!),
    enabled: editing && isTenantAdmin,
    staleTime: 30_000,
  });

  if (isLoading) return null;
  if (!isTenantAdmin) return <Navigate to={PROCESSORS} replace />;
  if (!editing) return <EditorForm key="new" datasets={datasets} />;

  if (stored.isPending) return <p className="processors-note">Loading…</p>;
  if (stored.isError) {
    return (
      <div className="processors-page">
        <p className="error-text" role="alert">
          Could not load {name}: {toErrorMessage(stored.error)}
        </p>
        <Link to={PROCESSORS}>Back to processors</Link>
      </div>
    );
  }
  return (
    <EditorForm key={name} datasets={datasets} stored={stored.data} />
  );
}

function EditorForm({
  datasets,
  stored,
}: {
  datasets: { id: string }[];
  stored?: {
    name: string;
    description?: string | null;
    signal: string;
    dataset?: string | null;
    enabled: boolean;
    priority: number;
    error_mode: string;
    statements: string[];
  };
}) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [name, setName] = useState(stored?.name ?? "");
  const [description, setDescription] = useState(stored?.description ?? "");
  const [signal, setSignal] = useState<Signal>(
    (stored?.signal as Signal) ?? "traces",
  );
  const [dataset, setDataset] = useState(stored?.dataset ?? ALL_DATASETS);
  const [enabled, setEnabled] = useState(stored?.enabled ?? true);
  const [priority, setPriority] = useState(stored?.priority ?? 100);
  const [errorMode, setErrorMode] = useState(stored?.error_mode ?? "ignore");
  const [statementsText, setStatementsText] = useState(
    (stored?.statements ?? []).join("\n"),
  );
  const [errors, setErrors] = useState<StatementError[]>([]);
  const [validatedText, setValidatedText] = useState<string | null>(null);
  const [appliesWithin, setAppliesWithin] = useState<number | null>(null);
  const [saveError, setSaveError] = useState<string | null>(null);

  const statementLines = useMemo(
    () => statementsText.split("\n").filter((line) => line.trim() !== ""),
    [statementsText],
  );

  const validation = useMutation({
    mutationFn: () => validateProcessor(signal, statementLines),
    onSuccess: (result) => {
      setErrors(result);
      setValidatedText(statementsText);
    },
    onError: (e) => setSaveError(toErrorMessage(e)),
  });

  const statementsCurrent = validatedText === statementsText;
  const isValidated = statementsCurrent && errors.length === 0;

  const buildSpec = (): ProcessorSpec => ({
    name,
    description: description.trim() === "" ? null : description,
    signal,
    dataset: dataset === ALL_DATASETS ? null : dataset,
    enabled,
    priority,
    error_mode: errorMode,
    statements: statementLines,
  });

  const save = useMutation({
    mutationFn: async () => {
      const spec = buildSpec();
      return stored
        ? replaceProcessor(stored.name, spec)
        : createProcessor(spec);
    },
    onSuccess: (response) => {
      setAppliesWithin(response.applies_within_seconds);
      setSaveError(null);
      void queryClient.invalidateQueries({ queryKey: ["processors"] });
      if (!stored) navigate(PROCESSORS);
    },
    onError: (e) => setSaveError(toErrorMessage(e)),
  });

  const errorForLine = (index: number): StatementError | undefined =>
    statementsCurrent
      ? errors.find((e) => e.statement === index)
      : undefined;

  const canSave = isValidated && name.trim() !== "" && !save.isPending;

  return (
    <div className="processors-page">
      <h1 className="processors-title">
        {stored ? `Edit ${stored.name}` : "New processor"}
      </h1>

      <label htmlFor="processor-name">Name</label>
      <input
        id="processor-name"
        value={name}
        onChange={(e) => setName(e.target.value)}
        disabled={!!stored}
      />

      <label htmlFor="processor-description">Description</label>
      <input
        id="processor-description"
        value={description}
        onChange={(e) => setDescription(e.target.value)}
      />

      <label htmlFor="processor-signal">Signal</label>
      <select
        id="processor-signal"
        value={signal}
        onChange={(e) => setSignal(e.target.value as Signal)}
      >
        {SIGNALS.map((s) => (
          <option key={s} value={s}>
            {s}
          </option>
        ))}
      </select>

      <label htmlFor="processor-dataset">Dataset</label>
      <select
        id="processor-dataset"
        value={dataset}
        onChange={(e) => setDataset(e.target.value)}
      >
        <option value={ALL_DATASETS}>All datasets</option>
        {datasets.map((d) => (
          <option key={d.id} value={d.id}>
            {d.id}
          </option>
        ))}
      </select>

      <label htmlFor="processor-enabled">
        <input
          id="processor-enabled"
          type="checkbox"
          checked={enabled}
          onChange={(e) => setEnabled(e.target.checked)}
        />
        Enabled
      </label>

      <label htmlFor="processor-priority">Priority</label>
      <input
        id="processor-priority"
        type="number"
        value={priority}
        onChange={(e) => setPriority(Number(e.target.value))}
      />

      <label htmlFor="processor-error-mode">Error mode</label>
      <select
        id="processor-error-mode"
        value={errorMode}
        onChange={(e) => setErrorMode(e.target.value)}
      >
        {ERROR_MODES.map((m) => (
          <option key={m} value={m}>
            {m}
          </option>
        ))}
      </select>

      <label htmlFor="processor-statements">Statements (one per line)</label>
      <textarea
        id="processor-statements"
        className="processors-statements-textarea"
        value={statementsText}
        onChange={(e) => setStatementsText(e.target.value)}
        onBlur={() => validation.mutate()}
        rows={8}
      />
      <button
        type="button"
        onClick={() => validation.mutate()}
        disabled={validation.isPending}
      >
        Validate
      </button>

      {statementsCurrent && errors.length > 0 && (
        <ul className="processors-line-errors">
          {statementLines.map((_line, index) => {
            const err = errorForLine(index);
            if (!err) return null;
            return (
              <li key={index} className="error-text" role="alert">
                Line {index + 1}
                {err.column != null ? `, column ${err.column}` : ""}:{" "}
                {err.message}
              </li>
            );
          })}
        </ul>
      )}

      <div className="processors-editor-actions">
        <button
          type="button"
          className="btn btn-primary"
          onClick={() => save.mutate()}
          disabled={!canSave}
        >
          Save
        </button>
        <Link to={PROCESSORS}>Cancel</Link>
      </div>

      {saveError && (
        <p className="error-text" role="alert">
          {saveError}
        </p>
      )}
      {appliesWithin != null && (
        <p className="processors-applies-hint">
          Applies within {appliesWithin} seconds.
        </p>
      )}

      <TestPanel
        signal={signal}
        dataset={dataset === ALL_DATASETS ? null : dataset}
        spec={buildSpec()}
      />
    </div>
  );
}
