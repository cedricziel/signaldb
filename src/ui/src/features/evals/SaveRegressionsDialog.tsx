// Compare → "Save N regressed cases as eval set": a new set holding the
// regressed cases' original inputs, expected tools and references, copied
// from the set the runs replayed. The set's cases load when the dialog
// opens.

import { useQueryClient } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { useNavigate } from "react-router";
import { createEvalSet, type EvalCase } from "../../api/evalSets";
import type { ExploreState } from "../../lib/urlState";
import {
  regressionCases,
  saveRegressionsBlocked,
  validSetName,
} from "./evalSetModel";
import { fmtCount } from "./evalFormat";
import { EvalDialog, setHref, WriteError } from "./EvalBits";
import { evalSetKeys, useEvalSet } from "./useEvalData";

const LABEL = "Save regressed cases as eval set";

interface DialogProps {
  state: ExploreState;
  caseIds: string[];
  sourceSet: string;
  candidateTraces: Map<string, string>;
  defaultName: string;
  /** Empty to take the source set's agent. */
  defaultAgent: string;
  description: string;
  onClose: () => void;
}

export function SaveRegressionsDialog(props: DialogProps) {
  const { state, sourceSet, onClose } = props;
  const set = useEvalSet(state, sourceSet);
  if (set.data)
    return (
      <SaveRegressionsForm
        {...props}
        setCases={set.data.cases}
        defaultAgent={props.defaultAgent || set.data.agent}
      />
    );
  const blocked = saveRegressionsBlocked({ sourceSet, loadError: set.error });
  return (
    <EvalDialog
      label={LABEL}
      onClose={onClose}
      footer={
        <>
          <span className="evals-bar-fill" />
          <button type="button" className="btn" onClick={onClose}>
            Cancel
          </button>
        </>
      }
    >
      {blocked ? (
        <div role="alert" className="evals-warn">
          {blocked}.
        </div>
      ) : (
        <div className="evals-note">
          Loading <span className="mono">{sourceSet}</span>…
        </div>
      )}
    </EvalDialog>
  );
}

function SaveRegressionsForm({
  state,
  caseIds,
  sourceSet,
  setCases,
  candidateTraces,
  defaultName,
  defaultAgent,
  description,
  onClose,
}: DialogProps & { setCases: EvalCase[] }) {
  const [name, setName] = useState(defaultName);
  const [agent, setAgent] = useState(defaultAgent);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const { cases, missing } = useMemo(
    () => regressionCases(caseIds, setCases, candidateTraces),
    [caseIds, setCases, candidateTraces],
  );
  const nameOk = validSetName(name);

  async function save() {
    setBusy(true);
    setError(null);
    try {
      await createEvalSet({ name, agent: agent.trim(), description, cases });
      await queryClient.invalidateQueries({ queryKey: evalSetKeys(state).all });
      onClose();
      navigate(setHref(state, name));
    } catch (e) {
      setError(e);
      setBusy(false);
    }
  }

  return (
    <EvalDialog
      label={LABEL}
      onClose={onClose}
      footer={
        <>
          <span className="evals-bar-fill" />
          <button type="button" className="btn" onClick={onClose}>
            Cancel
          </button>
          <button
            type="button"
            className="btn btn-primary"
            disabled={
              busy || !nameOk || agent.trim() === "" || cases.length === 0
            }
            onClick={() => void save()}
          >
            {busy ? "Creating…" : `Create with ${fmtCount(cases.length)} cases`}
          </button>
        </>
      }
    >
      <div className="evals-grid2">
        <label className="evals-field">
          Name
          <input
            className="evals-input"
            value={name}
            aria-invalid={!nameOk}
            onChange={(e) => setName(e.target.value)}
          />
        </label>
        <label className="evals-field">
          Agent
          <input
            className="evals-input"
            value={agent}
            onChange={(e) => setAgent(e.target.value)}
          />
        </label>
      </div>
      <div className="evals-note">
        {fmtCount(cases.length)} cases with their inputs, expected tools and
        references copied from <span className="mono">{sourceSet}</span>. Each
        case links to the candidate's trace where it has one.
      </div>
      {missing.length > 0 && (
        <div role="status" className="evals-warn">
          <b className="mono">{fmtCount(missing.length)}</b> regressed cases
          aren't in {sourceSet} and are left out: {missing.join(", ")}
        </div>
      )}
      {error !== null && <WriteError error={error} />}
    </EvalDialog>
  );
}
