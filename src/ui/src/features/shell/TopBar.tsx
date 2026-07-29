import { useState } from "react";
import { DEFAULT_DATASET, DEFAULT_TENANT } from "../../api/http";
import type { ExploreState } from "../../lib/urlState";
import "./TopBar.css";

interface Props {
  state: ExploreState;
  update: (patch: Partial<ExploreState>) => void;
}

export function TopBar({ state, update }: Props) {
  return (
    <header className="topbar">
      <span className="topbar-mark">
        <svg
          width="18"
          height="14"
          viewBox="0 0 18 14"
          fill="none"
          aria-hidden="true"
        >
          <path
            d="M1 7 L4 7 L6 2 L9 12 L12 4 L13.5 7 L17 7"
            stroke="var(--accent)"
            strokeWidth="1.8"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
        signal<b>db</b>
      </span>
      <span className="topbar-sep">/</span>
      <TenantSelector state={state} update={update} />
    </header>
  );
}

function TenantSelector({ state, update }: Props) {
  const [editing, setEditing] = useState(false);
  const effectiveTenant = state.tenant || DEFAULT_TENANT;
  const effectiveDataset = state.dataset || DEFAULT_DATASET;

  if (!editing) {
    return (
      <button
        className="tenant-chip"
        title="Tenant / dataset context for all queries"
        onClick={() => setEditing(true)}
      >
        {effectiveTenant || "tenant"}
        <span className="tenant-sep">·</span>
        {effectiveDataset || "default"}
        <span className="tenant-caret">▾</span>
      </button>
    );
  }

  return (
    <form
      className="tenant-form"
      onSubmit={(e) => {
        e.preventDefault();
        const data = new FormData(e.currentTarget);
        update({
          tenant: String(data.get("tenant") ?? "").trim(),
          dataset: String(data.get("dataset") ?? "").trim(),
        });
        setEditing(false);
      }}
    >
      <input
        name="tenant"
        aria-label="Tenant"
        placeholder="tenant"
        defaultValue={state.tenant || DEFAULT_TENANT}
        autoFocus
      />
      <input
        name="dataset"
        aria-label="Dataset"
        placeholder="default dataset"
        defaultValue={state.dataset || DEFAULT_DATASET}
      />
      <button type="submit">Apply</button>
      <button type="button" onClick={() => setEditing(false)}>
        Cancel
      </button>
    </form>
  );
}
