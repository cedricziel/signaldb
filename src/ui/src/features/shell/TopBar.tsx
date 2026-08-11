import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link } from "react-router";
import { DEFAULT_DATASET, DEFAULT_TENANT } from "../../api/http";
import { whoami } from "../../api/session";
import type { ExploreState } from "../../lib/urlState";
import { UserMenu } from "./UserMenu";
import "./TopBar.css";

interface Props {
  state: ExploreState;
  update: (patch: Partial<ExploreState>) => void;
}

export function TopBar({ state, update }: Props) {
  const { data: who } = useQuery({
    queryKey: ["whoami", state.tenant, state.dataset],
    queryFn: () => whoami(),
    staleTime: 60_000,
    retry: false,
  });
  const role = who?.memberships.find(
    (membership) => membership.tenant_id === who.tenant.id,
  )?.role;
  const canManage = who?.user?.is_instance_admin || role === "admin";
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
      <span style={{ flex: 1 }} />
      {canManage && (
        <Link className="manage-trigger" to="/manage">
          Manage
        </Link>
      )}
      <UserMenu state={state} update={update} />
    </header>
  );
}

function TenantSelector({ state, update }: Props) {
  const [editing, setEditing] = useState(false);
  // The server knows which tenant the session/key belongs to and which
  // datasets exist; when available (issue #771) the tenant becomes
  // read-only and the dataset a proper selector. On any failure (older
  // server without the endpoint, unauthenticated) the free-text form
  // remains as the fallback.
  const { data: who } = useQuery({
    queryKey: ["whoami", state.tenant, state.dataset],
    queryFn: () => whoami(),
    staleTime: 60_000,
    retry: false,
  });

  const effectiveTenant = state.tenant || who?.tenant.id || DEFAULT_TENANT;
  const effectiveDataset =
    state.dataset || who?.default_dataset || DEFAULT_DATASET;

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

  if (who) {
    return (
      <form
        className="tenant-form"
        onSubmit={(e) => {
          e.preventDefault();
          const data = new FormData(e.currentTarget);
          update({
            tenant: who.tenant.id,
            dataset: String(data.get("dataset") ?? ""),
          });
          setEditing(false);
        }}
      >
        {who.memberships.length > 1 ? (
          <select
            name="tenant"
            aria-label="Tenant"
            defaultValue={who.tenant.id}
            onChange={(event) => {
              update({ tenant: event.target.value, dataset: "" });
              setEditing(false);
            }}
          >
            {who.memberships.map((membership) => (
              <option key={membership.tenant_id} value={membership.tenant_id}>
                {membership.tenant_id} ({membership.role})
              </option>
            ))}
          </select>
        ) : (
          <span className="tenant-fixed" title={who.tenant.name}>
            {who.tenant.id}
          </span>
        )}
        <select
          name="dataset"
          aria-label="Dataset"
          defaultValue={state.dataset || who.default_dataset || ""}
          autoFocus
        >
          {who.datasets.map((d) => (
            <option key={d.id} value={d.id}>
              {d.is_default ? `${d.id} (default)` : d.id}
            </option>
          ))}
        </select>
        <button type="submit">Apply</button>
        <button type="button" onClick={() => setEditing(false)}>
          Cancel
        </button>
      </form>
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
