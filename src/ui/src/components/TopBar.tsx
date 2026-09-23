import { useState } from "react";
import { Link } from "react-router";
import { DEFAULT_DATASET, DEFAULT_TENANT } from "../api/http";
import type { WhoamiResponse } from "../api/session";
import { UserMenu } from "../features/shell/UserMenu";
import { crossSignalSearch, type ExploreState } from "../lib/urlState";
import "../features/shell/TopBar.css";

interface Props {
  state: ExploreState;
  update: (patch: Partial<ExploreState>) => void;
  /** The tenant-scoped whoami query's data, `undefined` while it's pending
   * or disabled (no tenant yet) — passed in rather than fetched here so
   * this stays presentational; see `TopBarContainer` for the live query. */
  who: WhoamiResponse | undefined;
  /** Whether the signed-in user can reach tenant-admin surfaces. */
  canManage: boolean;
  /** Whether the signed-in user is the `[demo]` read-only account. */
  isDemo: boolean;
}

export function TopBar({ state, update, who, canManage, isDemo }: Props) {
  return (
    <header className="topbar">
      <Link className="topbar-mark" to={`/logs${crossSignalSearch(state)}`}>
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
      </Link>
      <span className="topbar-sep">/</span>
      <TenantSelector state={state} update={update} who={who} />
      {isDemo && (
        <span className="demo-badge" title="Read-only public demo account">
          Demo · read-only
        </span>
      )}
      <span style={{ flex: 1 }} />
      {canManage && (
        <Link className="manage-trigger" to="/manage">
          Manage
        </Link>
      )}
      <UserMenu state={state} />
    </header>
  );
}

interface TenantSelectorProps {
  state: ExploreState;
  update: (patch: Partial<ExploreState>) => void;
  who: WhoamiResponse | undefined;
}

function TenantSelector({ state, update, who }: TenantSelectorProps) {
  const [editing, setEditing] = useState(false);
  // The server knows which tenant the session/key belongs to and which
  // datasets exist; when available (issue #771) the tenant becomes
  // read-only and the dataset a proper selector. On any failure (older
  // server without the endpoint, unauthenticated) the free-text form
  // remains as the fallback.

  const effectiveTenant = state.tenant || who?.tenant.id || DEFAULT_TENANT;
  const effectiveDataset =
    state.dataset || who?.default_dataset || DEFAULT_DATASET;

  if (!editing) {
    return (
      <button
        className="tenant-chip chip"
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
    const hasTenantChoice = who.memberships.length > 1;
    return (
      <form
        className="tenant-form"
        onSubmit={(e) => {
          e.preventDefault();
          const data = new FormData(e.currentTarget);
          const tenant = hasTenantChoice
            ? String(data.get("tenant") ?? who.tenant.id)
            : who.tenant.id;
          // The dataset select still lists the *previous* tenant's datasets
          // (switching tenants doesn't requery them here), so a tenant
          // change resets the dataset rather than submitting a stale pick.
          update({
            tenant,
            dataset:
              tenant === who.tenant.id ? String(data.get("dataset") ?? "") : "",
          });
          setEditing(false);
        }}
      >
        {hasTenantChoice ? (
          <select
            name="tenant"
            aria-label="Tenant"
            defaultValue={who.tenant.id}
            autoFocus
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
          autoFocus={!hasTenantChoice}
        >
          {who.datasets.map((d) => (
            <option key={d.id} value={d.id}>
              {d.is_default ? `${d.id} (default)` : d.id}
            </option>
          ))}
        </select>
        <button type="submit" className="btn btn-primary">
          Apply
        </button>
        <button type="button" className="btn" onClick={() => setEditing(false)}>
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
      <button type="submit" className="btn btn-primary">
        Apply
      </button>
      <button type="button" className="btn" onClick={() => setEditing(false)}>
        Cancel
      </button>
    </form>
  );
}
