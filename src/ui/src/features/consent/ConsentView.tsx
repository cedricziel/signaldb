// OAuth connector consent screen (change: mcp-oauth-dcr). Reached at
// `/oauth/consent` after the router's `/oauth/authorize` validates the request
// and redirects here with the OAuth parameters in the query string. It shows
// the requesting client, the read scopes it asked for, and a picker of the
// tenants the signed-in user may grant — then posts the decision and navigates
// to the URL the server returns (the client's redirect URI carrying the code
// or an error).

import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router";
import { isAuthError, toErrorMessage } from "../../api/http";
import {
  type ConsentContextResponse,
  type ConsentTenant,
  type ConsentTenantGrant,
  consentContext,
  submitConsentDecision,
} from "../../api/consent";
import { Dialog } from "../../components/Dialog";
import { loginRedirectPath } from "../../lib/redirectTarget";
import "../shell/LoginPanel.css";
import "./consent.css";

/** The read scopes SignalDB grants by default when a client requests none. */
const DEFAULT_READ_SCOPES = [
  "traces:read",
  "logs:read",
  "metrics:read",
  "profiles:read",
];

interface AuthorizeParams {
  clientId: string;
  redirectUri: string;
  codeChallenge: string;
  codeChallengeMethod: string;
  scope: string | null;
  state: string | null;
  resource: string | null;
}

function readParams(search: string): AuthorizeParams | null {
  const q = new URLSearchParams(search);
  const clientId = q.get("client_id");
  const redirectUri = q.get("redirect_uri");
  const codeChallenge = q.get("code_challenge");
  if (!clientId || !redirectUri || !codeChallenge) return null;
  return {
    clientId,
    redirectUri,
    codeChallenge,
    codeChallengeMethod: q.get("code_challenge_method") ?? "S256",
    scope: q.get("scope"),
    state: q.get("state"),
    resource: q.get("resource"),
  };
}

/** The read scopes to display, from the request's `scope` (read scopes only)
 * or the default set when none were requested. */
function requestedScopes(scope: string | null): string[] {
  if (!scope) return DEFAULT_READ_SCOPES;
  const read = scope
    .split(/\s+/)
    .filter((s) => DEFAULT_READ_SCOPES.includes(s));
  return read.length > 0 ? read : DEFAULT_READ_SCOPES;
}

/** Per-tenant consent state (D6): each tenant is independently checked, with
 * its own all-datasets/only-these-datasets sub-choice. Keyed by tenant id. */
interface TenantSelection {
  checked: boolean;
  datasetMode: "all" | "only";
  selectedDatasetIds: string[];
}

const UNRESTRICTED: TenantSelection = {
  checked: false,
  datasetMode: "all",
  selectedDatasetIds: [],
};

/** A lone tenant has no checkbox and is implicitly included (kept from the
 * single-tenant shortcut this component already had); with several tenants,
 * nothing is pre-checked so "at least one tenant" stays an explicit choice. */
function initialSelections(
  tenants: ConsentTenant[],
): Record<string, TenantSelection> {
  const soleTenant = tenants.length === 1;
  return Object.fromEntries(
    tenants.map((t) => [t.id, { ...UNRESTRICTED, checked: soleTenant }]),
  );
}

export function ConsentView() {
  // The query string is fixed for the page's lifetime; memoize so the effect
  // below doesn't re-run on every render (a fresh object would loop).
  const params = useMemo(() => readParams(window.location.search), []);
  const navigate = useNavigate();
  const [context, setContext] = useState<ConsentContextResponse | null>(null);
  const [selections, setSelections] = useState<Record<string, TenantSelection>>(
    {},
  );
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const setTenantSelection = (
    tenantId: string,
    update: Partial<TenantSelection>,
  ) => {
    setSelections((prev) => ({
      ...prev,
      [tenantId]: { ...(prev[tenantId] ?? UNRESTRICTED), ...update },
    }));
  };

  const toggleTenant = (tenantId: string, checked: boolean) => {
    // Unchecking discards the dataset restriction so re-checking later starts
    // fresh at "all datasets" rather than silently carrying over.
    setTenantSelection(tenantId, checked ? { checked } : { ...UNRESTRICTED });
  };

  useEffect(() => {
    if (!params) return;
    let cancelled = false;
    consentContext(params.clientId)
      .then((ctx) => {
        if (cancelled) return;
        setContext(ctx);
        setSelections(initialSelections(ctx.tenants));
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        if (isAuthError(err)) {
          navigate(
            loginRedirectPath(
              `${window.location.pathname}${window.location.search}`,
            ),
            { replace: true },
          );
        } else {
          setError(toErrorMessage(err));
        }
      });
    return () => {
      cancelled = true;
    };
  }, [params, navigate]);

  if (!params) {
    return (
      <Dialog label="Invalid authorization request" className="login-panel">
        <h2>Invalid authorization request</h2>
        <p className="login-hint">
          This page is missing required OAuth parameters. Start the connection
          again from your client (Claude or ChatGPT).
        </p>
      </Dialog>
    );
  }

  if (!context) {
    return (
      <Dialog label="Authorize access" className="login-panel">
        <p className="login-hint">Loading…</p>
        {error && (
          <p className="login-error" role="alert">
            {error}
          </p>
        )}
      </Dialog>
    );
  }

  const clientLabel = context.client_name?.trim() || "An application";
  const scopes = requestedScopes(params.scope);

  const checkedTenants = context.tenants
    .map((t) => ({ tenant: t, sel: selections[t.id] ?? UNRESTRICTED }))
    .filter(({ sel }) => sel.checked);
  // "All datasets" is always a valid submission for a checked tenant; "only
  // these" needs at least one box checked (D5) — never sent as an empty
  // array (D1a). At least one tenant must be checked overall.
  const incompleteRestriction = checkedTenants.some(
    ({ sel }) =>
      sel.datasetMode === "only" && sel.selectedDatasetIds.length === 0,
  );
  const canSubmit = checkedTenants.length > 0 && !incompleteRestriction;

  const decide = (approved: boolean) => {
    if (approved && checkedTenants.length === 0) {
      setError("Choose at least one tenant to grant access to.");
      return;
    }
    if (approved && incompleteRestriction) {
      // Redundant with the disabled submit button below (D5) — the server
      // also rejects an empty array (D1a), but this avoids a round trip.
      setError("Choose at least one dataset, or switch to all datasets.");
      return;
    }
    setBusy(true);
    setError(null);
    const tenantGrants: ConsentTenantGrant[] = checkedTenants.map(
      ({ tenant, sel }) =>
        sel.datasetMode === "only"
          ? { tenant_id: tenant.id, dataset_ids: sel.selectedDatasetIds }
          : { tenant_id: tenant.id },
    );
    submitConsentDecision({
      client_id: params.clientId,
      redirect_uri: params.redirectUri,
      code_challenge: params.codeChallenge,
      code_challenge_method: params.codeChallengeMethod,
      scope: params.scope ?? undefined,
      state: params.state ?? undefined,
      resource: params.resource ?? undefined,
      tenant_grants: tenantGrants,
      approved,
    })
      .then((redirect) => {
        window.location.href = redirect;
      })
      .catch((err: unknown) => {
        setBusy(false);
        setError(toErrorMessage(err));
      });
  };

  const single = context.tenants.length === 1 ? context.tenants[0] : null;

  /** The per-tenant all-datasets/only-these-datasets sub-choice (D6), nested
   * under a checked tenant's row (or the lone tenant's label). */
  const datasetPicker = (tenant: ConsentTenant) => {
    const sel = selections[tenant.id] ?? UNRESTRICTED;
    return (
      <div className="consent-field consent-tenant-datasets">
        <span className="consent-field-label">Access</span>
        <ul className="consent-dataset-modes">
          <li>
            <label>
              <input
                type="radio"
                name={`dataset-mode-${tenant.id}`}
                checked={sel.datasetMode === "all"}
                onChange={() =>
                  setTenantSelection(tenant.id, { datasetMode: "all" })
                }
              />
              <span>All datasets in {tenant.id}</span>
            </label>
          </li>
          <li>
            <label>
              <input
                type="radio"
                name={`dataset-mode-${tenant.id}`}
                checked={sel.datasetMode === "only"}
                onChange={() =>
                  setTenantSelection(tenant.id, { datasetMode: "only" })
                }
              />
              <span>Only these datasets in {tenant.id}:</span>
            </label>
          </li>
        </ul>
        {sel.datasetMode === "only" && (
          <ul className="consent-datasets">
            {tenant.datasets.map((dataset) => (
              <li key={dataset.id}>
                <label>
                  <input
                    type="checkbox"
                    checked={sel.selectedDatasetIds.includes(dataset.id)}
                    aria-label={`${dataset.name} in ${tenant.id}`}
                    onChange={(event) =>
                      setTenantSelection(tenant.id, {
                        selectedDatasetIds: event.target.checked
                          ? [...sel.selectedDatasetIds, dataset.id]
                          : sel.selectedDatasetIds.filter(
                              (id) => id !== dataset.id,
                            ),
                      })
                    }
                  />
                  <span>{dataset.name}</span>
                </label>
              </li>
            ))}
          </ul>
        )}
      </div>
    );
  };

  return (
    <Dialog label="Authorize access" className="login-panel consent-panel">
      <div className="consent-header">
        <span className="consent-badge" aria-hidden="true">
          <ShieldIcon />
        </span>
        <h2>Authorize {clientLabel}</h2>
        <p className="consent-sub">
          It's asking to read your observability data in SignalDB.
        </p>
      </div>

      <ul className="consent-perms">
        {scopes.map((s) => (
          <li key={s}>
            <EyeIcon />
            <span>{scopeLabel(s)}</span>
          </li>
        ))}
      </ul>

      <div className="consent-field">
        <span className="consent-field-label">
          {single ? "Tenant" : "Grant access to"}
        </span>
        {context.tenants.length === 0 ? (
          <p className="consent-empty">
            You aren't a member of any tenant, so there's nothing to grant.
          </p>
        ) : single ? (
          <>
            <div className="consent-single">
              <span className="consent-tenant-name">{single.id}</span>
              <span className="consent-tenant-meta">{single.role}</span>
            </div>
            {datasetPicker(single)}
          </>
        ) : (
          <ul className="consent-tenants">
            {context.tenants.map((t) => {
              const checked = selections[t.id]?.checked ?? false;
              return (
                <li key={t.id}>
                  <label>
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={(event) =>
                        toggleTenant(t.id, event.target.checked)
                      }
                    />
                    <span className="consent-tenant-name">{t.id}</span>
                    <span className="consent-tenant-meta">{t.role}</span>
                  </label>
                  {checked && datasetPicker(t)}
                </li>
              );
            })}
          </ul>
        )}
      </div>

      {error && (
        <p className="login-error" role="alert">
          {error}
        </p>
      )}

      <div className="consent-actions">
        <button
          type="button"
          className="consent-deny"
          disabled={busy}
          onClick={() => decide(false)}
        >
          Deny
        </button>
        <button
          type="button"
          className="consent-approve"
          disabled={busy || !canSubmit}
          onClick={() => decide(true)}
        >
          {busy ? "Authorizing…" : "Authorize"}
        </button>
      </div>

      <p className="consent-foot">
        Read-only access to the tenants you choose. You can revoke it anytime.
      </p>
    </Dialog>
  );
}

function scopeLabel(scope: string): string {
  switch (scope) {
    case "traces:read":
      return "Read traces";
    case "logs:read":
      return "Read logs";
    case "metrics:read":
      return "Read metrics";
    case "profiles:read":
      return "Read profiles";
    default:
      return scope;
  }
}

function ShieldIcon() {
  return (
    <svg
      width="22"
      height="22"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M12 2 4 5v6c0 5 3.4 8.5 8 11 4.6-2.5 8-6 8-11V5l-8-3Z" />
      <path d="m9 12 2 2 4-4" />
    </svg>
  );
}

function EyeIcon() {
  return (
    <svg
      width="15"
      height="15"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7-10-7-10-7Z" />
      <circle cx="12" cy="12" r="3" />
    </svg>
  );
}
