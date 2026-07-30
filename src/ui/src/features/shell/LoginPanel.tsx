// Session login for embedded deployments: when any query fails with 401
// the gate overlays a minimal email/password form that POSTs /ui/session.
// Accounts spanning several tenants then pick one from their memberships;
// on success the React Query cache is refetched with the new session
// cookie (after cancelling fetches that started without it, whose late
// 401s would otherwise re-open the gate).

import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { isAuthError, setTenantContext } from "../../api/http";
import type { SessionMembership } from "../../api/session";
import { createSession, whoami } from "../../api/session";
import "./LoginPanel.css";

export interface LoginResult {
  tenant: string;
  dataset: string;
}

interface GateProps {
  /** Called after a successful login, before queries are retried. */
  onLoggedIn?: (result: LoginResult) => void;
}

/**
 * Watches the query cache for 401 failures and shows the login panel when
 * one occurs. Successful login hides the panel and refetches everything.
 */
export function LoginGate({ onLoggedIn }: GateProps) {
  const client = useQueryClient();
  const [needsLogin, setNeedsLogin] = useState(false);

  useEffect(
    () =>
      client.getQueryCache().subscribe((event) => {
        // React only to freshly-settled 401s. Cache events for
        // invalidation and refetch still carry the query's stale error
        // state, which must not re-open the panel after a login.
        if (
          event.type === "updated" &&
          event.action.type === "error" &&
          isAuthError(event.action.error)
        ) {
          setNeedsLogin(true);
        }
      }),
    [client],
  );

  if (!needsLogin) return null;

  return (
    <LoginPanel
      onSuccess={(result) => {
        setNeedsLogin(false);
        // Make the resolved tenant visible to fetches immediately: the
        // URL-state update from onLoggedIn only reaches the tenant
        // context on the next render, and a refetch under the stale
        // (empty) context earns a fresh 401 that re-opens the gate.
        setTenantContext(result);
        onLoggedIn?.(result);
        // Abort fetches that started before the session cookie existed —
        // their 401s would land late and re-open the gate — then retry
        // everything with the fresh session.
        void client.cancelQueries().then(() => client.invalidateQueries());
      }}
    />
  );
}

interface PanelProps {
  onSuccess: (result: LoginResult) => void;
}

export function LoginPanel({ onSuccess }: PanelProps) {
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [choices, setChoices] = useState<SessionMembership[] | null>(null);

  const finishWithTenant = (tenant: string) => {
    setBusy(true);
    setError(null);
    // Resolve the tenant's default dataset; the session cookie is already
    // set, so a failure here only means starting without a dataset.
    whoami(tenant)
      .then((info) =>
        onSuccess({ tenant, dataset: info.default_dataset ?? "" }),
      )
      .catch(() => onSuccess({ tenant, dataset: "" }))
      .finally(() => setBusy(false));
  };

  if (choices) {
    return (
      <div className="login-backdrop" role="dialog" aria-label="Choose tenant">
        <div className="login-panel">
          <h2>Choose a tenant</h2>
          <p className="login-hint">
            Your account belongs to several tenants. Pick the one to explore —
            you can switch later from the top bar.
          </p>
          <ul className="login-tenants">
            {choices.map((membership) => (
              <li key={membership.tenant_id}>
                <button
                  type="button"
                  disabled={busy}
                  onClick={() => finishWithTenant(membership.tenant_id)}
                >
                  <span className="login-tenant-name">{membership.name}</span>
                  <span className="login-tenant-meta">
                    {membership.tenant_id} · {membership.role}
                  </span>
                </button>
              </li>
            ))}
          </ul>
          {error && (
            <p className="login-error" role="alert">
              {error}
            </p>
          )}
        </div>
      </div>
    );
  }

  return (
    <div className="login-backdrop" role="dialog" aria-label="Sign in">
      <form
        className="login-panel"
        onSubmit={(e) => {
          e.preventDefault();
          const data = new FormData(e.currentTarget);
          const creds = {
            email: String(data.get("email") ?? "").trim(),
            password: String(data.get("password") ?? ""),
          };
          setBusy(true);
          setError(null);
          createSession(creds)
            .then((result) => {
              if (result.tenant) {
                onSuccess({
                  tenant: result.tenant,
                  dataset: result.dataset ?? "",
                });
              } else {
                setChoices(result.memberships);
              }
            })
            .catch((err: unknown) => {
              setError(err instanceof Error ? err.message : String(err));
            })
            .finally(() => setBusy(false));
        }}
      >
        <h2>Sign in to SignalDB</h2>
        <p className="login-hint">
          Queries were rejected as unauthenticated. Sign in with your user
          account.
        </p>
        <label>
          Email
          <input
            name="email"
            type="email"
            aria-label="Email"
            autoComplete="username"
            required
            autoFocus
          />
        </label>
        <label>
          Password
          <input
            name="password"
            type="password"
            aria-label="Password"
            autoComplete="current-password"
            required
          />
        </label>
        {error && (
          <p className="login-error" role="alert">
            {error}
          </p>
        )}
        <button type="submit" disabled={busy}>
          {busy ? "Signing in…" : "Sign in"}
        </button>
      </form>
    </div>
  );
}
