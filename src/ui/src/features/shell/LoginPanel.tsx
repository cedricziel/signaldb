// Session login for embedded deployments: when any query fails with 401
// the gate overlays a minimal form that POSTs /ui/session; on success the
// React Query cache is invalidated so every view retries with the new
// session cookie.

import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { isAuthError } from "../../api/http";
import { createSession } from "../../api/session";
import "./LoginPanel.css";

export interface LoginResult {
  tenant: string;
  dataset: string;
}

interface GateProps {
  /** Initial values for the form (usually the current URL state). */
  tenant?: string;
  dataset?: string;
  /** Called after a successful login, before queries are retried. */
  onLoggedIn?: (result: LoginResult) => void;
}

/**
 * Watches the query cache for 401 failures and shows the login panel when
 * one occurs. Successful login hides the panel and refetches everything.
 */
export function LoginGate({ tenant, dataset, onLoggedIn }: GateProps) {
  const client = useQueryClient();
  const [needsLogin, setNeedsLogin] = useState(false);

  useEffect(
    () =>
      client.getQueryCache().subscribe((event) => {
        if (isAuthError(event.query.state.error)) {
          setNeedsLogin(true);
        }
      }),
    [client],
  );

  if (!needsLogin) return null;

  return (
    <LoginPanel
      tenant={tenant}
      dataset={dataset}
      onSuccess={(result) => {
        setNeedsLogin(false);
        onLoggedIn?.(result);
        // Retry everything with the fresh session cookie.
        void client.invalidateQueries();
      }}
    />
  );
}

interface PanelProps {
  tenant?: string;
  dataset?: string;
  onSuccess: (result: LoginResult) => void;
}

export function LoginPanel({ tenant, dataset, onSuccess }: PanelProps) {
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  return (
    <div className="login-backdrop" role="dialog" aria-label="Sign in">
      <form
        className="login-panel"
        onSubmit={(e) => {
          e.preventDefault();
          const data = new FormData(e.currentTarget);
          const creds = {
            apiKey: String(data.get("api_key") ?? ""),
            tenant: String(data.get("tenant") ?? "").trim(),
            dataset: String(data.get("dataset") ?? "").trim() || undefined,
          };
          setBusy(true);
          setError(null);
          createSession(creds)
            .then(() =>
              onSuccess({ tenant: creds.tenant, dataset: creds.dataset ?? "" }),
            )
            .catch((err: unknown) => {
              setError(err instanceof Error ? err.message : String(err));
            })
            .finally(() => setBusy(false));
        }}
      >
        <h2>Sign in to SignalDB</h2>
        <p className="login-hint">
          Queries were rejected as unauthenticated. Enter a tenant API key to
          start a browser session.
        </p>
        <label>
          API key
          <input
            name="api_key"
            type="password"
            aria-label="API key"
            autoComplete="off"
            required
            autoFocus
          />
        </label>
        <label>
          Tenant
          <input
            name="tenant"
            aria-label="Login tenant"
            defaultValue={tenant ?? ""}
            required
          />
        </label>
        <label>
          Dataset <span className="login-optional">(optional)</span>
          <input
            name="dataset"
            aria-label="Login dataset"
            defaultValue={dataset ?? ""}
            placeholder="tenant default"
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
