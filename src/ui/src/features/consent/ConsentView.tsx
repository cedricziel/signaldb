// OAuth connector consent screen (change: mcp-oauth-dcr). Reached at
// `/oauth/consent` after the router's `/oauth/authorize` validates the request
// and redirects here with the OAuth parameters in the query string. It shows
// the requesting client, the read scopes it asked for, and a picker of the
// tenants the signed-in user may grant — then posts the decision and navigates
// to the URL the server returns (the client's redirect URI carrying the code
// or an error).

import { useEffect, useMemo, useState } from "react";
import { isAuthError } from "../../api/http";
import {
  type ConsentContextResponse,
  consentContext,
  submitConsentDecision,
} from "../../api/consent";
import { LoginPanel } from "../shell/LoginPanel";
import "../shell/LoginPanel.css";
import "./consent.css";

/** The read scopes SignalDB grants by default when a client requests none. */
const DEFAULT_READ_SCOPES = ["traces:read", "logs:read", "metrics:read"];

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

export function ConsentView() {
  // The query string is fixed for the page's lifetime; memoize so the effect
  // below doesn't re-run on every render (a fresh object would loop).
  const params = useMemo(() => readParams(window.location.search), []);
  const [context, setContext] = useState<ConsentContextResponse | null>(null);
  const [needsLogin, setNeedsLogin] = useState(false);
  const [selectedTenant, setSelectedTenant] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    // While login is required, wait — the LoginPanel's onSuccess flips
    // needsLogin back to false, re-running this effect to re-fetch.
    if (!params || needsLogin) return;
    let cancelled = false;
    consentContext(params.clientId)
      .then((ctx) => {
        if (cancelled) return;
        setContext(ctx);
        setSelectedTenant(ctx.tenants[0]?.id ?? null);
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        if (isAuthError(err)) {
          setNeedsLogin(true);
        } else {
          setError(err instanceof Error ? err.message : String(err));
        }
      });
    return () => {
      cancelled = true;
    };
  }, [params, needsLogin]);

  if (!params) {
    return (
      <div className="login-backdrop">
        <div className="login-panel">
          <h2>Invalid authorization request</h2>
          <p className="login-hint">
            This page is missing required OAuth parameters. Start the connection
            again from your client (Claude or ChatGPT).
          </p>
        </div>
      </div>
    );
  }

  if (needsLogin) {
    return (
      <LoginPanel
        onSuccess={() => {
          // Session established; re-fetch the consent context.
          setNeedsLogin(false);
        }}
      />
    );
  }

  if (!context) {
    return (
      <div className="login-backdrop">
        <div className="login-panel">
          <p className="login-hint">Loading…</p>
          {error && (
            <p className="login-error" role="alert">
              {error}
            </p>
          )}
        </div>
      </div>
    );
  }

  const clientLabel = context.client_name?.trim() || "An application";
  const scopes = requestedScopes(params.scope);

  const decide = (approved: boolean) => {
    if (approved && !selectedTenant) {
      setError("Choose a tenant to grant access to.");
      return;
    }
    setBusy(true);
    setError(null);
    submitConsentDecision({
      client_id: params.clientId,
      redirect_uri: params.redirectUri,
      code_challenge: params.codeChallenge,
      code_challenge_method: params.codeChallengeMethod,
      scope: params.scope ?? undefined,
      state: params.state ?? undefined,
      resource: params.resource ?? undefined,
      tenant: selectedTenant ?? "",
      approved,
    })
      .then((redirect) => {
        window.location.href = redirect;
      })
      .catch((err: unknown) => {
        setBusy(false);
        setError(err instanceof Error ? err.message : String(err));
      });
  };

  const single = context.tenants.length === 1 ? context.tenants[0] : null;

  return (
    <div className="login-backdrop" role="dialog" aria-label="Authorize access">
      <div className="login-panel consent-panel">
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
            <div className="consent-single">
              <span className="consent-tenant-name">{single.id}</span>
              <span className="consent-tenant-meta">{single.role}</span>
            </div>
          ) : (
            <ul className="consent-tenants">
              {context.tenants.map((t) => (
                <li key={t.id}>
                  <label>
                    <input
                      type="radio"
                      name="tenant"
                      value={t.id}
                      checked={selectedTenant === t.id}
                      onChange={() => setSelectedTenant(t.id)}
                    />
                    <span className="consent-tenant-name">{t.id}</span>
                    <span className="consent-tenant-meta">{t.role}</span>
                  </label>
                </li>
              ))}
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
            disabled={busy || context.tenants.length === 0}
            onClick={() => decide(true)}
          >
            {busy ? "Authorizing…" : "Authorize"}
          </button>
        </div>

        <p className="consent-foot">
          Read-only access to one tenant. You can revoke it anytime.
        </p>
      </div>
    </div>
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
