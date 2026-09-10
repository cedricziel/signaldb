// Session login for embedded deployments: when any query fails with 401
// the gate overlays a minimal login dialog offering whatever credentials the
// login-configuration probe reports. Accounts spanning several tenants then
// pick one from their memberships; on success the React Query cache is
// refetched with the new session cookie (after cancelling fetches that
// started without it, whose late 401s would otherwise re-open the gate).

import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { useLocation } from "react-router";
import { isAuthError, setTenantContext } from "../../api/http";
import { useLoginConfig } from "../../lib/useLoginConfig";
import { safeRedirectTarget } from "../../lib/redirectTarget";
import { CHOOSE_TENANT_HINT, useTenantStep } from "../../lib/tenantResolution";
import { Dialog } from "../../components/Dialog";
import { LoginCard } from "./LoginCard";
import { LoginMethods } from "./LoginMethods";
import { TenantPicker } from "./TenantPicker";
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
      hint="Your session has expired. Sign in to continue."
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
  /** Copy shown above the credential controls; the caller names why a
   * login is required (session expiry, authorizing a client, ...). */
  hint: string;
  /** Same-app path the SSO control should return to. Defaults to the
   * current location (the page being gated) — pass it explicitly only when
   * that default is wrong. */
  redirect?: string;
  onSuccess: (result: LoginResult) => void;
}

export function LoginPanel({ hint, redirect, onSuccess }: PanelProps) {
  const config = useLoginConfig();
  const location = useLocation();
  // The same-app validation the shared helper applies everywhere else a
  // redirect target is derived from the current location (LoginRoute, the
  // app shell) — defence in depth, since `location` is already same-app in
  // practice.
  const ssoRedirect = safeRedirectTarget(
    redirect ?? `${location.pathname}${location.search}`,
  );
  const { pending, onAuthenticated, pick, busy } = useTenantStep(
    (tenant, dataset) => onSuccess({ tenant, dataset }),
  );

  if (pending) {
    return (
      <Dialog label="Choose tenant" className="login-panel" layer="system">
        <LoginCard title="Choose a tenant" hint={CHOOSE_TENANT_HINT}>
          <TenantPicker memberships={pending} onPicked={pick} busy={busy} />
        </LoginCard>
      </Dialog>
    );
  }

  return (
    <Dialog label="Sign in" className="login-panel" layer="system">
      <LoginCard title="Sign in">
        <LoginMethods
          config={config}
          redirect={ssoRedirect}
          onAuthenticated={onAuthenticated}
          hint={hint}
        />
      </LoginCard>
    </Dialog>
  );
}
