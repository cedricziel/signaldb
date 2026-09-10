// Dedicated, linkable sign-in page at `/login`: brand, a centred card, and a
// footer — outside the explore shell, no modal (design decision 1). The
// landing pad for every credential and every redirect-based login (decision
// 5): it runs the credential → context → target state machine (decision 2),
// shared with LoginPanel via LoginMethods/TenantPicker/the tenant-resolution
// helper.

import { useEffect, useState, type ReactNode } from "react";
import { Navigate, useNavigate, useSearchParams } from "react-router";
import { BrandMark } from "../../components/BrandMark";
import { isAuthError, toErrorMessage } from "../../api/http";
import { deleteSession } from "../../api/session";
import { useLoginConfig } from "../../lib/useLoginConfig";
import { safeRedirectTarget } from "../../lib/redirectTarget";
import { CHOOSE_TENANT_HINT, useTenantStep } from "../../lib/tenantResolution";
import { useCurrentSession } from "../../lib/useWhoami";
import { LoginCard } from "./LoginCard";
import { LoginMethods } from "./LoginMethods";
import { TenantPicker } from "./TenantPicker";
import "./LoginPanel.css";
import "./LoginPage.css";

/** `?error=<code>` messages for a redirect-based login (the OIDC callback,
 * see `openspec/changes/oidc-login`). One small table so that change can add
 * codes without touching the render path below. */
const ERROR_MESSAGES: Record<string, string> = {
  sso_failed:
    "Single sign-on failed. Try again, or sign in with your email and password.",
  no_membership:
    "Your account has no tenant access yet. Ask a tenant admin to add you, then sign in again.",
};

/** `target` with the resolved tenant/dataset appended, as a router path. */
function withTenantDataset(
  target: string,
  tenant: string,
  dataset: string,
): string {
  const url = new URL(target, window.location.origin);
  url.searchParams.set("tenant", tenant);
  url.searchParams.set("dataset", dataset);
  return `${url.pathname}${url.search}`;
}

function LoginPageShell({ children }: { children: ReactNode }) {
  return (
    <main className="login-page">
      <div className="login-main">
        <div className="login-brand">
          <BrandMark />
          <span>SignalDB</span>
        </div>
        <section className="login-card">{children}</section>
      </div>
      <footer className="login-foot">
        <a href="https://signaldb.dev/docs">Docs</a>
      </footer>
    </main>
  );
}

export function LoginRoute() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const target = safeRedirectTarget(searchParams.get("redirect"));

  const [alert] = useState<string | null>(
    () => ERROR_MESSAGES[searchParams.get("error") ?? ""] ?? null,
  );
  // Strip `error` from the URL once read, through the router (not a raw
  // history call) so it doesn't fight React Router's own location state; a
  // reload then doesn't re-report a stale failure. `redirect` (and anything
  // else) survives.
  useEffect(() => {
    if (!searchParams.has("error")) return;
    const next = new URLSearchParams(searchParams);
    next.delete("error");
    setSearchParams(next, { replace: true });
    // Run once on mount to strip a one-time `?error`; re-running on every
    // `searchParams` change would fight the very update this makes.
  }, []);

  const sessionQuery = useCurrentSession();
  const config = useLoginConfig();
  const {
    pending,
    onAuthenticated,
    pick: pickTenant,
    busy: tenantBusy,
  } = useTenantStep((tenant, dataset) =>
    navigate(withTenantDataset(target, tenant, dataset), { replace: true }),
  );

  // The session query is the primary gate: while it's in flight — the
  // first load, or a background refetch of stale/errored data (e.g. a
  // cached 401 from an earlier visit) — render the checking skeleton
  // instead of guessing from data that might be about to change. A locally
  // resolved tenant choice (`pending`, from a password login just now)
  // always wins once it exists — it can't go stale from a session refetch.
  if (!pending && sessionQuery.isFetching && !sessionQuery.isSuccess) {
    return (
      <LoginPageShell>
        <LoginCard as="h1" title="Sign in">
          <p className="login-hint">Checking session…</p>
        </LoginCard>
      </LoginPageShell>
    );
  }

  const session = sessionQuery.isSuccess ? sessionQuery.data : null;

  // Auto-resolved tenant (sole membership, or the SSO landing already named
  // one): go straight to the target.
  if (!pending && session?.tenant) {
    return (
      <Navigate
        to={withTenantDataset(target, session.tenant, session.dataset ?? "")}
        replace
      />
    );
  }

  // No tenant access at all is only a session-query outcome — a password
  // login's response always carries the memberships it authenticated
  // against, so `pending` is never empty in practice.
  if (!pending && session && session.memberships.length === 0) {
    return (
      <LoginPageShell>
        <LoginCard as="h1" title="No tenant access yet">
          <p className="login-hint">
            Your account <strong>{session.user.email}</strong> isn't a member of
            any tenant. Ask a tenant admin to add you, or see the{" "}
            <a href="https://signaldb.dev/docs">bootstrap guide</a> for a new
            instance.
          </p>
          <p className="login-account">
            <button
              type="button"
              onClick={() => {
                void deleteSession().finally(() => {
                  window.location.href = "/login";
                });
              }}
            >
              Sign out
            </button>
          </p>
        </LoginCard>
      </LoginPageShell>
    );
  }

  // Several memberships to choose from, from either credential: a password
  // login just now (`pending`) or the session's own report (an SSO
  // landing). The session's own `tenant` case already returned above, so
  // any remaining `session` here means memberships to choose from.
  const memberships = pending ?? session?.memberships ?? null;
  if (memberships) {
    return (
      <LoginPageShell>
        <LoginCard as="h1" title="Choose a tenant" hint={CHOOSE_TENANT_HINT}>
          <TenantPicker
            memberships={memberships}
            onPicked={pickTenant}
            busy={tenantBusy}
          />
        </LoginCard>
      </LoginPageShell>
    );
  }

  // Credential step: the session query settled to "not signed in" (401) or
  // another error. Only now does the probe matter — wait for it too so the
  // credential controls don't flash a stale "unavailable" fallback before
  // it resolves; the page-level alert above them doesn't depend on it, so
  // it renders immediately.
  const queryErrorMessage =
    sessionQuery.isError && !isAuthError(sessionQuery.error)
      ? toErrorMessage(sessionQuery.error)
      : null;

  return (
    <LoginPageShell>
      <LoginCard as="h1" title="Sign in">
        {(alert ?? queryErrorMessage) && (
          <p className="login-alert" role="alert">
            {alert ?? queryErrorMessage}
          </p>
        )}
        <LoginMethods
          config={config}
          redirect={target}
          onAuthenticated={onAuthenticated}
          hint="Use your account to explore logs, traces and metrics."
        />
      </LoginCard>
    </LoginPageShell>
  );
}
