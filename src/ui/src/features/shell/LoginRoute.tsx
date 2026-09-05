// Dedicated, linkable login screen at `/login`. Unlike LoginGate (which pops
// up reactively on a 401 mid-session), this is a standalone destination:
// sign-out lands here, and it accepts a `?redirect=` target to return to
// after a successful login. Reuses LoginPanel for the actual form/tenant
// picker rather than duplicating it.

import { Navigate, useNavigate } from "react-router";
import { useWhoamiGate } from "../../lib/useWhoami";
import { Dialog } from "../../components/Dialog";
import { LoginPanel, type LoginResult } from "./LoginPanel";
import "./LoginPanel.css";

const DEFAULT_TARGET = "/logs";

/** Only accept a same-app relative path as the redirect target. Parses the
 * candidate against the app's own origin and requires the two to match —
 * `//evil.com`, `https://evil.com`, and even a same-app-looking `/\evil.com`
 * (browsers normalize a leading backslash to a second slash, so this would
 * otherwise resolve to `evil.com` too) all fall back to the default. */
function safeRedirectTarget(raw: string | null): string {
  if (!raw || !raw.startsWith("/")) return DEFAULT_TARGET;
  try {
    const url = new URL(raw, window.location.origin);
    if (url.origin !== window.location.origin) return DEFAULT_TARGET;
    return `${url.pathname}${url.search}${url.hash}`;
  } catch {
    return DEFAULT_TARGET;
  }
}

export function LoginRoute() {
  const navigate = useNavigate();
  const target = safeRedirectTarget(
    new URLSearchParams(window.location.search).get("redirect"),
  );

  // whoami() succeeding means the visitor is already authenticated (e.g.
  // they bookmarked /login or followed a stale link), so send them straight
  // to the redirect target instead of showing the form.
  const { who, isLoading } = useWhoamiGate();

  if (isLoading) {
    return (
      <Dialog label="Sign in" className="login-panel">
        <p className="login-hint">Checking session…</p>
      </Dialog>
    );
  }
  if (who) return <Navigate to={target} replace />;

  const handleSuccess = (result: LoginResult) => {
    const url = new URL(target, window.location.origin);
    url.searchParams.set("tenant", result.tenant);
    url.searchParams.set("dataset", result.dataset);
    navigate(`${url.pathname}${url.search}`, { replace: true });
  };

  return <LoginPanel onSuccess={handleSuccess} />;
}
