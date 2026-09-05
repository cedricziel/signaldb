// Dedicated, linkable login screen at `/login`. Unlike LoginGate (which pops
// up reactively on a 401 mid-session), this is a standalone destination:
// sign-out lands here, and it accepts a `?redirect=` target to return to
// after a successful login. Reuses LoginPanel for the actual form/tenant
// picker rather than duplicating it.

import { Navigate, useNavigate } from "react-router";
import { useWhoamiGate } from "../../lib/useWhoami";
import { LoginPanel, type LoginResult } from "./LoginPanel";
import "./LoginPanel.css";

const DEFAULT_TARGET = "/logs";

/** Only accept a same-app relative path as the redirect target — a value
 * like `//evil.com` or `https://evil.com` would otherwise be an open
 * redirect. */
function safeRedirectTarget(raw: string | null): string {
  if (!raw) return DEFAULT_TARGET;
  if (!raw.startsWith("/") || raw.startsWith("//")) return DEFAULT_TARGET;
  return raw;
}

export function LoginRoute() {
  const navigate = useNavigate();
  const target = safeRedirectTarget(
    new URLSearchParams(window.location.search).get("redirect"),
  );

  // whoami() succeeding means the visitor is already authenticated (e.g.
  // they bookmarked /login or followed a stale link), so send them straight
  // to the redirect target. The common case — arriving here right after
  // sign-out — has no session left, so the form renders immediately rather
  // than waiting on this check: it only swaps to the redirect if whoami
  // later resolves truthy.
  const { who } = useWhoamiGate();

  if (who) return <Navigate to={target} replace />;

  const handleSuccess = (result: LoginResult) => {
    const url = new URL(target, window.location.origin);
    url.searchParams.set("tenant", result.tenant);
    url.searchParams.set("dataset", result.dataset);
    navigate(`${url.pathname}${url.search}`, { replace: true });
  };

  return <LoginPanel onSuccess={handleSuccess} />;
}
