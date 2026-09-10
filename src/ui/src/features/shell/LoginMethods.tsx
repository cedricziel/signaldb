// Renders the credential step purely from the login-configuration probe
// (design decision 4): password form, SSO control, both with a divider
// between them, or a probe-unavailable fallback. A future list-valued probe
// is a one-line change to `providers` below, not a new component.

import { useEffect, useRef } from "react";
import type { LoginConfigResponse, SessionResult } from "../../api/session";
import { CHECKING_LOGIN_OPTIONS_HINT } from "../../lib/useLoginConfig";
import { PasswordForm } from "./PasswordForm";
import { SsoButton } from "./SsoButton";

interface Props {
  /** `undefined` while the login-configuration probe is still in flight —
   * renders the checking hint in place of any credential control. */
  config: LoginConfigResponse | "unavailable" | undefined;
  /** Same-app path the SSO control should return to after the IdP round
   * trip; already validated by the caller. */
  redirect: string;
  onAuthenticated: (result: SessionResult) => void;
  /** Contextual copy shown above the credential controls (differs by
   * caller: the page's default, the gate's "session expired", consent's
   * "sign in to authorize"). */
  hint?: string;
}

export function LoginMethods({
  config,
  redirect,
  onAuthenticated,
  hint,
}: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const resolved = config !== undefined;

  useEffect(() => {
    if (!resolved) return;
    // First control: the SSO link when present (it renders before the
    // form), else the email field.
    containerRef.current
      ?.querySelector<HTMLElement>("a.login-sso, input")
      ?.focus();
    // Re-run once the probe answers (`resolved` flips false -> true), not
    // just on mount — while it's pending there's no control to focus yet.
  }, [resolved]);

  if (config === undefined) {
    return <p className="login-hint">{CHECKING_LOGIN_OPTIONS_HINT}</p>;
  }

  const providers =
    config !== "unavailable" && config.oidc ? [config.oidc] : [];
  // Never hide the password form on a probe failure, and never hide it when
  // the probe itself reports no method at all (a config error the server
  // would refuse anyway) — break-glass access must survive a UI bug.
  const showPassword =
    config === "unavailable" ||
    config.password_enabled ||
    providers.length === 0;

  return (
    <div className="login-methods" ref={containerRef}>
      {hint && <p className="login-hint">{hint}</p>}
      {config === "unavailable" && (
        <p className="login-notice">
          Couldn't load sign-in options — password sign-in is shown as a
          fallback.
        </p>
      )}
      {providers.map((provider) => (
        <SsoButton
          key={provider.name}
          name={provider.name}
          startUrl={`/ui/session/oidc/start?redirect=${encodeURIComponent(redirect)}`}
        />
      ))}
      {providers.length > 0 && showPassword && (
        <div className="login-divider">or</div>
      )}
      {showPassword && (
        <PasswordForm
          onAuthenticated={onAuthenticated}
          primary={providers.length === 0}
        />
      )}
      {providers.length > 0 && !showPassword && (
        <p className="login-hint">Password sign-in is off on this instance.</p>
      )}
    </div>
  );
}
