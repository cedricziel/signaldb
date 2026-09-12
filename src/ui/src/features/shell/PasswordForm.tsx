// Email/password sign-in form, POSTing to /ui/session. Rendered by
// LoginRoute, through LoginMethods.

import { useState } from "react";
import { toErrorMessage } from "../../api/http";
import { createSession, type SessionResult } from "../../api/session";

interface Props {
  /** Called with the raw POST /ui/session response; the caller decides
   * whether that's a resolved tenant or a set of memberships to choose
   * from. */
  onAuthenticated: (result: SessionResult) => void;
  /** Filled accent submit style vs the existing soft style — soft when an
   * SSO control is offered above this form. */
  primary: boolean;
}

export function PasswordForm({ onAuthenticated, primary }: Props) {
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  return (
    <form
      className="login-form"
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
          .then(onAuthenticated)
          .catch((err: unknown) => {
            setError(toErrorMessage(err));
          })
          .finally(() => setBusy(false));
      }}
    >
      <label>
        Email
        <input
          name="email"
          type="email"
          aria-label="Email"
          autoComplete="username"
          required
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
      <button
        type="submit"
        className={`login-submit ${primary ? "login-submit--primary" : "login-submit--soft"}`}
        disabled={busy}
      >
        {busy ? "Signing in…" : "Sign in"}
      </button>
    </form>
  );
}
