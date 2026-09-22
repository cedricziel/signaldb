// One-click sign-in with the read-only `[demo]` account (change:
// demo-mode). A real button, not a link like SsoButton: it POSTs
// credentials through the same /ui/session path PasswordForm uses rather
// than navigating to an IdP.

import { useState } from "react";
import { toErrorMessage } from "../api/http";
import { createSession, type SessionResult } from "../api/session";
// Own the stylesheet its classes come from rather than relying on a caller
// (LoginMethods) to have loaded it.
import "../features/shell/LoginPanel.css";

interface Props {
  username: string;
  password: string;
  onAuthenticated: (result: SessionResult) => void;
}

export function DemoButton({ username, password, onAuthenticated }: Props) {
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  return (
    <div className="login-demo">
      <button
        type="button"
        className="login-demo-button btn btn-primary"
        disabled={busy}
        onClick={() => {
          setBusy(true);
          setError(null);
          createSession({ email: username, password })
            .then(onAuthenticated)
            .catch((err: unknown) => {
              setError(toErrorMessage(err));
            })
            .finally(() => setBusy(false));
        }}
      >
        {busy ? "Signing in…" : "Explore the demo"}
      </button>
      <p className="login-demo-hint">
        {username} / {password}
      </p>
      {error && (
        <p className="error-text" role="alert">
          {error}
        </p>
      )}
    </div>
  );
}
