// A single-sign-on control. A real link, not a button with a click handler:
// it must support "open in new tab" / middle-click and never go through XHR
// (design decision 6) — styled as the primary action via .login-sso.

// Own the stylesheet `.login-sso` comes from rather than relying on the
// caller (LoginMethods) to have loaded it.
import "../features/shell/LoginPanel.css";

interface Props {
  name: string;
  startUrl: string;
}

export function SsoButton({ name, startUrl }: Props) {
  return (
    <a className="login-sso" href={startUrl}>
      Continue with {name}
    </a>
  );
}
