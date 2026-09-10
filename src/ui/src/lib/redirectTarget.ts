// Shared same-app redirect-target validation (change: oidc-login). Used by
// every place that carries a `redirect` query param across a full-page
// round trip — the login page, the SSO-start link, the app shell's tenant
// resolution, and `/select-tenant` — so all four validate identically and
// mirror the router's own `safe_redirect_target` (`src/router/src/oidc.rs`).

/** Fallback target for a missing or unsafe `redirect` value. */
export const DEFAULT_TARGET = "/logs";

/** Only accept a same-app relative path as the redirect target, other than
 * `/login` itself (looping the credential step back onto its own landing
 * pad). Parses the candidate against the app's own origin and requires the
 * two to match — `//evil.com`, `https://evil.com`, and even a same-app-
 * looking `/\evil.com` (browsers normalize a leading backslash to a second
 * slash, so this would otherwise resolve to `evil.com` too) all fall back
 * to the default. */
export function safeRedirectTarget(raw: string | null | undefined): string {
  if (!raw || !raw.startsWith("/")) return DEFAULT_TARGET;
  // Reject outright on any control character (CR, LF, tab, or anything below
  // 0x20) — checked against the raw input, not the parsed result, so this
  // can't be bypassed by whatever the URL parser does or doesn't strip.
  // Mirrors the router's own `safe_redirect_target`.
  for (let i = 0; i < raw.length; i++) {
    if (raw.charCodeAt(i) < 0x20) return DEFAULT_TARGET;
  }
  try {
    const url = new URL(raw, window.location.origin);
    if (url.origin !== window.location.origin) return DEFAULT_TARGET;
    if (url.pathname === "/login") return DEFAULT_TARGET;
    const target = `${url.pathname}${url.search}${url.hash}`;
    // Defense in depth against a dot-segment path escape: even though
    // `url`'s *origin* stayed same-origin, a normalized path starting with
    // `//` (e.g. from "/.//evil.example") reads as protocol-relative once
    // written alone into a navigation target, outside the origin `url` was
    // resolved against.
    if (target.startsWith("//")) return DEFAULT_TARGET;
    return target;
  } catch {
    return DEFAULT_TARGET;
  }
}
