// `sanitizeUrl` hook for `@opentelemetry/browser-instrumentation`'s
// `NavigationInstrumentation` (see `logs.ts`). The package's README documents
// a `defaultSanitizeUrl` helper, but the version pinned in package.json does
// not export one (checked against the installed dist), so this is our own
// minimal equivalent: strip embedded userinfo credentials and redact known
// sensitive query parameter values before the URL is written to a log
// record's `url.full` attribute.

const SENSITIVE_PARAMS = new Set([
  "api_key",
  "apikey",
  "access_token",
  "token",
  "password",
  "secret",
  "authorization",
  "auth",
]);

/**
 * Sanitize a navigation URL before it is emitted on a log record. Best-effort
 * and never throws: an unparseable value is returned unchanged rather than
 * dropped.
 */
export function sanitizeNavigationUrl(url: string): string {
  const base = typeof location !== "undefined" ? location.origin : undefined;
  let parsed: URL;
  try {
    parsed = new URL(url, base);
  } catch {
    return url;
  }
  parsed.username = "";
  parsed.password = "";
  for (const key of [...parsed.searchParams.keys()]) {
    if (SENSITIVE_PARAMS.has(key.toLowerCase())) {
      parsed.searchParams.set(key, "REDACTED");
    }
  }
  return parsed.toString();
}
