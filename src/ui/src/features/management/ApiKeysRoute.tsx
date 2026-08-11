import { ApiKeys } from "./ApiKeys";

/**
 * `/api-keys` — route for API key management.
 * The ApiKeys component handles its own authentication and admin check,
 * redirecting non-admins to /logs.
 */
export function ApiKeysRoute() {
  // ApiKeys fetches whoami internally and performs its own redirect.
  return <ApiKeys />;
}
