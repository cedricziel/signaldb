import { isAuthError, toErrorMessage } from "../api/http";

/** The one way a view reports a failed request: an alert naming what could
 * not be loaded and the server's message. */
export function QueryError({ what, error }: { what: string; error: unknown }) {
  return (
    <div className="query-error" role="alert">
      Could not load {what}: {toErrorMessage(error)}
    </div>
  );
}

/** A `whoami` failure for one of the route-level admin gates (`/manage`,
 * `/api-keys`, `/instrumentation`). A 401 is handled globally — the app
 * shell navigates to `/login` — so it renders nothing here; any other
 * failure (5xx, network, an older server) gets a visible `QueryError`
 * instead of being folded into the same "not admin" redirect those routes
 * use once the query actually resolves. */
export function whoamiQueryError(what: string, error: unknown) {
  return isAuthError(error) ? null : <QueryError what={what} error={error} />;
}
