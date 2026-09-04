import { toErrorMessage } from "../api/http";

/** The one way a view reports a failed request: an alert naming what could
 * not be loaded and the server's message. */
export function QueryError({ what, error }: { what: string; error: unknown }) {
  return (
    <div className="query-error" role="alert">
      Could not load {what}: {toErrorMessage(error)}
    </div>
  );
}
