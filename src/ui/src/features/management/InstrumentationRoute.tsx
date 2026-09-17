import { whoamiQueryError } from "../../components/QueryError";
import { useOutletState } from "../../lib/outletState";
import { useWhoami } from "../../lib/useWhoami";
import { Instrumentation } from "./Instrumentation";

/**
 * `/instrumentation` — route for the instrumentation guide.
 * Requires authentication (whoami succeeds) but no admin role. A 401 is
 * handled globally (the app shell sends it to `/login`); any other failure
 * shows an inline error instead of a silent redirect to /logs.
 */
export function InstrumentationRoute() {
  const { state } = useOutletState();
  const { data: who, isLoading, isError, error } = useWhoami(state);

  if (isLoading) return null;
  if (isError) return whoamiQueryError("your account", error);
  if (!who) return null;

  return <Instrumentation state={state} />;
}
