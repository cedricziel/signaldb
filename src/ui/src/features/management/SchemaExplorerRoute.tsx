import { SchemaExplorer } from "./SchemaExplorer";

/**
 * `/schema` — read-only logical + physical schema introspection for
 * instance admins. SchemaExplorer fetches whoami internally and performs
 * its own redirect for non-admins.
 */
export function SchemaExplorerRoute() {
  return <SchemaExplorer />;
}
