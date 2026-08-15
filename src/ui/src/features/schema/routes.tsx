// Route tree of the `/schema` hub, spliced into the app's routes. Every
// view is a real URL: tabs, registry browser, definition pages and the
// editor, so the browser back button walks between them.
import { Navigate, Route } from "react-router";
import { RegistryBrowser } from "./RegistryBrowser";
import { RegistryList } from "./RegistryList";
import { SchemaExplorer } from "./SchemaExplorer";
import { SchemaHub } from "./SchemaHub";

export function schemaRoutes() {
  return (
    <Route path="schema" element={<SchemaHub />}>
      <Route index element={<Navigate to="/schema/conventions" replace />} />
      <Route path="conventions" element={<RegistryList />} />
      <Route path="conventions/:ns/:version" element={<RegistryBrowser />} />
      <Route
        path="conventions/:ns/:version/:kind/:name"
        element={<RegistryBrowser />}
      />
      <Route path="storage" element={<SchemaExplorer />} />
    </Route>
  );
}
