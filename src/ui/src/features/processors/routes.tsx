// Route tree for the `/processors` feature, spliced into the app's routes.
import { Route } from "react-router";
import { ProcessorEditor } from "./ProcessorEditor";
import { ProcessorList } from "./ProcessorList";

export function processorsRoutes() {
  return (
    <Route path="processors">
      <Route index element={<ProcessorList />} />
      <Route path="new" element={<ProcessorEditor />} />
      <Route path=":name/edit" element={<ProcessorEditor />} />
    </Route>
  );
}
