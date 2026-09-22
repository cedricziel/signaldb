import type { ReactElement } from "react";
import { Outlet, Route, Routes } from "react-router";
import type { ShellContext } from "../lib/outletState";

/**
 * Supplies the `<Outlet context>` value the app shell normally provides
 * (`useOutletState`) to a page (or route tree) rendered standalone in a
 * story — the same `{ state, update }` pair `App.tsx` passes down.
 * `children` is one or more `<Route>` elements nested under the outlet, so
 * a feature's own multi-route tree (e.g. `processorsRoutes()`) can be
 * dropped in as-is alongside a single `<Route path="*" element={...} />`
 * for a standalone page.
 */
export function OutletContextProvider({
  value,
  children,
}: {
  value: ShellContext;
  children: ReactElement | ReactElement[];
}) {
  return (
    <Routes>
      <Route element={<Outlet context={value} />}>{children}</Route>
    </Routes>
  );
}
