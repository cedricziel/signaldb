import type { ReactNode } from "react";
import { Outlet, Route, Routes } from "react-router";
import type { ShellContext } from "../lib/outletState";

/**
 * Supplies the `<Outlet context>` value the app shell normally provides
 * (`useOutletState`) to a page rendered standalone in a story — the same
 * `{ state, update }` pair `App.tsx` passes down.
 */
export function OutletContextProvider({
  value,
  children,
}: {
  value: ShellContext;
  children: ReactNode;
}) {
  return (
    <Routes>
      <Route element={<Outlet context={value} />}>
        <Route path="*" element={children} />
      </Route>
    </Routes>
  );
}
