import { useEffect, useState, type ReactNode } from "react";
import { installFetchStub, type JsonRoute } from "./fetchStub";

/**
 * Installs {@link installFetchStub}'s route stub before the wrapped page
 * mounts (a `useState` lazy initializer runs during render, ahead of any
 * child's data-fetching effect — a `useEffect` here would run after
 * children's, too late for their first fetch) and restores real `fetch` on
 * unmount, so one story's stub never leaks into the next.
 */
export function StoryFetchStub({
  routes,
  children,
}: {
  routes: JsonRoute[];
  children: ReactNode;
}) {
  const [{ restore }] = useState(() => installFetchStub(routes));
  useEffect(() => restore, [restore]);
  return children;
}
