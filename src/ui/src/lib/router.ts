import type { NavigateFunction } from "react-router";

/**
 * Step back in in-app history when there is any, otherwise run `fallback`.
 *
 * React Router writes `{ idx, key, usr }` into `window.history.state` on
 * every navigation (`idx` is the entry's position in the session's history
 * stack); `location.key !== "default"` looks like the same signal but isn't:
 * a page reached via a `replace` navigation from `/login` gets a fresh,
 * non-"default" key even though there is nothing in-app behind it, since
 * `replace` overwrites the current entry rather than adding one — `idx`
 * stays `0` in that case, where a real `push` would have made it `1`. `idx`
 * is what actually answers "is there an in-app entry behind this one".
 */
export function goBackOr(navigate: NavigateFunction, fallback: () => void): void {
  const idx = (window.history.state as { idx?: number } | null)?.idx ?? 0;
  if (idx > 0) navigate(-1);
  else fallback();
}
