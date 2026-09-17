import type { Location, NavigateFunction } from "react-router";

/**
 * Step back in in-app history when there is any, otherwise run `fallback`.
 *
 * React Router marks the very first history entry of a session — a fresh
 * tab, or a deep link landing straight on a page — with `location.key ===
 * "default"`. There's nothing to go back to from there: `navigate(-1)` would
 * leave the app entirely (e.g. to `about:blank` or the referrer). Anywhere
 * else, `location.key` is a generated id, and stepping out with `-1` returns
 * to wherever the page was actually opened from instead of a fixed fallback
 * route.
 */
export function goBackOr(
  navigate: NavigateFunction,
  location: Location,
  fallback: () => void,
): void {
  if (location.key !== "default") navigate(-1);
  else fallback();
}
