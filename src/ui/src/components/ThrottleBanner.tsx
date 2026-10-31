// The one place the UI says "waiting" while `retryingFetch` (api/http.ts) is
// backing off after throttling: panels keep their natural loading state
// because their promise has not settled, and this unobtrusive shell banner
// explains why nothing has arrived yet. It renders nothing when no retry is
// pending.
import { useSyncExternalStore } from "react";
import { getThrottleState, subscribeThrottleState } from "../api/http";
import "./ThrottleBanner.css";

export function useThrottleState() {
  return useSyncExternalStore(
    subscribeThrottleState,
    getThrottleState,
    getThrottleState,
  );
}

export function ThrottleBanner() {
  const state = useThrottleState();
  if (state.pending === 0) return null;
  return (
    <div
      className="warn-callout throttle-banner"
      role="status"
      aria-live="polite"
    >
      Some panels are delayed. Retrying automatically…
    </div>
  );
}
