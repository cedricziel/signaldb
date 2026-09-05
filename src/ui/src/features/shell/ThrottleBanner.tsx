// The one place the UI says "waiting" while `retryingFetch` (api/http.ts) is
// backing off after throttling: panels keep their natural loading state
// because their promise has not settled, and this unobtrusive shell banner
// explains why nothing has arrived yet. It renders nothing when no retry is
// pending.
import { useSyncExternalStore } from "react";
import { getThrottleState, subscribeThrottleState } from "../../api/http";
import "./ThrottleBanner.css";

export function useThrottleState() {
  return useSyncExternalStore(
    subscribeThrottleState,
    getThrottleState,
    getThrottleState,
  );
}

function formatWait(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)} ms`;
  const s = ms / 1000;
  return Number.isInteger(s) ? `${s} s` : `${s.toFixed(1)} s`;
}

export function ThrottleBanner() {
  const state = useThrottleState();
  if (state.pending === 0) return null;
  const detail =
    state.lastWaitMs == null
      ? ""
      : ` (waiting ${formatWait(state.lastWaitMs)})`;
  return (
    <div
      className="warn-callout throttle-banner"
      role="status"
      aria-live="polite"
    >
      Some requests are being retried after throttling…{detail}
    </div>
  );
}
