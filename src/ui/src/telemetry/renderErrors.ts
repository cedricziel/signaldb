// Records a React render error as an OTel exception log record — the same
// event shape `ErrorsInstrumentation` (logs.ts) uses for uncaught
// window-level errors, so both land as the same `exception` event kind and
// a query for "browser errors" sees render errors too.
//
// React Router's default error boundary (`RootErrorBoundary` in routes.tsx)
// swallows a render error and shows its own fallback UI — it never reaches
// `window`'s `error` event, so `ErrorsInstrumentation` cannot see it. This
// closes that gap for two entry points:
//
//   - a route's `errorElement` (RootErrorBoundary), for errors under a route
//     that opts in
//   - `createRoot`'s `onUncaughtError`, for a render error with no boundary
//     above it at all (should not normally happen given a root-level
//     `errorElement`, but is a safety net for errors outside the router
//     tree, e.g. in `QueryClientProvider`)
//
// `onCaughtError` is deliberately NOT wired to this helper: React 19 calls
// it for every error any boundary in the tree catches, including the one
// powering `errorElement` — wiring both would double-record the same error.
// `RootErrorBoundary` calling this directly, once, is the single source of
// truth for boundary-caught errors.

import { logs, SeverityNumber } from "@opentelemetry/api-logs";
import {
  ATTR_EXCEPTION_MESSAGE,
  ATTR_EXCEPTION_STACKTRACE,
  ATTR_EXCEPTION_TYPE,
} from "@opentelemetry/semantic-conventions";
import { sanitizeNavigationUrl } from "./sanitizeNavigationUrl";

const EXCEPTION_EVENT_NAME = "exception";
const LOGGER_NAME = "signaldb-ui-render-errors";

/** Coerce whatever a boundary caught (an `Error`, a thrown string, a
 * rejected value) into an `Error` with a name/message/stack triple. */
function toError(value: unknown): Error {
  if (value instanceof Error) return value;
  return new Error(typeof value === "string" ? value : String(value));
}

/**
 * Emit one `exception` log record for a render error, tagged with the route
 * it happened on as `url.full` (same attribute name and sanitization as
 * `NavigationInstrumentation`'s log events — see `sanitizeNavigationUrl`).
 * Safe to call from a render-error path: never throws, since a telemetry
 * failure must not compound a render crash.
 */
export function recordRenderError(error: unknown, pathname: string): void {
  try {
    const err = toError(error);
    logs.getLogger(LOGGER_NAME).emit({
      eventName: EXCEPTION_EVENT_NAME,
      severityNumber: SeverityNumber.ERROR,
      attributes: {
        [ATTR_EXCEPTION_TYPE]: err.name,
        [ATTR_EXCEPTION_MESSAGE]: err.message,
        [ATTR_EXCEPTION_STACKTRACE]: err.stack,
        "url.full": sanitizeNavigationUrl(pathname),
      },
    });
  } catch {
    // Telemetry must never be the reason the error fallback fails to render.
  }
}
