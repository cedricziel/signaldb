// The route tree's `errorElement` (see routes.tsx's `RootLayout`). React
// Router's default error boundary would otherwise swallow a render error
// silently from telemetry's point of view — it never reaches `window`'s
// `error` event, so `ErrorsInstrumentation` (telemetry/logs.ts) can't see
// it. This one records it once via `recordRenderError` and shows a styled
// fallback instead of React Router's bare default.
import { useEffect } from "react";
import { isRouteErrorResponse, useLocation, useRouteError } from "react-router";
import { recordRenderError } from "../../telemetry/renderErrors";
import "./RouteErrorBoundary.css";

function describeError(error: unknown): { title: string; detail: string } {
  if (isRouteErrorResponse(error)) {
    return {
      title: `${error.status} ${error.statusText}`.trim(),
      detail:
        typeof error.data === "string" ? error.data : "Something went wrong.",
    };
  }
  return {
    title: "Something went wrong",
    detail: error instanceof Error ? error.message : String(error),
  };
}

export function RouteErrorBoundary() {
  const error = useRouteError();
  const location = useLocation();

  useEffect(() => {
    // Records once per distinct error, even across a React 19 dev
    // double-render in StrictMode — a fresh boundary instance still shares
    // this module-level set, so the second mount of the same error object
    // is a no-op. Only `Error`-like (object) errors can be tracked this way;
    // a thrown primitive (string, number) has no stable identity to dedupe
    // on, so it's recorded on every mount — rare in practice since React
    // Router's own boundary is what throws it forward.
    if (typeof error === "object" && error !== null) {
      if (recordedErrors.has(error)) return;
      recordedErrors.add(error);
    }
    recordRenderError(error, location.pathname);
  }, [error, location.pathname]);

  const { title, detail } = describeError(error);

  return (
    <div className="route-error" role="alert">
      <p className="route-error-title">{title}</p>
      <p className="route-error-detail">{detail}</p>
      <div className="route-error-actions">
        <button
          type="button"
          className="btn btn-primary"
          onClick={() => window.location.reload()}
        >
          Reload
        </button>
        <button
          type="button"
          className="btn"
          onClick={() => {
            window.location.href = "/";
          }}
        >
          Go home
        </button>
      </div>
    </div>
  );
}

// Module-scope so it survives the fresh component instance StrictMode's dev
// double-mount creates, keyed by error identity (works for the common case:
// a thrown `Error` object, which `getDerivedStateFromError` hands React
// Router's internal boundary the same reference both times).
const recordedErrors = new WeakSet<object>();
