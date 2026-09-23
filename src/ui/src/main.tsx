import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { RouterProvider } from "react-router";
import { createAppRouter } from "./routes";
import { initTelemetry } from "./telemetry";
import { recordRenderError } from "./telemetry/renderErrors";
import { initTheme } from "./lib/theme";
import { queryRetry } from "./lib/queryRetry";
import { sidebarWidth, spanDetailWidth } from "./lib/sidebarWidth";
import "./styles/global.css";

// Restore the saved theme before first paint to avoid a flash of the wrong
// theme.
initTheme();

// Same for the facet/field sidebar's saved width, and the trace waterfall's
// span-detail pane.
sidebarWidth.init();
spanDetailWidth.init();

// Start browser telemetry before anything issues a request, so the fetch
// instrumentation is patched in and API calls carry a `traceparent`.
initTelemetry();

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Observability queries are time-window scoped; refetching on focus
      // would silently shift results under the user.
      refetchOnWindowFocus: false,
      retry: queryRetry,
    },
  },
});

// A data router (rather than plain `<BrowserRouter>`) so the shell's
// `UnsavedChangesGuard` (App.tsx) can use `useBlocker` to intercept every
// in-app navigation — links, the tab strip, the user menu, browser
// Back/Forward — while a form is dirty, not just its own breadcrumb links.
const router = createAppRouter();

createRoot(document.getElementById("root")!, {
  // `onCaughtError` is deliberately left unset: every route render error is
  // caught by `RootErrorBoundary`'s `errorElement` (routes.tsx), which
  // already records it via `recordRenderError` — wiring `onCaughtError` too
  // would fire for that same catch and double-record it, since React 19
  // calls `onCaughtError` for any boundary in the tree, including the one
  // `errorElement` compiles to. `onUncaughtError` alone covers the gap: a
  // render error with no boundary above it at all (outside the router tree,
  // e.g. in `QueryClientProvider`), which should not happen given the
  // root-level `errorElement` but has no other backstop if it does.
  onUncaughtError: (error) =>
    recordRenderError(error, window.location.pathname),
}).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  </StrictMode>,
);

// Deferred and dynamically imported: registration itself already waits for
// the `load` event, and nothing here wants to compete with first paint or
// pull workbox-window into the entry chunk.
void import("./pwa").then((m) => m.initPwaUpdates());
