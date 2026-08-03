import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import { ConsentView } from "./features/consent/ConsentView";
import { initTelemetry } from "./telemetry";
import "./styles/global.css";

// The OAuth connector consent screen is a distinct top-level view reached at
// `/oauth/consent` (the router redirects here from `/oauth/authorize`); it
// bypasses the explore shell.
const isConsent = window.location.pathname === "/oauth/consent";

// Start browser telemetry before anything issues a request, so the fetch
// instrumentation is patched in and API calls carry a `traceparent`.
initTelemetry();

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Observability queries are time-window scoped; refetching on focus
      // would silently shift results under the user.
      refetchOnWindowFocus: false,
      retry: 1,
    },
  },
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      {isConsent ? <ConsentView /> : <App />}
    </QueryClientProvider>
  </StrictMode>,
);
