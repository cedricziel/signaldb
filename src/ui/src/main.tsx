import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import { initTelemetry } from "./telemetry";
import "./styles/global.css";

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
      <App />
    </QueryClientProvider>
  </StrictMode>,
);
