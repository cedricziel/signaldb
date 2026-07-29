/// <reference types="vitest/config" />
import react from "@vitejs/plugin-react";
import { defineConfig, loadEnv, type ProxyOptions } from "vite";

// Paths the SignalDB router serves; the dev server forwards them to a live
// instance so the browser only ever sees same-origin requests, exactly as in
// the embedded production build.
const PROXIED_PATHS = ["/loki", "/tempo", "/prometheus", "/api"];

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, __dirname, "SIGNALDB_");
  const target = env.SIGNALDB_TARGET || "http://localhost:3000";

  // Auth is injected by the proxy from .env.local so API keys never reach
  // browser code during development.
  const headers: Record<string, string> = {};
  if (env.SIGNALDB_API_KEY)
    headers["Authorization"] = `Bearer ${env.SIGNALDB_API_KEY}`;
  if (env.SIGNALDB_TENANT) headers["X-Tenant-ID"] = env.SIGNALDB_TENANT;
  if (env.SIGNALDB_DATASET) headers["X-Dataset-ID"] = env.SIGNALDB_DATASET;

  const proxy = Object.fromEntries(
    PROXIED_PATHS.map((path): [string, ProxyOptions] => [
      path,
      {
        target,
        changeOrigin: true,
        // Dev-only proxy: accept self-signed certs on homelab targets.
        secure: false,
        headers,
      },
    ]),
  );

  return {
    plugins: [react()],
    // Served by the router under /ui/ in production.
    base: "/ui/",
    server: { port: 5173, proxy },
    test: {
      environment: "jsdom",
      setupFiles: "./src/test/setup.ts",
      css: false,
      coverage: {
        provider: "v8",
        include: ["src/**/*.{ts,tsx}"],
        exclude: ["src/main.tsx", "src/test/**", "src/**/*.d.ts"],
      },
    },
  };
});
