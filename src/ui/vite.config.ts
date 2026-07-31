/// <reference types="vitest/config" />
import { createRequire } from "node:module";
import react from "@vitejs/plugin-react";
import { defineConfig, loadEnv, type ProxyOptions } from "vite";

const require = createRequire(import.meta.url);
const pkg = require("./package.json") as { version: string };

// Paths the SignalDB router serves; the dev server forwards them to a live
// instance so the browser only ever sees same-origin requests, exactly as in
// the embedded production build. /ui/session is the router's session login
// endpoint and /ui/runtime-config.js is its runtime telemetry config —
// everything else under /ui stays with the dev server itself.
const PROXIED_PATHS = [
  "/loki",
  "/tempo",
  "/prometheus",
  "/pyroscope",
  "/api",
  "/ui/session",
  "/ui/runtime-config.js",
];

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, __dirname, "SIGNALDB_");
  const target = env.SIGNALDB_TARGET || "http://localhost:3000";

  const proxy = Object.fromEntries(
    PROXIED_PATHS.map((path): [string, ProxyOptions] => [
      path,
      {
        target,
        changeOrigin: true,
        // Dev-only proxy: accept self-signed certs on homelab targets.
        secure: false,
        configure(proxyServer) {
          proxyServer.on("proxyReq", (proxyReq, req) => {
            // The API key never reaches browser code; always injected here.
            if (env.SIGNALDB_API_KEY) {
              proxyReq.setHeader(
                "Authorization",
                `Bearer ${env.SIGNALDB_API_KEY}`,
              );
            }
            // Tenant context: the UI's selector sends its own headers; the
            // env values are only defaults for requests without them.
            if (!req.headers["x-tenant-id"] && env.SIGNALDB_TENANT) {
              proxyReq.setHeader("X-Tenant-ID", env.SIGNALDB_TENANT);
            }
            if (!req.headers["x-dataset-id"] && env.SIGNALDB_DATASET) {
              proxyReq.setHeader("X-Dataset-ID", env.SIGNALDB_DATASET);
            }
          });
        },
      },
    ]),
  );

  return {
    plugins: [react()],
    // Served by the router under /ui/ in production.
    base: "/ui/",
    // Surface the dev defaults so the tenant selector can display them, plus
    // the telemetry config baked in at build time (see src/telemetry).
    define: {
      __SIGNALDB_DEFAULT_TENANT__: JSON.stringify(env.SIGNALDB_TENANT ?? ""),
      __SIGNALDB_DEFAULT_DATASET__: JSON.stringify(env.SIGNALDB_DATASET ?? ""),
      __SIGNALDB_OTLP_ENDPOINT__: JSON.stringify(
        env.SIGNALDB_OTLP_ENDPOINT ?? "",
      ),
      __SIGNALDB_TELEMETRY_SERVICE_NAME__: JSON.stringify(
        env.SIGNALDB_TELEMETRY_SERVICE_NAME ?? "signaldb-ui",
      ),
      __SIGNALDB_UI_VERSION__: JSON.stringify(pkg.version),
    },
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
