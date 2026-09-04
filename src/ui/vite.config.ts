/// <reference types="vitest/config" />
import { createRequire } from "node:module";
import react from "@vitejs/plugin-react";
import { defineConfig, loadEnv, type ProxyOptions } from "vite";
import { configDefaults } from "vitest/config";
import { proxyKey } from "./src/lib/proxyKey";

const require = createRequire(import.meta.url);
const pkg = require("./package.json") as { version: string };

// Paths the SignalDB router serves; the dev server forwards them to a live
// instance so the browser only ever sees same-origin requests, exactly as in
// the embedded production build. /ui/session is the router's session login
// endpoint and /runtime-config.js is its runtime telemetry config — the SPA is
// served from root, so everything else is served by the dev server itself.
//
// The OAuth endpoints proxy too, EXCEPT `/oauth/consent`, which is the SPA
// consent route (served by the dev server); its `/oauth/consent/context` API
// sibling is listed on its own.
//
// Every entry matches whole path segments (see proxyKey): a bare prefix
// would also swallow SPA routes that merely start with it, as `/api` once
// did to `/api-keys`.
const PROXIED_PATHS = [
  "/loki",
  "/tempo",
  "/prometheus",
  "/pyroscope",
  "/api",
  "/ui/session",
  "/runtime-config.js",
  "/.well-known/oauth-authorization-server",
  "/.well-known/oauth-protected-resource",
  "/oauth/authorize",
  "/oauth/consent/context",
  "/oauth/register",
  "/oauth/token",
];

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, __dirname, "SIGNALDB_");
  const target = env.SIGNALDB_TARGET || "http://localhost:3000";

  const proxy = Object.fromEntries(
    PROXIED_PATHS.map((path): [string, ProxyOptions] => [
      proxyKey(path),
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
    // Served by the router at root in production (SPA fallback).
    base: "/",
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
      // e2e/** are Playwright specs (see playwright.config.ts) — a
      // different runner, different test() import, not vitest's.
      exclude: [...configDefaults.exclude, "e2e/**"],
      coverage: {
        provider: "v8",
        include: ["src/**/*.{ts,tsx}"],
        exclude: [
          "src/main.tsx",
          "src/test/**",
          "src/**/*.d.ts",
          // Machine-generated from the OpenAPI spec (`pnpm generate:api`) —
          // not hand-written, and regenerating it isn't how bugs get fixed.
          "src/**/*.gen.ts",
          // Bootstraps the OTel SDK and patches global async primitives via
          // zone.js; the module comment warns it must never be imported
          // from test code (see src/telemetry/index.ts), so it can't be
          // exercised here — same rationale as excluding main.tsx.
          "src/telemetry/index.ts",
          // Same category as index.ts: thin LoggerProvider/instrumentation
          // wiring glue, not logic — the logic it calls into
          // (sessionLogRecordProcessor.ts, sanitizeNavigationUrl.ts,
          // resource.ts, runtimeConfig.ts) is unit-tested directly instead.
          // Unlike index.ts this one doesn't import zone.js and *could* be
          // imported from a test, but there is nothing worth asserting on
          // beyond "it calls registerInstrumentations with the right args",
          // which a type error already catches.
          "src/telemetry/logs.ts",
        ],
        thresholds: {
          lines: 80,
          statements: 80,
          functions: 80,
          branches: 80,
        },
      },
    },
  };
});
