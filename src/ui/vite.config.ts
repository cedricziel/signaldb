/// <reference types="vitest/config" />
import { createRequire } from "node:module";
import react from "@vitejs/plugin-react";
import { defineConfig, loadEnv, type ProxyOptions } from "vite";
import { VitePWA } from "vite-plugin-pwa";
import { configDefaults } from "vitest/config";
import { PROXIED_PATHS } from "./src/lib/proxiedPaths";
import { proxyKey } from "./src/lib/proxyKey";

const require = createRequire(import.meta.url);
const pkg = require("./package.json") as { version: string };

// See proxiedPaths.ts for what PROXIED_PATHS covers and why. One exception:
// `/oauth/consent` itself is the SPA consent route (served by the dev
// server), so only its `/oauth/consent/context` API sibling is listed.

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
    plugins: [
      react(),
      VitePWA({
        // Install new versions in the background but wait to activate them —
        // an ops dashboard is exactly where a half-typed API-key form, a
        // pending consent selection, or an in-progress registry edit lives,
        // and autoUpdate's silent reload can land mid-edit and wipe it. The
        // deferred apply (banner + auto-apply on the next route change once
        // no form is dirty) lives in src/pwa.ts and src/lib/pwaUpdate.ts.
        // Periodic re-checks are still wired in src/pwa.ts, since a
        // long-lived tab may never navigate again to trigger the browser's
        // own check.
        registerType: "prompt",
        // The install-only PNG icons aren't worth precaching for every
        // visitor; the browser fetches them itself if/when it installs the
        // app. Only the favicon (tiny) is added for an offline app shell.
        includeAssets: ["favicon.svg"],
        manifest: {
          name: "SignalDB",
          short_name: "SignalDB",
          description:
            "Observability signal database for metrics, logs, and traces.",
          theme_color: "#14181e",
          background_color: "#14181e",
          display: "standalone",
          start_url: "/",
          scope: "/",
          icons: [
            {
              src: "/pwa-192x192.png",
              sizes: "192x192",
              type: "image/png",
            },
            {
              src: "/pwa-512x512.png",
              sizes: "512x512",
              type: "image/png",
            },
            {
              src: "/pwa-512x512-maskable.png",
              sizes: "512x512",
              type: "image/png",
              purpose: "maskable",
            },
          ],
        },
        workbox: {
          // Only the app shell needs to be installable offline; the icons
          // are fetched by the browser itself when it installs the app
          // (see includeAssets above for the one exception).
          globPatterns: ["**/*.{js,css,html}"],
          // See proxiedPaths.ts: these are the backend's own routes, which
          // the SW's SPA navigate fallback must never intercept.
          navigateFallbackDenylist: PROXIED_PATHS.map(
            (path) => new RegExp(proxyKey(path)),
          ),
        },
      }),
    ],
    // Served by the router at root in production (SPA fallback).
    base: "/",
    // Shipped alongside the bundle so minified stack frames can be mapped
    // back to source (DevTools today, server-side symbolication per #1677).
    build: { sourcemap: true },
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
      // e2e/** and e2e-live/** are Playwright specs (see
      // playwright.config.ts / playwright.live.config.ts) — a different
      // runner, different test() import, not vitest's.
      exclude: [...configDefaults.exclude, "e2e/**", "e2e-live/**"],
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
          // Imports the `virtual:pwa-register` module, which only resolves
          // inside a real Vite/PWA build — not under vitest. The update
          // logic it wires up is tested directly in
          // src/lib/pwaUpdate.test.ts.
          "src/pwa.ts",
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
