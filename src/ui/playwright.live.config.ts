import { defineConfig, devices } from "@playwright/test";

import { ROUTER_HTTP_PORT, STORAGE_STATE_FILE } from "./e2e-live/env";

/**
 * The live end-to-end suite: exercises the built UI against a real
 * `signaldb` monolith seeded with real OTLP telemetry (see
 * `e2e-live/global-setup.ts`), asserting on the data that actually comes
 * back rather than a mocked response shape. This is what would have caught
 * PR #1695's `duration_nanos` regression — see `e2e-live/entity-detail.spec.ts`.
 *
 * Distinct from `playwright.config.ts` (the mocked routing suite), which
 * stays untouched. Run with `pnpm test:e2e:live`.
 *
 * Requires a Rust toolchain on PATH — global setup builds `signaldb`,
 * `signaldb-cli`, and `signal-producer` — and Node 24 (see
 * docs/contributing/ui-e2e-live.md).
 */
export default defineConfig({
  testDir: "./e2e-live",
  fullyParallel: false, // one shared backend instance; specs run serially
  workers: 1,
  forbidOnly: !!process.env.CI,
  retries: 0,
  timeout: 30_000,
  globalTimeout: 10 * 60_000,
  globalSetup: "./e2e-live/global-setup.ts",
  globalTeardown: "./e2e-live/global-teardown.ts",
  reporter: process.env.CI
    ? [["html", { open: "never" }], ["github"], ["list"]]
    : "list",
  use: {
    baseURL: `http://127.0.0.1:${ROUTER_HTTP_PORT}`,
    storageState: STORAGE_STATE_FILE,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
