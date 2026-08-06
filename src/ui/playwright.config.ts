import { defineConfig, devices } from "@playwright/test";

const PORT = 4173;

/**
 * Browser-level routing/navigation tests for the explore UI. These exercise
 * the client-side router against the production build (`vite preview`) —
 * the same static SPA the SignalDB router serves in production (see
 * src/router/src/ui.rs) — not the dev server's proxy. There is no live
 * SignalDB backend in this environment: specs mock the specific API calls
 * a scenario depends on (via `page.route`) and otherwise assert on
 * routing/DOM structure, not on data content.
 *
 * See https://playwright.dev/docs/test-configuration.
 */
export default defineConfig({
  testDir: "./e2e",
  fullyParallel: true,
  // Fail the build on CI if `test.only` was accidentally left in.
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  reporter: process.env.CI
    ? [["html", { open: "never" }], ["github"], ["list"]]
    : "list",
  use: {
    baseURL: `http://localhost:${PORT}`,
    trace: "on-first-retry",
  },
  webServer: {
    // Self-contained: builds and serves the actual production bundle, so
    // `pnpm test:e2e` doesn't depend on a prior build step having run.
    command: `pnpm run build && pnpm exec vite preview --port ${PORT} --strictPort`,
    url: `http://localhost:${PORT}`,
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
