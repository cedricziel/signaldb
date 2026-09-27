import { defineConfig, devices } from "@playwright/test";

const PORT = 6007;

/**
 * Responsive + contrast check over every page story (`Pages/*`, `Shell/*`)
 * in a static Storybook build: each story at 390, 768 and 1280 wide must
 * render without throwing, never scroll the document sideways, and carry no
 * axe `color-contrast` violation beyond `e2e-stories/allowlist.ts`. Dark
 * coverage comes from each page's own `Dark` stories.
 *
 * `STORYBOOK_STATIC_DIR` points at an existing build and skips the rebuild;
 * `CHROMIUM_PATH` runs a local Chromium instead of Playwright's download.
 */
const staticDir = process.env.STORYBOOK_STATIC_DIR ?? "storybook-static";
const build = process.env.STORYBOOK_STATIC_DIR
  ? ""
  : `pnpm exec storybook build -c .storybook -o ${staticDir} --quiet && `;

export default defineConfig({
  testDir: "./e2e-stories",
  outputDir: "test-results/stories",
  fullyParallel: true,
  // Rendering is CPU-bound; GitHub's ubuntu runners have 4 cores.
  workers: process.env.CI ? 4 : undefined,
  forbidOnly: !!process.env.CI,
  reporter: process.env.CI
    ? [
        ["html", { open: "never", outputFolder: "playwright-report/stories" }],
        ["github"],
        ["list"],
      ]
    : "list",
  use: {
    baseURL: `http://localhost:${PORT}`,
  },
  webServer: {
    command: `${build}pnpm exec vite preview --outDir ${staticDir} --port ${PORT} --strictPort`,
    url: `http://localhost:${PORT}/index.json`,
    reuseExistingServer: !process.env.CI,
    timeout: 180_000,
  },
  projects: [
    {
      name: "chromium",
      use: {
        ...devices["Desktop Chrome"],
        launchOptions: { executablePath: process.env.CHROMIUM_PATH },
      },
    },
  ],
});
