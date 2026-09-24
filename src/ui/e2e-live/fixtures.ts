// Shared fixture for the live suite: fails the test on any `/api/v1/*`
// response with status >= 400 (this is what would have caught PR #1695 —
// the entity detail page's slowest-traces query IR document naming a
// physical column) and on any uncaught page error.
import { test as base, expect } from "@playwright/test";

import { readLiveEnv, type LiveEnv } from "./env";

export const test = base.extend<{ liveEnv: LiveEnv }>({
  // eslint-disable-next-line no-empty-pattern -- Playwright fixture signature
  liveEnv: async ({}, use) => {
    await use(readLiveEnv());
  },

  page: async ({ page }, use) => {
    const apiFailures: string[] = [];
    const pageErrors: string[] = [];

    page.on("response", (response) => {
      const url = response.url();
      if (!url.includes("/api/v1/")) return;
      if (response.status() < 400) return;
      // Fire-and-forget: Playwright doesn't let a sync listener await body
      // reads reliably, so collect what we can without blocking the event.
      void (async () => {
        const request = response.request();
        const requestBody = request.postData() ?? "";
        const responseBody = await response.text().catch(() => "<unreadable>");
        apiFailures.push(
          `${request.method()} ${url} -> ${response.status()}\n` +
            `  request body: ${requestBody}\n` +
            `  response body: ${responseBody}`,
        );
      })();
    });

    page.on("pageerror", (error) => {
      pageErrors.push(error.stack ?? error.message);
    });

    await use(page);

    // Give in-flight response-body reads a moment to settle before asserting.
    await new Promise((r) => setTimeout(r, 100));

    expect(
      apiFailures,
      `one or more /api/v1/* requests failed:\n${apiFailures.join("\n\n")}`,
    ).toHaveLength(0);
    expect(
      pageErrors,
      `uncaught page error(s):\n${pageErrors.join("\n\n")}`,
    ).toHaveLength(0);
  },
});

export { expect };
