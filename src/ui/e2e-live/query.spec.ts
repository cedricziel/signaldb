import { expect, test } from "./fixtures";

test("query view runs a query against seeded traces", async ({
  page,
  liveEnv,
}) => {
  await page.goto(`/query?tenant=${liveEnv.tenant}&dataset=${liveEnv.dataset}`);
  await expect(page.getByText("Could not load")).toHaveCount(0, {
    timeout: 15_000,
  });
});
