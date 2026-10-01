import { expect, test } from "./fixtures";

test("logs list shows seeded log lines", async ({ page, liveEnv }) => {
  await page.goto(`/logs?tenant=${liveEnv.tenant}&dataset=${liveEnv.dataset}`);
  await expect(page.getByText(liveEnv.seededService).first()).toBeVisible({
    timeout: 15_000,
  });
  await expect(page.getByText("Could not load")).toHaveCount(0);
});
