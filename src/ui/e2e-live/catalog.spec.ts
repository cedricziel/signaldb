import { expect, test } from "./fixtures";

test("catalog list shows the seeded service", async ({ page, liveEnv }) => {
  await page.goto(
    `/catalog/service?tenant=${liveEnv.tenant}&dataset=${liveEnv.dataset}`,
  );
  await expect(page.getByText(liveEnv.seededService)).toBeVisible({
    timeout: 15_000,
  });
  await expect(page.getByText("Could not load")).toHaveCount(0);
});
