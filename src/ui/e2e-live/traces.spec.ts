import { expect, test } from "./fixtures";

test("traces list shows seeded groups and a group can be opened", async ({
  page,
  liveEnv,
}) => {
  await page.goto(
    `/traces?tenant=${liveEnv.tenant}&dataset=${liveEnv.dataset}`,
  );
  const firstGroup = page.getByRole("row").nth(1).getByRole("button").first();
  await expect(firstGroup).toBeVisible({ timeout: 15_000 });
  await expect(page.getByText("Could not load")).toHaveCount(0);

  await firstGroup.click();
  await expect(page.getByText("Could not load")).toHaveCount(0);
});
