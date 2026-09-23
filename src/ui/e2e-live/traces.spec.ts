import { expect, test } from "./fixtures";

test("traces list shows seeded spans and a trace can be opened", async ({
  page,
  liveEnv,
}) => {
  await page.goto(
    `/traces?tenant=${liveEnv.tenant}&dataset=${liveEnv.dataset}`,
  );
  await expect(page.getByText(liveEnv.seededService).first()).toBeVisible({
    timeout: 15_000,
  });
  await expect(page.getByText("Could not load")).toHaveCount(0);

  await page.getByText(liveEnv.seededService).first().click();
  await expect(page.getByText("Could not load")).toHaveCount(0);
});
