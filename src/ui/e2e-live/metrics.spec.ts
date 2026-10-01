import { expect, test } from "./fixtures";

test("metrics view loads without error against seeded data", async ({
  page,
  liveEnv,
}) => {
  await page.goto(
    `/metrics?tenant=${liveEnv.tenant}&dataset=${liveEnv.dataset}`,
  );
  await expect(page.getByText("Could not load")).toHaveCount(0, {
    timeout: 15_000,
  });
});
