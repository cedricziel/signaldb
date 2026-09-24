// The regression this suite exists for: PR #1695 fixed the entity detail
// page's "Slowest traces" section ordering by `duration_nanos`, a physical
// column the server rejects. The shared fixture fails this spec on any
// rejected query on the page.
import { expect, test } from "./fixtures";

test("entity detail page renders data for a seeded service", async ({
  page,
  liveEnv,
}) => {
  // Open the entity from the catalog list: a service's identity is
  // name + namespace, and the list's link carries both.
  await page.goto(
    `/catalog/service?tenant=${liveEnv.tenant}&dataset=${liveEnv.dataset}`,
  );
  await page
    .getByRole("button", { name: liveEnv.seededService, exact: true })
    .or(page.getByRole("link", { name: liveEnv.seededService, exact: true }))
    .first()
    .click({ timeout: 15_000 });

  await expect(page.getByText("Operations", { exact: true })).toBeVisible({
    timeout: 15_000,
  });
  await expect(page.getByText("Slowest traces", { exact: true })).toBeVisible();
  // KPIs and the operations table load from the seeded spans.
  await expect(page.locator(".entity-last-seen")).toBeVisible({
    timeout: 15_000,
  });
  await expect(page.getByText(/^[1-9]\d* operations?, by rate$/)).toBeVisible();
  await expect(page.getByText("No matching spans in this window.")).toHaveCount(
    0,
  );
  await expect(page.getByText("Could not load")).toHaveCount(0);
});
