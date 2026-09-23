// The regression this whole suite exists for: PR #1695 fixed the entity
// detail page's "Slowest traces" section sending a Query IR document that
// ordered by `duration_nanos` — a physical column the server rejects
// ("names a physical column or storage detail; use a logical name"). Every
// pre-existing spec mocks the server, so the rejection never surfaced. This
// spec hits a real backend and, via the shared `/api/v1/*` >=400 fixture,
// fails loudly if that query — or any other on this page — is rejected.
import { expect, test } from "./fixtures";

test("entity detail page renders data for a seeded service", async ({
  page,
  liveEnv,
}) => {
  await page.goto(
    `/catalog/service/${liveEnv.seededService}?tenant=${liveEnv.tenant}&dataset=${liveEnv.dataset}`,
  );

  await expect(
    page.getByRole("heading", { name: liveEnv.seededService }),
  ).toBeVisible({ timeout: 15_000 });

  // Operations, error groups, dependency breakdown, and slowest traces all
  // load without hitting the "Could not load" error path — each is a
  // separate Query IR document sent to the real backend.
  await expect(page.getByText("Operations")).toBeVisible({
    timeout: 15_000,
  });
  await expect(page.getByText("Slowest traces")).toBeVisible();
  // The query that PR #1695 fixed actually returned data: the empty-state
  // copy is absent.
  await expect(page.getByText("No traces in this range")).toHaveCount(0, {
    timeout: 15_000,
  });
  await expect(page.getByText("Could not load")).toHaveCount(0);
});
