// Browser-level coverage for the explore UI's client-side routing (see
// openspec/changes/spa-url-navigation and src/lib/routes.tsx). Run against
// the production build with no live SignalDB backend — see
// playwright.config.ts for what that implies: these mock only the specific
// API calls a scenario needs and otherwise assert on URL/DOM structure, not
// on data content.
import { expect, test } from "@playwright/test";

const ADMIN_WHOAMI = {
  user: {
    id: "user-1",
    email: "admin@example.com",
    display_name: "Admin",
    is_instance_admin: true,
  },
  memberships: [{ tenant_id: "acme", role: "admin" }],
  tenant: { id: "acme", slug: "acme", name: "Acme Corp" },
  datasets: [{ id: "production", slug: "production", is_default: true }],
  default_dataset: "production",
};

async function json(route: import("@playwright/test").Route, body: unknown) {
  await route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify(body),
  });
}

test("/ redirects to /logs with the Logs tab selected", async ({ page }) => {
  await page.goto("/");
  await expect(page).toHaveURL(/\/logs$/);
  await expect(page.getByRole("tab", { name: "Logs" })).toHaveAttribute(
    "aria-selected",
    "true",
  );
});

test("switching signal tabs updates the path", async ({ page }) => {
  await page.goto("/logs");
  await page.getByRole("tab", { name: "Traces" }).click();
  await expect(page).toHaveURL(/\/traces$/);
  await expect(page.getByRole("tab", { name: "Traces" })).toHaveAttribute(
    "aria-selected",
    "true",
  );
});

test("an unknown path redirects to /logs, preserving the query string", async ({
  page,
}) => {
  await page.goto("/bogus?range=15m");
  await expect(page).toHaveURL(/\/logs\?range=15m$/);
});

test("/manage redirects unauthenticated visitors to /logs", async ({
  page,
}) => {
  // No mocks: whoami naturally fails without a backend, so this exercises
  // the same "not an admin" redirect path as an authenticated non-admin.
  await page.goto("/manage");
  await expect(page).toHaveURL(/\/logs$/);
});

test("an admin can open /manage and the back button returns them", async ({
  page,
}) => {
  await page.route("**/api/v1/whoami", (route) => json(route, ADMIN_WHOAMI));
  await page.route("**/api/v1/manage/tenants/*/api-keys*", (route) =>
    json(route, []),
  );
  await page.route("**/api/v1/manage/tenants/*/memberships*", (route) =>
    json(route, []),
  );

  await page.goto("/logs");
  await page.getByRole("link", { name: "Manage" }).click();

  await expect(page).toHaveURL(/\/manage$/);
  await expect(
    page.getByRole("dialog", { name: "Manage tenant" }),
  ).toBeVisible();

  await page.goBack();
  await expect(page).toHaveURL(/\/logs$/);
  await expect(
    page.getByRole("dialog", { name: "Manage tenant" }),
  ).not.toBeVisible();
});

test("/oauth/consent renders standalone, without the explore shell", async ({
  page,
}) => {
  await page.goto("/oauth/consent");
  await expect(page).toHaveURL(/\/oauth\/consent$/);
  // No signal tabs, no top bar — this route bypasses the shell entirely.
  await expect(page.getByRole("tablist", { name: "Signal" })).toHaveCount(0);
  await expect(
    page.getByRole("heading", { name: "Invalid authorization request" }),
  ).toBeVisible();
});
