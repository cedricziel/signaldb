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

/** A page link in the sidebar's page list. */
function navLink(page: import("@playwright/test").Page, name: string) {
  return page
    .getByRole("navigation", { name: "Pages" })
    .getByRole("link", { name, exact: true });
}

test("/ lands on the Overview, current in the sidebar", async ({ page }) => {
  await page.goto("/");
  await expect(page).toHaveURL(/\/overview$/);
  await expect(navLink(page, "Overview")).toHaveAttribute(
    "aria-current",
    "page",
  );
  await expect(
    page.getByRole("button", { name: /Setup checklist/ }),
  ).toBeVisible();
});

test("the palette's setup action opens the checklist on the Overview", async ({
  page,
}) => {
  await page.goto("/logs");
  await expect(navLink(page, "Logs")).toBeVisible();
  await page.keyboard.press("ControlOrMeta+k");
  await page.keyboard.type("setup checklist");
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(/\/overview$/);
  await expect(
    page.getByRole("dialog", { name: "Setup checklist" }),
  ).toBeVisible();
});

test("switching pages in the sidebar updates the path", async ({ page }) => {
  await page.goto("/logs");
  await navLink(page, "Traces").click();
  await expect(page).toHaveURL(/\/traces$/);
  await expect(navLink(page, "Traces")).toHaveAttribute("aria-current", "page");
});

test("⌘K opens the command palette and Enter navigates", async ({ page }) => {
  await page.goto("/logs");
  await expect(navLink(page, "Logs")).toBeVisible();
  await page.keyboard.press("ControlOrMeta+k");
  const palette = page.getByRole("dialog", { name: "Command palette" });
  await expect(
    palette.getByRole("searchbox", { name: "Search" }),
  ).toBeFocused();
  await page.keyboard.type("metr");
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(/\/metrics$/);
  await expect(palette).toHaveCount(0);
});

test("the sidebar gives way to a top bar and drawer on a phone", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 800 });
  await page.goto("/logs");
  await expect(
    page.getByRole("complementary", { name: "Main navigation" }),
  ).toHaveCount(0);
  await page.getByRole("button", { name: "Open navigation" }).click();
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("link", { name: "Traces", exact: true })
    .click();
  await expect(page).toHaveURL(/\/traces$/);
  await expect(
    page.getByRole("navigation", { name: "Main navigation" }),
  ).toHaveCount(0);
});

test("an unknown path redirects to /logs, preserving the query string", async ({
  page,
}) => {
  await page.goto("/bogus?range=15m");
  await expect(page).toHaveURL(/\/logs\?range=15m$/);
});

test("/ redirects to /overview, preserving the query string", async ({
  page,
}) => {
  await page.goto("/?tenant=homelab&dataset=default");
  await expect(page).toHaveURL(/\/overview\?tenant=homelab&dataset=default$/);
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
  await page.route("**/api/v1/tenants/*/api-keys*", (route) => json(route, []));
  await page.route("**/api/v1/tenants/*/memberships*", (route) =>
    json(route, []),
  );

  // The shell only asks whoami once a tenant is known (a tenant-less
  // request is a 401 by design), so the admin gate needs the context in
  // the URL; the sticky context then rides along on the bare /manage link.
  await page.goto("/logs?tenant=acme&dataset=production");
  await page.getByRole("link", { name: "Manage" }).click();

  await expect(page).toHaveURL(/\/manage(\?.*)?$/);
  await expect(
    page.getByRole("dialog", { name: "Manage tenant" }),
  ).toBeVisible();

  await page.goBack();
  await expect(page).toHaveURL(/\/logs(\?.*)?$/);
  await expect(
    page.getByRole("dialog", { name: "Manage tenant" }),
  ).not.toBeVisible();
});

test("/oauth/consent renders standalone, without the explore shell", async ({
  page,
}) => {
  await page.goto("/oauth/consent");
  await expect(page).toHaveURL(/\/oauth\/consent$/);
  // No app navigation — this route bypasses the shell entirely.
  await expect(
    page.getByRole("complementary", { name: "Main navigation" }),
  ).toHaveCount(0);
  await expect(
    page.getByRole("heading", { name: "Invalid authorization request" }),
  ).toBeVisible();
});

test("/login renders standalone, without the explore shell", async ({
  page,
}) => {
  // No mocks: currentSession() naturally fails without a backend, so the
  // sign-in form renders.
  await page.goto("/login");
  await expect(page).toHaveURL(/\/login$/);
  // No app navigation — this route bypasses the shell entirely.
  await expect(
    page.getByRole("complementary", { name: "Main navigation" }),
  ).toHaveCount(0);
  // The page is a standalone destination, not a modal dialog (design
  // decision 1): a level-one "Sign in" heading, no role="dialog".
  await expect(
    page.getByRole("heading", { level: 1, name: "Sign in" }),
  ).toBeVisible();
  await expect(page.getByRole("dialog")).toHaveCount(0);
});

test("/login offers SSO when the login-configuration probe reports a provider", async ({
  page,
}) => {
  await page.route("**/ui/session/config", (route) =>
    json(route, { password_enabled: true, oidc: { name: "Acme" } }),
  );
  await page.route("**/ui/session", (route) =>
    route.request().method() === "GET"
      ? route.fulfill({ status: 401, body: "{}" })
      : route.continue(),
  );

  await page.goto("/login");

  const ssoLink = page.getByRole("link", { name: "Continue with Acme" });
  await expect(ssoLink).toBeVisible();
  await expect(ssoLink).toHaveAttribute("href", /^\/ui\/session\/oidc\/start/);
});

test("/login stays usable at a narrow (360x740) viewport", async ({ page }) => {
  await page.setViewportSize({ width: 360, height: 740 });
  await page.goto("/login");

  const submit = page.getByRole("button", { name: "Sign in" });
  await expect(submit).toBeVisible();

  const scrollWidth = await page.evaluate(
    () => document.documentElement.scrollWidth,
  );
  expect(scrollWidth).toBeLessThanOrEqual(360);

  const box = await submit.boundingBox();
  expect(box).not.toBeNull();
  expect(box!.x).toBeGreaterThanOrEqual(0);
  expect(box!.x + box!.width).toBeLessThanOrEqual(360);
});
