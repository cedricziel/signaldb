// Browser-level coverage for the OIDC login flows (change: oidc-login, task
// 4.5). Run against the production build with no live SignalDB backend — see
// playwright.config.ts / navigation.spec.ts for what that implies: these mock
// only the specific API calls a scenario needs (login config, session,
// consent context) and otherwise assert on URL/DOM structure.
import { expect, test } from "@playwright/test";

const SSO_NAME = "Acme SSO";

/** A realistic `/oauth/consent` authorize query string — client_id,
 * redirect_uri, scope, state, and the PKCE challenge an MCP client would
 * actually send. */
const CONSENT_QUERY =
  "client_id=claude-desktop&" +
  "redirect_uri=https%3A%2F%2Fclaude.ai%2Fapi%2Fmcp%2Fcallback&" +
  "code_challenge=E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM&" +
  "code_challenge_method=S256&" +
  "scope=traces%3Aread%20logs%3Aread&" +
  "state=xyz789&" +
  "resource=https%3A%2F%2Fsignaldb.example%2F";
const CONSENT_PATH = `/oauth/consent?${CONSENT_QUERY}`;

async function json(route: import("@playwright/test").Route, body: unknown) {
  await route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify(body),
  });
}

async function mockLoginConfig(
  page: import("@playwright/test").Page,
  passwordEnabled: boolean,
) {
  await page.route("**/ui/session/config", (route) =>
    json(route, {
      password_enabled: passwordEnabled,
      oidc: { name: SSO_NAME },
    }),
  );
}

/** No session: `consentContext()` 401s, which is how `ConsentView` decides to
 * render the login step (see `features/consent/ConsentView.tsx`). */
async function mockUnauthenticatedConsent(page: import("@playwright/test").Page) {
  await page.route("**/oauth/consent/context*", (route) =>
    route.fulfill({ status: 401, contentType: "application/json", body: "{}" }),
  );
}

test("consent's SSO redirect carries the full consent URL, authorize params included", async ({
  page,
}) => {
  await mockUnauthenticatedConsent(page);
  await mockLoginConfig(page, true);

  await page.goto(CONSENT_PATH);

  await expect(
    page.getByText("Sign in to authorize this application."),
  ).toBeVisible();
  const ssoLink = page.getByRole("link", { name: `Continue with ${SSO_NAME}` });
  await expect(ssoLink).toBeVisible();
  await expect(ssoLink).toHaveAttribute(
    "href",
    `/ui/session/oidc/start?redirect=${encodeURIComponent(CONSENT_PATH)}`,
  );
});

test("an SSO-only instance (password_enabled: false) hides the password form on /login and on consent", async ({
  page,
}) => {
  await mockLoginConfig(page, false);
  await page.route("**/ui/session", (route) =>
    route.request().method() === "GET"
      ? route.fulfill({ status: 401, contentType: "application/json", body: "{}" })
      : route.continue(),
  );

  await page.goto("/login");
  await expect(
    page.getByRole("link", { name: `Continue with ${SSO_NAME}` }),
  ).toBeVisible();
  await expect(page.getByLabel("Email")).toHaveCount(0);
  await expect(page.getByLabel("Password")).toHaveCount(0);

  await mockUnauthenticatedConsent(page);
  await page.goto(CONSENT_PATH);
  await expect(
    page.getByRole("link", { name: `Continue with ${SSO_NAME}` }),
  ).toBeVisible();
  await expect(page.getByLabel("Email")).toHaveCount(0);
  await expect(page.getByLabel("Password")).toHaveCount(0);
});

test("landing on a signal page with a session and a sole membership resolves the tenant/dataset into the URL", async ({
  page,
  context,
}) => {
  await context.addCookies([
    {
      name: "signaldb_session",
      value: "e2e-test-session-token",
      url: "http://localhost:4173",
    },
  ]);
  await page.route("**/ui/session", (route) =>
    route.request().method() === "GET"
      ? json(route, {
          user: {
            id: "user-1",
            email: "user@example.com",
            display_name: null,
            is_instance_admin: false,
          },
          memberships: [
            { tenant_id: "acme", name: "Acme Corp", role: "member" },
          ],
          tenant: "acme",
          dataset: "default",
        })
      : route.continue(),
  );

  await page.goto("/traces");

  await expect(page).toHaveURL(/\/traces\?tenant=acme&dataset=default$/);
});
