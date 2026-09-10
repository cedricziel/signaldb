import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BrowserRouter, Route, Routes, useLocation } from "react-router";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { LoginRoute } from "./LoginRoute";

const mockNavigate = vi.fn();

vi.mock("react-router", async (importOriginal) => {
  const mod = await importOriginal<typeof import("react-router")>();
  return {
    ...mod,
    useNavigate: () => mockNavigate,
  };
});

// `/ui/session` (currentSession/createSession) and `/ui/session/config`
// (loginConfig) share a URL prefix — anchor the base path so a stub for one
// doesn't also catch the other (see test/render.tsx's stubFetchRoutes docs).
const SESSION = /\/ui\/session$/;

afterEach(() => {
  vi.unstubAllGlobals();
  vi.clearAllMocks();
  window.history.replaceState({}, "", "/login");
});

/** `LoginRoute` reads `?redirect=`/`?error=` via `useSearchParams()`, which
 * needs real Router context synced to the browser URL (not `MemoryRouter`'s
 * detached history) — its own `?error` strip writes through `window.history`,
 * and tests assert on `window.location.search` afterwards (see App.test.tsx
 * for the same `BrowserRouter` + `window.history.replaceState` pattern). */
function renderLoginRoute(path: string) {
  window.history.replaceState({}, "", path);
  return renderWithClient(
    <BrowserRouter>
      <LoginRoute />
    </BrowserRouter>,
  );
}

/** Same as {@link renderLoginRoute}, but with a catch-all route rendering
 * `<LocationProbe>` so a real `<Navigate>` inside `LoginRoute` (the
 * already-authenticated case) can be observed — it uses the real
 * react-router navigation, not the mocked `useNavigate` above. */
function renderLoginRouteWithRouting(path: string) {
  window.history.replaceState({}, "", path);
  return renderWithClient(
    <BrowserRouter>
      <Routes>
        <Route path="/login" element={<LoginRoute />} />
        <Route path="*" element={<LocationProbe />} />
      </Routes>
    </BrowserRouter>,
  );
}

function LocationProbe() {
  const location = useLocation();
  return <div data-testid="location">{location.pathname}</div>;
}

const acmeMembership = {
  tenant_id: "acme",
  name: "Acme",
  role: "admin" as const,
};
const globexMembership = {
  tenant_id: "globex",
  name: "Globex Corp",
  role: "member" as const,
};

/** Stubs an unauthenticated currentSession() (401), a login-configuration
 * probe, and a successful POST /ui/session resolving to tenant "acme" /
 * dataset "prod". */
function stubSuccessfulLogin() {
  return stubFetchRoutes([
    { match: SESSION, method: "GET", body: {}, status: 401 },
    {
      match: "/ui/session/config",
      body: { password_enabled: true, oidc: null },
    },
    {
      match: SESSION,
      method: "POST",
      body: { tenant: "acme", dataset: "prod", memberships: [acmeMembership] },
    },
  ]);
}

/** Fills in and submits the sign-in form rendered by LoginMethods. Waits for
 * the email field specifically since the loading state ("Checking
 * session…") renders inside the same card. */
async function signIn() {
  await screen.findByLabelText("Email");
  await userEvent.type(screen.getByLabelText("Email"), "alice@example.com");
  await userEvent.type(screen.getByLabelText("Password"), "secret");
  await userEvent.click(screen.getByRole("button", { name: "Sign in" }));
}

describe("LoginRoute", () => {
  it("shows a loading state, not the form, while the auth check is pending", () => {
    stubFetchRoutes([{ match: SESSION, method: "GET", body: {}, status: 401 }]);
    renderLoginRoute("/login");

    expect(screen.getByText("Checking session…")).toBeInTheDocument();
    expect(screen.queryByLabelText("Email")).not.toBeInTheDocument();
  });

  it("renders as a standalone page: no dialog, brand mark, an h1, and a Docs footer link", async () => {
    stubFetchRoutes([{ match: SESSION, method: "GET", body: {}, status: 401 }]);
    renderLoginRoute("/login");

    expect(
      await screen.findByRole("heading", { level: 1, name: "Sign in" }),
    ).toBeInTheDocument();
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(screen.getByText("SignalDB")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Docs" })).toHaveAttribute(
      "href",
      "https://signaldb.dev/docs",
    );
  });

  it("shows the sign-in form for an unauthenticated visitor", async () => {
    stubFetchRoutes([{ match: SESSION, method: "GET", body: {}, status: 401 }]);
    renderLoginRoute("/login");

    expect(await screen.findByLabelText("Email")).toBeInTheDocument();
    expect(mockNavigate).not.toHaveBeenCalled();
  });

  it("navigates to /logs with tenant/dataset query params on success, when no redirect param was given", async () => {
    const fetchFn = stubSuccessfulLogin();
    renderLoginRoute("/login");

    await signIn();

    await waitFor(() =>
      expect(mockNavigate).toHaveBeenCalledWith(
        "/logs?tenant=acme&dataset=prod",
        { replace: true },
      ),
    );
    expect(fetchFn).toHaveBeenCalled();
  });

  it("honors a redirect param", async () => {
    stubSuccessfulLogin();
    renderLoginRoute("/login?redirect=%2Ftraces%3Frange%3D15m");

    await signIn();

    await waitFor(() =>
      expect(mockNavigate).toHaveBeenCalledWith(
        "/traces?range=15m&tenant=acme&dataset=prod",
        { replace: true },
      ),
    );
  });

  it.each([["//evil.com"], ["https://evil.com"], ["/\\evil.com"], ["/login"]])(
    "falls back to /logs when the redirect param %s is unsafe",
    async (unsafe) => {
      stubSuccessfulLogin();
      renderLoginRoute(`/login?redirect=${encodeURIComponent(unsafe)}`);

      await signIn();

      await waitFor(() =>
        expect(mockNavigate).toHaveBeenCalledWith(
          "/logs?tenant=acme&dataset=prod",
          { replace: true },
        ),
      );
    },
  );

  it("navigates straight to the redirect target when already authenticated", async () => {
    stubFetchRoutes([
      {
        match: SESSION,
        method: "GET",
        body: {
          user: {
            id: "1",
            email: "alice@example.com",
            display_name: "Alice",
            is_instance_admin: false,
          },
          tenant: "acme",
          dataset: "prod",
          memberships: [acmeMembership],
        },
      },
    ]);
    renderLoginRouteWithRouting("/login?redirect=%2Ftraces");

    await waitFor(() =>
      expect(screen.getByTestId("location")).toHaveTextContent("/traces"),
    );
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("navigates directly when an SSO landing resolves a single membership", async () => {
    stubFetchRoutes([
      {
        match: SESSION,
        method: "GET",
        body: {
          user: {
            id: "1",
            email: "alice@example.com",
            display_name: "Alice",
            is_instance_admin: false,
          },
          tenant: "acme",
          dataset: "prod",
          memberships: [acmeMembership],
        },
      },
    ]);
    renderLoginRouteWithRouting("/login");

    await waitFor(() =>
      expect(screen.getByTestId("location")).toHaveTextContent("/logs"),
    );
  });

  it("shows the tenant picker after a password login resolves to several memberships, and navigates once one is picked", async () => {
    stubFetchRoutes([
      { match: SESSION, method: "GET", body: {}, status: 401 },
      {
        match: "/ui/session/config",
        body: { password_enabled: true, oidc: null },
      },
      {
        match: SESSION,
        method: "POST",
        body: {
          tenant: null,
          dataset: null,
          memberships: [acmeMembership, globexMembership],
        },
      },
      {
        match: "/api/v1/whoami",
        body: {
          memberships: [{ tenant_id: "globex", role: "member" }],
          tenant: { id: "globex", slug: "globex", name: "Globex Corp" },
          datasets: [{ id: "main", slug: "main", is_default: true }],
          default_dataset: "main",
        },
      },
    ]);
    renderLoginRoute("/login");

    await signIn();

    expect(
      await screen.findByRole("heading", { level: 1, name: "Choose a tenant" }),
    ).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: /Globex Corp/ }));

    await waitFor(() =>
      expect(mockNavigate).toHaveBeenCalledWith(
        "/logs?tenant=globex&dataset=main",
        { replace: true },
      ),
    );
  });

  it("shows the tenant picker for an SSO landing with several memberships, and navigates once one is picked", async () => {
    stubFetchRoutes([
      {
        match: SESSION,
        method: "GET",
        body: {
          user: {
            id: "1",
            email: "alice@example.com",
            display_name: "Alice",
            is_instance_admin: false,
          },
          tenant: null,
          dataset: null,
          memberships: [acmeMembership, globexMembership],
        },
      },
      {
        match: "/api/v1/whoami",
        body: {
          memberships: [{ tenant_id: "globex", role: "member" }],
          tenant: { id: "globex", slug: "globex", name: "Globex Corp" },
          datasets: [{ id: "main", slug: "main", is_default: true }],
          default_dataset: "main",
        },
      },
    ]);
    renderLoginRoute("/login");

    expect(
      await screen.findByRole("heading", { level: 1, name: "Choose a tenant" }),
    ).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: /Globex Corp/ }));

    await waitFor(() =>
      expect(mockNavigate).toHaveBeenCalledWith(
        "/logs?tenant=globex&dataset=main",
        { replace: true },
      ),
    );
  });

  it("shows a no-access message for an SSO landing with no memberships, and does not navigate", async () => {
    stubFetchRoutes([
      {
        match: SESSION,
        method: "GET",
        body: {
          user: {
            id: "1",
            email: "alice@example.com",
            display_name: "Alice",
            is_instance_admin: false,
          },
          tenant: null,
          dataset: null,
          memberships: [],
        },
      },
    ]);
    renderLoginRoute("/login");

    expect(
      await screen.findByRole("heading", {
        level: 1,
        name: "No tenant access yet",
      }),
    ).toBeInTheDocument();
    expect(screen.getByText(/alice@example.com/)).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Sign out" }),
    ).toBeInTheDocument();
    expect(mockNavigate).not.toHaveBeenCalled();
  });

  it("shows a generic alert for a failed SSO round trip, then strips ?error from the URL", async () => {
    stubFetchRoutes([{ match: SESSION, method: "GET", body: {}, status: 401 }]);
    renderLoginRoute("/login?error=sso_failed&redirect=%2Flogs");

    expect(await screen.findByRole("alert")).toHaveTextContent(
      "Single sign-on failed. Try again, or sign in with your email and password.",
    );
    await waitFor(() =>
      expect(window.location.search).toBe("?redirect=%2Flogs"),
    );
  });

  it("shows a no-membership alert for ?error=no_membership, and keeps redirect", async () => {
    stubFetchRoutes([{ match: SESSION, method: "GET", body: {}, status: 401 }]);
    renderLoginRoute("/login?error=no_membership&redirect=%2Flogs");

    expect(await screen.findByRole("alert")).toHaveTextContent(
      "Your account has no tenant access yet. Ask a tenant admin to add you, then sign in again.",
    );
    await waitFor(() =>
      expect(window.location.search).toBe("?redirect=%2Flogs"),
    );
  });

  it("ignores an unrecognized ?error code", async () => {
    stubFetchRoutes([{ match: SESSION, method: "GET", body: {}, status: 401 }]);
    renderLoginRoute("/login?error=whatever");

    await screen.findByLabelText("Email");
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });
});
