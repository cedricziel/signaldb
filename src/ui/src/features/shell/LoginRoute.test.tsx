import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter, Route, Routes, useLocation } from "react-router";
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

afterEach(() => {
  vi.unstubAllGlobals();
  vi.clearAllMocks();
});

function goto(path: string) {
  window.history.replaceState({}, "", path);
}

/** Renders the resolved pathname so a `<Navigate>` inside `LoginRoute` (the
 * already-authenticated case) can be asserted on — it uses the real
 * react-router navigation, not the mocked `useNavigate` above. */
function LocationProbe() {
  const location = useLocation();
  return <div data-testid="location">{location.pathname}</div>;
}

/** Stubs an unauthenticated whoami() followed by a successful /ui/session
 * login resolving to tenant "acme" / dataset "prod". */
function stubSuccessfulLogin() {
  return stubFetchRoutes([
    { match: "/api/v1/whoami", body: {}, status: 401 },
    {
      match: "/ui/session",
      body: {
        tenant: "acme",
        dataset: "prod",
        memberships: [{ tenant_id: "acme", name: "Acme", role: "admin" }],
      },
    },
  ]);
}

/** Fills in and submits the sign-in form rendered by LoginPanel. Waits for
 * the email field specifically (not just any "Sign in"-labeled dialog) since
 * the loading state shown while the whoami check is in flight uses the same
 * dialog label. */
async function signIn() {
  await screen.findByLabelText("Email");
  await userEvent.type(screen.getByLabelText("Email"), "alice@example.com");
  await userEvent.type(screen.getByLabelText("Password"), "secret");
  await userEvent.click(screen.getByRole("button", { name: "Sign in" }));
}

describe("LoginRoute", () => {
  it("shows a loading state, not the form, while the auth check is pending", () => {
    goto("/login");
    stubFetchRoutes([{ match: "/api/v1/whoami", body: {}, status: 401 }]);
    renderWithClient(<LoginRoute />);

    expect(screen.getByText("Checking session…")).toBeInTheDocument();
    expect(screen.queryByLabelText("Email")).not.toBeInTheDocument();
  });

  it("shows the sign-in form for an unauthenticated visitor", async () => {
    goto("/login");
    stubFetchRoutes([{ match: "/api/v1/whoami", body: {}, status: 401 }]);
    renderWithClient(<LoginRoute />);

    expect(await screen.findByLabelText("Email")).toBeInTheDocument();
    expect(
      screen.getByRole("dialog", { name: "Sign in" }),
    ).toBeInTheDocument();
    expect(mockNavigate).not.toHaveBeenCalled();
  });

  it("navigates to /logs with tenant/dataset query params on success, when no redirect param was given", async () => {
    goto("/login");
    const fetchFn = stubSuccessfulLogin();
    renderWithClient(<LoginRoute />);

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
    goto("/login?redirect=%2Ftraces%3Frange%3D15m");
    stubSuccessfulLogin();
    renderWithClient(<LoginRoute />);

    await signIn();

    await waitFor(() =>
      expect(mockNavigate).toHaveBeenCalledWith(
        "/traces?range=15m&tenant=acme&dataset=prod",
        { replace: true },
      ),
    );
  });

  it.each([["//evil.com"], ["https://evil.com"], ["/\\evil.com"]])(
    "falls back to /logs when the redirect param %s is unsafe",
    async (unsafe) => {
      goto(`/login?redirect=${encodeURIComponent(unsafe)}`);
      stubSuccessfulLogin();
      renderWithClient(<LoginRoute />);

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
    goto("/login?redirect=%2Ftraces");
    stubFetchRoutes([
      {
        match: "/api/v1/whoami",
        body: {
          user: {
            id: "1",
            email: "alice@example.com",
            display_name: "Alice",
            is_instance_admin: false,
          },
          memberships: [{ tenant_id: "acme", role: "admin" }],
          tenant: { id: "acme", slug: "acme", name: "Acme" },
          datasets: [{ id: "prod", slug: "prod", is_default: true }],
          default_dataset: "prod",
        },
      },
    ]);
    renderWithClient(
      <MemoryRouter initialEntries={["/login"]}>
        <Routes>
          <Route path="/login" element={<LoginRoute />} />
          <Route path="*" element={<LocationProbe />} />
        </Routes>
      </MemoryRouter>,
    );

    await waitFor(() =>
      expect(screen.getByTestId("location")).toHaveTextContent("/traces"),
    );
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });
});
