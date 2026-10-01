import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter, Route, Routes, useLocation } from "react-router";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import {
  outletContextRoute,
  renderWithClient,
  stubFetchRoutes,
} from "../../test/render";
import { ManagementRoute } from "./ManagementRoute";

const WHOAMI_ADMIN = {
  user: {
    id: "user-1",
    email: "admin@acme.com",
    display_name: "Admin",
    is_instance_admin: false,
  },
  memberships: [{ tenant_id: "acme", role: "admin" }],
  tenant: { id: "acme", slug: "acme", name: "Acme Corp" },
  datasets: [{ id: "production", slug: "production", is_default: true }],
  default_dataset: "production",
};

const WHOAMI_VIEWER = {
  ...WHOAMI_ADMIN,
  memberships: [{ tenant_id: "acme", role: "viewer" }],
};

/** Reports the search string /overview (home) was reached with, so a
 * redirect that drops the tenant/dataset (a bare `/overview`) is
 * distinguishable from one that carries it via `crossSignalSearch`. */
function HomePage() {
  const location = useLocation();
  return (
    <div>
      Home page
      <span data-testid="home-search">{location.search}</span>
    </div>
  );
}

function renderManagementRoute(
  entries: string[] = ["/manage"],
  state: Partial<ExploreState> = {},
) {
  const contextState: ExploreState = {
    ...DEFAULT_STATE,
    tenant: "acme",
    dataset: "production",
    ...state,
  };
  return renderWithClient(
    <MemoryRouter initialEntries={entries}>
      <Routes>
        <Route element={outletContextRoute(contextState)}>
          <Route path="/manage" element={<ManagementRoute />} />
          <Route path="/overview" element={<HomePage />} />
          <Route path="/logs" element={<div>Logs page</div>} />
        </Route>
      </Routes>
    </MemoryRouter>,
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("ManagementRoute", () => {
  it("redirects non-admins home to /overview", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI_VIEWER }]);
    renderManagementRoute();
    expect(await screen.findByText("Home page")).toBeInTheDocument();
  });

  it("carries the tenant/dataset search along the non-admin redirect, like the close button does", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI_VIEWER }]);
    renderManagementRoute();
    await screen.findByText("Home page");
    expect(screen.getByTestId("home-search")).toHaveTextContent(
      "tenant=acme",
    );
  });

  it("shows an inline error on a non-401 whoami failure instead of redirecting home", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: { error: "boom" }, status: 500 },
    ]);
    renderManagementRoute();
    expect(await screen.findByRole("alert")).toHaveTextContent(/500/);
    expect(screen.queryByText("Home page")).not.toBeInTheDocument();
  });

  it("closes home to /overview (replace) when opened with no in-app history to go back to", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: "/memberships", body: [] },
    ]);
    renderManagementRoute(["/manage"]);
    await screen.findByRole("dialog", { name: "Manage tenant" });
    await userEvent.click(screen.getByRole("button", { name: "Close management" }));
    expect(await screen.findByText("Home page")).toBeInTheDocument();
  });

  it("closes via history back when reached through in-app navigation", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: "/memberships", body: [] },
    ]);
    // goBackOr reads the browser router's history index; the memory router
    // here doesn't write one, so stand in for an in-app entry behind /manage.
    window.history.replaceState({ idx: 1 }, "");
    try {
      renderManagementRoute(["/logs", "/manage"]);
      await screen.findByRole("dialog", { name: "Manage tenant" });
      await userEvent.click(
        screen.getByRole("button", { name: "Close management" }),
      );
      expect(await screen.findByText("Logs page")).toBeInTheDocument();
    } finally {
      window.history.replaceState(null, "");
    }
  });
});
