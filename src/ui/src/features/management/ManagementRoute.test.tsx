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

/** Reports the search string /logs was reached with, so a redirect that
 * drops the tenant/dataset (a bare `/logs`) is distinguishable from one that
 * carries it via `crossSignalSearch`. */
function LogsPage() {
  const location = useLocation();
  return (
    <div>
      Logs page
      <span data-testid="logs-search">{location.search}</span>
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
          <Route path="/logs" element={<LogsPage />} />
        </Route>
      </Routes>
    </MemoryRouter>,
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("ManagementRoute", () => {
  it("redirects non-admins to /logs", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI_VIEWER }]);
    renderManagementRoute();
    expect(await screen.findByText("Logs page")).toBeInTheDocument();
  });

  it("carries the tenant/dataset search along the non-admin redirect, like the close button does", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI_VIEWER }]);
    renderManagementRoute();
    await screen.findByText("Logs page");
    expect(screen.getByTestId("logs-search")).toHaveTextContent(
      "tenant=acme",
    );
  });

  it("shows an inline error on a non-401 whoami failure instead of redirecting to /logs", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: { error: "boom" }, status: 500 },
    ]);
    renderManagementRoute();
    expect(await screen.findByRole("alert")).toHaveTextContent(/500/);
    expect(screen.queryByText("Logs page")).not.toBeInTheDocument();
  });

  it("closes to /logs (replace) when opened with no in-app history to go back to", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: "/memberships", body: [] },
    ]);
    renderManagementRoute(["/manage"]);
    await screen.findByRole("dialog", { name: "Manage tenant" });
    await userEvent.click(screen.getByRole("button", { name: "Close management" }));
    expect(await screen.findByText("Logs page")).toBeInTheDocument();
  });

  it("closes via history back when reached through in-app navigation", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: "/memberships", body: [] },
    ]);
    renderManagementRoute(["/logs", "/manage"]);
    await screen.findByRole("dialog", { name: "Manage tenant" });
    await userEvent.click(screen.getByRole("button", { name: "Close management" }));
    expect(await screen.findByText("Logs page")).toBeInTheDocument();
  });
});
