import { screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter, Route, Routes } from "react-router";
import { connectionInfoBody } from "../../test/connectionInfo";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import {
  outletContextRoute,
  renderWithClient,
  stubFetchRoutes,
} from "../../test/render";
import { InstrumentationRoute } from "./InstrumentationRoute";

function renderInstrumentationRoute(state: Partial<ExploreState> = {}) {
  const contextState: ExploreState = {
    ...DEFAULT_STATE,
    tenant: "acme",
    dataset: "production",
    ...state,
  };
  return renderWithClient(
    <MemoryRouter initialEntries={["/instrumentation"]}>
      <Routes>
        <Route element={outletContextRoute(contextState)}>
          <Route
            path="/instrumentation"
            element={<InstrumentationRoute />}
          />
          <Route path="/logs" element={<div>Logs page</div>} />
        </Route>
      </Routes>
    </MemoryRouter>,
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

const WHOAMI = {
  user: {
    id: "user-1",
    email: "alice@example.com",
    display_name: "Alice",
    is_instance_admin: false,
  },
  memberships: [{ tenant_id: "acme", role: "viewer" }],
  tenant: { id: "acme", slug: "acme", name: "Acme Corp" },
  datasets: [{ id: "production", slug: "production", is_default: true }],
  default_dataset: "production",
};

describe("InstrumentationRoute", () => {
  it("renders for any authenticated user, admin or not", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI },
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    renderInstrumentationRoute();
    expect(
      await screen.findByRole("heading", { name: "Send data" }),
    ).toBeInTheDocument();
  });

  it("does not issue a whoami request without a tenant in state", async () => {
    const fetchMock = stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI },
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    renderInstrumentationRoute({ tenant: "" });
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(
      fetchMock.mock.calls.some((call) =>
        String(call[0]).includes("/api/v1/whoami"),
      ),
    ).toBe(false);
  });

  it("shows an inline error on a non-401 whoami failure instead of redirecting to /logs", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: { error: "boom" }, status: 500 },
    ]);
    renderInstrumentationRoute();
    expect(await screen.findByRole("alert")).toHaveTextContent(/500/);
    expect(screen.queryByText("Logs page")).not.toBeInTheDocument();
  });
});
