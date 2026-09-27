import { screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter, Route, Routes } from "react-router";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { SelectTenantRoute } from "./SelectTenantRoute";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("SelectTenantRoute", () => {
  it("sends a visitor without a session home to /overview", async () => {
    stubFetchRoutes([
      { match: "/ui/session", body: { error: "no session" }, status: 401 },
    ]);
    renderWithClient(
      <MemoryRouter initialEntries={["/select-tenant"]}>
        <Routes>
          <Route path="/select-tenant" element={<SelectTenantRoute />} />
          <Route path="/overview" element={<div>Home page</div>} />
          <Route path="/logs" element={<div>Logs page</div>} />
        </Routes>
      </MemoryRouter>,
    );
    expect(await screen.findByText("Home page")).toBeInTheDocument();
  });
});
