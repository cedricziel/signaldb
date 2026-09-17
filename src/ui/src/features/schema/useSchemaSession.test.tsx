// The schema hub's shared "who is looking, and what may they do" hook.
import { screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_STATE } from "../../lib/urlState";
import {
  outletContextRoute,
  renderWithClient,
  stubFetchRoutes,
} from "../../test/render";
import { useSchemaSession } from "./useSchemaSession";
import { WHOAMI_INSTANCE_ADMIN } from "./testFixtures";

afterEach(() => {
  vi.restoreAllMocks();
});

function Probe() {
  const session = useSchemaSession();
  return <div>isLoading: {String(session.isLoading)}</div>;
}

function renderProbe(tenant: string) {
  return renderWithClient(
    <MemoryRouter initialEntries={["/probe"]}>
      <Routes>
        <Route
          element={outletContextRoute({
            ...DEFAULT_STATE,
            tenant,
            dataset: "",
          })}
        >
          <Route path="/probe" element={<Probe />} />
        </Route>
      </Routes>
    </MemoryRouter>,
  );
}

describe("useSchemaSession", () => {
  it("stays loading while no tenant has resolved yet, rather than reading as settled", () => {
    // `useWhoami` disables its query when `tenant === ""` — that must not
    // read as "loaded, and not an admin" to a caller gating on isLoading.
    renderProbe("");
    expect(screen.getByText("isLoading: true")).toBeInTheDocument();
  });

  it("reflects the whoami query's own loading state once a tenant is set", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_INSTANCE_ADMIN },
    ]);
    renderProbe("acme");
    expect(await screen.findByText("isLoading: false")).toBeInTheDocument();
  });
});
