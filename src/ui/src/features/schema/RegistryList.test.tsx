import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter, Outlet, Route, Routes } from "react-router";
import { setTenantContext } from "../../api/http";
import { DEFAULT_STATE } from "../../lib/urlState";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { RegistryList } from "./RegistryList";
import {
  EMPTY_RESOLUTION,
  K8S_POD_ENTITY_RESOLUTION,
  REGISTRIES,
  SERVICE_NAME_RESOLUTION,
  shellOutlet,
  WHOAMI_MEMBER,
  WHOAMI_TENANT_ADMIN,
} from "./testFixtures";

function renderList(tenant = "acme") {
  setTenantContext({ tenant, dataset: "" });
  return renderWithClient(
    <MemoryRouter initialEntries={["/schema/conventions"]}>
      <Routes>
        <Route element={shellOutlet(tenant)}>
          <Route path="/schema/conventions" element={<RegistryList />} />
          <Route
            path="/schema/conventions/new"
            element={<div>Editor page</div>}
          />
          <Route
            path="/schema/conventions/:ns/:version"
            element={<div>Browser page</div>}
          />
        </Route>
      </Routes>
    </MemoryRouter>,
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
  setTenantContext({ tenant: "", dataset: "" });
});

describe("RegistryList", () => {
  it("lists registries with source, counts and read-only marker on bundled", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_MEMBER },
      { match: "/api/v1/schema/registries", body: REGISTRIES },
    ]);
    renderList();

    const otelRow = (await screen.findByText("otel")).closest("tr")!;
    expect(otelRow).toHaveTextContent("1.43.0");
    expect(otelRow).toHaveTextContent("bundled");
    expect(otelRow).toHaveTextContent("921");
    expect(otelRow).toHaveTextContent("40");
    expect(otelRow).toHaveTextContent("310");
    expect(within(otelRow).getByLabelText("read-only")).toBeInTheDocument();

    const acmeRow = screen.getByText("acme").closest("tr")!;
    expect(acmeRow).toHaveTextContent("custom");
    expect(within(acmeRow).queryByLabelText("read-only")).toBeNull();

    expect(screen.getByText(/Precedence:/)).toHaveTextContent(
      "acme → signaldb → otel",
    );
  });

  it("refetches registries and whoami when the active tenant changes", async () => {
    // Mimics the shell: a top-bar tenant switch rewrites the outlet state
    // (and the imperative header context) without unmounting the page.
    function Harness() {
      const [tenant, setTenant] = useState("acme-corp");
      return (
        <Routes>
          <Route
            element={
              <Outlet
                context={{
                  state: { ...DEFAULT_STATE, tenant, dataset: "" },
                  update: vi.fn(),
                }}
              />
            }
          >
            <Route
              path="/schema/conventions"
              element={
                <>
                  <button
                    type="button"
                    onClick={() => {
                      setTenantContext({ tenant: "globex", dataset: "" });
                      setTenant("globex");
                    }}
                  >
                    switch tenant
                  </button>
                  <RegistryList />
                </>
              }
            />
          </Route>
        </Routes>
      );
    }
    setTenantContext({ tenant: "acme-corp", dataset: "" });
    const fetchMock = stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_MEMBER },
      { match: "/api/v1/schema/registries", body: REGISTRIES },
    ]);
    renderWithClient(
      <MemoryRouter initialEntries={["/schema/conventions"]}>
        <Harness />
      </MemoryRouter>,
    );
    const user = userEvent.setup();
    await screen.findByText("otel");
    const registryCalls = () =>
      fetchMock.mock.calls.filter(
        ([input]) =>
          input instanceof Request &&
          input.url.includes("/api/v1/schema/registries"),
      ).length;
    const before = registryCalls();

    await user.click(screen.getByRole("button", { name: "switch tenant" }));

    // A new tenant is an unseen query key: it refetches at once instead of
    // reusing the previous tenant's 60s-fresh cache entry.
    await waitFor(() => expect(registryCalls()).toBeGreaterThan(before));
    const lastCall = fetchMock.mock.calls
      .filter(
        ([input]) =>
          input instanceof Request &&
          input.url.includes("/api/v1/schema/registries"),
      )
      .at(-1)![0] as Request;
    expect(lastCall.headers.get("X-Tenant-ID")).toBe("globex");
  });

  it("hides mutation actions from non-admins and shows them to tenant admins", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_MEMBER },
      { match: "/api/v1/schema/registries", body: REGISTRIES },
    ]);
    const { unmount } = renderList();
    await screen.findByText("otel");
    expect(screen.queryByRole("link", { name: "New" })).toBeNull();
    expect(screen.queryByRole("link", { name: "Upload registry" })).toBeNull();
    unmount();

    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_TENANT_ADMIN },
      { match: "/api/v1/schema/registries", body: REGISTRIES },
    ]);
    renderList();
    await screen.findByText("otel");
    expect(
      await screen.findByRole("link", { name: "New" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "Upload registry" }),
    ).toBeInTheDocument();
  });

  it("looks up an attribute key and shows precedence-ordered hits, first primary", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_MEMBER },
      { match: "/api/v1/schema/registries", body: REGISTRIES },
      {
        match: "/api/v1/schema/attributes/service.name",
        body: SERVICE_NAME_RESOLUTION,
      },
    ]);
    renderList();
    const user = userEvent.setup();
    await screen.findByText("otel");

    await user.type(screen.getByLabelText("Lookup"), "service.name{enter}");

    const hits = await screen.findAllByRole("listitem", { name: /hit/ });
    expect(hits).toHaveLength(2);
    expect(hits[0]).toHaveTextContent("acme");
    expect(hits[0]).toHaveTextContent("1.0.0");
    expect(hits[0]).toHaveTextContent("custom");
    expect(hits[0]).toHaveTextContent("primary");
    expect(hits[0]).toHaveTextContent("Our service naming");
    expect(hits[1]).toHaveTextContent("otel");
    expect(hits[1]).toHaveTextContent("Logical name of the service.");
    expect(hits[1]).not.toHaveTextContent("primary");
    // Each hit links to its definition page.
    expect(within(hits[0]!).getByRole("link")).toHaveAttribute(
      "href",
      "/schema/conventions/acme/1.0.0/attributes/service.name",
    );
  });

  it("falls through to entities when the name is not an attribute", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_MEMBER },
      { match: "/api/v1/schema/registries", body: REGISTRIES },
      {
        match: "/api/v1/schema/attributes/k8s.pod",
        body: EMPTY_RESOLUTION("k8s.pod"),
      },
      {
        match: "/api/v1/schema/entities/k8s.pod",
        body: K8S_POD_ENTITY_RESOLUTION,
      },
    ]);
    renderList();
    const user = userEvent.setup();
    await screen.findByText("otel");

    await user.type(screen.getByLabelText("Lookup"), "k8s.pod{enter}");

    const hit = await screen.findByRole("listitem", { name: /hit/ });
    expect(hit).toHaveTextContent("entity");
    expect(hit).toHaveTextContent("A Kubernetes Pod object.");
    expect(within(hit).getByRole("link")).toHaveAttribute(
      "href",
      "/schema/conventions/otel/1.43.0/entities/k8s.pod",
    );
  });

  it("says so when nothing matches", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_MEMBER },
      { match: "/api/v1/schema/registries", body: REGISTRIES },
      { match: "/api/v1/schema/attributes/", body: EMPTY_RESOLUTION("nope") },
      { match: "/api/v1/schema/entities/", body: EMPTY_RESOLUTION("nope") },
      { match: "/api/v1/schema/metrics/", body: EMPTY_RESOLUTION("nope") },
    ]);
    renderList();
    const user = userEvent.setup();
    await screen.findByText("otel");

    await user.type(screen.getByLabelText("Lookup"), "nope{enter}");
    expect(await screen.findByText(/No definition of/)).toBeInTheDocument();
  });
});
