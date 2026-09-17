import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter, Route, Routes, useLocation } from "react-router";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { RegistryBrowser } from "./RegistryBrowser";
import {
  ACME_REGISTRY,
  K8S_POD_CPU_TIME_RESOLUTION,
  K8S_POD_ENTITY_RESOLUTION,
  K8S_POD_UID_RESOLUTION,
  OTEL_REGISTRY,
  SERVICE_NAME_RESOLUTION,
  shellOutlet,
  WHOAMI_MEMBER,
  WHOAMI_TENANT_ADMIN,
} from "./testFixtures";

function LocationProbe() {
  const { pathname } = useLocation();
  return <div data-testid="location">{pathname}</div>;
}

function renderBrowser(path: string) {
  return renderWithClient(
    <MemoryRouter initialEntries={[path]}>
      <LocationProbe />
      <Routes>
        <Route element={shellOutlet()}>
          <Route
            path="/schema/conventions/:ns/:version"
            element={<RegistryBrowser />}
          />
          <Route
            path="/schema/conventions/:ns/:version/:kind/:name"
            element={<RegistryBrowser />}
          />
          <Route path="/schema/conventions" element={<div>List page</div>} />
        </Route>
      </Routes>
    </MemoryRouter>,
  );
}

const OTEL_ROUTES = [
  { match: "/api/v1/whoami", body: WHOAMI_MEMBER },
  { match: "/api/v1/schema/registries/otel/1.43.0", body: OTEL_REGISTRY },
];

afterEach(() => {
  vi.unstubAllGlobals();
  // jsdom doesn't implement `scrollIntoView`; a few tests below stub it
  // directly on the prototype (`vi.spyOn` needs the property to exist).
  delete (HTMLElement.prototype as { scrollIntoView?: unknown })
    .scrollIntoView;
});

describe("RegistryBrowser", () => {
  it("filters attributes, entities and metrics by the search text", async () => {
    stubFetchRoutes(OTEL_ROUTES);
    renderBrowser("/schema/conventions/otel/1.43.0");
    const user = userEvent.setup();

    const attributes = await screen.findByRole("region", {
      name: "Attributes",
    });
    // Unfiltered: everything is listed.
    expect(within(attributes).getByText("service.name")).toBeInTheDocument();

    await user.type(screen.getByLabelText("Search definitions"), "k8s.pod");

    expect(within(attributes).getByText("k8s.pod.uid")).toBeInTheDocument();
    expect(within(attributes).getByText("k8s.pod.name")).toBeInTheDocument();
    expect(within(attributes).getByText("k8s.pod.label")).toBeInTheDocument();
    expect(within(attributes).queryByText("k8s.namespace.name")).toBeNull();
    expect(within(attributes).queryByText("service.name")).toBeNull();

    const entities = screen.getByRole("region", { name: "Entities" });
    expect(within(entities).getByText("k8s.pod")).toBeInTheDocument();
    expect(within(entities).queryByText("service")).toBeNull();

    const metrics = screen.getByRole("region", { name: "Metrics" });
    expect(within(metrics).getByText("k8s.pod.cpu.time")).toBeInTheDocument();
  });

  it("opens an attribute definition at its own URL", async () => {
    stubFetchRoutes([
      ...OTEL_ROUTES,
      {
        match: "/api/v1/schema/attributes/k8s.pod.uid",
        body: K8S_POD_UID_RESOLUTION,
      },
    ]);
    renderBrowser("/schema/conventions/otel/1.43.0");
    const user = userEvent.setup();

    const attributes = await screen.findByRole("region", {
      name: "Attributes",
    });
    await user.click(within(attributes).getByText("k8s.pod.uid"));

    expect(screen.getByTestId("location")).toHaveTextContent(
      "/schema/conventions/otel/1.43.0/attributes/k8s.pod.uid",
    );
    const pane = await screen.findByRole("article");
    expect(within(pane).getByRole("heading", { level: 2 })).toHaveTextContent(
      "k8s.pod.uid",
    );
    expect(pane).toHaveTextContent("The UID of the Pod.");
    expect(pane).toHaveTextContent("Kubernetes Attributes");
    expect(pane).toHaveTextContent("stable");
    expect(pane).toHaveTextContent("275ecb36-5aa8-4c2a-9c47-d8bb681b9aff");
    // Type and entity role.
    expect(within(pane).getByText("string")).toBeInTheDocument();
    expect(
      within(pane).getByRole("link", { name: /k8s\.pod/ }),
    ).toHaveAttribute(
      "href",
      "/schema/conventions/otel/1.43.0/entities/k8s.pod",
    );
    expect(pane).toHaveTextContent("identifying");
  });

  it("deep-links an attribute and links to alternatives, marking the primary", async () => {
    stubFetchRoutes([
      ...OTEL_ROUTES,
      {
        match: "/api/v1/schema/attributes/service.name",
        body: SERVICE_NAME_RESOLUTION,
      },
    ]);
    renderBrowser("/schema/conventions/otel/1.43.0/attributes/service.name");

    const pane = await screen.findByRole("article");
    expect(within(pane).getByRole("heading", { level: 2 })).toHaveTextContent(
      "service.name",
    );
    // This is otel's definition, shadowed by acme's.
    expect(pane).toHaveTextContent("Logical name of the service.");
    expect(pane).toHaveTextContent("otel");
    expect(pane).toHaveTextContent("1.43.0");
    const also = within(pane).getByRole("list", { name: "Also defined in" });
    const alt = within(also).getByRole("link", { name: /acme\/service\.name/ });
    expect(alt).toHaveAttribute(
      "href",
      "/schema/conventions/acme/1.0.0/attributes/service.name",
    );
    expect(within(also).getByText("primary")).toBeInTheDocument();
    expect(within(pane).getByText("shadowed")).toBeInTheDocument();
  });

  it("renders the entity page with roles, metrics and extensions", async () => {
    stubFetchRoutes([
      ...OTEL_ROUTES,
      {
        match: "/api/v1/schema/entities/k8s.pod",
        body: K8S_POD_ENTITY_RESOLUTION,
      },
    ]);
    renderBrowser("/schema/conventions/otel/1.43.0/entities/k8s.pod");

    const pane = await screen.findByRole("article");
    expect(within(pane).getByRole("heading", { level: 2 })).toHaveTextContent(
      "k8s.pod",
    );
    expect(pane).toHaveTextContent("A Kubernetes Pod object.");
    const identifying = within(pane).getByRole("list", { name: "Identifying" });
    expect(identifying).toHaveTextContent("k8s.pod.uid");
    expect(identifying).toHaveTextContent("required");
    const descriptive = within(pane).getByRole("list", { name: "Descriptive" });
    expect(descriptive).toHaveTextContent("k8s.pod.name");
    expect(descriptive).toHaveTextContent("k8s.pod.label");
    const metrics = within(pane).getByRole("list", {
      name: "Associated metrics",
    });
    expect(
      within(metrics).getByRole("link", { name: "k8s.pod.cpu.time" }),
    ).toHaveAttribute(
      "href",
      "/schema/conventions/otel/1.43.0/metrics/k8s.pod.cpu.time",
    );
    const extended = within(pane).getByRole("list", { name: "Extended by" });
    expect(extended).toHaveTextContent("acme/acme.k8s.pod");
    // Roles link back to attribute pages.
    expect(
      within(identifying).getByRole("link", { name: "k8s.pod.uid" }),
    ).toHaveAttribute(
      "href",
      "/schema/conventions/otel/1.43.0/attributes/k8s.pod.uid",
    );
  });

  it("renders the metric page with instrument, unit and associated entities", async () => {
    stubFetchRoutes([
      ...OTEL_ROUTES,
      {
        match: "/api/v1/schema/metrics/k8s.pod.cpu.time",
        body: K8S_POD_CPU_TIME_RESOLUTION,
      },
    ]);
    renderBrowser("/schema/conventions/otel/1.43.0/metrics/k8s.pod.cpu.time");

    const pane = await screen.findByRole("article");
    expect(pane).toHaveTextContent("Total CPU time consumed.");
    expect(within(pane).getByText("counter")).toBeInTheDocument();
    expect(within(pane).getByText("s")).toBeInTheDocument();
    const entities = within(pane).getByRole("list", {
      name: "Associated entities",
    });
    expect(
      within(entities).getByRole("link", { name: "k8s.pod" }),
    ).toHaveAttribute(
      "href",
      "/schema/conventions/otel/1.43.0/entities/k8s.pod",
    );
  });

  it("scrolls the definition pane into view when a definition is selected", async () => {
    const scrollSpy = vi.fn();
    HTMLElement.prototype.scrollIntoView = scrollSpy;
    stubFetchRoutes([
      ...OTEL_ROUTES,
      {
        match: "/api/v1/schema/attributes/k8s.pod.uid",
        body: K8S_POD_UID_RESOLUTION,
      },
    ]);
    renderBrowser("/schema/conventions/otel/1.43.0");
    const user = userEvent.setup();

    const attributes = await screen.findByRole("region", {
      name: "Attributes",
    });
    scrollSpy.mockClear();
    await user.click(within(attributes).getByText("k8s.pod.uid"));

    await screen.findByRole("article");
    expect(scrollSpy).toHaveBeenCalledWith({ block: "start" });
  });

  it("scrolls the active item's nav entry into view when the selection changes", async () => {
    const scrollSpy = vi.fn();
    HTMLElement.prototype.scrollIntoView = scrollSpy;
    stubFetchRoutes([
      ...OTEL_ROUTES,
      {
        match: "/api/v1/schema/attributes/k8s.pod.uid",
        body: K8S_POD_UID_RESOLUTION,
      },
    ]);
    renderBrowser("/schema/conventions/otel/1.43.0/attributes/k8s.pod.uid");

    await screen.findByRole("article");
    expect(scrollSpy).toHaveBeenCalledWith({ block: "nearest" });
  });

  it("keeps the selected item visible even past the listed cap", async () => {
    HTMLElement.prototype.scrollIntoView = vi.fn();
    const manyAttributes = Array.from({ length: 520 }, (_, i) => ({
      id: `a.${String(i).padStart(4, "0")}`,
      type: "string",
      stability: "stable",
      brief: "filler",
    }));
    const BIG_REGISTRY = {
      ...OTEL_REGISTRY,
      document: {
        ...OTEL_REGISTRY.document,
        groups: [
          {
            id: "registry.big",
            type: "attribute_group",
            brief: "Many attributes.",
            attributes: [
              ...manyAttributes,
              { id: "zzz.selected", type: "string", brief: "Past the cap." },
            ],
          },
        ],
      },
    };
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_MEMBER },
      { match: "/api/v1/schema/registries/otel/1.43.0", body: BIG_REGISTRY },
    ]);
    renderBrowser("/schema/conventions/otel/1.43.0/attributes/zzz.selected");

    const attributes = await screen.findByRole("region", {
      name: "Attributes",
    });
    expect(
      within(attributes).getByRole("link", { name: "zzz.selected" }),
    ).toBeInTheDocument();
  });

  it("offers Edit only to tenant admins on custom registries", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_TENANT_ADMIN },
      { match: "/api/v1/schema/registries/otel/1.43.0", body: OTEL_REGISTRY },
      { match: "/api/v1/schema/registries/acme/1.0.0", body: ACME_REGISTRY },
    ]);
    const { unmount } = renderBrowser("/schema/conventions/otel/1.43.0");
    await screen.findByRole("region", { name: "Attributes" });
    expect(screen.queryByRole("link", { name: "Edit" })).toBeNull();
    expect(screen.getByLabelText("read-only")).toBeInTheDocument();
    unmount();

    const second = renderBrowser("/schema/conventions/acme/1.0.0");
    await screen.findByRole("region", { name: "Attributes" });
    expect(screen.getByRole("link", { name: "Edit" })).toHaveAttribute(
      "href",
      "/schema/conventions/acme/1.0.0/edit",
    );
    second.unmount();

    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_MEMBER },
      { match: "/api/v1/schema/registries/acme/1.0.0", body: ACME_REGISTRY },
    ]);
    renderBrowser("/schema/conventions/acme/1.0.0");
    await screen.findByRole("region", { name: "Attributes" });
    expect(screen.queryByRole("link", { name: "Edit" })).toBeNull();
  });
});
