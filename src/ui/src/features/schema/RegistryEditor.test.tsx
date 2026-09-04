import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter, Route, Routes, useLocation } from "react-router";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { RegistryEditor } from "./RegistryEditor";
import {
  ACME_DOCUMENT,
  ACME_REGISTRY,
  OTEL_REGISTRY,
  VALIDATION_FAILED,
  VALIDATION_OK,
  WHOAMI_MEMBER,
  WHOAMI_TENANT_ADMIN,
} from "./testFixtures";

function LocationProbe() {
  const { pathname } = useLocation();
  return <div data-testid="location">{pathname}</div>;
}

function renderEditor(path: string) {
  return renderWithClient(
    <MemoryRouter initialEntries={[path]}>
      <LocationProbe />
      <Routes>
        <Route path="/schema/conventions/new" element={<RegistryEditor />} />
        <Route
          path="/schema/conventions/:ns/:version/edit"
          element={<RegistryEditor />}
        />
        <Route
          path="/schema/conventions/:ns/:version"
          element={<div>Browser page</div>}
        />
        <Route path="/schema/conventions" element={<div>List page</div>} />
      </Routes>
    </MemoryRouter>,
  );
}

const ACME_YAML = `name: acme
version: 1.0.0
groups:
  - id: registry.acme.order
    type: attribute_group
    brief: Attributes describing an Acme order.
    attributes:
      - id: acme.order.id
        type: string
        stability: development
        brief: Internal order identifier.
`;

/** Body of the request the stubbed fetch received at `urlPart`. */
async function requestBody(
  fetchMock: ReturnType<typeof stubFetchRoutes>,
  urlPart: string | RegExp,
  method: string,
): Promise<unknown> {
  const call = fetchMock.mock.calls.find(([input]) => {
    if (!(input instanceof Request)) return false;
    const urlMatch =
      typeof urlPart === "string"
        ? input.url.includes(urlPart)
        : urlPart.test(input.url);
    return urlMatch && input.method === method;
  });
  expect(call, `no ${method} ${urlPart} request`).toBeDefined();
  const req = call![0] as Request;
  return JSON.parse(await req.clone().text());
}

afterEach(() => vi.unstubAllGlobals());

describe("RegistryEditor", () => {
  it("redirects non-admins away from the editor", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI_MEMBER }]);
    renderEditor("/schema/conventions/new");
    expect(await screen.findByText("List page")).toBeInTheDocument();
  });

  it("never edits a bundled registry, even for admins", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_TENANT_ADMIN },
      { match: "/api/v1/schema/registries/otel/1.43.0", body: OTEL_REGISTRY },
    ]);
    renderEditor("/schema/conventions/otel/1.43.0/edit");
    expect(await screen.findByText("Browser page")).toBeInTheDocument();
  });

  it("validates pasted YAML, shows counts, then creates on Save", async () => {
    const fetchMock = stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_TENANT_ADMIN },
      {
        match: "/api/v1/schema/registries:validate",
        method: "POST",
        body: VALIDATION_OK,
      },
      {
        match: /\/api\/v1\/schema\/registries$/,
        method: "POST",
        body: ACME_REGISTRY,
        status: 201,
      },
    ]);
    renderEditor("/schema/conventions/new");
    const user = userEvent.setup();

    const source = await screen.findByLabelText("Registry document");
    const save = screen.getByRole("button", { name: "Save" });
    expect(save).toBeDisabled();

    await user.click(source);
    await user.paste(ACME_YAML);
    expect(save).toBeDisabled();

    await user.click(screen.getByRole("button", { name: "Validate" }));

    const report = await screen.findByRole("status");
    expect(report).toHaveTextContent("Valid");
    expect(report).toHaveTextContent("acme@1.0.0");
    expect(report).toHaveTextContent("3 attributes");
    expect(report).toHaveTextContent("1 entity");
    expect(report).toHaveTextContent("1 metric");
    // The YAML was parsed client-side and posted as JSON.
    const validated = (await requestBody(
      fetchMock,
      "registries:validate",
      "POST",
    )) as { name: string; groups: unknown[] };
    expect(validated.name).toBe("acme");
    expect(validated.groups).toHaveLength(1);

    expect(save).toBeEnabled();
    await user.click(save);
    await waitFor(() =>
      expect(screen.getByTestId("location")).toHaveTextContent(
        "/schema/conventions/acme/1.0.0",
      ),
    );
    const created = (await requestBody(
      fetchMock,
      /\/api\/v1\/schema\/registries$/,
      "POST",
    )) as { name: string };
    expect(created.name).toBe("acme");
  });

  it("shows validation errors at their path and keeps Save blocked", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_TENANT_ADMIN },
      {
        match: "/api/v1/schema/registries:validate",
        method: "POST",
        body: VALIDATION_FAILED,
      },
    ]);
    renderEditor("/schema/conventions/new");
    const user = userEvent.setup();

    await user.click(await screen.findByLabelText("Registry document"));
    await user.paste(ACME_YAML);
    await user.click(screen.getByRole("button", { name: "Validate" }));

    const report = await screen.findByRole("status");
    expect(report).toHaveTextContent("groups[1].attributes[0].ref");
    expect(report).toHaveTextContent(
      "unresolvable attribute ref `acme.order.missing`",
    );
    expect(screen.getByRole("button", { name: "Save" })).toBeDisabled();
  });

  it("re-blocks Save when the document changes after validation", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_TENANT_ADMIN },
      {
        match: "/api/v1/schema/registries:validate",
        method: "POST",
        body: VALIDATION_OK,
      },
    ]);
    renderEditor("/schema/conventions/new");
    const user = userEvent.setup();

    const source = await screen.findByLabelText("Registry document");
    await user.click(source);
    await user.paste(ACME_YAML);
    await user.click(screen.getByRole("button", { name: "Validate" }));
    await screen.findByRole("status");
    const save = screen.getByRole("button", { name: "Save" });
    expect(save).toBeEnabled();

    await user.type(source, "\n# touched");
    expect(save).toBeDisabled();
  });

  it("reports a document that does not parse without calling the server", async () => {
    const fetchMock = stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_TENANT_ADMIN },
    ]);
    renderEditor("/schema/conventions/new");
    const user = userEvent.setup();

    await user.click(await screen.findByLabelText("Registry document"));
    await user.paste("groups: [unclosed");
    await user.click(screen.getByRole("button", { name: "Validate" }));

    expect(await screen.findByRole("status")).toHaveTextContent(/parse/i);
    expect(
      fetchMock.mock.calls.some(
        ([input]) =>
          input instanceof Request && input.url.includes("registries:validate"),
      ),
    ).toBe(false);
  });

  it("fills the document from an uploaded file", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI_TENANT_ADMIN }]);
    renderEditor("/schema/conventions/new");
    const user = userEvent.setup();

    const input = (await screen.findByLabelText(
      "Upload registry file",
    )) as HTMLInputElement;
    const file = new File([ACME_YAML], "acme.yaml", {
      type: "application/yaml",
    });
    await user.upload(input, file);

    await waitFor(() =>
      expect(screen.getByLabelText("Registry document")).toHaveValue(ACME_YAML),
    );
  });

  it("edits an existing registry: diff summary, replace, save as new version, delete", async () => {
    const fetchMock = stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_TENANT_ADMIN },
      { match: "/api/v1/schema/registries/acme/1.0.0", body: ACME_REGISTRY },
      {
        match: "/api/v1/schema/registries:validate",
        method: "POST",
        body: { ...VALIDATION_OK, attribute_count: 3 },
      },
      {
        match: "/api/v1/schema/registries/acme/1.0.0",
        method: "PUT",
        body: ACME_REGISTRY,
      },
      {
        match: /\/api\/v1\/schema\/registries$/,
        method: "POST",
        body: { ...ACME_REGISTRY, version: "1.1.0" },
        status: 201,
      },
      {
        match: "/api/v1/schema/registries/acme/1.0.0",
        method: "DELETE",
        body: {},
      },
    ]);
    renderEditor("/schema/conventions/acme/1.0.0/edit");
    const user = userEvent.setup();

    const source = (await screen.findByLabelText(
      "Registry document",
    )) as HTMLTextAreaElement;
    await waitFor(() => expect(source.value).toContain("name: acme"));
    expect(source.value).toContain("acme.order.total");
    // Bundled-only "Save" label becomes Replace for an existing registry.
    expect(screen.getByRole("button", { name: "Replace" })).toBeDisabled();

    // Rewrite the document: drop acme.order.total + the metric, change a
    // brief, add a new attribute.
    type Doc = {
      groups: Array<Record<string, unknown> & { attributes?: unknown[] }>;
    };
    const base = structuredClone(ACME_DOCUMENT) as unknown as Doc;
    const [order, entity] = base.groups;
    const [id, , serviceName] = order!.attributes!;
    const edited = {
      ...base,
      groups: [
        {
          ...order,
          attributes: [
            { ...(id as object), brief: "Changed." },
            serviceName,
            { id: "acme.order.sku", type: "string", brief: "SKU." },
          ],
        },
        entity,
      ],
    };
    await user.clear(source);
    await user.click(source);
    await user.paste(JSON.stringify(edited, null, 2));
    await user.click(screen.getByRole("button", { name: "Validate" }));
    await screen.findByRole("status");

    const diff = screen.getByRole("region", { name: "Changes" });
    expect(within(diff).getByText(/Added/).parentElement).toHaveTextContent(
      "attributes/acme.order.sku",
    );
    expect(within(diff).getByText(/Changed/).parentElement).toHaveTextContent(
      "attributes/acme.order.id",
    );
    const removed = within(diff).getByText(/Removed/).parentElement!;
    expect(removed).toHaveTextContent("attributes/acme.order.total");
    expect(removed).toHaveTextContent("metrics/acme.orders.placed");

    // Replace → PUT.
    await user.click(screen.getByRole("button", { name: "Replace" }));
    await waitFor(() =>
      expect(screen.getByTestId("location")).toHaveTextContent(
        "/schema/conventions/acme/1.0.0",
      ),
    );
    const replaced = (await requestBody(
      fetchMock,
      "/registries/acme/1.0.0",
      "PUT",
    )) as { groups: unknown[] };
    expect(replaced.groups).toHaveLength(2);
  });

  it("saves as a new version by rewriting the document version", async () => {
    const fetchMock = stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_TENANT_ADMIN },
      { match: "/api/v1/schema/registries/acme/1.0.0", body: ACME_REGISTRY },
      {
        match: "/api/v1/schema/registries:validate",
        method: "POST",
        body: VALIDATION_OK,
      },
      {
        match: /\/api\/v1\/schema\/registries$/,
        method: "POST",
        body: { ...ACME_REGISTRY, version: "1.1.0" },
        status: 201,
      },
    ]);
    renderEditor("/schema/conventions/acme/1.0.0/edit");
    const user = userEvent.setup();

    const source = (await screen.findByLabelText(
      "Registry document",
    )) as HTMLTextAreaElement;
    await waitFor(() => expect(source.value).toContain("name: acme"));
    await user.click(screen.getByRole("button", { name: "Validate" }));
    await screen.findByRole("status");

    await user.type(screen.getByLabelText("New version"), "1.1.0");
    await user.click(
      screen.getByRole("button", { name: "Save as new version" }),
    );
    await waitFor(() =>
      expect(screen.getByTestId("location")).toHaveTextContent(
        "/schema/conventions/acme/1.1.0",
      ),
    );
    const created = (await requestBody(
      fetchMock,
      /\/api\/v1\/schema\/registries$/,
      "POST",
    )) as { version: string; name: string };
    expect(created.version).toBe("1.1.0");
    expect(created.name).toBe("acme");
  });

  it("deletes after confirmation", async () => {
    const fetchMock = stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_TENANT_ADMIN },
      { match: "/api/v1/schema/registries/acme/1.0.0", body: ACME_REGISTRY },
      {
        match: "/api/v1/schema/registries/acme/1.0.0",
        method: "DELETE",
        body: {},
      },
    ]);
    renderEditor("/schema/conventions/acme/1.0.0/edit");
    const user = userEvent.setup();

    await user.click(await screen.findByRole("button", { name: "Delete" }));
    expect(screen.getByText(/Delete acme@1\.0\.0\?/)).toBeInTheDocument();
    // Cancel keeps it.
    await user.click(screen.getByRole("button", { name: "Cancel" }));
    expect(
      fetchMock.mock.calls.some(
        ([input]) => input instanceof Request && input.method === "DELETE",
      ),
    ).toBe(false);

    await user.click(screen.getByRole("button", { name: "Delete" }));
    await user.click(screen.getByRole("button", { name: "Confirm" }));
    await waitFor(() =>
      expect(screen.getByTestId("location")).toHaveTextContent(
        /^\/schema\/conventions$/,
      ),
    );
    expect(
      fetchMock.mock.calls.some(
        ([input]) => input instanceof Request && input.method === "DELETE",
      ),
    ).toBe(true);
  });
});
