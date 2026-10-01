import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { ManagementPanel } from "./ManagementPanel";
import type { WhoamiIdentityResponse } from "../../api/session";

const WHO: WhoamiIdentityResponse = {
  user: {
    id: "user-1",
    email: "alice@example.com",
    display_name: "Alice",
    is_instance_admin: false,
  },
  memberships: [{ tenant_id: "acme", role: "admin" }],
  tenant: { id: "acme", slug: "acme", name: "Acme Corp" },
  datasets: [{ id: "production", slug: "production", is_default: true }],
  default_dataset: "production",
  user_id: "user-1",
  dataset: "production",
  granted_tenants: [{ tenant_id: "acme" }],
};

function renderPanel() {
  return renderWithClient(
    <MemoryRouter>
      <ManagementPanel
        who={WHO}
        onClose={() => {}}
        onTenantCreated={() => {}}
      />
    </MemoryRouter>,
  );
}

const WHO_WITH_TWO_DATASETS: WhoamiIdentityResponse = {
  ...WHO,
  datasets: [
    { id: "default", slug: "default", is_default: true },
    { id: "apps", slug: "apps", is_default: false },
  ],
};

function renderPanelWithTwoDatasets() {
  return renderWithClient(
    <MemoryRouter>
      <ManagementPanel
        who={WHO_WITH_TWO_DATASETS}
        onClose={() => {}}
        onTenantCreated={() => {}}
      />
    </MemoryRouter>,
  );
}

function stubDatasetsSectionRoutes() {
  stubFetchRoutes([
    { match: "/api/v1/tenants/acme/api-keys", body: [] },
    { match: "/api/v1/tenants/acme/memberships", body: [] },
    { match: TABLES_PATH, body: { tenant_id: "acme", tables: [] } },
  ]);
}

afterEach(() => {
  vi.unstubAllGlobals();
});

const TABLES_PATH = "/api/v1/tenants/acme/tables";

describe("ManagementPanel API keys section", () => {
  it("points to the API keys page instead of offering a second create form", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/tenants/acme/api-keys",
        body: [
          { id: "k1", name: "collector", revoked: false, created_at: "" },
          { id: "k2", name: "old", revoked: true, created_at: "" },
        ],
      },
      { match: "/api/v1/tenants/acme/memberships", body: [] },
      { match: TABLES_PATH, body: { tenant_id: "acme", tables: [] } },
    ]);

    renderPanel();

    const link = await screen.findByRole("link", { name: /api keys/i });
    expect(link).toHaveAttribute("href", "/api-keys");
    expect(await screen.findByText(/1 active key\b/)).toBeInTheDocument();
    expect(screen.queryByText("Create API key")).not.toBeInTheDocument();
    expect(
      document.querySelector('input[name="logs:write"]'),
    ).not.toBeInTheDocument();
  });
});

describe("ManagementPanel tables section", () => {
  it("lists the tenant's provisioned signal tables", async () => {
    stubFetchRoutes([
      { match: "/api/v1/tenants/acme/api-keys", body: [] },
      { match: "/api/v1/tenants/acme/memberships", body: [] },
      {
        match: TABLES_PATH,
        body: {
          tenant_id: "acme",
          tables: [
            {
              name: "traces",
              schema_type: "traces",
              description: "OpenTelemetry traces and spans",
              dataset: "production",
            },
            {
              name: "logs",
              schema_type: "logs",
              description: "OpenTelemetry log entries",
              dataset: "production",
            },
          ],
          datasets: [
            {
              dataset: "production",
              tables: [
                {
                  name: "traces",
                  schema_type: "traces",
                  description: "OpenTelemetry traces and spans",
                  dataset: "production",
                },
                {
                  name: "logs",
                  schema_type: "logs",
                  description: "OpenTelemetry log entries",
                  dataset: "production",
                },
              ],
            },
          ],
        },
        method: "GET",
      },
    ]);

    renderPanel();

    await waitFor(() => {
      expect(screen.getByText("traces")).toBeInTheDocument();
      expect(screen.getByText("logs")).toBeInTheDocument();
    });
  });

  it("groups tables by dataset, one heading per dataset", async () => {
    stubFetchRoutes([
      { match: "/api/v1/tenants/acme/api-keys", body: [] },
      { match: "/api/v1/tenants/acme/memberships", body: [] },
      {
        match: TABLES_PATH,
        body: {
          tenant_id: "acme",
          tables: [
            {
              name: "traces",
              schema_type: "traces",
              description: "d",
              dataset: "production",
            },
            {
              name: "profiles",
              schema_type: "profiles",
              description: "d",
              dataset: "archive",
            },
          ],
          datasets: [
            {
              dataset: "production",
              tables: [
                {
                  name: "traces",
                  schema_type: "traces",
                  description: "d",
                  dataset: "production",
                },
              ],
            },
            {
              dataset: "archive",
              tables: [
                {
                  name: "profiles",
                  schema_type: "profiles",
                  description: "d",
                  dataset: "archive",
                },
              ],
            },
          ],
        },
        method: "GET",
      },
    ]);

    renderPanel();

    await waitFor(() => {
      expect(
        screen.getByRole("heading", { level: 4, name: "production" }),
      ).toBeInTheDocument();
      expect(
        screen.getByRole("heading", { level: 4, name: "archive" }),
      ).toBeInTheDocument();
      expect(screen.getByText("traces")).toBeInTheDocument();
      expect(screen.getByText("profiles")).toBeInTheDocument();
    });
  });

  it("falls back to client-side grouping, with an 'Unknown dataset' heading, when a response omits the dataset grouping", async () => {
    stubFetchRoutes([
      { match: "/api/v1/tenants/acme/api-keys", body: [] },
      { match: "/api/v1/tenants/acme/memberships", body: [] },
      {
        match: TABLES_PATH,
        // No `dataset` on the table and no `datasets` grouping at all — an
        // older cached response shape the client must still render sanely.
        body: {
          tenant_id: "acme",
          tables: [{ name: "traces", schema_type: "traces", description: "d" }],
        },
        method: "GET",
      },
    ]);

    renderPanel();

    await waitFor(() => {
      expect(
        screen.getByRole("heading", { level: 4, name: "Unknown dataset" }),
      ).toBeInTheDocument();
      expect(screen.getByText("traces")).toBeInTheDocument();
    });
  });

  it("shows an empty state when no tables are provisioned yet", async () => {
    stubFetchRoutes([
      { match: "/api/v1/tenants/acme/api-keys", body: [] },
      { match: "/api/v1/tenants/acme/memberships", body: [] },
      {
        match: TABLES_PATH,
        body: { tenant_id: "acme", tables: [] },
        method: "GET",
      },
    ]);

    renderPanel();

    await waitFor(() =>
      expect(
        screen.getByText("No signal tables provisioned yet for this dataset."),
      ).toBeInTheDocument(),
    );
  });

  it("displays each membership's grant source, human-friendly", async () => {
    stubFetchRoutes([
      { match: "/api/v1/tenants/acme/api-keys", body: [] },
      {
        match: "/api/v1/tenants/acme/memberships",
        body: [
          {
            user_id: "user-2",
            email: "bob@example.com",
            role: "member",
            granted_by: "local",
          },
          {
            user_id: "user-2",
            email: "bob@example.com",
            role: "viewer",
            granted_by: "oidc_mapping",
          },
        ],
        method: "GET",
      },
      { match: TABLES_PATH, body: { tenant_id: "acme", tables: [] } },
    ]);

    renderPanel();

    await waitFor(() => {
      expect(screen.getByText("Local")).toBeInTheDocument();
      expect(screen.getByText("SSO group")).toBeInTheDocument();
    });
  });

  it("renders both a local and a mapped row for the same user without a React key warning, and only the local row is removable", async () => {
    const warnSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    stubFetchRoutes([
      { match: "/api/v1/tenants/acme/api-keys", body: [] },
      {
        match: "/api/v1/tenants/acme/memberships",
        body: [
          {
            user_id: "user-2",
            email: "bob@example.com",
            role: "member",
            granted_by: "local",
          },
          {
            user_id: "user-2",
            email: "bob@example.com",
            role: "viewer",
            granted_by: "oidc_mapping",
          },
        ],
        method: "GET",
      },
      { match: TABLES_PATH, body: { tenant_id: "acme", tables: [] } },
    ]);

    renderPanel();

    await waitFor(() => {
      expect(screen.getAllByText("bob@example.com")).toHaveLength(2);
    });

    const removeButtons = screen.getAllByRole("button", { name: "Remove" });
    expect(removeButtons).toHaveLength(1);
    expect(screen.getByText("Managed by SSO group")).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "Managed by SSO group" }),
    ).not.toBeInTheDocument();

    const keyWarning = warnSpy.mock.calls.some((call) =>
      String(call[0]).includes('unique "key"'),
    );
    expect(keyWarning).toBe(false);
    warnSpy.mockRestore();
  });

  it("provisioning tables calls createTenantTables and refreshes the list", async () => {
    const fetchMock = stubFetchRoutes([
      { match: "/api/v1/tenants/acme/api-keys", body: [] },
      { match: "/api/v1/tenants/acme/memberships", body: [] },
      {
        match: TABLES_PATH,
        body: { tenant_id: "acme", tables: [] },
        method: "GET",
      },
      {
        match: `${TABLES_PATH}/create`,
        body: {
          message: "Default tables created for tenant 'acme'",
          tenant_id: "acme",
        },
        status: 201,
        method: "POST",
      },
    ]);

    renderPanel();

    await waitFor(() =>
      expect(screen.getByText("Provision tables")).toBeInTheDocument(),
    );
    await userEvent.click(screen.getByText("Provision tables"));

    await waitFor(() => {
      const posted = fetchMock.mock.calls
        .map((call) => call[0])
        .filter((req): req is Request => req instanceof Request)
        .some(
          (req) =>
            req.url.includes(`${TABLES_PATH}/create`) && req.method === "POST",
        );
      expect(posted).toBe(true);
    });
  });
});

function datasetsList(): HTMLElement {
  const section = screen
    .getByRole("heading", { name: "Datasets" })
    .closest("section")!;
  return within(section).getByRole("list");
}

describe("ManagementPanel datasets section", () => {
  beforeEach(async () => {
    stubDatasetsSectionRoutes();
    renderPanelWithTwoDatasets();
    await waitFor(() =>
      expect(within(datasetsList()).getByText("apps")).toBeInTheDocument(),
    );
  });

  it("does not repeat the dataset id as the default badge's text", () => {
    expect(within(datasetsList()).getAllByText("default")).toHaveLength(1);
    expect(within(datasetsList()).getByText("Default")).toBeInTheDocument();
  });

  it("explains why the default dataset has no delete button", () => {
    const defaultRow = within(datasetsList())
      .getByText("Default")
      .closest("li")!;
    expect(defaultRow).not.toHaveTextContent("Delete");
    expect(
      within(defaultRow).getByText(/can't be deleted/i),
    ).toBeInTheDocument();
  });

  it("still shows a working Delete button for a non-default dataset", () => {
    const appsRow = within(datasetsList()).getByText("apps").closest("li")!;
    expect(
      within(appsRow).getByRole("button", { name: "Delete" }),
    ).toBeInTheDocument();
  });
});
