import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { ManagementPanel } from "./ManagementPanel";
import type { WhoamiResponse } from "../../api/session";

const WHO: WhoamiResponse = {
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
};

function renderPanel() {
  return renderWithClient(
    <ManagementPanel who={WHO} onClose={() => {}} onTenantCreated={() => {}} />,
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

const TABLES_PATH = "/api/v1/tenants/acme/tables";

describe("ManagementPanel tables section", () => {
  it("lists the tenant's provisioned signal tables", async () => {
    stubFetchRoutes([
      { match: "/api/v1/manage/tenants/acme/api-keys", body: [] },
      { match: "/api/v1/manage/tenants/acme/memberships", body: [] },
      {
        match: TABLES_PATH,
        body: {
          tenant_id: "acme",
          tables: [
            {
              name: "traces",
              schema_type: "traces",
              description: "OpenTelemetry traces and spans",
            },
            {
              name: "logs",
              schema_type: "logs",
              description: "OpenTelemetry log entries",
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

  it("shows an empty state when no tables are provisioned yet", async () => {
    stubFetchRoutes([
      { match: "/api/v1/manage/tenants/acme/api-keys", body: [] },
      { match: "/api/v1/manage/tenants/acme/memberships", body: [] },
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

  it("provisioning tables calls createTenantTables and refreshes the list", async () => {
    const fetchMock = stubFetchRoutes([
      { match: "/api/v1/manage/tenants/acme/api-keys", body: [] },
      { match: "/api/v1/manage/tenants/acme/memberships", body: [] },
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
