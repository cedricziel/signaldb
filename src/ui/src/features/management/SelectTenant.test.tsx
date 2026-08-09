import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SelectTenant } from "./SelectTenant";
import { stubFetchRoutes } from "../../test/render";
import { renderWithClient } from "../../test/render";

// Mock useOutletState and useNavigate
const mockUpdate = vi.fn();
const mockNavigate = vi.fn();

vi.mock("react-router", async (importOriginal) => {
  const mod = await importOriginal<typeof import("react-router")>();
  return {
    ...mod,
    useNavigate: () => mockNavigate,
  };
});
vi.mock("../../lib/outletState", () => ({
  useOutletState: () => ({
    state: { tenant: "acme", dataset: "production" },
    update: mockUpdate,
  }),
}));

describe("SelectTenant", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.clearAllMocks();
  });

  const mockWhoami = (tenant?: string) => {
    if (tenant === "acme") {
      return {
        user: {
          id: "1",
          email: "admin@acme.com",
          display_name: "Admin",
          is_instance_admin: true,
        },
        memberships: [
          { tenant_id: "acme", role: "admin" },
          { tenant_id: "acme-eu", role: "member" },
          { tenant_id: "sandbox", role: "viewer" },
        ],
        tenant: { id: "acme", slug: "acme", name: "Acme Corporation" },
        datasets: [
          { id: "production", slug: "production", is_default: true },
          { id: "staging", slug: "staging", is_default: false },
        ],
        default_dataset: "production",
      };
    }
    if (tenant === "acme-eu") {
      return {
        user: {
          id: "1",
          email: "admin@acme.com",
          display_name: "Admin",
          is_instance_admin: true,
        },
        memberships: [],
        tenant: { id: "acme-eu", slug: "acme-eu", name: "Acme EU" },
        datasets: [
          { id: "europe", slug: "europe", is_default: true },
          { id: "testing", slug: "testing", is_default: false },
        ],
        default_dataset: "europe",
      };
    }
    if (tenant === "sandbox") {
      return {
        user: {
          id: "1",
          email: "admin@acme.com",
          display_name: "Admin",
          is_instance_admin: true,
        },
        memberships: [],
        tenant: { id: "sandbox", slug: "sandbox", name: "Sandbox" },
        datasets: [{ id: "playground", slug: "playground", is_default: true }],
        default_dataset: "playground",
      };
    }
    // default whoami (no tenant param)
    return {
      user: {
        id: "1",
        email: "admin@acme.com",
        display_name: "Admin",
        is_instance_admin: true,
      },
      memberships: [
        { tenant_id: "acme", role: "admin" },
        { tenant_id: "acme-eu", role: "member" },
        { tenant_id: "sandbox", role: "viewer" },
      ],
      tenant: { id: "acme", slug: "acme", name: "Acme Corporation" },
      datasets: [
        { id: "production", slug: "production", is_default: true },
        { id: "staging", slug: "staging", is_default: false },
      ],
      default_dataset: "production",
    };
  };

  it("shows tenant list from whoami memberships", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: mockWhoami() }]);
    renderWithClient(<SelectTenant />);
    // Tenant names render in separate <span class="tenant-name"> elements
    expect(
      await screen.findByText((_c, el) => el?.textContent === "acme"),
    ).toBeInTheDocument();
    expect(
      screen.getByText((_c, el) => el?.textContent === "acme-eu"),
    ).toBeInTheDocument();
    expect(
      screen.getByText((_c, el) => el?.textContent === "sandbox"),
    ).toBeInTheDocument();
    // Roles render in <span class="tenant-role"> elements
    expect(screen.getByText("admin")).toBeInTheDocument();
    expect(screen.getByText("member")).toBeInTheDocument();
    expect(screen.getByText("viewer")).toBeInTheDocument();
  });

  it("current tenant is expanded by default", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: mockWhoami() }]);
    renderWithClient(<SelectTenant />);
    // Dataset names render with "· " prefix in the component
    expect(await screen.findByText(/production/)).toBeInTheDocument();
    expect(screen.getByText(/staging/)).toBeInTheDocument();
    // Other tenants collapsed (datasets not visible)
    expect(screen.queryByText(/europe/)).toBeNull();
    expect(screen.queryByText(/playground/)).toBeNull();
  });

  it("shows datasets for current tenant", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: mockWhoami() }]);
    renderWithClient(<SelectTenant />);
    expect(await screen.findByText(/production/)).toBeInTheDocument();
    expect(screen.getByText(/staging/)).toBeInTheDocument();
    expect(screen.getByText("default")).toBeInTheDocument();
  });

  it("clicking a dataset navigates to /logs with updated state", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: mockWhoami() }]);
    renderWithClient(<SelectTenant />);
    const dataset = await screen.findByText(/staging/);
    await userEvent.click(dataset);
    expect(mockUpdate).toHaveBeenCalledWith({
      tenant: "acme",
      dataset: "staging",
    });
    expect(mockNavigate).toHaveBeenCalledWith("/logs");
  });

  it("other tenants are collapsed initially", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: mockWhoami() }]);
    renderWithClient(<SelectTenant />);
    await screen.findByText((_c, el) => el?.textContent === "acme");
    expect(screen.queryByText(/europe/)).toBeNull();
    expect(screen.queryByText(/testing/)).toBeNull();
  });

  it("clicking a collapsed tenant fetches its datasets", async () => {
    // The X-Tenant-ID is sent as a header, not a query param. stubFetchRoutes
    // only matches on URL, so both whoami calls hit the same mock. Use a regex
    // to match the scoped call and return the correct tenant data.
    stubFetchRoutes([{ match: /\/api\/v1\/whoami$/, body: mockWhoami() }]);
    renderWithClient(<SelectTenant />);
    const tenantRow = await screen.findByText(
      (_c, el) => el?.textContent === "acme-eu",
    );
    await userEvent.click(tenantRow);
    // The tenant row should now be expanded (aria-expanded=true)
    expect(tenantRow.closest("button")).toHaveAttribute(
      "aria-expanded",
      "true",
    );
  });
});
