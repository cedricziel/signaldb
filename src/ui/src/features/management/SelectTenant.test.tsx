import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router";
import { SelectTenant } from "./SelectTenant";
import type { CurrentSessionResponse } from "../../api/session";
import { renderWithClient, stubFetchRoutes } from "../../test/render";

// Mock useOutletState and useNavigate. `useSearchParams` stays real so a
// `redirect` query param can be exercised via `MemoryRouter`'s location.
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

function renderSelectTenant(
  session: CurrentSessionResponse,
  path = "/select-tenant",
) {
  return renderWithClient(
    <MemoryRouter initialEntries={[path]}>
      <SelectTenant session={session} />
    </MemoryRouter>,
  );
}

const sessionWithMemberships: CurrentSessionResponse = {
  tenant: "acme",
  dataset: "production",
  user: {
    id: "1",
    email: "admin@acme.com",
    display_name: "Admin",
    is_instance_admin: true,
  },
  memberships: [
    { tenant_id: "acme", name: "Acme", role: "admin" },
    { tenant_id: "acme-eu", name: "Acme EU", role: "member" },
    { tenant_id: "sandbox", name: "Sandbox", role: "viewer" },
  ],
};

describe("SelectTenant", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.clearAllMocks();
  });

  // The tenant-name span renders "{arrow} {tenantId}" as separate JSX text
  // nodes, so textContent is "▼ acme" / "▶ acme-eu" — match the exact
  // rendered form rather than a substring, since "acme" is itself a
  // substring of "acme-eu".
  const tenantNameMatcher =
    (tenantId: string) => (_c: string, el: Element | null) => {
      const text = el?.textContent ?? "";
      return text === `▼ ${tenantId}` || text === `▶ ${tenantId}`;
    };

  it("shows tenant list from the session's memberships", () => {
    renderSelectTenant(sessionWithMemberships);
    expect(screen.getByText(tenantNameMatcher("acme"))).toBeInTheDocument();
    expect(screen.getByText(tenantNameMatcher("acme-eu"))).toBeInTheDocument();
    expect(screen.getByText(tenantNameMatcher("sandbox"))).toBeInTheDocument();
    expect(screen.getByText("admin")).toBeInTheDocument();
    expect(screen.getByText("member")).toBeInTheDocument();
    expect(screen.getByText("viewer")).toBeInTheDocument();
  });

  it("current tenant is expanded by default and shows its datasets", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/whoami",
        body: {
          datasets: [
            { id: "production", slug: "production", is_default: true },
            { id: "staging", slug: "staging", is_default: false },
          ],
        },
      },
    ]);
    renderSelectTenant(sessionWithMemberships);
    expect(await screen.findByText(/production/)).toBeInTheDocument();
    expect(screen.getByText(/staging/)).toBeInTheDocument();
    expect(screen.getByText("default")).toBeInTheDocument();
    // Other tenants collapsed (datasets not visible)
    expect(screen.queryByText(/europe/)).toBeNull();
  });

  it("clicking a dataset navigates to /logs (no redirect param) with updated state", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/whoami",
        body: {
          datasets: [
            { id: "production", slug: "production", is_default: true },
            { id: "staging", slug: "staging", is_default: false },
          ],
        },
      },
    ]);
    renderSelectTenant(sessionWithMemberships);
    const dataset = await screen.findByText(/staging/);
    await userEvent.click(dataset);
    expect(mockUpdate).toHaveBeenCalledWith({
      tenant: "acme",
      dataset: "staging",
    });
    expect(mockNavigate).toHaveBeenCalledWith("/logs");
  });

  it("navigates to a validated redirect target after a dataset pick", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/whoami",
        body: {
          datasets: [{ id: "production", slug: "production", is_default: true }],
        },
      },
    ]);
    renderSelectTenant(
      sessionWithMemberships,
      "/select-tenant?redirect=%2Ftraces%3Frange%3D15m",
    );
    const dataset = await screen.findByText(/production/);
    await userEvent.click(dataset);
    expect(mockNavigate).toHaveBeenCalledWith("/traces?range=15m");
  });

  it.each([["//evil.com"], ["https://evil.com"], ["/\\evil.com"]])(
    "falls back to /logs for the unsafe redirect target %s",
    async (unsafe) => {
      stubFetchRoutes([
        {
          match: "/api/v1/whoami",
          body: {
            datasets: [
              { id: "production", slug: "production", is_default: true },
            ],
          },
        },
      ]);
      renderSelectTenant(
        sessionWithMemberships,
        `/select-tenant?redirect=${encodeURIComponent(unsafe)}`,
      );
      const dataset = await screen.findByText(/production/);
      await userEvent.click(dataset);
      expect(mockNavigate).toHaveBeenCalledWith("/logs");
    },
  );

  it("shows an error with a retry control when the datasets query fails, not an empty list", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ error: "boom" }), { status: 500 }),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            datasets: [
              { id: "production", slug: "production", is_default: true },
            ],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      );
    vi.stubGlobal("fetch", fetchMock);

    renderSelectTenant(sessionWithMemberships);
    expect(await screen.findByText(/Failed to load datasets/)).toBeInTheDocument();
    expect(screen.queryByText(/production/)).toBeNull();

    await userEvent.click(screen.getByRole("button", { name: "Retry" }));

    expect(await screen.findByText(/production/)).toBeInTheDocument();
    expect(screen.queryByText(/Failed to load datasets/)).toBeNull();
  });

  it("other tenants are collapsed initially and fetch their own datasets on click", async () => {
    const fetchMock = vi.fn().mockImplementation((_url, init?: RequestInit) => {
      const tenantId = (init?.headers as Record<string, string> | undefined)?.[
        "X-Tenant-ID"
      ];
      const body =
        tenantId === "acme-eu"
          ? { datasets: [{ id: "europe", slug: "europe", is_default: true }] }
          : { datasets: [] };
      return Promise.resolve(
        new Response(JSON.stringify(body), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      );
    });
    vi.stubGlobal("fetch", fetchMock);

    renderSelectTenant(sessionWithMemberships);
    expect(screen.queryByText(/europe/)).toBeNull();
    const tenantRow = await screen.findByText(tenantNameMatcher("acme-eu"));
    await userEvent.click(tenantRow);

    expect(tenantRow.closest("button")).toHaveAttribute(
      "aria-expanded",
      "true",
    );
    expect(await screen.findByText(/europe/)).toBeInTheDocument();

    const scopedCall = fetchMock.mock.calls.find((call) => {
      const init = call[1] as RequestInit | undefined;
      return (
        (init?.headers as Record<string, string> | undefined)?.[
          "X-Tenant-ID"
        ] === "acme-eu"
      );
    });
    expect(scopedCall).toBeDefined();
  });

  it("shows an instance-admin-specific explanation (not an empty picker) when an instance admin has no memberships", () => {
    renderSelectTenant({
      tenant: null,
      dataset: null,
      user: {
        id: "3",
        email: "root@example.com",
        display_name: null,
        is_instance_admin: true,
      },
      memberships: [],
    });
    expect(
      screen.getByRole("heading", { name: /instance admin/i }),
    ).toBeInTheDocument();
    expect(screen.getByText(/root@example.com/)).toBeInTheDocument();
    expect(
      screen.queryByRole("heading", { name: "Select tenant" }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("heading", { name: "No tenant access yet" }),
    ).not.toBeInTheDocument();
    // No empty tenant picker rendered underneath the explanation.
    expect(document.querySelector(".tenant-list")).toBeNull();
  });

  it("shows a no-membership explanation when the session has no memberships and the user isn't an instance admin", () => {
    renderSelectTenant({
      tenant: null,
      dataset: null,
      user: {
        id: "2",
        email: "orphan@example.com",
        display_name: null,
        is_instance_admin: false,
      },
      memberships: [],
    });
    expect(
      screen.getByRole("heading", { name: "No tenant access yet" }),
    ).toBeInTheDocument();
    expect(screen.getByText(/orphan@example.com/)).toBeInTheDocument();
    expect(
      screen.queryByRole("heading", { name: "Select tenant" }),
    ).not.toBeInTheDocument();
  });
});
