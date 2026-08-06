import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router";
import { DEFAULT_STATE } from "../../lib/urlState";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { TopBar } from "./TopBar";

function renderTopBar(props: Parameters<typeof TopBar>[0]) {
  return renderWithClient(
    <MemoryRouter>
      <TopBar {...props} />
    </MemoryRouter>,
  );
}

const WHOAMI = {
  user: {
    id: "user-1",
    email: "alice@example.com",
    display_name: "Alice",
    is_instance_admin: false,
  },
  memberships: [{ tenant_id: "acme", role: "admin" }],
  tenant: { id: "acme", slug: "acme", name: "Acme Corp" },
  datasets: [
    { id: "production", slug: "production", is_default: true },
    { id: "staging", slug: "staging", is_default: false },
  ],
  default_dataset: "production",
};

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("TenantSelector with whoami", () => {
  it("shows the tenant read-only and datasets as a selector", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    const update = vi.fn();
    renderTopBar({ state: DEFAULT_STATE, update });

    // Chip reflects the server-reported tenant and default dataset.
    await waitFor(() =>
      expect(
        screen.getByTitle("Tenant / dataset context for all queries"),
      ).toHaveTextContent("acme·production"),
    );
    await userEvent.click(
      screen.getByTitle("Tenant / dataset context for all queries"),
    );

    // Tenant is plain text, not an editable input.
    expect(screen.queryByLabelText("Tenant")).not.toBeInTheDocument();
    expect(screen.getByText("acme")).toBeInTheDocument();

    // Dataset is a select over the tenant's datasets, defaulting to the
    // default dataset.
    const select = screen.getByLabelText("Dataset");
    expect(select.tagName).toBe("SELECT");
    expect(select).toHaveValue("production");
    const options = screen
      .getAllByRole("option")
      .map((o) => (o as HTMLOptionElement).value);
    expect(options).toEqual(["production", "staging"]);

    await userEvent.selectOptions(select, "staging");
    await userEvent.click(screen.getByRole("button", { name: "Apply" }));
    expect(update).toHaveBeenCalledWith({ tenant: "acme", dataset: "staging" });
  });

  it("keeps an explicitly selected dataset over the default", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    renderTopBar({
      state: { ...DEFAULT_STATE, tenant: "acme", dataset: "staging" },
      update: vi.fn(),
    });
    await userEvent.click(
      screen.getByTitle("Tenant / dataset context for all queries"),
    );
    await waitFor(() =>
      expect(screen.getByLabelText("Dataset")).toHaveValue("staging"),
    );
  });

  it("falls back to free-text inputs when whoami is unavailable", async () => {
    // Older servers: /api/v1/whoami does not exist (404).
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: { error: "not found" }, status: 404 },
    ]);
    const update = vi.fn();
    renderTopBar({ state: DEFAULT_STATE, update });

    await userEvent.click(
      screen.getByTitle("Tenant / dataset context for all queries"),
    );
    const tenantInput = await screen.findByLabelText("Tenant");
    expect(tenantInput.tagName).toBe("INPUT");
    await userEvent.type(tenantInput, "acme");
    await userEvent.type(screen.getByLabelText("Dataset"), "prod");
    await userEvent.click(screen.getByRole("button", { name: "Apply" }));
    expect(update).toHaveBeenCalledWith({ tenant: "acme", dataset: "prod" });
  });

  it("links to /manage for tenant administrators", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    renderTopBar({ state: DEFAULT_STATE, update: vi.fn() });
    // The panel itself (Ingestion scopes, etc.) is owned by ManagementRoute
    // now — see App.test.tsx's "/manage" tests — TopBar just links there.
    expect(await screen.findByRole("link", { name: "Manage" })).toHaveAttribute(
      "href",
      "/manage",
    );
  });

  it("hides the manage link from viewers", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/whoami",
        body: {
          ...WHOAMI,
          memberships: [{ tenant_id: "acme", role: "viewer" }],
        },
      },
    ]);
    renderTopBar({ state: DEFAULT_STATE, update: vi.fn() });
    await waitFor(() =>
      expect(
        screen.getByTitle("Tenant / dataset context for all queries"),
      ).toHaveTextContent("acme"),
    );
    expect(screen.queryByRole("link", { name: "Manage" })).toBeNull();
  });
});
