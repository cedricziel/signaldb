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

const WHOAMI_MULTI = {
  ...WHOAMI,
  memberships: [
    { tenant_id: "acme", role: "admin" },
    { tenant_id: "globex", role: "member" },
  ],
};

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("TopBar whoami gating", () => {
  it("issues no whoami request without a tenant in state", async () => {
    const fetchMock = stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI },
    ]);
    renderTopBar({ state: DEFAULT_STATE, update: vi.fn() });
    // Let any (wrongly) enabled query fire before asserting the negative.
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(
      fetchMock.mock.calls.some((call) =>
        String(call[0]).includes("/api/v1/whoami"),
      ),
    ).toBe(false);
  });

  it("issues a whoami request once a tenant is present in state", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    renderTopBar({
      state: { ...DEFAULT_STATE, tenant: "acme", dataset: "production" },
      update: vi.fn(),
    });
    expect(
      await screen.findByRole("link", { name: "Manage" }),
    ).toBeInTheDocument();
  });
});

describe("TenantSelector with whoami", () => {
  it("shows the tenant read-only and datasets as a selector", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    const update = vi.fn();
    renderTopBar({
      state: { ...DEFAULT_STATE, tenant: "acme", dataset: "production" },
      update,
    });

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
    // Inputs are prefilled from the build-time SIGNALDB_TENANT/_DATASET
    // defaults (empty in CI, but not necessarily in a dev's local env) —
    // clear before typing so the test doesn't depend on that ambient value.
    await userEvent.clear(tenantInput);
    await userEvent.type(tenantInput, "acme");
    const datasetInput = screen.getByLabelText("Dataset");
    await userEvent.clear(datasetInput);
    await userEvent.type(datasetInput, "prod");
    await userEvent.click(screen.getByRole("button", { name: "Apply" }));
    expect(update).toHaveBeenCalledWith({ tenant: "acme", dataset: "prod" });
  });

  it("links the logo back to Explore, carrying tenant/dataset context", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    renderTopBar({
      state: { ...DEFAULT_STATE, tenant: "acme", dataset: "staging" },
      update: vi.fn(),
    });
    expect(
      await screen.findByRole("link", { name: /signaldb/i }),
    ).toHaveAttribute("href", "/logs?tenant=acme&dataset=staging");
  });

  it("links to /manage for tenant administrators", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    renderTopBar({
      state: { ...DEFAULT_STATE, tenant: "acme", dataset: "production" },
      update: vi.fn(),
    });
    // The panel itself (Ingestion scopes, etc.) is owned by ManagementRoute
    // now — see App.test.tsx's "/manage" tests — TopBar just links there.
    expect(await screen.findByRole("link", { name: "Manage" })).toHaveAttribute(
      "href",
      "/manage",
    );
  });

  it("does not commit on tenant selection alone; Apply resets the dataset when the tenant changed", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI_MULTI }]);
    const update = vi.fn();
    renderTopBar({
      state: { ...DEFAULT_STATE, tenant: "acme", dataset: "production" },
      update,
    });
    await userEvent.click(
      screen.getByTitle("Tenant / dataset context for all queries"),
    );
    const tenantSelect = await screen.findByLabelText("Tenant");
    expect(tenantSelect).toHaveFocus();

    await userEvent.selectOptions(tenantSelect, "globex");
    // Picking a tenant alone must not commit or close the form.
    expect(update).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "Apply" })).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Apply" }));
    expect(update).toHaveBeenCalledTimes(1);
    expect(update).toHaveBeenCalledWith({ tenant: "globex", dataset: "" });
  });

  it("puts autofocus on the dataset select when the tenant is fixed (single membership)", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI }]);
    renderTopBar({
      state: { ...DEFAULT_STATE, tenant: "acme", dataset: "production" },
      update: vi.fn(),
    });
    await userEvent.click(
      screen.getByTitle("Tenant / dataset context for all queries"),
    );
    expect(await screen.findByLabelText("Dataset")).toHaveFocus();
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
    renderTopBar({
      state: { ...DEFAULT_STATE, tenant: "acme", dataset: "production" },
      update: vi.fn(),
    });
    await waitFor(() =>
      expect(
        screen.getByTitle("Tenant / dataset context for all queries"),
      ).toHaveTextContent("acme"),
    );
    expect(screen.queryByRole("link", { name: "Manage" })).toBeNull();
  });
});
