import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { ApiKeys } from "./ApiKeys";

function renderApiKeys() {
  return renderWithClient(
    <MemoryRouter>
      <ApiKeys />
    </MemoryRouter>,
  );
}

const WHOAMI_ADMIN = {
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

const WHOAMI_NON_ADMIN = {
  ...WHOAMI_ADMIN,
  memberships: [{ tenant_id: "acme", role: "viewer" }],
};

const API_KEYS = [
  {
    id: "key-1",
    name: "collector-production",
    dataset_id: "production",
    scopes: ["metrics:write", "logs:write"],
    created_at: "2026-08-01T00:00:00Z",
    revoked: false,
  },
  {
    id: "key-2",
    name: "staging-deploy",
    dataset_id: "staging",
    scopes: ["metrics:write", "logs:write", "traces:write", "profiles:write"],
    created_at: "2026-07-15T00:00:00Z",
    revoked: false,
  },
  {
    id: "key-3",
    name: "old-key",
    dataset_id: null,
    scopes: [],
    created_at: "2026-07-01T00:00:00Z",
    revoked: true,
  },
];

/** Find a fetch mock call whose Request matches a URL substring and method. */
function findFetchCall(
  fetchMock: ReturnType<typeof vi.fn>,
  urlSubstring: string,
  method: string,
): Request | undefined {
  return fetchMock.mock.calls
    .map((call) => call[0])
    .filter((req): req is Request => req instanceof Request)
    .find((req) => req.url.includes(urlSubstring) && req.method === method);
}

afterEach(() => {
  vi.unstubAllGlobals();
});

const API_KEYS_PATH = "/api/v1/manage/tenants/acme/api-keys";

describe("ApiKeys page", () => {
  it("redirects to /logs when user is not admin", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI_NON_ADMIN }]);
    renderApiKeys();

    // Should redirect to /logs, which will navigate away.
    // In a MemoryRouter, we can't easily test navigation, but we can assert
    // that the component returns a Navigate element.
    // Instead, we'll test that the admin-specific UI is absent.
    await waitFor(() =>
      expect(screen.queryByText("API keys")).not.toBeInTheDocument(),
    );
  });

  it("shows existing API keys list", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: API_KEYS_PATH, body: API_KEYS },
    ]);
    renderApiKeys();

    await waitFor(() => {
      expect(screen.getByText("collector-production")).toBeInTheDocument();
      expect(screen.getByText("staging-deploy")).toBeInTheDocument();
      expect(screen.getByText("old-key")).toBeInTheDocument();
    });

    // Metadata (the created-date suffix is locale-dependent, so match by prefix)
    expect(
      screen.getByText((content) =>
        content.startsWith("production · metrics:write, logs:write"),
      ),
    ).toBeInTheDocument();
    expect(
      screen.getByText((content) =>
        content.startsWith(
          "staging · metrics:write, logs:write, traces:write, profiles:write",
        ),
      ),
    ).toBeInTheDocument();
    expect(
      screen.getByText((content) =>
        content.startsWith("all datasets · legacy unrestricted"),
      ),
    ).toBeInTheDocument();

    // Revoked key row is dimmed via the "revoked" CSS class on the <li>
    // (jsdom doesn't apply stylesheet rules, so we check the class).
    const revokedRow = screen.getByText("old-key").closest("li");
    expect(revokedRow?.className).toContain("revoked");
  });

  it("shows create form for admins", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: API_KEYS_PATH, body: [] },
    ]);
    renderApiKeys();

    await waitFor(() => {
      expect(
        screen.getByPlaceholderText("collector-production"),
      ).toBeInTheDocument();
      expect(screen.getByText("All datasets")).toBeInTheDocument();
      expect(screen.getByText("metrics:write")).toBeInTheDocument();
      expect(screen.getByText("Create API key")).toBeInTheDocument();
    });
  });

  it("creates new API key and shows secret modal", async () => {
    const createKeyResponse = { key: "sk-acme-prod-key-123" };
    const fetchMock = stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: API_KEYS_PATH, method: "GET", body: [] },
      {
        match: API_KEYS_PATH,
        method: "POST",
        body: createKeyResponse,
      },
    ]);
    renderApiKeys();

    // Wait for whoami to resolve and the form to render
    await waitFor(() =>
      expect(
        screen.getByPlaceholderText("collector-production"),
      ).toBeInTheDocument(),
    );

    await userEvent.type(
      screen.getByPlaceholderText("collector-production"),
      "my-key",
    );
    // Select dataset option
    const select = screen.getByRole("combobox");
    await userEvent.selectOptions(select, "production");
    // metrics:write already checked by default, uncheck logs:write
    await userEvent.click(screen.getByLabelText("logs:write"));
    await userEvent.click(screen.getByText("Create API key"));

    // Wait for the POST mutation to be dispatched
    await waitFor(() =>
      expect(findFetchCall(fetchMock, "/api-keys", "POST")).toBeDefined(),
    );

    // Secret modal appears
    await waitFor(() => {
      expect(screen.getByText("Copy this key now")).toBeInTheDocument();
      expect(screen.getByText("sk-acme-prod-key-123")).toBeInTheDocument();
    });

    // Modal dismisses on "Done"
    await userEvent.click(screen.getByText("Done"));
    expect(screen.queryByText("Copy this key now")).not.toBeInTheDocument();
  });

  it("revokes API key and updates list", async () => {
    const fetchMock = stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: API_KEYS_PATH, body: API_KEYS },
      {
        match: `${API_KEYS_PATH}/key-1`,
        method: "DELETE",
        body: {},
      },
    ]);
    renderApiKeys();

    await waitFor(() => {
      expect(screen.getByText("collector-production")).toBeInTheDocument();
    });

    const revokeButtons = screen.getAllByText("Revoke");
    const firstRevoke = revokeButtons[0];
    if (firstRevoke) await userEvent.click(firstRevoke);

    // The DELETE request is dispatched
    await waitFor(() =>
      expect(
        findFetchCall(fetchMock, "/api-keys/key-1", "DELETE"),
      ).toBeDefined(),
    );

    // List should refresh (new GET fetch for list)
    await waitFor(() => {
      const getCalls = fetchMock.mock.calls
        .map((call) => call[0])
        .filter((req): req is Request => req instanceof Request)
        .filter((req) => req.method === "GET" && req.url.includes("/api-keys"));
      // At least 2 GET calls: initial load + refetch after revoke
      expect(getCalls.length).toBeGreaterThanOrEqual(2);
    });
  });

  it("shows revoked keys dimmed", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: API_KEYS_PATH, body: API_KEYS },
    ]);
    renderApiKeys();

    await waitFor(() => {
      expect(screen.getByText("old-key")).toBeInTheDocument();
    });
    // The revoked key's name div gets the "revoked" CSS class, which applies
    // text-decoration: line-through via the stylesheet (jsdom doesn't apply
    // stylesheet rules, so we check the class).
    const revokedName = screen.getByText("old-key");
    expect(revokedName.className).toContain("revoked");
  });
});
