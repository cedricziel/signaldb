import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter, Route, Routes } from "react-router";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { ApiKeys } from "./ApiKeys";

function renderApiKeys() {
  return renderWithClient(
    <MemoryRouter initialEntries={["/api-keys"]}>
      <Routes>
        <Route path="/api-keys" element={<ApiKeys />} />
        <Route path="/logs" element={<div>Logs page</div>} />
      </Routes>
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
  {
    id: "key-4",
    name: "ci-provisioner",
    dataset_id: null,
    scopes: ["tenant:manage"],
    created_at: "2026-08-10T00:00:00Z",
    revoked: false,
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

    await waitFor(() =>
      expect(screen.getByText("Logs page")).toBeInTheDocument(),
    );
    expect(screen.queryByText("API keys")).not.toBeInTheDocument();
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
    // A management key lists its tenant:manage scope like any other scope.
    expect(
      screen.getByText((content) =>
        content.startsWith("all datasets · tenant:manage"),
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

  it("pre-checks all four ingestion scopes by default, leaving other scopes unchecked", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: API_KEYS_PATH, body: [] },
    ]);
    renderApiKeys();

    await waitFor(() =>
      expect(screen.getByText("Create API key")).toBeInTheDocument(),
    );
    for (const scope of [
      "metrics:write",
      "logs:write",
      "traces:write",
      "profiles:write",
    ]) {
      expect(screen.getByLabelText(scope)).toBeChecked();
    }
    expect(screen.getByLabelText("schema:read")).not.toBeChecked();
    expect(screen.getByLabelText("schema:write")).not.toBeChecked();
    expect(screen.getByLabelText("tenant:manage")).not.toBeChecked();
  });

  it("groups the scope picker into Ingestion and Schema with descriptions", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: API_KEYS_PATH, body: [] },
    ]);
    renderApiKeys();

    await waitFor(() =>
      expect(screen.getByText("Create API key")).toBeInTheDocument(),
    );
    const ingestion = screen.getByRole("group", { name: "Ingestion" });
    const schema = screen.getByRole("group", { name: "Schema" });
    for (const scope of [
      "metrics:write",
      "logs:write",
      "traces:write",
      "profiles:write",
    ]) {
      expect(ingestion).toContainElement(screen.getByLabelText(scope));
    }
    expect(schema).toContainElement(screen.getByLabelText("schema:read"));
    expect(schema).toContainElement(screen.getByLabelText("schema:write"));
    // One-line descriptions accompany each scope.
    expect(schema).toHaveTextContent(/read the schema registry/i);
    expect(schema).toHaveTextContent(/custom registries/i);
    expect(ingestion).toHaveTextContent(/ingest traces/i);
  });

  it("offers the tenant:manage scope with a description", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: API_KEYS_PATH, body: [] },
    ]);
    renderApiKeys();

    await waitFor(() =>
      expect(screen.getByText("Create API key")).toBeInTheDocument(),
    );
    const management = screen.getByRole("group", { name: "Management" });
    expect(management).toContainElement(screen.getByLabelText("tenant:manage"));
    expect(management).toHaveTextContent(/datasets, keys, and members/i);
  });

  it("creates a key with the tenant:manage scope", async () => {
    const fetchMock = stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: API_KEYS_PATH, method: "GET", body: [] },
      { match: API_KEYS_PATH, method: "POST", body: { key: "sdbk_manage" } },
    ]);
    renderApiKeys();
    await waitFor(() =>
      expect(screen.getByText("Create API key")).toBeInTheDocument(),
    );

    // Deselect the default ingestion scopes, pick only tenant:manage.
    for (const scope of [
      "metrics:write",
      "logs:write",
      "traces:write",
      "profiles:write",
    ]) {
      await userEvent.click(screen.getByLabelText(scope));
    }
    await userEvent.click(screen.getByLabelText("tenant:manage"));
    await userEvent.click(screen.getByText("Create API key"));

    await waitFor(() =>
      expect(findFetchCall(fetchMock, "/api-keys", "POST")).toBeDefined(),
    );
    const post = findFetchCall(fetchMock, "/api-keys", "POST")!;
    expect(await post.clone().json()).toEqual({ scopes: ["tenant:manage"] });
  });

  it("creates a key with schema scopes", async () => {
    const fetchMock = stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: API_KEYS_PATH, method: "GET", body: [] },
      { match: API_KEYS_PATH, method: "POST", body: { key: "sdbk_schema" } },
    ]);
    renderApiKeys();
    await waitFor(() =>
      expect(screen.getByText("Create API key")).toBeInTheDocument(),
    );

    // Deselect the default ingestion scopes, pick both schema scopes.
    for (const scope of [
      "metrics:write",
      "logs:write",
      "traces:write",
      "profiles:write",
    ]) {
      await userEvent.click(screen.getByLabelText(scope));
    }
    await userEvent.click(screen.getByLabelText("schema:read"));
    await userEvent.click(screen.getByLabelText("schema:write"));
    await userEvent.click(screen.getByText("Create API key"));

    await waitFor(() =>
      expect(findFetchCall(fetchMock, "/api-keys", "POST")).toBeDefined(),
    );
    const post = findFetchCall(fetchMock, "/api-keys", "POST")!;
    expect(await post.clone().json()).toEqual({
      scopes: ["schema:read", "schema:write"],
    });
  });

  it("edits the scopes of a live key via PATCH", async () => {
    const fetchMock = stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: API_KEYS_PATH, method: "GET", body: API_KEYS },
      {
        match: `${API_KEYS_PATH}/key-1`,
        method: "PATCH",
        body: { ...API_KEYS[0], scopes: ["metrics:write", "schema:read"] },
      },
    ]);
    renderApiKeys();
    await waitFor(() =>
      expect(screen.getByText("collector-production")).toBeInTheDocument(),
    );

    // Revoked keys offer no editor; live keys do.
    const editButtons = screen.getAllByText("Edit scopes");
    expect(editButtons).toHaveLength(3);
    await userEvent.click(editButtons[0]!);

    const editor = screen.getByRole("form", { name: "Edit scopes" });
    // key-1 carries metrics:write + logs:write: drop logs:write, add schema:read.
    const { getByLabelText, getByText } = within(editor);
    expect(getByLabelText("metrics:write")).toBeChecked();
    expect(getByLabelText("logs:write")).toBeChecked();
    expect(getByLabelText("schema:read")).not.toBeChecked();
    await userEvent.click(getByLabelText("logs:write"));
    await userEvent.click(getByLabelText("schema:read"));
    await userEvent.click(getByText("Save scopes"));

    await waitFor(() =>
      expect(
        findFetchCall(fetchMock, "/api-keys/key-1", "PATCH"),
      ).toBeDefined(),
    );
    const patch = findFetchCall(fetchMock, "/api-keys/key-1", "PATCH")!;
    expect(await patch.clone().json()).toEqual({
      scopes: ["metrics:write", "schema:read"],
    });
    // Editor closes and the list refetches.
    await waitFor(() =>
      expect(
        screen.queryByRole("form", { name: "Edit scopes" }),
      ).not.toBeInTheDocument(),
    );
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
    // All four ingestion scopes are checked by default; uncheck logs:write
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

    const revokeButtons = screen.getAllByRole("button", { name: "Revoke" });
    const firstRevoke = revokeButtons[0];
    if (firstRevoke) await userEvent.click(firstRevoke);
    await userEvent.click(screen.getByRole("button", { name: "Confirm" }));

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
