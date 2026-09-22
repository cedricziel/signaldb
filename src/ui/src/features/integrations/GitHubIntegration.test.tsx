import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter, Route, Routes, useLocation } from "react-router";
import {
  outletContextRoute,
  renderWithClient,
  stubFetchRoutes,
} from "../../test/render";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import { GitHubIntegration } from "./GitHubIntegration";

vi.mock("../../lib/navigateExternal", () => ({
  navigateExternal: vi.fn(),
}));

const { navigateExternal } = await import("../../lib/navigateExternal");

/** Exposes the current route's search string so tests can assert the
 * callback params (`?github=…`) were stripped after mount. */
function LocationProbe() {
  const { search } = useLocation();
  return <div data-testid="search">{search}</div>;
}

function renderGitHubIntegration(
  path = "/integrations/github",
  state: Partial<ExploreState> = {},
) {
  const contextState: ExploreState = {
    ...DEFAULT_STATE,
    tenant: "acme",
    dataset: "production",
    ...state,
  };
  return renderWithClient(
    <MemoryRouter initialEntries={[path]}>
      <LocationProbe />
      <Routes>
        <Route element={outletContextRoute(contextState)}>
          <Route path="/integrations/github" element={<GitHubIntegration />} />
          <Route path="/logs" element={<div>Logs page</div>} />
        </Route>
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

// The attach form is instance-admin-gated (the router requires it too: a
// `tenant:manage` grant alone is not enough, since attach skips the OAuth
// flow's GitHub-side ownership check — see docs/operations/github-app.md).
const WHOAMI_INSTANCE_ADMIN = {
  ...WHOAMI_ADMIN,
  user: { ...WHOAMI_ADMIN.user, is_instance_admin: true },
};

const NOT_CONFIGURED = {
  configured: false,
  app_slug: null,
  installations: [],
};

const CONFIGURED_EMPTY = {
  configured: true,
  app_slug: "signaldb",
  installations: [],
};

const INSTALLATIONS_PATH = "/api/v1/manage/tenants/acme/github-installations";

const ONE_INSTALLATION = {
  configured: true,
  app_slug: "signaldb",
  installations: [
    {
      installation_id: 42,
      account_login: "acme-org",
      account_type: "Organization",
      repositories: ["acme-org/api", "acme-org/web"],
      repositories_synced_at: "2026-09-01T00:00:00Z",
      stale: true,
      linked_by_github_login: "alice",
      manage_url:
        "https://github.com/organizations/acme-org/settings/installations/42",
      created_at: "2026-08-01T00:00:00Z",
      updated_at: "2026-09-01T00:00:00Z",
    },
  ],
};

afterEach(() => {
  vi.unstubAllGlobals();
  vi.clearAllMocks();
});

describe("GitHubIntegration page", () => {
  it("redirects to /logs when user is not admin", async () => {
    stubFetchRoutes([{ match: "/api/v1/whoami", body: WHOAMI_NON_ADMIN }]);
    renderGitHubIntegration();

    await waitFor(() =>
      expect(screen.getByText("Logs page")).toBeInTheDocument(),
    );
    expect(screen.queryByText("GitHub")).not.toBeInTheDocument();
  });

  it("shows the not-configured empty state and no Connect button", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: INSTALLATIONS_PATH, body: NOT_CONFIGURED },
    ]);
    renderGitHubIntegration();

    expect(
      await screen.findByText("GitHub is not configured on this server"),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "Connect GitHub" }),
    ).not.toBeInTheDocument();
  });

  it("renders installations with repos, stale marker and manage link", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: INSTALLATIONS_PATH, body: ONE_INSTALLATION },
    ]);
    renderGitHubIntegration();

    await waitFor(() =>
      expect(screen.getByText("acme-org")).toBeInTheDocument(),
    );
    expect(screen.getByText("Organization")).toBeInTheDocument();
    expect(screen.getByText("acme-org/api")).toBeInTheDocument();
    expect(screen.getByText("acme-org/web")).toBeInTheDocument();
    expect(
      screen.getByText("(stale — GitHub could not be reached)"),
    ).toBeInTheDocument();
    expect(screen.getByText("Linked by @alice", { exact: false })).toBeTruthy();
    const manageLink = screen.getByRole("link", { name: /manage on github/i });
    expect(manageLink).toHaveAttribute(
      "href",
      "https://github.com/organizations/acme-org/settings/installations/42",
    );
    expect(manageLink).toHaveAttribute("target", "_blank");
    expect(manageLink).toHaveAttribute("rel", "noopener noreferrer");
  });

  it("shows the empty state when configured with no installations", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: INSTALLATIONS_PATH, body: CONFIGURED_EMPTY },
    ]);
    renderGitHubIntegration();

    expect(
      await screen.findByText("No GitHub installations linked yet."),
    ).toBeInTheDocument();
  });

  it("Connect calls the start endpoint and hands install_url to navigateExternal", async () => {
    const fetchMock = stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: INSTALLATIONS_PATH, method: "GET", body: CONFIGURED_EMPTY },
      {
        match: `${INSTALLATIONS_PATH}/link`,
        method: "POST",
        body: {
          install_url:
            "https://github.com/apps/signaldb/installations/new?state=abc",
          expires_at: "2026-09-01T00:10:00Z",
        },
        status: 201,
      },
    ]);
    renderGitHubIntegration();

    const connectButton = await screen.findByRole("button", {
      name: "Connect GitHub",
    });
    await userEvent.click(connectButton);

    await waitFor(() =>
      expect(navigateExternal).toHaveBeenCalledWith(
        "https://github.com/apps/signaldb/installations/new?state=abc",
      ),
    );
    const postCall = fetchMock.mock.calls
      .map((call) => call[0])
      .filter((req): req is Request => req instanceof Request)
      .find((req) => req.url.includes("/link") && req.method === "POST");
    expect(postCall).toBeDefined();
  });

  it("Link existing installation is hidden for a tenant admin who is not instance-admin", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: INSTALLATIONS_PATH, method: "GET", body: CONFIGURED_EMPTY },
    ]);
    renderGitHubIntegration();

    await screen.findByRole("button", { name: "Connect GitHub" });
    expect(
      screen.queryByLabelText("GitHub installation ID"),
    ).not.toBeInTheDocument();
  });

  it("Link existing installation calls the attach endpoint and refetches", async () => {
    const fetchMock = stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_INSTANCE_ADMIN },
      { match: INSTALLATIONS_PATH, method: "GET", body: CONFIGURED_EMPTY },
      {
        match: `${INSTALLATIONS_PATH}/attach`,
        method: "POST",
        body: ONE_INSTALLATION.installations[0],
        status: 201,
      },
    ]);
    renderGitHubIntegration();

    const input = await screen.findByLabelText("GitHub installation ID");
    await userEvent.type(input, "42");
    await userEvent.click(
      screen.getByRole("button", { name: "Link existing installation" }),
    );

    await waitFor(() => {
      const postCall = fetchMock.mock.calls
        .map((call) => call[0])
        .filter((req): req is Request => req instanceof Request)
        .find((req) => req.url.includes("/attach") && req.method === "POST");
      expect(postCall).toBeDefined();
    });
    await waitFor(() => expect(input).toHaveValue(null));

    await waitFor(() => {
      const getCalls = fetchMock.mock.calls
        .map((call) => call[0])
        .filter((req): req is Request => req instanceof Request)
        .filter(
          (req) =>
            req.method === "GET" &&
            req.url.includes("github-installations") &&
            !req.url.includes("/attach"),
        );
      // Initial load + refetch after attach.
      expect(getCalls.length).toBeGreaterThanOrEqual(2);
    });
  });

  it("Remove confirms then DELETEs and refetches", async () => {
    const fetchMock = stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: INSTALLATIONS_PATH, method: "GET", body: ONE_INSTALLATION },
      {
        match: `${INSTALLATIONS_PATH}/42`,
        method: "DELETE",
        body: {},
      },
    ]);
    renderGitHubIntegration();

    await waitFor(() =>
      expect(screen.getByText("acme-org")).toBeInTheDocument(),
    );
    await userEvent.click(screen.getByRole("button", { name: "Remove" }));
    await userEvent.click(screen.getByRole("button", { name: "Confirm" }));

    await waitFor(() => {
      const deleteCall = fetchMock.mock.calls
        .map((call) => call[0])
        .filter((req): req is Request => req instanceof Request)
        .find(
          (req) =>
            req.url.includes("/github-installations/42") &&
            req.method === "DELETE",
        );
      expect(deleteCall).toBeDefined();
    });

    await waitFor(() => {
      const getCalls = fetchMock.mock.calls
        .map((call) => call[0])
        .filter((req): req is Request => req instanceof Request)
        .filter(
          (req) =>
            req.method === "GET" &&
            req.url.includes("github-installations") &&
            !req.url.includes("/42"),
        );
      // Initial load + refetch after remove.
      expect(getCalls.length).toBeGreaterThanOrEqual(2);
    });
  });

  it("shows the success banner for a linked callback and strips the params", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: INSTALLATIONS_PATH, body: CONFIGURED_EMPTY },
    ]);
    renderGitHubIntegration(
      "/integrations/github?github=linked&installation_id=777",
    );

    expect(
      await screen.findByText("GitHub installation 777 linked."),
    ).toBeInTheDocument();
    await waitFor(() =>
      expect(screen.getByTestId("search")).toHaveTextContent(""),
    );
  });

  it("shows the permissions error sentence for ?github=error&reason=permissions", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_ADMIN },
      { match: INSTALLATIONS_PATH, body: CONFIGURED_EMPTY },
    ]);
    renderGitHubIntegration(
      "/integrations/github?github=error&reason=permissions",
    );

    expect(
      await screen.findByText(
        "That installation grants write permissions; SignalDB only accepts read-only installations.",
      ),
    ).toBeInTheDocument();
  });
});
