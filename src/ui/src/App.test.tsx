import { act, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { RouterProvider } from "react-router";
import { TENANT_CONTEXT_STORAGE_KEY, getTenantContext } from "./api/http";
import * as catalogApi from "./api/catalog";
import { WHOAMI_TENANT_ADMIN } from "./features/schema/testFixtures";
import { markDirty, resetDirtyForms } from "./lib/dirtyForms";
import {
  getUpdateState,
  resetUpdateState,
  setUpdateAvailable,
} from "./lib/pwaUpdate";
import { createAppRouter } from "./routes";
import {
  emptyIrLogs,
  emptyMatrix,
  emptyStreams,
  renderWithClient,
  stubFetchRoutes,
} from "./test/render";

// The catalog's server-side aggregates are mocked at the module boundary (as
// in CatalogView.test.tsx); everything else in the shell keeps real fetches.
vi.mock("./api/catalog", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api/catalog")>();
  return {
    ...actual,
    fetchCatalogEntities: vi
      .fn()
      .mockResolvedValue({ groups: [], truncated: false }),
  };
});
vi.mock("./api/traceGroupMembers", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("./api/traceGroupMembers")>();
  return {
    ...actual,
    fetchTraceGroupMembers: vi.fn().mockResolvedValue([]),
  };
});

// The same data router as main.tsx (`createAppRouter`), not a plain
// `<BrowserRouter>` — the shell's `UnsavedChangesGuard` needs `useBlocker`,
// which throws under a declarative router. It still drives `window.location`
// via the DOM history API (jsdom implements it), so every assertion below
// that reads `window.location` keeps working unchanged.
function renderApp(path = "/") {
  window.history.replaceState(null, "", path);
  return renderWithClient(<RouterProvider router={createAppRouter()} />);
}

/** A page link in the sidebar's page list. */
function navLink(name: string) {
  return within(screen.getByRole("navigation", { name: "Pages" })).getByRole(
    "link",
    { name },
  );
}

const WHOAMI_TWO_TENANTS = {
  user: {
    id: "u1",
    email: "a@b",
    display_name: "A",
    is_instance_admin: false,
  },
  memberships: [
    { tenant_id: "acme", role: "admin" },
    { tenant_id: "globex", role: "member" },
  ],
  tenant: { id: "acme", slug: "acme", name: "Acme" },
  datasets: [
    { id: "prod", slug: "prod", is_default: true },
    { id: "staging", slug: "staging", is_default: false },
  ],
  default_dataset: "prod",
};

afterEach(() => {
  vi.unstubAllGlobals();
  window.history.replaceState(null, "", "/");
  localStorage.clear();
  resetUpdateState();
  resetDirtyForms();
});

describe("App", () => {
  it("renders the shell with the product mark and the page nav", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/api/v1/query", body: emptyIrLogs },
    ]);
    renderApp("/logs");
    expect(
      screen.getByRole("complementary", { name: "Main navigation" }),
    ).toHaveTextContent(/signaldb/i);
    expect(navLink("Logs")).toHaveAttribute("aria-current", "page");
    expect(
      screen.getByRole("navigation", { name: "Current page" }),
    ).toHaveTextContent("Investigate/Logs");
    expect(
      await screen.findByText(/No log lines in this range/),
    ).toBeInTheDocument();
  });

  it("redirects / to /overview", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/api/v1/query", body: emptyIrLogs },
    ]);
    renderApp("/?tenant=acme");
    await waitFor(() => expect(window.location.pathname).toBe("/overview"));
    expect(window.location.search).toContain("tenant=acme");
    expect(
      within(screen.getByRole("navigation", { name: "Pages" })).getByRole(
        "link",
        { name: "Overview" },
      ),
    ).toHaveAttribute("aria-current", "page");
  });

  it("redirects an unknown path to /logs", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/api/v1/query", body: emptyIrLogs },
    ]);
    renderApp("/bogus");
    await screen.findByText(/No log lines in this range/);
    expect(window.location.pathname).toBe("/logs");
  });

  it("changes the tenant and dataset from the sidebar switcher", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/api/v1/query", body: emptyIrLogs },
      { match: "/api/v1/whoami", body: WHOAMI_TWO_TENANTS },
    ]);
    renderApp("/logs?tenant=acme&dataset=prod");
    const user = (await import("@testing-library/user-event")).default;
    await user.click(
      await screen.findByRole("button", { name: /Switch tenant or dataset/ }),
    );
    const tenants = screen.getByRole("listbox", { name: "Tenant" });
    expect(
      within(tenants).getByRole("option", { name: "acme" }),
    ).toHaveAttribute("aria-selected", "true");
    // Picking a tenant resets the dataset and keeps the popover open …
    await user.click(within(tenants).getByRole("option", { name: "globex" }));
    expect(window.location.search).toContain("tenant=globex");
    expect(window.location.search).not.toContain("dataset=");
    // … picking a dataset applies it and closes the popover.
    await user.click(
      within(screen.getByRole("listbox", { name: "Dataset" })).getByRole(
        "option",
        { name: "staging" },
      ),
    );
    expect(window.location.search).toContain("dataset=staging");
    expect(screen.queryByRole("listbox", { name: "Tenant" })).toBeNull();
    expect(window.location.pathname).toBe("/logs");
  });

  it("changing the dataset from the switcher stays on a non-explore route", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/api/v1/query", body: emptyIrLogs },
      { match: "/api/v1/schema/registries", body: { registries: [] } },
      { match: "/api/v1/whoami", body: WHOAMI_TWO_TENANTS },
    ]);
    renderApp("/schema/conventions?tenant=acme&dataset=prod");
    const user = (await import("@testing-library/user-event")).default;
    await user.click(
      await screen.findByRole("button", { name: /Switch tenant or dataset/ }),
    );
    await user.click(await screen.findByRole("option", { name: "staging" }));
    expect(window.location.search).toContain("dataset=staging");
    // Only the context changed; the route must not fall back to /logs.
    expect(window.location.pathname).toBe("/schema/conventions");
  });

  it("keeps the last tenant/dataset when navigating to a route without them in the URL", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/api/v1/query", body: emptyIrLogs },
      { match: "/api/v1/schema/registries", body: { registries: [] } },
      {
        match: "/api/v1/whoami",
        body: {
          user: {
            id: "u1",
            email: "a@b",
            display_name: "A",
            is_instance_admin: false,
          },
          memberships: [{ tenant_id: "acme", role: "admin" }],
          tenant: { id: "acme", slug: "acme", name: "Acme" },
          dataset: "prod",
          datasets: [],
        },
      },
    ]);
    renderApp("/logs?tenant=acme&dataset=prod");
    await screen.findByRole("button", { name: /acme/ });
    expect(getTenantContext()).toEqual({ tenant: "acme", dataset: "prod" });
    // A link that carries no search params (user menu → Schema, /manage, …)
    window.history.pushState(null, "", "/schema/conventions");
    window.dispatchEvent(new PopStateEvent("popstate"));
    await waitFor(() =>
      expect(window.location.search).toContain("tenant=acme"),
    );
    // Writing the context back must not rewrite the path: /schema is not
    // an explore route, and rebuilding it via the explore state would send
    // the user to /logs.
    expect(window.location.pathname).toBe("/schema/conventions");
    expect(getTenantContext()).toEqual({ tenant: "acme", dataset: "prod" });
  });

  it("restores the last tenant/dataset in a fresh tab that opens a route without them", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/api/v1/query", body: emptyIrLogs },
      { match: "/api/v1/schema/registries", body: { registries: [] } },
      { match: "/api/v1/schema", body: { logical: [], physical: [] } },
      {
        match: "/api/v1/whoami",
        body: {
          user: {
            id: "u1",
            email: "a@b",
            display_name: "A",
            is_instance_admin: true,
          },
          memberships: [{ tenant_id: "acme", role: "admin" }],
          tenant: { id: "acme", slug: "acme", name: "Acme" },
          dataset: "prod",
          datasets: [],
        },
      },
    ]);
    // A previous visit (any tab) settled on acme/prod …
    localStorage.setItem(
      TENANT_CONTEXT_STORAGE_KEY,
      JSON.stringify({ tenant: "acme", dataset: "prod" }),
    );
    // … and a bookmark/deep link opens a bare route in a new tab.
    renderApp("/schema/storage");
    await waitFor(() =>
      expect(window.location.search).toContain("tenant=acme"),
    );
    expect(window.location.pathname).toBe("/schema/storage");
    expect(getTenantContext()).toEqual({ tenant: "acme", dataset: "prod" });
  });

  it("persists the tenant/dataset context for later tabs", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/api/v1/query", body: emptyIrLogs },
    ]);
    localStorage.removeItem(TENANT_CONTEXT_STORAGE_KEY);
    renderApp("/logs?tenant=globex&dataset=main");
    await screen.findByRole("button", { name: /globex/ });
    expect(
      JSON.parse(localStorage.getItem(TENANT_CONTEXT_STORAGE_KEY) ?? "{}"),
    ).toEqual({ tenant: "globex", dataset: "main" });
  });

  it("routes catalog entity detail under /catalog/:entity/:primary", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/api/v1/query", body: emptyIrLogs },
    ]);
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/api/v1/query", body: emptyIrLogs },
      { match: "/api/v1/query", body: { rows: [], columns: [] } },
    ]);
    renderApp("/catalog/host/db-01?tenant=acme");
    const crumb = await screen.findByRole("navigation", {
      name: "Breadcrumb",
    });
    expect(crumb).toHaveTextContent("Hosts");
    expect(crumb).toHaveTextContent("db-01");
    expect(window.location.pathname).toBe("/catalog/host/db-01");
    expect(window.location.search).toContain("tenant=acme");
  });

  it("drills from the catalog list into an entity route and back", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/api/v1/query", body: emptyIrLogs },
    ]);
    vi.mocked(catalogApi.fetchCatalogEntities).mockResolvedValue({
      entities: [
        {
          values: ["gateway", "edge"],
          observations: [{ source: "traces", count: 12 }],
          lastNs: "1700000000000000000",
          red: { traces: 12, errors: 0, p50Ms: 1, p95Ms: 2 },
        },
      ],
      truncated: false,
    });
    // Detail-only aggregates (span kinds, dependency breakdown) go through
    // the generic query endpoint; an empty envelope is enough here.
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/api/v1/query", body: emptyIrLogs },
      { match: "/api/v1/query", body: { rows: [], columns: [] } },
    ]);
    renderApp("/catalog/service?tenant=acme");
    const user = (await import("@testing-library/user-event")).default;
    await user.click(await screen.findByText("gateway"));
    await waitFor(() =>
      expect(window.location.pathname).toBe("/catalog/service/gateway,edge"),
    );
    expect(window.location.search).toContain("tenant=acme");
    expect(
      await screen.findByRole("navigation", { name: "Breadcrumb" }),
    ).toHaveTextContent("gateway · edge");
    window.history.back();
    await waitFor(() =>
      expect(window.location.pathname).toBe("/catalog/service"),
    );
  });

  it("ignores legacy catalog query params and shows the default list", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/api/v1/query", body: emptyIrLogs },
    ]);
    renderApp("/catalog?entity=host&primary=x&tenant=acme");
    await screen.findByRole("complementary", { name: "Entity types" });
    expect(
      screen.queryByRole("navigation", { name: "Breadcrumb" }),
    ).not.toBeInTheDocument();
    expect(window.location.pathname).toBe("/catalog");
  });

  it("switches pages via the sidebar, updating the path", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyMatrix },
      { match: "/tempo/api/search", body: { traces: [], metrics: {} } },
    ]);
    renderApp("/logs");
    navLink("Traces").click();
    expect(await screen.findByLabelText("Trace ID")).toBeInTheDocument();
    expect(window.location.pathname).toBe("/traces");
  });

  it("pushes a history entry per signal switch, so back steps through them", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyMatrix },
      { match: "/tempo/api/search", body: { traces: [], metrics: {} } },
    ]);
    renderApp("/logs");
    const user = (await import("@testing-library/user-event")).default;

    await user.click(navLink("Traces"));
    await screen.findByLabelText("Trace ID");
    await user.click(navLink("Metrics"));
    await screen.findByText("Pick a metric above, then Run to chart it.");
    expect(window.location.pathname).toBe("/metrics");

    window.history.back();
    await waitFor(() => expect(window.location.pathname).toBe("/traces"));

    window.history.back();
    await waitFor(() => expect(window.location.pathname).toBe("/logs"));
  });

  it("drops signal-specific state when switching pages, keeping range/tenant/dataset/live", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyMatrix },
      { match: "/tempo/api/search", body: { traces: [], metrics: {} } },
    ]);
    renderApp("/logs?q=boom&range=6h&tenant=acme&dataset=prod&live=1");
    const user = (await import("@testing-library/user-event")).default;

    await user.click(navLink("Traces"));
    await screen.findByLabelText("Trace ID");

    expect(window.location.search).not.toContain("q=boom");
    expect(window.location.search).toContain("range=6h");
    expect(window.location.search).toContain("tenant=acme");
    expect(window.location.search).toContain("dataset=prod");
    expect(window.location.search).toContain("live=1");
  });

  it("opening a trace navigates to /traces/:traceId, a real route", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyMatrix },
      { match: "/tempo/api/search", body: { traces: [], metrics: {} } },
      {
        match: "/tempo/api/traces/t1cafe",
        body: {
          traceID: "t1cafe",
          rootServiceName: "gateway",
          rootTraceName: "POST /api/checkout",
          startTimeUnixNano: "1000",
          durationMs: 412,
          spanSets: [
            {
              matched: 1,
              spans: [
                {
                  spanID: "root",
                  startTimeUnixNano: "1000000000",
                  durationNanos: "412000000",
                  name: "POST /api/checkout",
                  serviceName: "gateway",
                  status: "ok",
                  attributes: {},
                },
              ],
            },
          ],
        },
      },
    ]);
    renderApp("/traces");
    const user = (await import("@testing-library/user-event")).default;

    await user.type(await screen.findByLabelText("Trace ID"), "t1cafe{enter}");

    // "POST /api/checkout" is ambiguous once the waterfall renders (it's
    // also the root span's name and its detail-panel heading) — the
    // trace-id chip in the header is unique.
    expect(await screen.findByText("t1cafe")).toBeInTheDocument();
    expect(window.location.pathname).toBe("/traces/t1cafe");
  });

  it("clicking Traces in the sidebar while viewing a trace returns to the traces list", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyMatrix },
      { match: "/tempo/api/search", body: { traces: [], metrics: {} } },
      {
        match: "/tempo/api/traces/t1cafe",
        body: {
          traceID: "t1cafe",
          rootServiceName: "gateway",
          rootTraceName: "POST /api/checkout",
          startTimeUnixNano: "1000",
          durationMs: 412,
          spanSets: [{ matched: 0, spans: [] }],
        },
      },
    ]);
    renderApp("/traces/t1cafe");
    const user = (await import("@testing-library/user-event")).default;

    await screen.findByText("t1cafe");
    await user.click(navLink("Traces"));

    expect(await screen.findByLabelText("Trace ID")).toBeInTheDocument();
    expect(window.location.pathname).toBe("/traces");
  });

  it("round-trips a trace id containing a literal % without double-decoding", async () => {
    // react-router's useParams() already URL-decodes the :traceId route
    // param; a second decodeURIComponent would corrupt "a%b" or throw
    // URIError outright, since "%b" isn't a valid escape sequence.
    stubFetchRoutes([
      { match: "query_range", body: emptyMatrix },
      { match: "/tempo/api/search", body: { traces: [], metrics: {} } },
      {
        match: "/tempo/api/traces/a%25b",
        body: {
          traceID: "a%b",
          rootServiceName: "gateway",
          rootTraceName: "weird-id-trace",
          startTimeUnixNano: "1000",
          durationMs: 5,
          spanSets: [{ matched: 0, spans: [] }],
        },
      },
    ]);
    renderApp("/traces");
    const user = (await import("@testing-library/user-event")).default;

    await user.type(await screen.findByLabelText("Trace ID"), "a%b{enter}");

    expect(await screen.findByText("a%b")).toBeInTheDocument();
    expect(window.location.pathname).toBe("/traces/a%25b");
  });

  it("re-clicking the current page returns to its's main view", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/api/v1/query", body: emptyIrLogs },
    ]);
    renderApp("/logs?q=boom");
    const user = (await import("@testing-library/user-event")).default;

    await user.click(navLink("Logs"));

    // Same as clicking a different page: back to the bare main view, filters
    // and search dropped — re-clicking isn't a no-op.
    expect(window.location.pathname).toBe("/logs");
    expect(window.location.search).toBe("");
  });

  it("navigates to /manage and back via the Manage link", async () => {
    const WHOAMI = {
      user: {
        id: "user-1",
        email: "admin@example.com",
        display_name: "Admin",
        is_instance_admin: true,
      },
      memberships: [{ tenant_id: "acme", role: "admin" }],
      tenant: { id: "acme", slug: "acme", name: "Acme Corp" },
      datasets: [{ id: "production", slug: "production", is_default: true }],
      default_dataset: "production",
    };
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/api/v1/query", body: emptyIrLogs },
      { match: "/api/v1/whoami", body: WHOAMI },
      { match: "/api-keys", body: [] },
      { match: "/memberships", body: [] },
    ]);
    // A tenant already resolved into the URL — the whoami-backed Manage link
    // only queries once one exists (see useWhoami's gating).
    renderApp("/logs?tenant=acme&dataset=production");
    const user = (await import("@testing-library/user-event")).default;
    await user.click(await screen.findByRole("link", { name: "Manage" }));
    expect(
      await screen.findByRole("dialog", { name: "Manage tenant" }),
    ).toBeInTheDocument();
    expect(window.location.pathname).toBe("/manage");

    await user.click(screen.getByRole("button", { name: "Close management" }));
    expect(window.location.pathname).toBe("/logs");
  });

  it("redirects /manage to /logs for non-admins", async () => {
    const WHOAMI = {
      user: {
        id: "user-1",
        email: "viewer@example.com",
        display_name: "Viewer",
        is_instance_admin: false,
      },
      memberships: [{ tenant_id: "acme", role: "viewer" }],
      tenant: { id: "acme", slug: "acme", name: "Acme Corp" },
      datasets: [{ id: "production", slug: "production", is_default: true }],
      default_dataset: "production",
    };
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/api/v1/query", body: emptyIrLogs },
      { match: "/api/v1/whoami", body: WHOAMI },
    ]);
    renderApp("/manage?tenant=acme&dataset=production");
    await screen.findByText(/No log lines in this range/);
    expect(window.location.pathname).toBe("/logs");
  });

  // `/ui/session` (GET, currentSession) and `/ui/session/config`
  // (loginConfig) share a URL prefix — anchor the base path so a stub for
  // one doesn't also answer the other (see LoginRoute.test.tsx).
  const SESSION = /\/ui\/session$/;

  describe("401 redirect", () => {
    it("navigates to /login with a redirect back to the current page on a 401 query failure", async () => {
      stubFetchRoutes([
        {
          match: "/api/v1/query",
          body: { error: "unauthenticated" },
          status: 401,
        },
      ]);
      renderApp("/logs?range=15m");
      await waitFor(() => expect(window.location.pathname).toBe("/login"));
      expect(window.location.search).toBe(
        `?redirect=${encodeURIComponent("/logs?range=15m")}`,
      );
    });

    it("stays put when only the cookie-session probe is a 401 (API-key auth)", async () => {
      stubFetchRoutes([
        { match: "query_range", body: emptyStreams },
        { match: "/api/v1/query", body: emptyIrLogs },
        {
          match: SESSION,
          method: "GET",
          body: { error: "unauthenticated" },
          status: 401,
        },
      ]);
      renderApp("/logs?tenant=acme&dataset=production");
      await screen.findByText(/No log lines in this range/);
      await act(() => new Promise((r) => setTimeout(r, 50)));
      expect(window.location.pathname).toBe("/logs");
    });

    it("does not navigate to /login on a non-auth query failure", async () => {
      stubFetchRoutes([
        { match: "/api/v1/query", body: { error: "boom" }, status: 500 },
      ]);
      renderApp("/logs");
      expect(await screen.findByRole("alert")).toHaveTextContent(/boom/);
      expect(window.location.pathname).toBe("/logs");
    });
  });

  describe("post-SSO tenant resolution", () => {
    it("resolves a sole membership from the session, staying on the landing path", async () => {
      stubFetchRoutes([
        { match: "query_range", body: emptyStreams },
        { match: "/api/v1/query", body: emptyIrLogs },
        {
          match: SESSION,
          method: "GET",
          body: {
            user: {
              id: "u1",
              email: "alice@example.com",
              display_name: "Alice",
              is_instance_admin: false,
            },
            tenant: "acme",
            dataset: "prod",
            memberships: [{ tenant_id: "acme", name: "Acme", role: "admin" }],
          },
        },
      ]);
      renderApp("/logs");
      await waitFor(() =>
        expect(window.location.search).toContain("tenant=acme"),
      );
      expect(window.location.search).toContain("dataset=prod");
      expect(window.location.pathname).toBe("/logs");
    });

    it("sends several (or zero) memberships to /select-tenant with the current path as the redirect target", async () => {
      stubFetchRoutes([
        { match: "query_range", body: emptyStreams },
        { match: "/api/v1/query", body: emptyIrLogs },
        {
          match: SESSION,
          method: "GET",
          body: {
            user: {
              id: "u1",
              email: "alice@example.com",
              display_name: "Alice",
              is_instance_admin: false,
            },
            tenant: null,
            dataset: null,
            memberships: [
              { tenant_id: "acme", name: "Acme", role: "admin" },
              { tenant_id: "globex", name: "Globex", role: "member" },
            ],
          },
        },
      ]);
      renderApp("/logs");
      await waitFor(() =>
        expect(window.location.pathname).toBe("/select-tenant"),
      );
      expect(window.location.search).toBe(
        `?redirect=${encodeURIComponent("/logs")}`,
      );
    });

    it("preserves the location fragment in the /select-tenant redirect target", async () => {
      stubFetchRoutes([
        { match: "query_range", body: emptyStreams },
        { match: "/api/v1/query", body: emptyIrLogs },
        {
          match: SESSION,
          method: "GET",
          body: {
            user: {
              id: "u1",
              email: "alice@example.com",
              display_name: "Alice",
              is_instance_admin: false,
            },
            tenant: null,
            dataset: null,
            memberships: [
              { tenant_id: "acme", name: "Acme", role: "admin" },
              { tenant_id: "globex", name: "Globex", role: "member" },
            ],
          },
        },
      ]);
      renderApp("/logs#section-1");
      await waitFor(() =>
        expect(window.location.pathname).toBe("/select-tenant"),
      );
      expect(window.location.search).toBe(
        `?redirect=${encodeURIComponent("/logs#section-1")}`,
      );
    });

    it("does not resolve from the session when a tenant is already in the URL", async () => {
      const fetchFn = stubFetchRoutes([
        { match: "query_range", body: emptyStreams },
        { match: "/api/v1/query", body: emptyIrLogs },
      ]);
      renderApp("/logs?tenant=acme&dataset=prod");
      await screen.findByText(/No log lines in this range/);
      expect(
        fetchFn.mock.calls.some((call) => SESSION.test(String(call[0]))),
      ).toBe(false);
    });
  });

  describe("PWA update", () => {
    it("shows the update banner once an update is pending", async () => {
      stubFetchRoutes([
        { match: "query_range", body: emptyStreams },
        { match: "/api/v1/query", body: emptyIrLogs },
      ]);
      renderApp("/logs");
      await screen.findByText(/No log lines in this range/);

      act(() => {
        setUpdateAvailable(vi.fn());
      });

      expect(screen.getByText("A new version is ready")).toBeInTheDocument();
    });

    it("auto-applies a pending update on the next route change when no form is dirty", async () => {
      stubFetchRoutes([
        { match: "query_range", body: emptyMatrix },
        { match: "/tempo/api/search", body: { traces: [], metrics: {} } },
      ]);
      renderApp("/logs");
      const updateSW = vi.fn().mockResolvedValue(undefined);
      setUpdateAvailable(updateSW);

      const user = (await import("@testing-library/user-event")).default;
      await user.click(navLink("Traces"));
      await screen.findByLabelText("Trace ID");

      expect(updateSW).toHaveBeenCalledWith(true);
      expect(getUpdateState().updateSW).toBeNull();
    });

    it("never auto-applies a pending update while a form is dirty", async () => {
      stubFetchRoutes([
        { match: "query_range", body: emptyMatrix },
        { match: "/tempo/api/search", body: { traces: [], metrics: {} } },
      ]);
      renderApp("/logs");
      const updateSW = vi.fn().mockResolvedValue(undefined);
      setUpdateAvailable(updateSW);
      markDirty("test-form", true);

      const user = (await import("@testing-library/user-event")).default;
      await user.click(navLink("Traces"));
      // The dirty form now also blocks the navigation itself
      // (UnsavedChangesGuard) — leave anyway to reach the point where the
      // pending-update check runs.
      await user.click(await screen.findByRole("button", { name: "Leave" }));
      await screen.findByLabelText("Trace ID");

      expect(updateSW).not.toHaveBeenCalled();
      expect(getUpdateState().updateSW).toBe(updateSW);
      markDirty("test-form", false);
    });
  });

  describe("unsaved changes guard", () => {
    it("prompts before leaving the registry editor via the brand link, discarding the edit only after Leave", async () => {
      stubFetchRoutes([
        { match: "query_range", body: emptyStreams },
        { match: "/api/v1/query", body: emptyIrLogs },
        { match: "/api/v1/whoami", body: WHOAMI_TENANT_ADMIN },
      ]);
      renderApp("/schema/conventions/new?tenant=acme&dataset=prod");
      const user = (await import("@testing-library/user-event")).default;

      const source = await screen.findByLabelText("Registry document");
      await user.click(source);
      await user.paste("name: acme");

      const brand = screen.getByRole("link", { name: /signaldb/i });
      await user.click(brand);
      const dialog = await screen.findByRole("dialog", {
        name: "Unsaved changes",
      });
      expect(screen.getByLabelText("Registry document")).toBeInTheDocument();

      // Stay: still on the editor, text preserved.
      await user.click(within(dialog).getByRole("button", { name: "Stay" }));
      expect(screen.queryByRole("dialog")).toBeNull();
      expect(screen.getByLabelText("Registry document")).toHaveValue(
        "name: acme",
      );

      // Leave: the brand link's navigation (home, the Overview) goes through.
      await user.click(brand);
      await user.click(screen.getByRole("button", { name: "Leave" }));
      await waitFor(() => expect(window.location.pathname).toBe("/overview"));
    });

    it("prompts before an in-app navigation away from a dirty consent form, even though /oauth/consent sits outside the explore shell", async () => {
      const CONSENT_QUERY =
        "client_id=client-1&redirect_uri=https%3A%2F%2Fclaude.ai%2Fcb&code_challenge=chal";
      stubFetchRoutes([
        { match: "query_range", body: emptyStreams },
        { match: "/api/v1/query", body: emptyIrLogs },
        {
          match: "/oauth/consent/context",
          body: {
            client_name: "Claude",
            tenants: [
              {
                id: "acme",
                role: "member",
                datasets: [{ id: "production", name: "production" }],
              },
            ],
          },
        },
      ]);
      // A prior in-app page, so `router.navigate(-1)` below (browser Back)
      // has somewhere to go back to.
      window.history.replaceState(null, "", "/logs");
      const router = createAppRouter();
      renderWithClient(<RouterProvider router={router} />);
      await router.navigate(`/oauth/consent?${CONSENT_QUERY}`);

      const user = (await import("@testing-library/user-event")).default;
      await screen.findByRole("heading", { name: /Claude/ });
      // The lone tenant is pre-checked with no checkbox of its own; changing
      // its dataset restriction is what dirties the form.
      await user.click(
        screen.getByRole("radio", { name: /Only these datasets in acme/ }),
      );

      // Browser Back is an in-app (POP) navigation the data router — and so
      // the guard — sees, unlike the final `window.location.href` redirect
      // ConsentView makes itself on submit, which a router blocker can't and
      // shouldn't intercept.
      void router.navigate(-1);
      const dialog = await screen.findByRole("dialog", {
        name: "Unsaved changes",
      });
      expect(
        screen.getByRole("heading", { name: /Claude/ }),
      ).toBeInTheDocument();

      await user.click(within(dialog).getByRole("button", { name: "Leave" }));
      await waitFor(() => expect(window.location.pathname).toBe("/logs"));
    });
  });
});
