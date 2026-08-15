import { screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { BrowserRouter } from "react-router";
import { TENANT_CONTEXT_STORAGE_KEY, getTenantContext } from "./api/http";
import { AppRoutes } from "./routes";
import {
  emptyLabels,
  emptyMatrix,
  emptyStreams,
  renderWithClient,
  stubFetchRoutes,
} from "./test/render";

function renderApp(path = "/") {
  window.history.replaceState(null, "", path);
  return renderWithClient(
    <BrowserRouter>
      <AppRoutes />
    </BrowserRouter>,
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
  window.history.replaceState(null, "", "/");
  localStorage.clear();
});

describe("App", () => {
  it("renders the shell with the product mark and explore tabs", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/labels?", body: emptyLabels },
    ]);
    renderApp();
    expect(screen.getByRole("banner")).toHaveTextContent(/signaldb/i);
    expect(screen.getByRole("tab", { name: "Logs" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    expect(
      await screen.findByText(/No log lines match this query/),
    ).toBeInTheDocument();
  });

  it("redirects / to /logs", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/labels?", body: emptyLabels },
    ]);
    renderApp("/");
    await screen.findByText(/No log lines match this query/);
    expect(window.location.pathname).toBe("/logs");
  });

  it("redirects an unknown path to /logs", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/labels?", body: emptyLabels },
    ]);
    renderApp("/bogus");
    await screen.findByText(/No log lines match this query/);
    expect(window.location.pathname).toBe("/logs");
  });

  it("changes the tenant context from the top bar", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/labels?", body: emptyLabels },
    ]);
    renderApp("/logs");
    const user = (await import("@testing-library/user-event")).default;
    await user.click(
      screen.getByTitle("Tenant / dataset context for all queries"),
    );
    await user.clear(screen.getByLabelText("Tenant"));
    await user.type(screen.getByLabelText("Tenant"), "acme");
    await user.clear(screen.getByLabelText("Dataset"));
    await user.type(screen.getByLabelText("Dataset"), "prod");
    await user.click(screen.getByRole("button", { name: "Apply" }));
    expect(window.location.search).toContain("tenant=acme");
    expect(window.location.search).toContain("dataset=prod");
    expect(screen.getByRole("button", { name: /acme/ })).toHaveTextContent(
      "acme·prod",
    );
  });

  it("keeps the last tenant/dataset when navigating to a route without them in the URL", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/labels?", body: emptyLabels },
      { match: "/api/v1/schema/registries", body: { registries: [] } },
      {
        match: "/api/v1/whoami",
        body: {
          user: { id: "u1", email: "a@b", display_name: "A", is_instance_admin: false },
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
    expect(getTenantContext()).toEqual({ tenant: "acme", dataset: "prod" });
  });

  it("restores the last tenant/dataset in a fresh tab that opens a route without them", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/labels?", body: emptyLabels },
      { match: "/api/v1/schema/registries", body: { registries: [] } },
      { match: "/api/v1/manage/schema", body: { logical: [], physical: [] } },
      {
        match: "/api/v1/whoami",
        body: {
          user: { id: "u1", email: "a@b", display_name: "A", is_instance_admin: true },
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
    expect(getTenantContext()).toEqual({ tenant: "acme", dataset: "prod" });
  });

  it("persists the tenant/dataset context for later tabs", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/labels?", body: emptyLabels },
    ]);
    localStorage.removeItem(TENANT_CONTEXT_STORAGE_KEY);
    renderApp("/logs?tenant=globex&dataset=main");
    await screen.findByRole("button", { name: /globex/ });
    expect(
      JSON.parse(localStorage.getItem(TENANT_CONTEXT_STORAGE_KEY) ?? "{}"),
    ).toEqual({ tenant: "globex", dataset: "main" });
  });

  it("switches signals via tabs, updating the path", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyMatrix },
      { match: "/labels?", body: emptyLabels },
      { match: "/tempo/api/search", body: { traces: [], metrics: {} } },
    ]);
    renderApp("/logs");
    screen.getByRole("tab", { name: "Traces" }).click();
    expect(await screen.findByLabelText("Trace ID")).toBeInTheDocument();
    expect(window.location.pathname).toBe("/traces");
  });

  it("pushes a history entry per signal switch, so back steps through them", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyMatrix },
      { match: "/labels?", body: emptyLabels },
      { match: "/tempo/api/search", body: { traces: [], metrics: {} } },
    ]);
    renderApp("/logs");
    const user = (await import("@testing-library/user-event")).default;

    await user.click(screen.getByRole("tab", { name: "Traces" }));
    await screen.findByLabelText("Trace ID");
    await user.click(screen.getByRole("tab", { name: "Metrics" }));
    await screen.findByText(
      "Build a query above, or switch to PromQL, then Run to chart metrics.",
    );
    expect(window.location.pathname).toBe("/metrics");

    window.history.back();
    await waitFor(() => expect(window.location.pathname).toBe("/traces"));

    window.history.back();
    await waitFor(() => expect(window.location.pathname).toBe("/logs"));
  });

  it("drops signal-specific state when switching tabs, keeping range/tenant/dataset/live", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyMatrix },
      { match: "/labels?", body: emptyLabels },
      { match: "/tempo/api/search", body: { traces: [], metrics: {} } },
    ]);
    renderApp("/logs?q=boom&range=6h&tenant=acme&dataset=prod&live=1");
    const user = (await import("@testing-library/user-event")).default;

    await user.click(screen.getByRole("tab", { name: "Traces" }));
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
      { match: "/labels?", body: emptyLabels },
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

  it("clicking the Traces tab while viewing a trace returns to the traces list", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyMatrix },
      { match: "/labels?", body: emptyLabels },
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
    await user.click(screen.getByRole("tab", { name: "Traces" }));

    expect(await screen.findByLabelText("Trace ID")).toBeInTheDocument();
    expect(window.location.pathname).toBe("/traces");
  });

  it("round-trips a trace id containing a literal % without double-decoding", async () => {
    // react-router's useParams() already URL-decodes the :traceId route
    // param; a second decodeURIComponent would corrupt "a%b" or throw
    // URIError outright, since "%b" isn't a valid escape sequence.
    stubFetchRoutes([
      { match: "query_range", body: emptyMatrix },
      { match: "/labels?", body: emptyLabels },
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

  it("re-clicking the active tab returns to that tab's main view", async () => {
    stubFetchRoutes([
      { match: "query_range", body: emptyStreams },
      { match: "/labels?", body: emptyLabels },
    ]);
    renderApp("/logs?q=boom");
    const user = (await import("@testing-library/user-event")).default;

    await user.click(screen.getByRole("tab", { name: "Logs" }));

    // Same as clicking a different tab: back to the bare main view, filters
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
      { match: "/labels?", body: emptyLabels },
      { match: "/api/v1/whoami", body: WHOAMI },
      { match: "/api-keys", body: [] },
      { match: "/memberships", body: [] },
    ]);
    renderApp("/logs");
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
      { match: "/labels?", body: emptyLabels },
      { match: "/api/v1/whoami", body: WHOAMI },
    ]);
    renderApp("/manage");
    await screen.findByText(/No log lines match this query/);
    expect(window.location.pathname).toBe("/logs");
  });
});
