import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import * as rumApi from "../../api/rum";
import type { RumApp, RumPageRow, RumRequestRow } from "../../api/rum";
import { connectionInfoBody } from "../../test/connectionInfo";
import { createAppRouter } from "../../routes";
import { RouterProvider } from "react-router";
import { renderWithClient, stubFetchRoutes } from "../../test/render";

vi.mock("../../api/rum", async (orig) => ({
  ...(await orig<typeof import("../../api/rum")>()),
  fetchRumApps: vi.fn(),
  fetchKpis: vi.fn().mockResolvedValue({}),
  fetchVitals: vi.fn().mockResolvedValue(new Map()),
  fetchSessionsOverTime: vi
    .fn()
    .mockResolvedValue({ total: [], withErrors: [] }),
  fetchBreakdown: vi.fn().mockResolvedValue([]),
  fetchNetworkRequests: vi.fn().mockResolvedValue([]),
  fetchResources: vi.fn().mockResolvedValue([]),
  fetchTracedShare: vi.fn().mockResolvedValue({ traced: [], total: [] }),
  fetchPages: vi.fn().mockResolvedValue([]),
  fetchLoadBreakdown: vi.fn().mockResolvedValue(undefined),
  fetchBackendCalls: vi.fn().mockResolvedValue([]),
}));
vi.mock("../../api/errors", async (orig) => ({
  ...(await orig<typeof import("../../api/errors")>()),
  fetchErrorGroups: vi.fn().mockResolvedValue({ groups: [], truncated: false }),
}));

function rumApp(overrides: Partial<RumApp> = {}): RumApp {
  return {
    serviceName: "storefront-web",
    sdkLanguage: "webjs",
    env: null,
    version: null,
    count: 500,
    ...overrides,
  };
}

function renderRum(path: string) {
  window.history.replaceState(null, "", path);
  return renderWithClient(<RouterProvider router={createAppRouter()} />);
}

describe("RealUsersView", () => {
  it("shows an empty state pointing to Setup when no app has sent RUM data", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([]);
    renderRum("/rum/overview");
    expect(
      await screen.findByText(/No frontend app has sent real-user data yet/),
    ).toBeInTheDocument();
  });

  it("defaults to the busiest app and writes it to the URL", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([
      rumApp({ serviceName: "storefront-web", count: 500 }),
      rumApp({ serviceName: "admin-web", count: 10 }),
    ]);
    renderRum("/rum/overview");
    await waitFor(() =>
      expect(window.location.search).toContain("app=storefront-web"),
    );
    expect(
      await screen.findByRole("button", { name: /storefront-web/ }),
    ).toBeInTheDocument();
  });

  it("switches apps through the app switcher's menu", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([
      rumApp({ serviceName: "storefront-web", count: 500 }),
      rumApp({ serviceName: "admin-web", count: 10 }),
    ]);
    renderRum("/rum/overview?app=storefront-web");
    const user = userEvent.setup();
    await user.click(
      await screen.findByRole("button", { name: /storefront-web/ }),
    );
    const menu = await screen.findByRole("listbox", { name: "Frontend apps" });
    await user.click(within(menu).getByRole("option", { name: /admin-web/ }));
    await waitFor(() =>
      expect(window.location.search).toContain("app=admin-web"),
    );
  });

  it("shows the app's environment and version next to the switcher", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([
      rumApp({
        serviceName: "storefront-web",
        env: "production",
        version: "2026.09.26-3",
      }),
    ]);
    renderRum("/rum/overview?app=storefront-web");
    expect(
      await screen.findByText("production · 2026.09.26-3"),
    ).toBeInTheDocument();
  });

  it("renders the KPI strip and vitals for the selected app", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([rumApp()]);
    vi.mocked(rumApi.fetchKpis).mockResolvedValue({
      sessions: [
        { tMs: 0, value: 3 },
        { tMs: 60_000, value: 5 },
      ],
    });
    renderRum("/rum/overview?app=storefront-web");
    // "Sessions" labels both the KPI card and the sessions-over-time panel.
    expect(await screen.findAllByText("Sessions")).toHaveLength(2);
    expect(await screen.findByText("Core Web Vitals")).toBeInTheDocument();
  });

  it("colours a rising traced-request share as good", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([rumApp()]);
    const now = Date.now();
    const previous = now - 3 * 3_600_000;
    const current = now - 60_000;
    vi.mocked(rumApi.fetchTracedShare).mockResolvedValue({
      traced: [
        { tMs: previous, value: 5 },
        { tMs: current, value: 9 },
      ],
      total: [
        { tMs: previous, value: 10 },
        { tMs: current, value: 10 },
      ],
    });
    renderRum("/rum/overview?app=storefront-web");
    const card = (await screen.findByText("Traced requests")).closest(
      ".kpi-card",
    )!;
    await waitFor(() =>
      expect(card.querySelector(".kpi-change-good")).not.toBeNull(),
    );
  });

  it("issues a new request for every RUM query when the app switches, not stale data under the same key", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([
      rumApp({ serviceName: "storefront-web" }),
      rumApp({ serviceName: "admin-web" }),
    ]);
    renderRum("/rum/overview?app=storefront-web");
    await screen.findByText("Core Web Vitals");
    expect(vi.mocked(rumApi.fetchKpis)).toHaveBeenCalledWith(
      "storefront-web",
      expect.anything(),
      expect.anything(),
      expect.anything(),
    );
    expect(vi.mocked(rumApi.fetchVitals)).toHaveBeenCalledWith(
      "storefront-web",
      expect.anything(),
    );

    const user = userEvent.setup();
    await user.click(
      await screen.findByRole("button", { name: /storefront-web/ }),
    );
    const menu = await screen.findByRole("listbox", { name: "Frontend apps" });
    await user.click(within(menu).getByRole("option", { name: /admin-web/ }));

    await waitFor(() =>
      expect(vi.mocked(rumApi.fetchKpis)).toHaveBeenCalledWith(
        "admin-web",
        expect.anything(),
        expect.anything(),
        expect.anything(),
      ),
    );
    expect(vi.mocked(rumApi.fetchVitals)).toHaveBeenCalledWith(
      "admin-web",
      expect.anything(),
    );
    expect(vi.mocked(rumApi.fetchSessionsOverTime)).toHaveBeenCalledWith(
      "admin-web",
      expect.anything(),
      expect.anything(),
    );
    expect(vi.mocked(rumApi.fetchBreakdown)).toHaveBeenCalledWith(
      "admin-web",
      expect.anything(),
      expect.anything(),
      expect.anything(),
    );
  });

  it("switches tabs by navigating the path, not a search param", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([rumApp()]);
    renderRum("/rum/overview?app=storefront-web");
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: "Setup" }));
    await waitFor(() => expect(window.location.pathname).toBe("/rum/setup"));
    expect(window.location.search).toContain("app=storefront-web");
  });

  it("redirects an unknown tab to overview, preserving the query string", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([]);
    renderRum("/rum/not-a-real-tab?tenant=acme");
    await waitFor(() => expect(window.location.pathname).toBe("/rum/overview"));
    expect(window.location.search).toContain("tenant=acme");
  });

  it("redirects the bare /rum path to overview", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([]);
    renderRum("/rum?tenant=acme");
    await waitFor(() => expect(window.location.pathname).toBe("/rum/overview"));
    expect(window.location.search).toContain("tenant=acme");
  });
});
function networkRow(overrides: Partial<RumRequestRow> = {}): RumRequestRow {
  return {
    method: "GET",
    origin: "api.storefront.example.com",
    template: "/orders/:id",
    calls: 150,
    tracedCalls: 140,
    errorCalls: 4,
    totalP75Ms: 240,
    backendP75Ms: 120,
    backendService: "orders-svc",
    isSdkExport: false,
    tracedKnown: true,
    ...overrides,
  };
}

describe("Network tab", () => {
  it("shows the requests table, the client/backend split and the traced share", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([rumApp()]);
    vi.mocked(rumApi.fetchNetworkRequests).mockResolvedValue([
      networkRow(),
      networkRow({
        origin: "reviews.partner-cdn.com",
        template: "/widget/:id",
        calls: 38100,
        tracedCalls: 0,
        errorCalls: 0,
        backendP75Ms: undefined,
        backendService: undefined,
      }),
      networkRow({
        method: "POST",
        origin: "ingest.acme.example.com",
        template: "/v1/traces",
        calls: 184210,
        tracedCalls: 0,
        errorCalls: 0,
        backendP75Ms: undefined,
        backendService: undefined,
        isSdkExport: true,
      }),
    ]);
    renderRum("/rum/network?app=storefront-web");

    expect(
      await screen.findByText("/orders/:id", { exact: false }),
    ).toBeInTheDocument();
    expect(await screen.findByText("orders-svc")).toBeInTheDocument();
    expect(await screen.findByText("SDK export")).toBeInTheDocument();
    expect(await screen.findByText("no trace")).toBeInTheDocument();
  });

  it("callouts an untraced origin, naming it and its count, excluding SDK export", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([rumApp()]);
    vi.mocked(rumApi.fetchNetworkRequests).mockResolvedValue([
      networkRow({
        origin: "reviews.partner-cdn.com",
        template: "/widget/:id",
        calls: 38100,
        tracedCalls: 0,
        errorCalls: 0,
        backendP75Ms: undefined,
        backendService: undefined,
      }),
      networkRow({
        method: "POST",
        origin: "ingest.acme.example.com",
        template: "/v1/traces",
        calls: 184210,
        tracedCalls: 0,
        errorCalls: 0,
        backendP75Ms: undefined,
        backendService: undefined,
        isSdkExport: true,
      }),
      networkRow({
        origin: "cdn.capped.example.com",
        tracedCalls: 0,
        backendService: undefined,
        tracedKnown: false,
      }),
    ]);
    renderRum("/rum/network?app=storefront-web");

    const callout = await screen.findByText(/couldn't be joined/);
    expect(callout.textContent).toContain("reviews.partner-cdn.com");
    expect(callout.textContent).toContain("38K");
    expect(callout.textContent).not.toContain("ingest.acme.example.com");
    expect(callout.textContent).not.toContain("cdn.capped.example.com");
  });
});

function pageRow(overrides: Partial<RumPageRow> = {}): RumPageRow {
  return {
    route: "/orders/:id",
    views: 500,
    vitals: new Map([["lcp", { p75: 4200, counts: { good: 200, poor: 300 } }]]),
    errorShare: 0.08,
    ...overrides,
  };
}

describe("Pages tab", () => {
  it("lists routes and writes ?route= when one is picked", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([rumApp()]);
    vi.mocked(rumApi.fetchPages).mockResolvedValue([pageRow()]);
    renderRum("/rum/pages?app=storefront-web");

    const routeButton = await screen.findByRole("button", {
      name: /\/orders\/:id/,
    });
    const user = userEvent.setup();
    await user.click(routeButton);

    await waitFor(() =>
      expect(window.location.search).toContain("route=%2Forders%2F%3Aid"),
    );
  });

  it("callouts page views with no attributable route", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([rumApp()]);
    vi.mocked(rumApi.fetchPages).mockResolvedValue([
      pageRow(),
      pageRow({ route: null, views: 12, errorShare: null }),
    ]);
    renderRum("/rum/pages?app=storefront-web");

    expect(
      await screen.findByText(/carry no/, { exact: false }),
    ).toBeInTheDocument();
  });

  it("shows the route detail panel when ?route= names a known route", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([rumApp()]);
    vi.mocked(rumApi.fetchPages).mockResolvedValue([pageRow()]);
    renderRum("/rum/pages?app=storefront-web&route=%2Forders%2F%3Aid");

    expect(await screen.findByText("Load breakdown")).toBeInTheDocument();
    expect(await screen.findByText("Backend calls")).toBeInTheDocument();
    expect(
      await screen.findByText("No navigation timing recorded for this route"),
    ).toBeInTheDocument();
  });
});

describe("Overview tab's Slowest pages panel", () => {
  it("opens the Pages tab with the clicked route selected", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([rumApp()]);
    vi.mocked(rumApi.fetchPages).mockResolvedValue([pageRow()]);
    renderRum("/rum/overview?app=storefront-web");

    const user = userEvent.setup();
    await user.click(
      await screen.findByRole("button", { name: /\/orders\/:id/ }),
    );

    await waitFor(() => expect(window.location.pathname).toBe("/rum/pages"));
    expect(window.location.search).toContain("route=%2Forders%2F%3Aid");
  });
});
