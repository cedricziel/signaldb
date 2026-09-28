import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import * as rumApi from "../../api/rum";
import type { RumApp, RumPageRow, RumRequestRow } from "../../api/rum";
import * as rumErrorGroupsApi from "../../api/rumErrorGroups";
import type { RumErrorGroupWithCause } from "../../api/rumErrorGroups";
import * as rumSessionsApi from "../../api/rumSessions";
import type { RumSessionRow } from "../../api/rumSessions";
import * as rumSessionDetailApi from "../../api/rumSessionDetail";
import type { SessionEvent } from "../../api/rumSessionDetail";
import * as traceDetailApi from "../../api/traceDetail";
import type { TempoTrace } from "../../api/traceTypes";
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
  fetchInteractions: vi.fn().mockResolvedValue([]),
}));
vi.mock("../../api/rumErrorGroups", async (orig) => ({
  ...(await orig<typeof import("../../api/rumErrorGroups")>()),
  fetchRumErrorGroupsWithBackendCause: vi.fn().mockResolvedValue([]),
}));
vi.mock("../../api/rumSessions", async (orig) => ({
  ...(await orig<typeof import("../../api/rumSessions")>()),
  fetchSessions: vi.fn().mockResolvedValue([]),
}));
vi.mock("../../api/rumSessionDetail", async (orig) => ({
  ...(await orig<typeof import("../../api/rumSessionDetail")>()),
  fetchSessionDetail: vi.fn().mockResolvedValue({ events: [], hasMore: false }),
}));
vi.mock("../../api/traceDetail", async (orig) => ({
  ...(await orig<typeof import("../../api/traceDetail")>()),
  fetchTraceDetail: vi.fn().mockResolvedValue(null),
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
    // "Sessions" labels the KPI card, the sessions-over-time panel, and now
    // the Sessions tab in the tab strip.
    expect(await screen.findAllByText("Sessions")).toHaveLength(3);
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

function errorGroupRow(
  overrides: Partial<RumErrorGroupWithCause> = {},
): RumErrorGroupWithCause {
  return {
    exceptionType: "TypeError",
    exceptionMessage: "Cannot read properties of null",
    escaped: "true",
    count: 42,
    firstMs: 1_700_000_000_000,
    lastMs: 1_700_000_060_000,
    lastSessionId: "sess-1",
    users: 12,
    sessions: 18,
    newInCurrentRelease: false,
    ...overrides,
  };
}

describe("Overview tab's Top errors panel", () => {
  it("opens the Errors tab with the clicked group selected", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([rumApp()]);
    vi.mocked(
      rumErrorGroupsApi.fetchRumErrorGroupsWithBackendCause,
    ).mockResolvedValue([errorGroupRow()]);
    renderRum("/rum/overview?app=storefront-web");

    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: /TypeError/ }));

    await waitFor(() => expect(window.location.pathname).toBe("/rum/errors"));
    expect(window.location.search).toContain("errgroup=");
  });
});

describe("Errors tab", () => {
  it("lists error groups with their new-release and backend-cause pills", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([
      rumApp({ version: "2026.09.26-3" }),
    ]);
    vi.mocked(
      rumErrorGroupsApi.fetchRumErrorGroupsWithBackendCause,
    ).mockResolvedValue([
      errorGroupRow({
        newInCurrentRelease: true,
        backendCause: {
          sessionId: "sess-1",
          startMs: 1_700_000_050_000,
          traceId: "trace-1",
          spanId: "span-1",
          method: "GET",
          urlFull: "https://api.example.com/orders",
          statusCode: 500,
          durationNs: "12000000",
        },
      }),
    ]);
    renderRum("/rum/errors?app=storefront-web");

    expect(await screen.findByText(/TypeError/)).toBeInTheDocument();
    expect(await screen.findByText("new in 2026.09.26-3")).toBeInTheDocument();
    expect(await screen.findByText("backend cause")).toBeInTheDocument();
  });

  it("writes ?errgroup= when a row is picked", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([rumApp()]);
    vi.mocked(
      rumErrorGroupsApi.fetchRumErrorGroupsWithBackendCause,
    ).mockResolvedValue([errorGroupRow()]);
    renderRum("/rum/errors?app=storefront-web");

    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: /TypeError/ }));

    await waitFor(() => expect(window.location.search).toContain("errgroup="));
  });
});

describe("Interactions tab", () => {
  it("shows clicks by target and page, joined to the page's own INP p75", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([rumApp()]);
    vi.mocked(rumApi.fetchPages).mockResolvedValue([
      pageRow({
        route: "/orders/:id",
        vitals: new Map([["inp", { p75: 240, counts: {} }]]),
      }),
    ]);
    vi.mocked(rumApi.fetchInteractions).mockResolvedValue([
      {
        target: "html > body > div.app > button.buy",
        route: "/orders/:id",
        clicks: 812,
      },
    ]);
    renderRum("/rum/interactions?app=storefront-web");

    expect(await screen.findByText("812")).toBeInTheDocument();
    expect(await screen.findByText("/orders/:id")).toBeInTheDocument();
    expect(
      await screen.findByText("body > div.app > button.buy"),
    ).toBeInTheDocument();
  });
});

function sessionRow(overrides: Partial<RumSessionRow> = {}): RumSessionRow {
  return {
    sessionId: "sess-1",
    firstMs: 1_700_000_000_000,
    lastMs: 1_700_000_060_000,
    durationMs: 60_000,
    views: 4,
    errors: 0,
    slow: 0,
    entry: "/checkout",
    exit: "/thanks",
    userId: "user-42",
    browser: "Chrome",
    mobile: false,
    ...overrides,
  };
}

describe("Sessions tab", () => {
  it("lists sessions and writes ?session= when one is picked", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([rumApp()]);
    vi.mocked(rumSessionsApi.fetchSessions).mockResolvedValue([sessionRow()]);
    renderRum("/rum/sessions?app=storefront-web");

    const row = await screen.findByRole("button", { name: /sess-1/ });
    const user = userEvent.setup();
    await user.click(row);

    await waitFor(() =>
      expect(window.location.search).toContain("session=sess-1"),
    );
  });

  it("filters to sessions with errors via the quick filter", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([rumApp()]);
    vi.mocked(rumSessionsApi.fetchSessions).mockResolvedValue([
      sessionRow({ sessionId: "clean", errors: 0 }),
      sessionRow({ sessionId: "errored", errors: 3 }),
    ]);
    renderRum("/rum/sessions?app=storefront-web");

    expect(
      await screen.findByRole("button", { name: /clean/ }),
    ).toBeInTheDocument();
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "With errors" }));

    expect(
      screen.queryByRole("button", { name: /clean/ }),
    ).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: /errored/ })).toBeInTheDocument();
  });

  it("shows the session detail timeline and events when ?session= is set", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([rumApp()]);
    vi.mocked(rumSessionsApi.fetchSessions).mockResolvedValue([sessionRow()]);
    const events: SessionEvent[] = [
      {
        kind: "log",
        tsNs: "1700000000000000000",
        lane: "views",
        eventName: "browser.navigation",
        traceId: null,
        urlTemplate: "/checkout",
        urlFull: null,
        vitalName: null,
        vitalRating: null,
        vitalValue: null,
        cssSelector: null,
        tagName: null,
        exceptionType: null,
        exceptionMessage: null,
        exceptionStacktrace: null,
        resourceAttributes: { "user.id": "user-42" },
      },
    ];
    vi.mocked(rumSessionDetailApi.fetchSessionDetail).mockResolvedValue({
      events,
      hasMore: false,
    });
    renderRum("/rum/sessions?app=storefront-web&session=sess-1");

    expect(
      await screen.findByText("Navigated to /checkout"),
    ).toBeInTheDocument();
    expect(await screen.findByText("user.id")).toBeInTheDocument();
  });

  it("resets the selected event when ?session= switches to a different session", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([rumApp()]);
    vi.mocked(rumSessionsApi.fetchSessions).mockResolvedValue([
      sessionRow({ sessionId: "sess-1" }),
      sessionRow({ sessionId: "sess-2" }),
    ]);
    vi.mocked(rumSessionDetailApi.fetchSessionDetail).mockImplementation(
      async (sessionId) => ({
        events: [
          {
            kind: "log",
            tsNs: "1700000000000000000",
            lane: "views",
            eventName: "browser.navigation",
            traceId: null,
            urlTemplate: `/${sessionId}`,
            urlFull: null,
            vitalName: null,
            vitalRating: null,
            vitalValue: null,
            cssSelector: null,
            tagName: null,
            exceptionType: null,
            exceptionMessage: null,
            exceptionStacktrace: null,
            resourceAttributes: {},
          },
        ],
        hasMore: false,
      }),
    );
    renderRum("/rum/sessions?app=storefront-web&session=sess-1");

    const user = userEvent.setup();
    const eventRow = await screen.findByRole("button", {
      name: /Navigated to \/sess-1/,
    });
    await user.click(eventRow);
    expect(eventRow).toHaveAttribute("aria-current", "true");

    await user.click(await screen.findByRole("button", { name: /sess-2/ }));
    await waitFor(() =>
      expect(window.location.search).toContain("session=sess-2"),
    );

    const eventsPanel = (
      await screen.findByText("Navigated to /sess-2")
    ).closest(".rum-session-events") as HTMLElement;
    for (const button of within(eventsPanel).getAllByRole("button")) {
      expect(button).not.toHaveAttribute("aria-current", "true");
    }
  });

  it("shows an inline trace waterfall split when a network event is selected", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([rumApp()]);
    vi.mocked(rumSessionsApi.fetchSessions).mockResolvedValue([sessionRow()]);
    const networkEvent: SessionEvent = {
      kind: "span",
      tsNs: "1700000000000000000",
      lane: "network",
      traceId: "trace-501",
      spanId: "client-1",
      parentSpanId: null,
      name: "POST",
      spanKind: "Client",
      serviceName: "storefront-web",
      durationNs: "500000000",
      isError: false,
      httpMethod: "POST",
      urlFull: "https://api.storefront.example.com/checkout",
      httpStatusCode: 200,
    };
    vi.mocked(rumSessionDetailApi.fetchSessionDetail).mockResolvedValue({
      events: [networkEvent],
      hasMore: false,
    });
    const trace: TempoTrace = {
      traceId: "trace-501",
      rootServiceName: "storefront-web",
      rootTraceName: "POST /checkout",
      startNs: "1700000000000000000",
      durationMs: 500,
      rootAttributes: {},
      rootError: false,
      profiles: [],
      spans: [
        {
          spanId: "client-1",
          parentSpanId: null,
          name: "POST",
          serviceName: "storefront-web",
          status: "unset",
          kind: "Client",
          startNs: "1700000000000000000",
          durNs: "500000000",
          attributes: {},
          events: [],
        },
        {
          spanId: "server-1",
          parentSpanId: "client-1",
          name: "checkout",
          serviceName: "checkout-svc",
          status: "unset",
          kind: "Server",
          startNs: "1700000000010000000",
          durNs: "300000000",
          attributes: {},
          events: [],
        },
      ],
    };
    vi.mocked(traceDetailApi.fetchTraceDetail).mockResolvedValue(trace);
    renderRum("/rum/sessions?app=storefront-web&session=sess-1");

    const timeline = await screen.findByTestId("rum-session-timeline");
    const mark = within(timeline).getByRole("button", { name: /POST/ });
    const user = userEvent.setup();
    await user.click(mark);

    expect(
      await screen.findByRole("link", { name: "Open in Traces" }),
    ).toHaveAttribute("href", expect.stringContaining("/traces/trace-501"));
    expect(await screen.findByText(/checkout-svc/)).toBeInTheDocument();
  });

  it("names the preceding failed request as the likely cause of an exception", async () => {
    stubFetchRoutes([
      { match: "/api/v1/connection", body: connectionInfoBody() },
    ]);
    vi.mocked(rumApi.fetchRumApps).mockResolvedValue([rumApp()]);
    vi.mocked(rumSessionsApi.fetchSessions).mockResolvedValue([sessionRow()]);
    const failedRequest: SessionEvent = {
      kind: "span",
      tsNs: "1700000000000000000",
      lane: "errors",
      traceId: "trace-502",
      spanId: "client-2",
      parentSpanId: null,
      name: "POST",
      spanKind: "Client",
      serviceName: "storefront-web",
      durationNs: "100000000",
      isError: true,
      httpMethod: "POST",
      urlFull: "https://api.storefront.example.com/checkout",
      httpStatusCode: 502,
    };
    const exception: SessionEvent = {
      kind: "log",
      tsNs: "1700000002800000000",
      lane: "errors",
      eventName: "exception",
      traceId: null,
      urlTemplate: null,
      urlFull: null,
      vitalName: null,
      vitalRating: null,
      vitalValue: null,
      cssSelector: null,
      tagName: null,
      exceptionType: "TypeError",
      exceptionMessage: "boom",
      exceptionStacktrace: "TypeError: boom\n  at checkout.js:1",
      resourceAttributes: {},
    };
    vi.mocked(rumSessionDetailApi.fetchSessionDetail).mockResolvedValue({
      events: [failedRequest, exception],
      hasMore: false,
    });
    renderRum("/rum/sessions?app=storefront-web&session=sess-1");

    // Generous waits: under coverage instrumentation the detail's chained
    // renders can take over the default 1 s.
    const timeline = await screen.findByTestId(
      "rum-session-timeline",
      {},
      { timeout: 5000 },
    );
    const exceptionMark = within(timeline).getByRole("button", {
      name: /TypeError/,
    });
    const user = userEvent.setup();
    await user.click(exceptionMark);

    const cause = await screen.findByText(
      /Likely cause/,
      {},
      { timeout: 5000 },
    );
    expect(cause.textContent).toContain("502");
  });
});
