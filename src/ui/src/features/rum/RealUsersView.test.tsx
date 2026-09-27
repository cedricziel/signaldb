import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import * as rumApi from "../../api/rum";
import type { RumApp } from "../../api/rum";
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
