import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { RouterProvider } from "react-router";
import * as catalogApi from "../../api/catalog";
import * as errorsApi from "../../api/errors";
import * as overviewApi from "../../api/overview";
import * as graphApi from "../../api/serviceGraph";
import { createAppRouter } from "../../routes";
import { renderWithClient, stubFetchRoutes } from "../../test/render";

vi.mock("../../api/catalog", async (orig) => ({
  ...(await orig<typeof import("../../api/catalog")>()),
  fetchCatalogEntities: vi.fn(),
}));
vi.mock("../../api/errors", async (orig) => ({
  ...(await orig<typeof import("../../api/errors")>()),
  fetchErrorGroups: vi.fn(),
  fetchErrorGroupVolume: vi.fn().mockResolvedValue([]),
}));
vi.mock("../../api/serviceGraph", async (orig) => ({
  ...(await orig<typeof import("../../api/serviceGraph")>()),
  fetchServiceGraph: vi.fn(),
}));
vi.mock("../../api/overview", async (orig) => ({
  ...(await orig<typeof import("../../api/overview")>()),
  fetchSystemKpis: vi.fn(),
  fetchServiceActivity: vi.fn().mockResolvedValue(new Map()),
  fetchVersionSightings: vi.fn(),
  fetchIngestVolume: vi.fn(),
  fetchSlowestEndpoints: vi.fn(),
}));
vi.mock("../../api/ir/discovery", async (orig) => ({
  ...(await orig<typeof import("../../api/ir/discovery")>()),
  values: vi.fn().mockResolvedValue([
    { value: "production", partial: false },
    { value: "staging", partial: false },
  ]),
}));

const NOW = Date.now();

beforeEach(() => {
  stubFetchRoutes([]);
  vi.mocked(catalogApi.fetchCatalogEntities).mockResolvedValue({
    entities: [
      {
        values: ["checkout", null],
        observations: [{ source: "traces", count: 10 }],
        lastNs: "0",
        red: { traces: 3600, errors: 90, p50Ms: 40, p95Ms: 318 },
      },
      {
        values: ["search", null],
        observations: [
          { source: "traces", count: 10 },
          { source: "logs", count: 3 },
        ],
        lastNs: "0",
        red: { traces: 7200, errors: 0, p50Ms: 20, p95Ms: 120 },
      },
    ],
    truncated: false,
  });
  vi.mocked(overviewApi.fetchSystemKpis).mockResolvedValue({
    current: {
      count: 10800,
      ratePerSec: 3,
      errorRate: 0.0083,
      p50Ms: 30,
      p95Ms: 214,
      p99Ms: 800,
      peakRatePerSec: 5,
      lastNs: "0",
    },
    series: { rate: [], errorRate: [], p95: [] },
  });
  vi.mocked(overviewApi.fetchVersionSightings).mockResolvedValue([
    { service: "checkout", version: "v1", firstMs: NOW - 3_000_000, lastMs: 0 },
    { service: "checkout", version: "v2", firstMs: NOW - 960_000, lastMs: 0 },
  ]);
  vi.mocked(overviewApi.fetchIngestVolume).mockResolvedValue([
    { key: "logs", points: [[NOW, 300]] },
    { key: "traces", points: [[NOW, 100]] },
  ]);
  vi.mocked(overviewApi.fetchSlowestEndpoints).mockResolvedValue([
    {
      name: "POST /api/checkout",
      service: "checkout",
      count: 3,
      p95Ms: 318,
      p99Ms: 2400,
    },
  ]);
  vi.mocked(errorsApi.fetchErrorGroups).mockResolvedValue({
    groups: [
      {
        source: "traces",
        exceptionType: "DeadlineExceeded",
        exceptionMessage: "context deadline exceeded",
        serviceName: "checkout",
        escaped: "true",
        count: 8391,
        firstNs: "0",
        lastNs: String(NOW * 1e6),
      },
    ],
    truncated: false,
  });
  vi.mocked(graphApi.fetchServiceGraph).mockResolvedValue({
    graph: {
      nodes: [
        { id: "checkout", name: "checkout", kind: "service" },
        { id: "stripe", name: "stripe", kind: "external" },
      ],
      edges: [
        {
          source: "checkout",
          target: "stripe",
          count: 5,
          rate: 1,
          error_rate: 0,
        },
      ],
    },
    warnings: [],
  });
});

afterEach(() => {
  vi.unstubAllGlobals();
  window.history.replaceState(null, "", "/");
  localStorage.clear();
});

function renderOverview(search = "?tenant=acme&dataset=prod") {
  window.history.replaceState(null, "", `/overview${search}`);
  return renderWithClient(<RouterProvider router={createAppRouter()} />);
}

describe("OverviewView", () => {
  it("shows the KPIs, the services worst-first and the deploy they carry", async () => {
    renderOverview();
    expect(await screen.findByText("0.83")).toBeInTheDocument();
    expect(screen.getByText("214")).toBeInTheDocument();
    expect(
      await screen.findByText(/2 services · 1 external/),
    ).toBeInTheDocument();

    const table = screen.getByRole("table", { name: "Services" });
    const rows = within(table).getAllByRole("row").slice(1);
    expect(
      rows.map((r) => within(r).getAllByRole("link")[0]!.textContent),
    ).toEqual(["checkout", "search"]);
    expect(
      within(rows[0]!).getByRole("img", { name: "critical" }),
    ).toBeInTheDocument();
    expect(
      within(rows[0]!).getByRole("link", { name: "checkout" }),
    ).toHaveAttribute(
      "href",
      expect.stringMatching(/^\/catalog\/service\/checkout,.*tenant=acme/),
    );
    expect(
      await within(rows[0]!).findByText("v2 · 16m ago"),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("img", { name: /Deploys in window: checkout v2 at/ }),
    ).toBeInTheDocument();
  });

  it("links the rail rows into Errors and Traces", async () => {
    renderOverview();
    const error = await screen.findByRole("link", { name: /DeadlineExceeded/ });
    expect(error.getAttribute("href")).toMatch(/^\/errors\?.*group=/);
    const endpoint = await screen.findByRole("row", {
      name: /POST \/api\/checkout/,
    });
    expect(endpoint.getAttribute("href")).toMatch(/^\/traces\?.*tf=/);
  });

  it("scopes everything to the picked environment via the URL", async () => {
    const user = userEvent.setup();
    renderOverview();
    const select = await screen.findByRole("combobox", { name: "Environment" });
    await screen.findByRole("option", { name: "staging" });
    await user.selectOptions(select, "staging");
    await waitFor(() =>
      expect(window.location.search).toContain("env=staging"),
    );
    await waitFor(() =>
      expect(overviewApi.fetchSystemKpis).toHaveBeenCalledWith(
        expect.anything(),
        "staging",
        expect.any(Number),
      ),
    );
  });

  it("opens the setup checklist from ?setup and drops the flag", async () => {
    renderOverview("?tenant=acme&dataset=prod&setup");
    const dialog = await screen.findByRole("dialog", {
      name: "Setup checklist",
    });
    expect(
      within(dialog).getByText("Instrument every service"),
    ).toBeInTheDocument();
    await waitFor(() => expect(window.location.search).not.toContain("setup"));
    expect(window.location.search).toContain("tenant=acme");
  });

  it("shows an empty state when no service reported", async () => {
    vi.mocked(catalogApi.fetchCatalogEntities).mockResolvedValue({
      entities: [],
      truncated: false,
    });
    renderOverview();
    expect(
      await screen.findByText("No services in this window"),
    ).toBeInTheDocument();
  });
});
