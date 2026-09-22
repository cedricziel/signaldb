import { render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { MetricsView } from "./MetricsView";
import * as queryIrApi from "../../api/queryIr";

// uPlot needs a real canvas; the chart wrapper is exercised as a stub and
// the data pipeline is covered by api/ir/metrics + promSeries unit tests.
vi.mock("./MetricsChart", () => ({
  MetricsChart: ({ series }: { series: unknown[] }) => (
    <div data-testid="metrics-chart">chart:{series.length}</div>
  ),
}));

// Every builder run — and the pickers' own discovery calls — go through
// runIrQuery; only a `result: "series"` call means an actual chart run.
vi.mock("../../api/queryIr", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/queryIr")>();
  return { ...actual, runIrQuery: vi.fn() };
});

const runIrQuery = vi.mocked(queryIrApi.runIrQuery);

const discoveryRoutes = [
  { match: /label\/__name__\/values/, body: { status: "success", data: [] } },
  { match: /\/labels\?/, body: { status: "success", data: [] } },
];

afterEach(() => {
  vi.unstubAllGlobals();
});

beforeEach(() => {
  runIrQuery.mockReset();
});

const IR_SERIES = {
  result: "series",
  window: { start_ns: 0, end_ns: 1_000_000_000 },
  series: [
    { labels: { service_name: "checkout" }, points: [[1_000_000_000, 1]] },
    { labels: { service_name: "payments" }, points: [[1_000_000_000, 0]] },
  ],
};

function renderView(state: Partial<ExploreState> = {}) {
  const update = vi.fn();
  renderWithClient(
    <MetricsView
      state={{ ...DEFAULT_STATE, signal: "metrics", ...state }}
      update={update}
    />,
  );
  return update;
}

describe("MetricsView", () => {
  it("prompts for a query when none has run", () => {
    stubFetchRoutes(discoveryRoutes);
    renderView();
    expect(screen.getByText(/Pick a metric above/)).toBeInTheDocument();
  });

  it("runs a solo builder query via Query IR and writes it to ?mq= for reload", async () => {
    stubFetchRoutes(discoveryRoutes);
    runIrQuery.mockResolvedValue(IR_SERIES);
    const update = renderView();
    await userEvent.type(
      screen.getByLabelText("Metric"),
      "signaldb.wal.entries_processed",
    );
    await userEvent.click(screen.getByRole("button", { name: "Run" }));
    expect(update).toHaveBeenCalledWith({
      metricQuery: expect.stringContaining("signaldb.wal.entries_processed"),
    });
    const [{ metricQuery }] = update.mock.calls[0] as [{ metricQuery: string }];
    expect(JSON.parse(metricQuery)).toMatchObject({
      queries: [{ metric: "signaldb.wal.entries_processed" }],
      formula: "",
    });
    expect(await screen.findByTestId("metrics-chart")).toHaveTextContent(
      "chart:2",
    );
    expect(runIrQuery).toHaveBeenCalledWith(
      expect.objectContaining({ from: "metrics", result: "series" }),
    );
  });

  it("reloads a shared ?mq= link with the builder populated, querying via IR", async () => {
    stubFetchRoutes(discoveryRoutes);
    runIrQuery.mockResolvedValue(IR_SERIES);
    const mq = JSON.stringify({
      queries: [
        { ref: "a", metric: "signaldb.wal.entries_processed", filters: [] },
      ],
      formula: "",
    });
    renderView({ metricQuery: mq });

    expect(screen.getByLabelText("Metric")).toHaveValue(
      "signaldb.wal.entries_processed",
    );
    expect(await screen.findByTestId("metrics-chart")).toHaveTextContent(
      "chart:2",
    );
    expect(runIrQuery).toHaveBeenCalledWith(
      expect.objectContaining({ from: "metrics", result: "series" }),
    );
  });

  it("reloads the legacy single-MetricQuery ?mq= encoding", async () => {
    stubFetchRoutes(discoveryRoutes);
    runIrQuery.mockResolvedValue(IR_SERIES);
    const legacyMq = JSON.stringify({
      ref: "a",
      metric: "legacy_metric",
      filters: [],
    });
    renderView({ metricQuery: legacyMq });

    expect(screen.getByLabelText("Metric")).toHaveValue("legacy_metric");
    expect(await screen.findByTestId("metrics-chart")).toHaveTextContent(
      "chart:2",
    );
  });

  it("runs a two-query formula as a multi-query IR document", async () => {
    stubFetchRoutes(discoveryRoutes);
    runIrQuery.mockResolvedValue(IR_SERIES);
    const update = renderView();

    const metricA = screen.getByLabelText("Metric");
    await userEvent.type(metricA, "http_errors");
    await userEvent.click(screen.getByRole("button", { name: "+ query" }));

    const metricB = screen.getAllByLabelText("Metric")[1]!;
    await userEvent.type(metricB, "http_total");
    await userEvent.type(screen.getByLabelText("Formula"), "(a / b) * 100");

    await userEvent.click(screen.getByRole("button", { name: "Run" }));
    expect(await screen.findByTestId("metrics-chart")).toHaveTextContent(
      "chart:2",
    );
    expect(runIrQuery).toHaveBeenCalledWith(
      expect.objectContaining({
        queries: expect.objectContaining({
          a: expect.objectContaining({ from: "metrics" }),
          b: expect.objectContaining({ from: "metrics" }),
        }),
        formulas: [{ name: "formula", expr: "(a / b) * 100" }],
        result: "series",
      }),
    );
    expect(update).toHaveBeenCalledWith({
      metricQuery: expect.stringContaining("(a / b) * 100"),
    });
  });

  it("renders the chart and a legend entry per series", async () => {
    stubFetchRoutes(discoveryRoutes);
    runIrQuery.mockResolvedValue(IR_SERIES);
    const mq = JSON.stringify({
      queries: [{ ref: "a", metric: "up", filters: [] }],
      formula: "",
    });
    renderView({ metricQuery: mq });
    expect(await screen.findByTestId("metrics-chart")).toHaveTextContent(
      "chart:2",
    );
    const legend = screen.getByRole("list", { name: "Series" });
    expect(legend).toHaveTextContent('service_name="checkout"');
    expect(legend).toHaveTextContent('service_name="payments"');
  });

  it("shows the shared empty state for zero series", async () => {
    stubFetchRoutes(discoveryRoutes);
    runIrQuery.mockResolvedValue({
      result: "series",
      window: { start_ns: 0, end_ns: 1 },
      series: [],
    });
    const mq = JSON.stringify({
      queries: [{ ref: "a", metric: "up", filters: [] }],
      formula: "",
    });
    renderView({ metricQuery: mq });
    expect(await screen.findByRole("status")).toHaveTextContent(
      "No series in this range",
    );
  });

  it("surfaces query errors", async () => {
    stubFetchRoutes(discoveryRoutes);
    runIrQuery.mockRejectedValue(new Error("unknown field foo"));
    const mq = JSON.stringify({
      queries: [{ ref: "a", metric: "up", filters: [] }],
      formula: "",
    });
    renderView({ metricQuery: mq });
    expect(await screen.findByRole("alert")).toHaveTextContent(
      /unknown field foo/,
    );
  });

  it("resyncs the builder to a ?mq= that changed via Back/Forward", async () => {
    stubFetchRoutes(discoveryRoutes);
    runIrQuery.mockResolvedValue(IR_SERIES);
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const mqFor = (metric: string) =>
      JSON.stringify({
        queries: [{ ref: "a", metric, filters: [] }],
        formula: "",
      });
    const stateA: ExploreState = {
      ...DEFAULT_STATE,
      signal: "metrics",
      metricQuery: mqFor("metric_one"),
    };
    const stateB: ExploreState = {
      ...DEFAULT_STATE,
      signal: "metrics",
      metricQuery: mqFor("metric_two"),
    };
    const { rerender } = render(
      <QueryClientProvider client={client}>
        <MetricsView state={stateA} update={vi.fn()} />
      </QueryClientProvider>,
    );
    expect(await screen.findByLabelText("Metric")).toHaveValue("metric_one");

    rerender(
      <QueryClientProvider client={client}>
        <MetricsView state={stateB} update={vi.fn()} />
      </QueryClientProvider>,
    );

    // A URL-driven change (browser Back/Forward) must resync the builder —
    // not just the initial-mount seeding — or the view keeps showing/running
    // the query from before the navigation.
    expect(await screen.findByLabelText("Metric")).toHaveValue("metric_two");
  });

  it("does not show the empty-builder note once a builder query has actually run", async () => {
    stubFetchRoutes(discoveryRoutes);
    runIrQuery.mockResolvedValue(IR_SERIES);
    renderView({
      metricQuery: JSON.stringify({
        queries: [
          {
            ref: "a",
            metric: "signaldb.wal.entries_processed",
            filters: [],
          },
        ],
        formula: "",
      }),
    });

    await screen.findByTestId("metrics-chart");
    expect(screen.queryByText(/Pick a metric above/)).not.toBeInTheDocument();
  });
});
