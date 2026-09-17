import { render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { MetricsView } from "./MetricsView";
import * as queryIrApi from "../../api/queryIr";

// uPlot needs a real canvas; the chart wrapper is exercised as a stub and
// the data pipeline is covered by prom/promSeries unit tests.
vi.mock("./MetricsChart", () => ({
  MetricsChart: ({ series }: { series: unknown[] }) => (
    <div data-testid="metrics-chart">chart:{series.length}</div>
  ),
}));

// The builder's default (no range function, no formula) path queries Query
// IR, not PromQL — see api/metricsIr.ts. Mocked at the module boundary, the
// same way TracesView.test.tsx mocks fetchTraceGroups.
vi.mock("../../api/queryIr", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/queryIr")>();
  return { ...actual, runIrQuery: vi.fn() };
});

const runIrQuery = vi.mocked(queryIrApi.runIrQuery);

afterEach(() => {
  vi.unstubAllGlobals();
});

beforeEach(() => {
  runIrQuery.mockReset();
});

const MATRIX = {
  status: "success",
  data: {
    resultType: "matrix",
    result: [
      {
        metric: { __name__: "up", service_name: "checkout" },
        values: [[1000, "1"]],
      },
      {
        metric: { __name__: "up", service_name: "payments" },
        values: [[1000, "0"]],
      },
    ],
  },
};

const IR_SERIES = {
  result: "series",
  window: { start_ns: 0, end_ns: 1_000_000_000 },
  series: [
    {
      labels: { service_name: "checkout" },
      points: [[1_000_000_000, 1]],
    },
    {
      labels: { service_name: "payments" },
      points: [[1_000_000_000, 0]],
    },
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
  it("prompts for a query when none is set", () => {
    stubFetchRoutes([{ match: "query_range", body: MATRIX }]);
    renderView();
    expect(screen.getByText(/Build a query above/)).toBeInTheDocument();
  });

  it("submits a raw query from the PromQL escape hatch, staying on PromQL", async () => {
    stubFetchRoutes([{ match: "query_range", body: MATRIX }]);
    const update = renderView();
    await userEvent.click(screen.getByRole("tab", { name: "PromQL" }));
    await userEvent.type(screen.getByLabelText("PromQL query"), "up ");
    await userEvent.click(screen.getByRole("button", { name: "Run" }));
    expect(update).toHaveBeenCalledWith({ promql: "up", metricQuery: "" });
    expect(runIrQuery).not.toHaveBeenCalled();
  });

  it("runs a solo builder query via Query IR, not PromQL, and writes it to ?mq= for reload", async () => {
    stubFetchRoutes([
      {
        match: /label\/__name__\/values/,
        body: { status: "success", data: [] },
      },
      { match: /\/labels\?/, body: { status: "success", data: [] } },
    ]);
    runIrQuery.mockResolvedValue(IR_SERIES);
    const update = renderView();
    await userEvent.type(
      screen.getByLabelText("Metric"),
      "signaldb.wal.entries_processed",
    );
    await userEvent.click(screen.getByRole("button", { name: "Run" }));
    expect(update).toHaveBeenCalledWith({
      metricQuery: expect.stringContaining("signaldb.wal.entries_processed"),
      promql: "",
    });
    const [{ metricQuery }] = update.mock.calls[0] as [{ metricQuery: string }];
    expect(JSON.parse(metricQuery)).toMatchObject({
      metric: "signaldb.wal.entries_processed",
    });
    expect(await screen.findByTestId("metrics-chart")).toHaveTextContent(
      "chart:2",
    );
    expect(runIrQuery).toHaveBeenCalledWith(
      expect.objectContaining({
        from: "metrics",
        result: "series",
      }),
    );
  });

  it("reloads a shared ?mq= link with the builder populated, querying via IR", async () => {
    stubFetchRoutes([
      {
        match: /label\/__name__\/values/,
        body: { status: "success", data: [] },
      },
      { match: /\/labels\?/, body: { status: "success", data: [] } },
    ]);
    runIrQuery.mockResolvedValue(IR_SERIES);
    const mq = JSON.stringify({
      ref: "a",
      metric: "signaldb.wal.entries_processed",
      filters: [],
    });
    renderView({ metricQuery: mq, promql: "" });

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

  it("runs a two-query formula as a single composed expression", async () => {
    stubFetchRoutes([
      {
        match: /label\/__name__\/values/,
        body: { status: "success", data: [] },
      },
      { match: /\/labels\?/, body: { status: "success", data: [] } },
      { match: "query_range", body: MATRIX },
    ]);
    const update = renderView();

    const metricA = screen.getByLabelText("Metric");
    await userEvent.type(metricA, "http_errors");
    await userEvent.click(screen.getByRole("button", { name: "+ query" }));

    const metricB = screen.getAllByLabelText("Metric")[1]!;
    await userEvent.type(metricB, "http_total");
    await userEvent.type(screen.getByLabelText("Formula"), "(a / b) * 100");

    await userEvent.click(screen.getByRole("button", { name: "Run" }));
    expect(update).toHaveBeenCalledWith({
      promql: "((http_errors) / (http_total)) * 100",
      metricQuery: "",
    });
    // A formula spans multiple queries — no single metric.name to filter on
    // in the minimal metrics IR source — so it stays on PromQL.
    expect(runIrQuery).not.toHaveBeenCalled();
  });

  it("renders the chart and a legend entry per series", async () => {
    stubFetchRoutes([{ match: "query_range", body: MATRIX }]);
    renderView({ promql: "up" });
    expect(await screen.findByTestId("metrics-chart")).toHaveTextContent(
      "chart:2",
    );
    const legend = screen.getByRole("list", { name: "Series" });
    expect(legend).toHaveTextContent('up{service_name="checkout"}');
    expect(legend).toHaveTextContent('up{service_name="payments"}');
    // A `?promql=` link with no `?mq=` stays on the PromQL path.
    expect(runIrQuery).not.toHaveBeenCalled();
  });

  it("copies a rendered series label", async () => {
    const writeText = vi.fn();
    vi.stubGlobal("navigator", { clipboard: { writeText } });
    stubFetchRoutes([{ match: "query_range", body: MATRIX }]);
    renderView({ promql: "up" });

    await userEvent.click(
      await screen.findByRole("button", {
        name: 'Copy series up{service_name="checkout"}',
      }),
    );

    expect(writeText).toHaveBeenCalledWith('up{service_name="checkout"}');
  });

  it("shows the shared empty state for zero series", async () => {
    stubFetchRoutes([
      {
        match: "query_range",
        body: { status: "success", data: { resultType: "matrix", result: [] } },
      },
    ]);
    renderView({ promql: "up" });
    expect(await screen.findByRole("status")).toHaveTextContent(
      "No series in this range",
    );
  });

  it("surfaces query errors", async () => {
    stubFetchRoutes([
      {
        match: "query_range",
        body: {
          status: "error",
          error: "unknown function foo",
          data: { resultType: "matrix", result: [] },
        },
      },
    ]);
    renderView({ promql: "foo(up)" });
    expect(await screen.findByRole("alert")).toHaveTextContent(
      /unknown function foo/,
    );
  });

  it("resyncs the builder/draft/ranQuery to a ?mq= that changed via Back/Forward", async () => {
    stubFetchRoutes([
      {
        match: /label\/__name__\/values/,
        body: { status: "success", data: [] },
      },
      { match: /\/labels\?/, body: { status: "success", data: [] } },
    ]);
    runIrQuery.mockResolvedValue(IR_SERIES);
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const stateA: ExploreState = {
      ...DEFAULT_STATE,
      signal: "metrics",
      metricQuery: JSON.stringify({ ref: "a", metric: "metric_one", filters: [] }),
      promql: "",
    };
    const stateB: ExploreState = {
      ...DEFAULT_STATE,
      signal: "metrics",
      metricQuery: JSON.stringify({ ref: "a", metric: "metric_two", filters: [] }),
      promql: "",
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

  it("clears a stale formula when a ?mq= change re-seeds the builder via Back/Forward", async () => {
    stubFetchRoutes([
      {
        match: /label\/__name__\/values/,
        body: { status: "success", data: [] },
      },
      { match: /\/labels\?/, body: { status: "success", data: [] } },
    ]);
    runIrQuery.mockResolvedValue(IR_SERIES);
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const stateA: ExploreState = {
      ...DEFAULT_STATE,
      signal: "metrics",
      metricQuery: JSON.stringify({ ref: "a", metric: "metric_one", filters: [] }),
      promql: "",
    };
    const stateB: ExploreState = {
      ...DEFAULT_STATE,
      signal: "metrics",
      metricQuery: JSON.stringify({ ref: "a", metric: "metric_two", filters: [] }),
      promql: "",
    };
    const { rerender } = render(
      <QueryClientProvider client={client}>
        <MetricsView state={stateA} update={vi.fn()} />
      </QueryClientProvider>,
    );
    const formulaInput = await screen.findByLabelText("Formula");
    await userEvent.type(formulaInput, "a - b");
    expect(formulaInput).toHaveValue("a - b");

    rerender(
      <QueryClientProvider client={client}>
        <MetricsView state={stateB} update={vi.fn()} />
      </QueryClientProvider>,
    );

    // A formula referencing a query letter the reseed just dropped must not
    // survive the navigation — it would silently compile against whatever
    // letters happen to still exist.
    expect(await screen.findByLabelText("Formula")).toHaveValue("");
  });

  it("resyncs the PromQL draft to a ?promql= that changed via Back/Forward", async () => {
    stubFetchRoutes([{ match: "query_range", body: MATRIX }]);
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const stateA: ExploreState = {
      ...DEFAULT_STATE,
      signal: "metrics",
      promql: "up",
    };
    const stateB: ExploreState = {
      ...DEFAULT_STATE,
      signal: "metrics",
      promql: "down",
    };
    const { rerender } = render(
      <QueryClientProvider client={client}>
        <MetricsView state={stateA} update={vi.fn()} />
      </QueryClientProvider>,
    );
    await screen.findByTestId("metrics-chart");

    rerender(
      <QueryClientProvider client={client}>
        <MetricsView state={stateB} update={vi.fn()} />
      </QueryClientProvider>,
    );
    await userEvent.click(screen.getByRole("tab", { name: "PromQL" }));
    expect(screen.getByLabelText("PromQL query")).toHaveValue("down");
  });

  it("does not show the empty-builder note once a builder query has actually run", async () => {
    stubFetchRoutes([
      {
        match: /label\/__name__\/values/,
        body: { status: "success", data: [] },
      },
      { match: /\/labels\?/, body: { status: "success", data: [] } },
    ]);
    runIrQuery.mockResolvedValue(IR_SERIES);
    // `?mq=` runs via IR, leaving `state.promql` at "" — the empty-builder
    // note must key off `ranQuery`, not `promql === ""` alone.
    renderView({
      metricQuery: JSON.stringify({
        ref: "a",
        metric: "signaldb.wal.entries_processed",
        filters: [],
      }),
      promql: "",
    });

    await screen.findByTestId("metrics-chart");
    expect(
      screen.queryByText(/Build a query above/),
    ).not.toBeInTheDocument();
  });
});
