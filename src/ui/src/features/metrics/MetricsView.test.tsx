import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { MetricsView } from "./MetricsView";

// uPlot needs a real canvas; the chart wrapper is exercised as a stub and
// the data pipeline is covered by prom/promSeries unit tests.
vi.mock("./MetricsChart", () => ({
  MetricsChart: ({ series }: { series: unknown[] }) => (
    <div data-testid="metrics-chart">chart:{series.length}</div>
  ),
}));

afterEach(() => {
  vi.unstubAllGlobals();
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
    renderView();
    expect(screen.getByText(/Enter a PromQL expression/)).toBeInTheDocument();
  });

  it("submits the drafted query via update", async () => {
    stubFetchRoutes([{ match: "query_range", body: MATRIX }]);
    const update = renderView();
    await userEvent.type(screen.getByLabelText("PromQL query"), "up ");
    await userEvent.click(screen.getByRole("button", { name: "Run" }));
    expect(update).toHaveBeenCalledWith({ promql: "up" });
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
  });

  it("shows an empty state for zero series", async () => {
    stubFetchRoutes([
      {
        match: "query_range",
        body: { status: "success", data: { resultType: "matrix", result: [] } },
      },
    ]);
    renderView({ promql: "up" });
    expect(await screen.findByText("No series returned.")).toBeInTheDocument();
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
});
