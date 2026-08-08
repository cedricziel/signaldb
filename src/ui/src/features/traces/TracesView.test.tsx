import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { TracesView } from "./TracesView";

afterEach(() => {
  vi.unstubAllGlobals();
});

function searchTrace(
  traceID: string,
  service: string,
  name: string,
  startNs: string,
  durationMs: number,
  env: string,
  status = "ok",
) {
  return {
    traceID,
    rootServiceName: service,
    rootTraceName: name,
    startTimeUnixNano: startNs,
    durationMs,
    spanSets: [
      {
        matched: 1,
        spans: [
          {
            spanID: `${traceID}-root`,
            startTimeUnixNano: startNs,
            durationNanos: String(durationMs * 1e6),
            name,
            serviceName: service,
            status,
            attributes: {
              "resource.env": {
                key: "resource.env",
                value: { stringValue: env },
              },
            },
          },
        ],
      },
    ],
  };
}

const SEARCH_BODY = {
  traces: [
    searchTrace(
      "t1cafe",
      "gateway",
      "POST /api/checkout",
      "1700000000000000000",
      412,
      "prod",
      "error",
    ),
    searchTrace(
      "t2feed",
      "gateway",
      "POST /api/checkout",
      "1700000060000000000",
      88,
      "staging",
    ),
    searchTrace(
      "t3beef",
      "auth",
      "GET /login",
      "1700000030000000000",
      51,
      "prod",
    ),
  ],
  metrics: {},
};

const TRACE_BODY = {
  traceID: "t1cafe",
  rootServiceName: "gateway",
  rootTraceName: "POST /api/checkout",
  startTimeUnixNano: "1000",
  durationMs: 412,
  spanSets: [
    {
      matched: 3,
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
        {
          spanID: "charge",
          parentSpanID: "root",
          startTimeUnixNano: "1040000000",
          durationNanos: "258000000",
          name: "charge",
          serviceName: "payments",
          status: "error",
          attributes: {
            "payment.provider": {
              key: "payment.provider",
              value: { stringValue: "stripe" },
            },
          },
        },
      ],
    },
  ],
};

function renderView(state: Partial<ExploreState> = {}) {
  const update = vi.fn();
  renderWithClient(
    <TracesView
      state={{ ...DEFAULT_STATE, signal: "traces", ...state }}
      update={update}
    />,
  );
  return update;
}

describe("TracesView group list", () => {
  it("groups traces by root, largest group first", async () => {
    stubFetchRoutes([{ match: "/tempo/api/search", body: SEARCH_BODY }]);
    renderView();
    const rows = await screen.findAllByRole("row");
    // Header row + one row per group, not per trace.
    expect(rows).toHaveLength(3);
    expect(rows[1]).toHaveTextContent("POST /api/checkout");
    expect(rows[1]).toHaveTextContent("2");
    expect(rows[2]).toHaveTextContent("GET /login");
  });

  it("shows rate, error rate, and latency percentiles per group", async () => {
    stubFetchRoutes([{ match: "/tempo/api/search", body: SEARCH_BODY }]);
    renderView();
    const rows = await screen.findAllByRole("row");
    expect(rows[0]).toHaveTextContent("Rate");
    expect(rows[0]).toHaveTextContent("Errors");
    expect(rows[0]).toHaveTextContent("P50");
    // Default range is 1h; 2 checkout traces, one of them errored.
    expect(rows[1]).toHaveTextContent("2/h");
    expect(rows[1]).toHaveTextContent("50%");
    expect(rows[2]).toHaveTextContent("1/h");
  });

  it("adds a secondary dimension via the then-by picker", async () => {
    stubFetchRoutes([{ match: "/tempo/api/search", body: SEARCH_BODY }]);
    const update = renderView();
    await screen.findByRole("button", { name: "POST /api/checkout" });
    await userEvent.selectOptions(
      screen.getByLabelText("Then by"),
      "service.name",
    );
    expect(update).toHaveBeenCalledWith({
      groupBy: "span.name,service.name",
      group: "",
    });
  });

  it("renders one column per dimension and drills in with a composite key", async () => {
    stubFetchRoutes([{ match: "/tempo/api/search", body: SEARCH_BODY }]);
    const update = renderView({ groupBy: "span.name,service.name" });
    const rows = await screen.findAllByRole("row");
    expect(rows).toHaveLength(3);
    expect(rows[1]).toHaveTextContent("POST /api/checkout");
    expect(rows[1]).toHaveTextContent("gateway");
    await userEvent.click(
      screen.getByRole("button", { name: "POST /api/checkout" }),
    );
    expect(update).toHaveBeenCalledWith({
      group: "POST /api/checkout\u001fgateway",
    });
  });

  it("dives into a group on click", async () => {
    stubFetchRoutes([{ match: "/tempo/api/search", body: SEARCH_BODY }]);
    const update = renderView();
    await userEvent.click(
      await screen.findByRole("button", { name: "POST /api/checkout" }),
    );
    expect(update).toHaveBeenCalledWith({ group: "POST /api/checkout" });
  });

  it("offers observed attributes as grouping dimensions", async () => {
    stubFetchRoutes([{ match: "/tempo/api/search", body: SEARCH_BODY }]);
    const update = renderView();
    // Attribute options appear once results arrive.
    await screen.findByRole("button", { name: "POST /api/checkout" });
    await userEvent.selectOptions(
      screen.getByLabelText("Group by"),
      "resource.env",
    );
    expect(update).toHaveBeenCalledWith({
      groupBy: "resource.env",
      group: "",
    });
  });

  it("groups by the selected attribute dimension", async () => {
    stubFetchRoutes([{ match: "/tempo/api/search", body: SEARCH_BODY }]);
    renderView({ groupBy: "resource.env" });
    const rows = await screen.findAllByRole("row");
    expect(rows).toHaveLength(3);
    expect(rows[1]).toHaveTextContent("prod");
    expect(rows[1]).toHaveTextContent("2");
    expect(rows[2]).toHaveTextContent("staging");
  });

  it("sorts by a clicked column and flips on the second click", async () => {
    stubFetchRoutes([{ match: "/tempo/api/search", body: SEARCH_BODY }]);
    renderView();
    await screen.findByRole("button", { name: "POST /api/checkout" });
    // String column: first click sorts ascending.
    await userEvent.click(screen.getByRole("button", { name: "span.name" }));
    let rows = screen.getAllByRole("row");
    expect(rows[1]).toHaveTextContent("GET /login");
    expect(rows[1]?.closest("table")?.querySelector("[aria-sort]")).toBe(
      rows[0]?.querySelector("th"),
    );
    await userEvent.click(screen.getByRole("button", { name: "span.name" }));
    rows = screen.getAllByRole("row");
    expect(rows[1]).toHaveTextContent("POST /api/checkout");
  });

  it("sorts metric columns descending first", async () => {
    stubFetchRoutes([{ match: "/tempo/api/search", body: SEARCH_BODY }]);
    renderView();
    await screen.findByRole("button", { name: "POST /api/checkout" });
    // checkout has 50% errors, login none: desc keeps checkout on top,
    // the flip to ascending puts login first.
    await userEvent.click(screen.getByRole("button", { name: "Errors" }));
    expect(screen.getAllByRole("row")[1]).toHaveTextContent(
      "POST /api/checkout",
    );
    await userEvent.click(screen.getByRole("button", { name: "Errors" }));
    expect(screen.getAllByRole("row")[1]).toHaveTextContent("GET /login");
  });

  it("opens a trace by pasted id", async () => {
    stubFetchRoutes([{ match: "/tempo/api/search", body: { metrics: {} } }]);
    const update = renderView();
    await userEvent.type(screen.getByLabelText("Trace ID"), "  deadbeef  ");
    await userEvent.click(screen.getByRole("button", { name: "Open" }));
    expect(update).toHaveBeenCalledWith({ trace: "deadbeef" });
  });

  it("surfaces search errors", async () => {
    stubFetchRoutes([
      { match: "/tempo/api/search", body: { error: "boom" }, status: 500 },
    ]);
    renderView();
    expect(await screen.findByRole("alert")).toHaveTextContent(/500/);
  });
});

describe("TracesView group detail", () => {
  it("lists only the selected group's traces, newest first", async () => {
    stubFetchRoutes([{ match: "/tempo/api/search", body: SEARCH_BODY }]);
    renderView({ group: "POST /api/checkout" });
    const rows = await screen.findAllByRole("row");
    expect(rows).toHaveLength(3);
    expect(rows[1]).toHaveTextContent("t2feed");
    expect(rows[2]).toHaveTextContent("t1cafe");
    expect(screen.queryByText("t3beef")).not.toBeInTheDocument();
  });

  it("filters by the active dimension, not just span name", async () => {
    stubFetchRoutes([{ match: "/tempo/api/search", body: SEARCH_BODY }]);
    renderView({ groupBy: "resource.env", group: "prod" });
    const rows = await screen.findAllByRole("row");
    expect(rows).toHaveLength(3);
    expect(screen.getByText("t1cafe")).toBeInTheDocument();
    expect(screen.getByText("t3beef")).toBeInTheDocument();
    expect(screen.queryByText("t2feed")).not.toBeInTheDocument();
  });

  it("filters by every dimension of a composite group", async () => {
    stubFetchRoutes([{ match: "/tempo/api/search", body: SEARCH_BODY }]);
    renderView({
      groupBy: "span.name,service.name",
      group: "POST /api/checkout\u001fgateway",
    });
    // Title shows the values joined for humans.
    expect(
      await screen.findByRole("heading", {
        name: "POST /api/checkout · gateway",
      }),
    ).toBeInTheDocument();
    expect(screen.getByText("t1cafe")).toBeInTheDocument();
    expect(screen.getByText("t2feed")).toBeInTheDocument();
    expect(screen.queryByText("t3beef")).not.toBeInTheDocument();
  });

  it("opens a trace on click", async () => {
    stubFetchRoutes([{ match: "/tempo/api/search", body: SEARCH_BODY }]);
    const update = renderView({ group: "POST /api/checkout" });
    await userEvent.click(await screen.findByText("t1cafe"));
    expect(update).toHaveBeenCalledWith({ trace: "t1cafe" });
  });

  it("navigates back to the group list", async () => {
    stubFetchRoutes([{ match: "/tempo/api/search", body: SEARCH_BODY }]);
    const update = renderView({ group: "POST /api/checkout" });
    await userEvent.click(
      await screen.findByRole("button", { name: "← groups" }),
    );
    expect(update).toHaveBeenCalledWith({ group: "" });
  });

  it("notes when the group has no traces in range", async () => {
    stubFetchRoutes([{ match: "/tempo/api/search", body: { metrics: {} } }]);
    renderView({ group: "POST /api/checkout" });
    expect(await screen.findByText(/no traces/i)).toBeInTheDocument();
  });

  it("sorts the trace list by a clicked column", async () => {
    stubFetchRoutes([{ match: "/tempo/api/search", body: SEARCH_BODY }]);
    renderView({ group: "POST /api/checkout" });
    await screen.findByText("t1cafe");
    // Newest first by default; duration sorts descending first.
    expect(screen.getAllByRole("row")[1]).toHaveTextContent("t2feed");
    await userEvent.click(screen.getByRole("button", { name: "Duration" }));
    expect(screen.getAllByRole("row")[1]).toHaveTextContent("t1cafe");
    await userEvent.click(screen.getByRole("button", { name: "Duration" }));
    expect(screen.getAllByRole("row")[1]).toHaveTextContent("t2feed");
  });
});

describe("TracesView detail", () => {
  it("renders the waterfall with the error span preselected", async () => {
    stubFetchRoutes([{ match: "/tempo/api/traces/t1cafe", body: TRACE_BODY }]);
    renderView({ trace: "t1cafe" });
    const spans = await screen.findAllByRole("listitem");
    expect(spans).toHaveLength(2);
    // Error span is preselected, so its attributes show in the detail panel.
    expect(screen.getByText("payment.provider")).toBeInTheDocument();
    expect(screen.getByText("stripe")).toBeInTheDocument();
    expect(screen.getByText(/1 error/)).toBeInTheDocument();
  });

  it("selects a span on click and shows its details", async () => {
    stubFetchRoutes([{ match: "/tempo/api/traces/t1cafe", body: TRACE_BODY }]);
    renderView({ trace: "t1cafe" });
    const rows = await screen.findAllByRole("listitem");
    await userEvent.click(rows[0]!);
    expect(rows[0]).toHaveAttribute("aria-selected", "true");
    expect(
      screen.getByRole("heading", { name: "POST /api/checkout", level: 4 }),
    ).toBeInTheDocument();
  });

  it("pivots to logs filtered by trace_id", async () => {
    stubFetchRoutes([{ match: "/tempo/api/traces/t1cafe", body: TRACE_BODY }]);
    const update = renderView({ trace: "t1cafe" });
    await userEvent.click(
      await screen.findByRole("button", { name: "Logs for this trace →" }),
    );
    expect(update).toHaveBeenCalledWith({
      signal: "logs",
      trace: "",
      raw: "",
      filters: [{ label: "trace_id", op: "=", value: "t1cafe" }],
    });
  });

  it("navigates back to the search list", async () => {
    stubFetchRoutes([{ match: "/tempo/api/traces/t1cafe", body: TRACE_BODY }]);
    const update = renderView({ trace: "t1cafe" });
    await userEvent.click(
      await screen.findByRole("button", { name: "← traces" }),
    );
    expect(update).toHaveBeenCalledWith({ trace: "" });
  });

  it("surfaces trace lookup failures", async () => {
    stubFetchRoutes([
      {
        match: "/tempo/api/traces/missing",
        body: { error: "not found" },
        status: 404,
      },
    ]);
    renderView({ trace: "missing" });
    expect(await screen.findByRole("alert")).toHaveTextContent(/404/);
  });
});

const VOLUME_BODY = {
  result: "series",
  window: {
    start_ns: 1_700_000_000_000_000_000,
    end_ns: 1_700_003_600_000_000_000,
  },
  step_ns: 300_000_000_000,
  series: [
    {
      labels: { status_code: "Unspecified" },
      points: [
        [1_700_000_000_000_000_000, 2018],
        [1_700_000_300_000_000_000, 41],
      ],
    },
    {
      labels: { status_code: "Error" },
      points: [[1_700_000_300_000_000_000, 7]],
    },
  ],
};

describe("TracesView volume chart", () => {
  const routes = [
    { match: "/tempo/api/search", body: SEARCH_BODY },
    { match: "/api/v1/query", body: VOLUME_BODY },
  ];

  it("renders trace volume stacked by status", async () => {
    stubFetchRoutes(routes);
    renderView();
    await screen.findByRole("img", { name: /trace volume/i });
    const cols = screen.getAllByTestId("svol-col");
    expect(cols.length).toBeGreaterThan(1);
    // 2018 unset, then 41 unset + 7 error.
    expect(cols[0]).toHaveAccessibleName(/2,018 traces/);
  });

  it("asks for the volume aggregate without a row limit", async () => {
    stubFetchRoutes(routes);
    renderView({ limit: 25 });
    await screen.findByRole("img", { name: /trace volume/i });
    const body = vi
      .mocked(globalThis.fetch)
      .mock.calls.map(([input]) => input)
      .find(
        (i): i is Request =>
          i instanceof Request && i.url.includes("/api/v1/query"),
      );
    expect(body).toBeDefined();
    const doc = await body!.clone().json();
    expect(doc.from).toBe("traces");
    expect(doc.result).toBe("series");
    expect(JSON.stringify(doc)).not.toContain("limit");
  });

  it("keeps the chart when the trace list is empty", async () => {
    stubFetchRoutes([
      { match: "/tempo/api/search", body: { traces: [], metrics: {} } },
      { match: "/api/v1/query", body: VOLUME_BODY },
    ]);
    renderView();
    await screen.findByRole("img", { name: /trace volume/i });
    expect(
      screen.getByText(/no traces in this time range/i),
    ).toBeInTheDocument();
  });
});
