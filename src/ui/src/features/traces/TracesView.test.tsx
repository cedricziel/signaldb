import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { TracesView } from "./TracesView";

afterEach(() => {
  vi.unstubAllGlobals();
});

const SEARCH_BODY = {
  traces: [
    {
      traceID: "t1cafe",
      rootServiceName: "gateway",
      rootTraceName: "POST /api/checkout",
      startTimeUnixNano: "1700000000000000000",
      durationMs: 412,
    },
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

describe("TracesView search", () => {
  it("lists traces and opens one on click", async () => {
    stubFetchRoutes([{ match: "/tempo/api/search", body: SEARCH_BODY }]);
    const update = renderView();
    await userEvent.click(
      await screen.findByRole("button", { name: "POST /api/checkout" }),
    );
    expect(update).toHaveBeenCalledWith({ trace: "t1cafe" });
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
