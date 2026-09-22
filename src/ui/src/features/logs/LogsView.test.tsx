import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import {
  emptyIrSeries,
  emptyLabels,
  irLogRowsResponse,
  renderWithClient,
  stubFetchRoutes,
} from "../../test/render";
import { LogsView } from "./LogsView";

afterEach(() => {
  vi.unstubAllGlobals();
});

const isRowsQuery = (b: unknown) =>
  (b as { result?: string }).result === "rows";
const isSeriesQuery = (b: unknown) =>
  (b as { result?: string }).result === "series";

function routes() {
  return stubFetchRoutes([
    {
      match: "/api/v1/query",
      bodyMatch: isRowsQuery,
      body: irLogRowsResponse([
        {
          tsNs: "2000000000",
          body: "charge failed: card_declined",
          serviceName: "payments",
          severityText: "error",
        },
        {
          tsNs: "1000000000",
          body: "checkout started",
          serviceName: "checkout",
          severityText: "info",
        },
      ]),
    },
    { match: "/api/v1/query", bodyMatch: isSeriesQuery, body: emptyIrSeries },
    {
      match: "/loki/api/v1/labels",
      body: { status: "success", data: ["level", "service_name"] },
    },
  ]);
}

function renderView(state: Partial<ExploreState> = {}) {
  const update = vi.fn();
  const view = renderWithClient(
    <LogsView state={{ ...DEFAULT_STATE, ...state }} update={update} />,
  );
  return { update, view };
}

describe("LogsView", () => {
  it("renders fetched log rows and the row count", async () => {
    routes();
    renderView();
    expect(
      await screen.findByText("charge failed: card_declined"),
    ).toBeInTheDocument();
    expect(screen.getByText("checkout started")).toBeInTheDocument();
    expect(screen.getByText("2 rows")).toBeInTheDocument();
  });

  it("issues a series query for the volume histogram", async () => {
    const fetchFn = routes();
    renderView();
    await waitFor(() => {
      const bodies = fetchFn.mock.calls
        .filter((c) => c[0] instanceof Request)
        .map((c) => (c[0] as Request).clone().text());
      expect(bodies.length).toBeGreaterThan(0);
    });
    const bodies = await Promise.all(
      fetchFn.mock.calls
        .filter((c) => c[0] instanceof Request)
        .map((c) => (c[0] as Request).clone().text()),
    );
    expect(bodies.some((b) => JSON.parse(b).result === "series")).toBe(true);
  });

  it("adding a filter from a row updates state", async () => {
    routes();
    const { update } = renderView();
    await userEvent.click(
      await screen.findByText("charge failed: card_declined"),
    );
    // The resource section (where `service_name` lives) is collapsed
    // behind a summary by default — see LogList.tsx.
    await userEvent.click(screen.getByRole("button", { name: /Resource/ }));
    await userEvent.click(
      screen.getByRole("button", {
        name: "Filter for service.name = payments",
      }),
    );
    expect(update).toHaveBeenCalledWith({
      filters: [{ label: "service.name", op: "=", value: "payments" }],
    });
  });

  it("surfaces query errors", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/query",
        body: { error: "parse error: unexpected token" },
        status: 400,
      },
      { match: "/loki/api/v1/labels", body: emptyLabels },
    ]);
    renderView();
    expect(await screen.findByRole("alert")).toHaveTextContent(
      /Could not load logs:.*parse error/,
    );
  });

  it("opens and closes the mobile filters drawer", async () => {
    routes();
    renderView();
    const toggleBtn = screen.getByRole("button", { name: "Filters" });
    expect(toggleBtn).toHaveAttribute("aria-expanded", "false");

    await userEvent.click(toggleBtn);
    expect(toggleBtn).toHaveAttribute("aria-expanded", "true");
    const closeBtn = screen.getByRole("button", { name: /close/i });

    await userEvent.click(closeBtn);
    expect(toggleBtn).toHaveAttribute("aria-expanded", "false");
    expect(
      screen.queryByRole("button", { name: /close/i }),
    ).not.toBeInTheDocument();
  });

  it("pivots to the trace view from a log row", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/query",
        bodyMatch: isRowsQuery,
        body: irLogRowsResponse([
          {
            tsNs: "2000000000",
            body: "traced line",
            severityText: "info",
            traceId: "abcd1234",
          },
        ]),
      },
      { match: "/api/v1/query", bodyMatch: isSeriesQuery, body: emptyIrSeries },
      { match: "/loki/api/v1/labels", body: emptyLabels },
    ]);
    const { update } = renderView();
    await userEvent.click(await screen.findByText("traced line"));
    await userEvent.click(
      screen.getByRole("button", { name: /View trace abcd1234/ }),
    );
    expect(update).toHaveBeenCalledWith(
      { signal: "traces", trace: "abcd1234" },
      { push: true },
    );
  });

  it("resyncs the search box when state.search changes externally", async () => {
    routes();
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const { rerender } = render(
      <QueryClientProvider client={client}>
        <LogsView
          state={{ ...DEFAULT_STATE, search: "checkout" }}
          update={vi.fn()}
        />
      </QueryClientProvider>,
    );
    const input = await screen.findByLabelText("Search in log lines");
    expect(input).toHaveValue("checkout");

    // A re-click of the Logs tab (crossSignalSearch drops `q`) or Back/
    // Forward changes state.search without remounting LogsView.
    rerender(
      <QueryClientProvider client={client}>
        <LogsView state={{ ...DEFAULT_STATE, search: "" }} update={vi.fn()} />
      </QueryClientProvider>,
    );
    expect(screen.getByLabelText("Search in log lines")).toHaveValue("");
  });

  it("clearing the native search box submits the empty query", async () => {
    routes();
    const { update } = renderView({ search: "checkout" });
    const input = await screen.findByLabelText("Search in log lines");
    expect(input).toHaveValue("checkout");
    fireEvent.change(input, { target: { value: "" } });
    expect(update).toHaveBeenCalledWith({ search: "" });
  });
});
