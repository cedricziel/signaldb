import { fireEvent, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import {
  emptyLabels,
  emptyMatrix,
  logsResponse,
  renderWithClient,
  stubFetchRoutes,
} from "../../test/render";
import { LogsView } from "./LogsView";

afterEach(() => {
  vi.unstubAllGlobals();
});

function routes() {
  return stubFetchRoutes([
    // Histogram queries carry a step param; log queries a direction param.
    {
      match: /query_range.*direction=backward/,
      body: logsResponse([
        {
          tsNs: "2000000000",
          line: "charge failed: card_declined",
          labels: { level: "error", service_name: "payments" },
        },
        {
          tsNs: "1000000000",
          line: "checkout started",
          labels: { level: "info", service_name: "checkout" },
        },
      ]),
    },
    { match: /query_range.*step=/, body: emptyMatrix },
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

  it("issues a histogram query grouped by level", async () => {
    const fetchFn = routes();
    renderView();
    await waitFor(() => {
      // URLSearchParams encodes spaces as "+". Calls through the generated
      // client pass a `Request`; raw-fetch calls pass the URL directly.
      const urls = fetchFn.mock.calls.map((c) => {
        const raw = c[0] instanceof Request ? c[0].url : String(c[0]);
        return decodeURIComponent(raw).replace(/\+/g, " ");
      });
      expect(
        urls.some((u) => u.includes("sum by (level) (count_over_time(")),
      ).toBe(true);
    });
  });

  it("adding a filter from a row updates state and clears raw mode", async () => {
    routes();
    const { update } = renderView();
    await userEvent.click(
      await screen.findByText("charge failed: card_declined"),
    );
    await userEvent.click(
      screen.getByRole("button", {
        name: "Filter for service_name = payments",
      }),
    );
    expect(update).toHaveBeenCalledWith({
      filters: [{ label: "service_name", op: "=", value: "payments" }],
      raw: "",
    });
  });

  it("switches to raw LogQL editing prefilled with the compiled query", async () => {
    routes();
    const { update } = renderView({
      filters: [{ label: "level", op: "=", value: "error" }],
    });
    await userEvent.click(
      screen.getByRole("button", { name: "{ } edit as text" }),
    );
    const textarea = screen.getByLabelText("LogQL query");
    expect(textarea).toHaveValue('{level="error"}');
    // fireEvent instead of userEvent.type: "{" is a userEvent escape char.
    fireEvent.change(textarea, {
      target: { value: '{service_name="api"}' },
    });
    await userEvent.click(screen.getByRole("button", { name: "Run" }));
    expect(update).toHaveBeenCalledWith({ raw: '{service_name="api"}' });
  });

  it("skips the histogram for raw queries", async () => {
    const fetchFn = routes();
    renderView({ raw: '{service_name="api"}' });
    await screen.findByText("charge failed: card_declined");
    const urls = fetchFn.mock.calls.map((c) =>
      decodeURIComponent(String(c[0])),
    );
    expect(urls.some((u) => u.includes("count_over_time"))).toBe(false);
  });

  it("surfaces query errors", async () => {
    stubFetchRoutes([
      {
        match: /query_range/,
        body: { error: "parse error: unexpected token" },
        status: 400,
      },
      { match: "/loki/api/v1/labels", body: emptyLabels },
    ]);
    renderView();
    expect(await screen.findByRole("alert")).toHaveTextContent(
      /Query failed:.*400/,
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
        match: /query_range.*direction=backward/,
        body: logsResponse([
          {
            tsNs: "2000000000",
            line: "traced line",
            labels: { level: "info", trace_id: "abcd1234" },
          },
        ]),
      },
      { match: /query_range.*step=/, body: emptyMatrix },
      { match: "/loki/api/v1/labels", body: emptyLabels },
    ]);
    const { update } = renderView();
    await userEvent.click(await screen.findByText("traced line"));
    await userEvent.click(
      screen.getByRole("button", { name: /View trace abcd1234/ }),
    );
    expect(update).toHaveBeenCalledWith({
      signal: "traces",
      trace: "abcd1234",
    });
  });
});
