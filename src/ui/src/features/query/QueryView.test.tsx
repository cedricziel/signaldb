import { fireEvent, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { renderWithClient } from "../../test/render";
import { resetApiClient, stubApiFetch } from "../../test/apiClient";
import { QueryView } from "./QueryView";

// uPlot needs a real <canvas> 2D context, which jsdom doesn't implement; the
// series chart's wiring is what these tests exercise (see MetricsChart.test).
const { uPlotCtor } = vi.hoisted(() => {
  const uPlotCtor = vi.fn(function uPlotMock() {
    return { destroy: vi.fn() };
  });
  return { uPlotCtor };
});
vi.mock("uplot", () => ({ default: uPlotCtor }));
vi.mock("uplot/dist/uPlot.min.css", () => ({}));

afterEach(() => {
  resetApiClient();
  uPlotCtor.mockClear();
});

describe("QueryView", () => {
  // Task 9.2 — the view is chosen from the declared envelope before results.
  it("selects the view from the declared envelope up front", () => {
    stubApiFetch({});
    renderWithClient(<QueryView />);

    // Default `rows` → list view, no query run yet.
    expect(screen.getByTestId("ir-view-list")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("result"), {
      target: { value: "series" },
    });
    expect(screen.getByTestId("ir-view-chart")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("result"), {
      target: { value: "table" },
    });
    expect(screen.getByTestId("ir-view-table")).toBeInTheDocument();
  });

  // Task 9.1 — the builder emits a valid IR document via the generated client.
  it("emits an IR document to /api/v1/query and renders the rows result", async () => {
    const calls = stubApiFetch({
      result: "rows",
      window: { start_ns: 0, end_ns: 1 },
      columns: [{ name: "service_name", type: "string" }],
      rows: [["checkout"]],
    });
    renderWithClient(<QueryView />);

    fireEvent.click(screen.getByText("Run"));

    await waitFor(() => expect(calls.length).toBeGreaterThan(0));
    const first = calls[0]!;
    // The request went to the native IR endpoint via the generated client.
    expect(first.url).toContain("/api/v1/query");
    // The body is a structured IR document (versioned), not a dialect string.
    const doc = first.body as { irVersion?: number; from?: string };
    expect(doc.irVersion).toBe(1);
    expect(doc.from).toBe("logs");

    // The rows envelope renders.
    await screen.findByText("checkout");
  });

  // Regression: the query-IR validator now rejects physical column names
  // (`service_name`) in the `fields`/`aggregate.by` list, requiring the
  // logical OTel name (`service.name`) — for logs too, not just traces.
  it("groups logs by the logical service.name field, not the physical column", async () => {
    const calls = stubApiFetch({
      result: "table",
      window: { start_ns: 0, end_ns: 1 },
      columns: [
        { name: "service.name", type: "string" },
        { name: "n", type: "int64" },
      ],
      rows: [["checkout", 1]],
    });
    renderWithClient(<QueryView />);

    fireEvent.change(screen.getByLabelText("result"), {
      target: { value: "table" },
    });
    fireEvent.click(screen.getByText("Run"));

    await waitFor(() => expect(calls.length).toBeGreaterThan(0));
    const doc = calls[0]!.body as {
      pipeline?: { aggregate?: { by?: string[] } }[];
    };
    const aggregateStage = doc.pipeline?.find((s) => s.aggregate)?.aggregate;
    expect(aggregateStage?.by).toEqual(["service.name"]);
  });

  it("selects profile summaries and renders their generic rows envelope", async () => {
    const calls = stubApiFetch({
      result: "rows",
      window: { start_ns: 0, end_ns: 1 },
      columns: [{ name: "profile_id", type: "string" }],
      rows: [["profile-1"]],
    });
    renderWithClient(<QueryView />);

    fireEvent.change(screen.getByLabelText("source"), {
      target: { value: "profiles" },
    });
    fireEvent.click(screen.getByText("Run"));

    await waitFor(() => expect(calls.length).toBeGreaterThan(0));
    expect((calls[0]!.body as { from?: string }).from).toBe("profiles");
    await screen.findByText("profile-1");
  });

  it("copies rendered table cells", async () => {
    const writeText = vi.fn();
    vi.stubGlobal("navigator", { clipboard: { writeText } });
    stubApiFetch({
      result: "rows",
      window: { start_ns: 0, end_ns: 1 },
      columns: [{ name: "service_name", type: "string" }],
      rows: [["checkout"]],
    });
    renderWithClient(<QueryView />);

    fireEvent.click(screen.getByText("Run"));
    await userEvent.click(
      await screen.findByRole("button", { name: "Copy cell checkout" }),
    );

    expect(writeText).toHaveBeenCalledWith("checkout");
  });

  it("copies rendered series labels", async () => {
    const writeText = vi.fn();
    vi.stubGlobal("navigator", { clipboard: { writeText } });
    stubApiFetch({
      result: "series",
      window: { start_ns: 0, end_ns: 1 },
      series: [{ labels: { service_name: "checkout" }, points: [] }],
    });
    renderWithClient(<QueryView />);

    fireEvent.change(screen.getByLabelText("result"), {
      target: { value: "series" },
    });
    fireEvent.click(screen.getByText("Run"));
    await userEvent.click(
      await screen.findByRole("button", {
        name: "Copy series service_name=checkout",
      }),
    );

    expect(writeText).toHaveBeenCalledWith("service_name=checkout");
  });

  it("charts a series envelope through the metrics chart", async () => {
    stubApiFetch({
      result: "series",
      window: { start_ns: 0, end_ns: 120_000_000_000 },
      step_ns: 60_000_000_000,
      series: [
        {
          labels: { service_name: "checkout" },
          points: [
            ["0", 1],
            ["60000000000", 2],
          ],
        },
        {
          labels: { service_name: "payments" },
          points: [
            ["0", 3],
            ["60000000000", 4],
          ],
        },
      ],
    });
    renderWithClient(<QueryView />);

    fireEvent.change(screen.getByLabelText("result"), {
      target: { value: "series" },
    });
    fireEvent.click(screen.getByText("Run"));

    expect(await screen.findByTestId("metrics-chart")).toBeInTheDocument();
    await waitFor(() => expect(uPlotCtor).toHaveBeenCalled());
    const [opts, data] = uPlotCtor.mock.calls[0]! as unknown as [
      { series: { label?: string }[]; hooks: { setCursor: unknown[] } },
      number[][],
    ];
    // x-axis slot + two coloured series, with the cursor→tooltip hook wired.
    expect(opts.series).toHaveLength(3);
    expect(opts.series.map((s) => s.label)).toEqual([
      undefined,
      "service_name=checkout",
      "service_name=payments",
    ]);
    expect(opts.hooks.setCursor).toHaveLength(1);
    // Nanosecond timestamps became milliseconds on the shared axis.
    expect(data[0]).toEqual([0, 60_000]);
    expect(data[2]).toEqual([3, 4]);
  });
});
