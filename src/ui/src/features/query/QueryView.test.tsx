import { useState } from "react";
import { fireEvent, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { renderWithClient } from "../../test/render";
import { resetApiClient, stubApiFetch } from "../../test/apiClient";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
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

/**
 * Renders `QueryView` the way the real app does: `state`/`update` round-trip
 * through a stateful wrapper (standing in for the URL) instead of `QueryView`
 * holding its own state — the Query tab's builder/result/filters/run are now
 * URL-backed (see lib/urlState.ts). `onUpdate` also records every patch, for
 * tests asserting on the exact call.
 */
function renderView(
  initial: Partial<ExploreState> = {},
  onUpdate?: (patch: Partial<ExploreState>) => void,
) {
  function Harness() {
    const [state, setState] = useState<ExploreState>({
      ...DEFAULT_STATE,
      signal: "query",
      ...initial,
    });
    const update = (patch: Partial<ExploreState>) => {
      onUpdate?.(patch);
      setState((s) => ({ ...s, ...patch }));
    };
    return <QueryView state={state} update={update} />;
  }
  renderWithClient(<Harness />);
}

describe("QueryView", () => {
  // Task 9.2 — the view is chosen from the declared envelope up front.
  it("selects the view from the declared envelope up front", () => {
    stubApiFetch({});
    renderView();

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
    renderView();

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
    renderView();

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

  // #1070 — a group-by field nothing carries returns a real (null-labelled)
  // result, so the envelope's warning is the only thing telling the user the
  // grouping meant nothing. It must reach the screen.
  it("renders the envelope warnings with their suggestions", async () => {
    stubApiFetch({
      result: "table",
      window: { start_ns: 0, end_ns: 1 },
      columns: [{ name: "statusCode", type: "string" }],
      rows: [[null]],
      warnings: [
        {
          code: "unknown_group_by_field",
          message: "'statusCode' is not a logical field of 'traces'.",
          field: "statusCode",
          suggestions: ["status.code"],
        },
      ],
    });
    renderView();

    fireEvent.click(screen.getByText("Run"));

    await screen.findByText(/statusCode' is not a logical field/);
    await screen.findByText(/Did you mean status\.code\?/);
  });

  it("selects profile summaries and renders their generic rows envelope", async () => {
    const calls = stubApiFetch({
      result: "rows",
      window: { start_ns: 0, end_ns: 1 },
      columns: [{ name: "profile_id", type: "string" }],
      rows: [["profile-1"]],
    });
    renderView();

    fireEvent.change(screen.getByLabelText("source"), {
      target: { value: "profiles" },
    });
    fireEvent.click(screen.getByText("Run"));

    await waitFor(() => expect(calls.length).toBeGreaterThan(0));
    expect((calls[0]!.body as { from?: string }).from).toBe("profiles");
    await screen.findByText("profile-1");
  });

  it("copies a rendered table cell longer than 40 characters", async () => {
    const writeText = vi.fn();
    vi.stubGlobal("navigator", { clipboard: { writeText } });
    const longValue = "a".repeat(41);
    stubApiFetch({
      result: "rows",
      window: { start_ns: 0, end_ns: 1 },
      columns: [{ name: "service_name", type: "string" }],
      rows: [[longValue]],
    });
    renderView();

    fireEvent.click(screen.getByText("Run"));
    await userEvent.click(
      await screen.findByRole("button", { name: `Copy cell ${longValue}` }),
    );

    expect(writeText).toHaveBeenCalledWith(longValue);
    vi.unstubAllGlobals();
  });

  it("renders a short cell as plain text with no copy button", async () => {
    stubApiFetch({
      result: "rows",
      window: { start_ns: 0, end_ns: 1 },
      columns: [{ name: "service_name", type: "string" }],
      rows: [["checkout"]],
    });
    renderView();

    fireEvent.click(screen.getByText("Run"));
    await screen.findByText("checkout");
    expect(
      screen.queryByRole("button", { name: /Copy cell/ }),
    ).not.toBeInTheDocument();
  });

  it("formats a *_timestamp column as an absolute date/time, not a raw epoch-nanosecond integer", async () => {
    stubApiFetch({
      result: "rows",
      window: { start_ns: 0, end_ns: 1 },
      columns: [{ name: "start_timestamp", type: "int64" }],
      rows: [["1700000000000000000"]],
    });
    renderView();

    fireEvent.click(screen.getByText("Run"));
    expect(await screen.findByText(/^\d{4}-\d{2}-\d{2} /)).toBeInTheDocument();
    expect(screen.queryByText("1700000000000000000")).not.toBeInTheDocument();
  });

  it("keeps a 19-digit id column verbatim and copyable rather than reading it as an epoch timestamp", async () => {
    const id = "1234567890123456789";
    stubApiFetch({
      result: "rows",
      window: { start_ns: 0, end_ns: 1 },
      columns: [{ name: "request_id", type: "string" }],
      rows: [[id]],
    });
    renderView();

    fireEvent.click(screen.getByText("Run"));
    expect(await screen.findByText(id)).toBeInTheDocument();
    expect(screen.queryByText(/^\d{4}-\d{2}-\d{2} /)).not.toBeInTheDocument();
  });

  it("does not treat a column merely ending in the letters 'time' (e.g. runtime) as a timestamp", async () => {
    stubApiFetch({
      result: "rows",
      window: { start_ns: 0, end_ns: 1 },
      columns: [{ name: "runtime", type: "int64" }],
      rows: [["42"]],
    });
    renderView();

    fireEvent.click(screen.getByText("Run"));
    expect(await screen.findByText("42")).toBeInTheDocument();
  });

  it("formats a column the server's own metadata declares as timestamp_ns, even without a *_timestamp name", async () => {
    stubApiFetch({
      result: "rows",
      window: { start_ns: 0, end_ns: 1 },
      columns: [{ name: "observed_at", type: "timestamp_ns" }],
      rows: [["1700000000000000000"]],
    });
    renderView();

    fireEvent.click(screen.getByText("Run"));
    expect(await screen.findByText(/^\d{4}-\d{2}-\d{2} /)).toBeInTheDocument();
  });

  it("wraps the rows table in a scrollable container", async () => {
    stubApiFetch({
      result: "rows",
      window: { start_ns: 0, end_ns: 1 },
      columns: [{ name: "service_name", type: "string" }],
      rows: [["checkout"]],
    });
    renderView();

    fireEvent.click(screen.getByText("Run"));
    const cell = await screen.findByText("checkout");
    expect(cell.closest(".table-scroll")).not.toBeNull();
  });

  it("copies rendered series labels", async () => {
    const writeText = vi.fn();
    vi.stubGlobal("navigator", { clipboard: { writeText } });
    stubApiFetch({
      result: "series",
      window: { start_ns: 0, end_ns: 1 },
      series: [{ labels: { service_name: "checkout" }, points: [] }],
    });
    renderView();

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
    vi.unstubAllGlobals();
  });

  it("shows the shared empty-state style for a series envelope with no series", async () => {
    stubApiFetch({
      result: "series",
      window: { start_ns: 0, end_ns: 1 },
      series: [],
    });
    renderView();

    fireEvent.change(screen.getByLabelText("result"), {
      target: { value: "series" },
    });
    fireEvent.click(screen.getByText("Run"));

    const note = await screen.findByRole("status");
    expect(note).toHaveTextContent("No series in this range");
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
    renderView();

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

  // Fix: source/result/filters/run used to be local component state — a tab
  // switch, Back, or reload wiped them. Now they round-trip through `update`.
  describe("URL-backed builder state", () => {
    it("persists source, result, and filters via update", async () => {
      const patches: Partial<ExploreState>[] = [];
      renderView({}, (p) => patches.push(p));

      fireEvent.change(screen.getByLabelText("source"), {
        target: { value: "traces" },
      });
      expect(patches).toContainEqual({ querySource: "traces" });

      fireEvent.change(screen.getByLabelText("result"), {
        target: { value: "table" },
      });
      expect(patches).toContainEqual({ queryResult: "table" });

      await userEvent.click(screen.getByRole("button", { name: "+ filter" }));
      await userEvent.type(screen.getByLabelText("Filter label"), "level");
      await userEvent.type(screen.getByLabelText("Filter value"), "error");
      await userEvent.click(screen.getByRole("button", { name: "Add" }));
      expect(patches).toContainEqual({
        queryFilters: [{ label: "level", op: "=", value: "error" }],
      });
    });

    it("reloading with querySource/queryResult/queryFilters/queryRun already set runs immediately", async () => {
      const calls = stubApiFetch({
        result: "table",
        window: { start_ns: 0, end_ns: 1 },
        columns: [{ name: "service.name", type: "string" }],
        rows: [["checkout"]],
      });
      renderView({
        querySource: "traces",
        queryResult: "table",
        queryFilters: [{ label: "level", op: "=", value: "error" }],
        queryRun: true,
      });

      await waitFor(() => expect(calls.length).toBeGreaterThan(0));
      const doc = calls[0]!.body as { from?: string };
      expect(doc.from).toBe("traces");
      await screen.findByText("checkout");
    });

    it("Run persists queryRun so a reload reproduces the same result", () => {
      const patches: Partial<ExploreState>[] = [];
      stubApiFetch({ result: "rows", window: { start_ns: 0, end_ns: 1 } });
      renderView({}, (p) => patches.push(p));

      fireEvent.click(screen.getByText("Run"));

      expect(patches).toContainEqual({ queryRun: true });
    });

    it("clicking Run again on an already-run query refetches instead of no-op", async () => {
      const calls = stubApiFetch({
        result: "rows",
        window: { start_ns: 0, end_ns: 1 },
        columns: [{ name: "service_name", type: "string" }],
        rows: [["checkout"]],
      });
      renderView({ queryRun: true });

      await waitFor(() => expect(calls.length).toBeGreaterThan(0));
      const before = calls.length;

      fireEvent.click(screen.getByText("Run"));

      await waitFor(() => expect(calls.length).toBeGreaterThan(before));
    });
  });
});
