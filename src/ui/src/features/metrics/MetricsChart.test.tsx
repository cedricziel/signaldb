import { render } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MetricsChart } from "./MetricsChart";
import type { PromSeries } from "../../api/prom";

// uPlot needs a real <canvas> 2D context, which jsdom doesn't implement.
// Mock the constructor so the component's wiring (series count, labels,
// resize handling, cleanup) is exercised without touching canvas. `new
// uPlot(...)` requires a real constructor function (an arrow function can't
// be `new`-ed), and returning an object from it overrides `this` per normal
// JS `new` semantics — so the mock instance is exactly `{ destroy }`.
const { destroy, uPlotCtor } = vi.hoisted(() => {
  const destroy = vi.fn();
  const uPlotCtor = vi.fn(function uPlotMock(
    _opts: unknown,
    _data: unknown,
    _host: unknown,
  ) {
    return { destroy };
  });
  return { destroy, uPlotCtor };
});
vi.mock("uplot", () => ({ default: uPlotCtor }));
vi.mock("uplot/dist/uPlot.min.css", () => ({}));

const SERIES: PromSeries[] = [
  {
    labels: { __name__: "http_requests_total", service_name: "router" },
    points: [
      [0, 1],
      [1000, 2],
    ],
  },
];

afterEach(() => {
  uPlotCtor.mockClear();
  destroy.mockClear();
});

describe("MetricsChart", () => {
  it("renders nothing (skips uPlot) when there is no series data", () => {
    render(<MetricsChart series={[]} />);
    expect(uPlotCtor).not.toHaveBeenCalled();
  });

  it("builds one uPlot series per input series, labeled from its metric", () => {
    render(<MetricsChart series={SERIES} />);

    expect(uPlotCtor).toHaveBeenCalledTimes(1);
    const [opts, data] = uPlotCtor.mock.calls[0]!;
    expect((opts as { series: { label?: string }[] }).series).toHaveLength(2); // x-axis slot + 1 series
    expect((opts as { series: { label?: string }[] }).series[1]?.label).toBe(
      'http_requests_total{service_name="router"}',
    );
    expect(data).toBeInstanceOf(Array);
  });

  it("destroys the chart on unmount", () => {
    const { unmount } = render(<MetricsChart series={SERIES} />);
    // Isolate this unmount's call: the setup-wide `cleanup()` afterEach (from
    // src/test/setup.ts) tears down the *previous* test's tree after this
    // test's own `afterEach` above has already cleared the mock, so a stray
    // destroy() from that leftover teardown can land here otherwise.
    destroy.mockClear();
    unmount();
    expect(destroy).toHaveBeenCalledTimes(1);
  });
});
