import { act, render, screen, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MetricsChart, rowsForCursorIndex } from "./MetricsChart";
import type { PromSeries } from "../../api/prom";
import { formatTimestamp } from "../../lib/vizFormat";

// uPlot needs a real <canvas> 2D context, which jsdom doesn't implement.
// Mock the constructor so the component's wiring (series count, labels,
// resize handling, cleanup) is exercised without touching canvas. `new
// uPlot(...)` requires a real constructor function (an arrow function can't
// be `new`-ed), and returning an object from it overrides `this` per normal
// JS `new` semantics — so the mock instance is exactly `{ destroy }`.
const { destroy, setSize, uPlotCtor } = vi.hoisted(() => {
  const destroy = vi.fn();
  const setSize = vi.fn();
  const uPlotCtor = vi.fn(function uPlotMock(
    _opts: unknown,
    _data: unknown,
    _host: unknown,
  ) {
    return { destroy, setSize };
  });
  return { destroy, setSize, uPlotCtor };
});
vi.mock("uplot", () => ({ default: uPlotCtor }));
vi.mock("uplot/dist/uPlot.min.css", () => ({}));

/**
 * A controllable `ResizeObserver` stand-in — unlike the global stub in
 * `src/test/setup.ts` (which fires once, synchronously, from `observe()`),
 * this only reports a size when the test calls `trigger()`, so debouncing
 * and "only redraw on an actual change" behavior can be asserted directly.
 */
class ManualResizeObserver implements ResizeObserver {
  static instances: ManualResizeObserver[] = [];
  private readonly cb: ResizeObserverCallback;
  disconnected = false;

  constructor(cb: ResizeObserverCallback) {
    this.cb = cb;
    ManualResizeObserver.instances.push(this);
  }

  observe() {}
  unobserve() {}
  disconnect() {
    this.disconnected = true;
  }

  trigger(width: number) {
    this.cb(
      [{ contentRect: { width } } as ResizeObserverEntry],
      this as unknown as ResizeObserver,
    );
  }
}

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
  setSize.mockClear();
  ManualResizeObserver.instances = [];
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

/** The slice of a uPlot instance the tooltip reads. */
function fakePlot(idx: number | null) {
  return {
    data: [
      [60_000, 120_000, 180_000],
      [1, null, 3],
      [0.5, 2.25, 4],
    ],
    series: [
      {},
      { label: "router", stroke: "red" },
      { label: "writer", stroke: "blue" },
    ],
    cursor: { idx, left: 50, top: 20 },
    over: document.createElement("div"),
  };
}

describe("MetricsChart container resize", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.stubGlobal("ResizeObserver", ManualResizeObserver);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it("resizes the plot in place when its own container changes size, without a window resize", () => {
    render(<MetricsChart series={SERIES} />);
    const observer = ManualResizeObserver.instances[0]!;

    observer.trigger(640);
    vi.runAllTimers();

    expect(setSize).toHaveBeenCalledWith({ width: 640, height: 260 });
    // Resized in place, not torn down and rebuilt.
    expect(uPlotCtor).toHaveBeenCalledTimes(1);
    expect(destroy).not.toHaveBeenCalled();
  });

  it("debounces rapid container size changes into a single resize", () => {
    render(<MetricsChart series={SERIES} />);
    const observer = ManualResizeObserver.instances[0]!;

    observer.trigger(500);
    observer.trigger(550);
    observer.trigger(600);
    vi.runAllTimers();

    expect(setSize).toHaveBeenCalledTimes(1);
    expect(setSize).toHaveBeenCalledWith({ width: 600, height: 260 });
  });

  it("skips a no-op resize report at the same width", () => {
    render(<MetricsChart series={SERIES} />);
    const observer = ManualResizeObserver.instances[0]!;

    observer.trigger(1200); // matches host.clientWidth used at construction
    vi.runAllTimers();

    expect(setSize).not.toHaveBeenCalled();
  });

  it("disconnects the observer on unmount", () => {
    const { unmount } = render(<MetricsChart series={SERIES} />);
    const observer = ManualResizeObserver.instances[0]!;
    unmount();
    expect(observer.disconnected).toBe(true);
  });
});

describe("rowsForCursorIndex", () => {
  it("names the timestamp and lists every series with swatch and value", () => {
    const { title, rows } = rowsForCursorIndex(fakePlot(2), 2, "req/s");
    expect(title).toBe(formatTimestamp(180_000, 60_000));
    expect(rows).toEqual([
      { swatch: "red", label: "router", value: "3 req/s", muted: false },
      { swatch: "blue", label: "writer", value: "4 req/s", muted: false },
    ]);
  });

  it("keeps a muted dash row for a series with a gap", () => {
    const { rows } = rowsForCursorIndex(fakePlot(1), 1);
    expect(rows[0]).toEqual({
      swatch: "red",
      label: "router",
      value: "–",
      muted: true,
    });
    expect(rows[1]?.value).toBe("2.25");
  });
});

describe("MetricsChart tooltip", () => {
  const TWO: PromSeries[] = [
    {
      labels: { __name__: "rps", service_name: "router" },
      points: [
        [60_000, 1],
        [120_000, 2],
      ],
    },
    {
      labels: { __name__: "rps", service_name: "writer" },
      points: [
        [60_000, 0.5],
        [120_000, 2.25],
      ],
    },
  ];

  function cursorHook() {
    const [opts] = uPlotCtor.mock.calls[0]!;
    const hooks = (opts as { hooks: { setCursor: ((u: unknown) => void)[] } })
      .hooks;
    return hooks.setCursor[0]!;
  }

  it("shows every series at the cursor's timestamp", () => {
    render(<MetricsChart series={TWO} unit="req/s" />);
    act(() => cursorHook()(fakePlot(2)));
    const tip = screen.getByRole("tooltip");
    expect(tip).toHaveTextContent(formatTimestamp(180_000, 60_000));
    const rows = within(tip).getAllByTestId("viz-tip-row");
    expect(rows).toHaveLength(2);
    expect(rows[0]).toHaveTextContent("router");
    expect(rows[0]).toHaveTextContent("3 req/s");
    expect(
      within(rows[1]!).getByTestId("viz-tip-swatch").style.background,
    ).toBe("blue");
  });

  it("hides the tooltip when the cursor leaves the plot", () => {
    render(<MetricsChart series={TWO} />);
    act(() => cursorHook()(fakePlot(1)));
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
    act(() => cursorHook()(fakePlot(null)));
    expect(screen.queryByRole("tooltip")).toBeNull();
  });
});
