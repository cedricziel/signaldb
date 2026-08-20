import { screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";
import * as seriesApi from "../../api/entityMetricSeries";
import * as metricsHook from "./useEntityMetrics";
import type { MetricHit } from "../schema/api";
import type { EntityTypeDef } from "./entityTypes";
import { EntityMetricsPanel, METRIC_TILE_CAP } from "./EntityMetricsPanel";

vi.mock("../../api/entityMetricSeries", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../api/entityMetricSeries")>();
  return { ...actual, fetchEntityMetricSeries: vi.fn() };
});
vi.mock("./useEntityMetrics", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./useEntityMetrics")>();
  return { ...actual, useEntityMetrics: vi.fn() };
});

// uPlot needs a canvas jsdom does not provide; the chart's own behavior is
// covered by MetricsChart's tests, so here it stands in as a marker that a
// tile was drawn at all.
vi.mock("../metrics/MetricsChart", () => ({
  MetricsChart: ({ unit }: { unit?: string }) => (
    <div data-testid="chart" data-unit={unit} />
  ),
}));

const fetchEntityMetricSeries = vi.mocked(seriesApi.fetchEntityMetricSeries);
const useEntityMetrics = vi.mocked(metricsHook.useEntityMetrics);

const host: EntityTypeDef = {
  id: "host",
  label: "Hosts",
  singular: "host",
  identity: ["host.name"],
  registryEntity: "host",
};

const pins = [{ field: "host.name", value: "db-01" }];
const range = { fromMs: 1_000_000, toMs: 4_600_000 };

function metric(name: string, instrument = "gauge", unit = "1"): MetricHit {
  return {
    name,
    brief: "",
    group_id: `metric.${name}`,
    instrument,
    unit,
    attributes: [],
    entity_associations: ["host"],
    namespace: "otel",
    source: "bundled",
    version: "1.43.0",
  } as MetricHit;
}

function seriesFor(names: string[]) {
  return new Map(
    names.map((n) => [
      n,
      [
        {
          labels: { metric_name: n },
          points: [["1000", 1]] as [string, number][],
        },
      ],
    ]),
  );
}

beforeEach(() => {
  useEntityMetrics.mockReset();
  fetchEntityMetricSeries.mockReset();
  useEntityMetrics.mockReturnValue({
    metrics: [],
    isPending: false,
    isError: false,
  });
  fetchEntityMetricSeries.mockResolvedValue(new Map());
});

afterEach(() => {
  vi.restoreAllMocks();
});

function render(entity: EntityTypeDef = host) {
  renderWithClient(
    <EntityMetricsPanel
      entity={entity}
      pinned={pins}
      range={range}
      rangeKey="1h|acme|prod"
    />,
  );
}

describe("EntityMetricsPanel", () => {
  it("charts one tile per observed metric, labelled with unit and instrument", async () => {
    useEntityMetrics.mockReturnValue({
      metrics: [
        metric("system.cpu.utilization", "gauge", "1"),
        metric("system.cpu.time", "counter", "s"),
      ],
      isPending: false,
      isError: false,
    });
    fetchEntityMetricSeries.mockResolvedValue(
      seriesFor(["system.cpu.utilization", "system.cpu.time"]),
    );

    render();

    const tiles = await screen.findAllByRole("figure");
    expect(tiles).toHaveLength(2);
    // A cumulative counter must not read as a rate — naming the instrument is
    // what keeps it honest while the IR has no rate stage.
    const counter = tiles.find((t) =>
      within(t).queryByText("system.cpu.time"),
    )!;
    expect(counter).toHaveTextContent("counter");
    expect(counter).toHaveTextContent("s");
  });

  it("says so when the association lookup itself failed", () => {
    // An empty list because the lookup broke must not render like an empty
    // list because the entity has no metrics.
    useEntityMetrics.mockReturnValue({
      metrics: [],
      isPending: false,
      isError: true,
    });

    render();

    expect(screen.getByRole("alert")).toHaveTextContent(
      "Could not look up which metrics describe this host",
    );
  });

  it("renders nothing for an entity type the registry associates no metric with", () => {
    useEntityMetrics.mockReturnValue({
    metrics: [],
    isPending: false,
    isError: false,
  });

    render();

    // Not an empty panel: no heading, nothing to explain away.
    expect(screen.queryByText("Metrics")).not.toBeInTheDocument();
    expect(fetchEntityMetricSeries).not.toHaveBeenCalled();
  });

  it("pins every series to the entity's identity", async () => {
    useEntityMetrics.mockReturnValue({
      metrics: [metric("system.cpu.utilization")],
      isPending: false,
      isError: false,
    });
    fetchEntityMetricSeries.mockResolvedValue(
      seriesFor(["system.cpu.utilization"]),
    );

    render();

    await waitFor(() => expect(fetchEntityMetricSeries).toHaveBeenCalled());
    for (const call of fetchEntityMetricSeries.mock.calls) {
      expect(call[1]).toEqual(pins);
    }
  });

  it("says the window holds no data rather than charting zeroes", async () => {
    useEntityMetrics.mockReturnValue({
      metrics: [metric("system.cpu.utilization")],
      isPending: false,
      isError: false,
    });
    fetchEntityMetricSeries.mockResolvedValue(new Map());

    render();

    expect(
      await screen.findByText(/No metric data for this host in this window/),
    ).toBeInTheDocument();
    expect(screen.queryByRole("figure")).not.toBeInTheDocument();
  });

  it("states how many metrics it is not showing when over the cap", async () => {
    // A silently truncated panel reads as "this is everything this host
    // reports", which is exactly the wrong impression.
    const names = Array.from(
      { length: METRIC_TILE_CAP + 4 },
      (_, i) => `system.metric.${i}`,
    );
    useEntityMetrics.mockReturnValue({
      metrics: names.map((n) => metric(n)),
      isPending: false,
      isError: false,
    });
    fetchEntityMetricSeries.mockResolvedValue(seriesFor(names));

    render();

    expect(await screen.findAllByRole("figure")).toHaveLength(METRIC_TILE_CAP);
    expect(
      screen.getByText(
        `Showing ${METRIC_TILE_CAP} of ${METRIC_TILE_CAP + 4} metrics.`,
      ),
    ).toBeInTheDocument();
  });

  it("names the registry association its selection came from", async () => {
    useEntityMetrics.mockReturnValue({
      metrics: [metric("system.cpu.utilization")],
      isPending: false,
      isError: false,
    });
    fetchEntityMetricSeries.mockResolvedValue(
      seriesFor(["system.cpu.utilization"]),
    );

    render();

    const provenance = await screen.findByText(
      /discovered from registry entity/,
    );
    expect(provenance).toHaveTextContent("host");
  });
});
